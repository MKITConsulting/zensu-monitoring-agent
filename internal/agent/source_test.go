package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestMetricsServerSourceReportsUnavailableAPI(t *testing.T) {
	reader := &fakeReader{
		inner:      NewClientsetLister(fake.NewSimpleClientset(), nil),
		metricsErr: ErrMetricsAPIUnavailable,
	}
	src := newMetricsServerSource(reader, nil)

	_, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "default", Selector: "app=api"}})
	if !errors.Is(err, ErrMetricsAPIUnavailable) {
		t.Fatalf("err = %v, want ErrMetricsAPIUnavailable so auto can fall through", err)
	}
}

func TestAutoSourceFallsThroughOnceAndStays(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
`)
	reader := &fakeReader{
		inner:      NewClientsetLister(fake.NewSimpleClientset(), nil),
		metricsErr: ErrMetricsAPIUnavailable,
	}
	src, err := NewMetricSource(SourceConfig{
		Mode:       SourceAuto,
		Exposition: ExpositionConfig{URL: srv.URL},
	}, reader, nil, nil)
	if err != nil {
		t.Fatalf("NewMetricSource: %v", err)
	}
	targets := []ServiceTarget{{Slug: "api", Namespace: "default", Selector: "app=api"}}

	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 250 {
		t.Errorf("cpu = %v, want 250 from the exposition fallback", v)
	}
	if src.Name() != SourceExposition {
		t.Errorf("Name() = %q, want %q after the switch", src.Name(), SourceExposition)
	}

	hitsAfterSwitch := reader.metricsHits
	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	if reader.metricsHits != hitsAfterSwitch {
		t.Errorf("metrics-server was probed again after the switch (%d -> %d); the switch must be one-way",
			hitsAfterSwitch, reader.metricsHits)
	}
}

func TestAutoSourceKeepsMetricsServerWhenAvailable(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 99
`)
	reader := &fakeReader{
		inner:      NewClientsetLister(fake.NewSimpleClientset(), nil),
		metricsCPU: 400,
		metricsMem: 530_000_000,
		available:  true,
	}
	src, err := NewMetricSource(SourceConfig{
		Mode:       SourceAuto,
		Exposition: ExpositionConfig{URL: srv.URL},
	}, reader, nil, nil)
	if err != nil {
		t.Fatalf("NewMetricSource: %v", err)
	}

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "default", Selector: "app=api"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 400 {
		t.Errorf("cpu = %v, want 400 from metrics-server — the exposition must not be consulted", v)
	}
	if src.Name() != SourceMetricsServer {
		t.Errorf("Name() = %q, want %q", src.Name(), SourceMetricsServer)
	}
}

// TestValidateAPIURL pins that the heartbeat base URL is held to the scrape
// URL's rules. Both are rendered into the same ConfigMap, so a credential in
// either would sit there in clear text.
func TestValidateAPIURL(t *testing.T) {
	cases := []struct {
		name            string
		raw             string
		wantErrContains string
	}{
		{name: "plain https", raw: "https://api.zensu.dev"},
		{name: "plain http with a port", raw: "http://zensu.internal:8080/base"},
		{name: "credentials", raw: "https://u:p@zensu.internal", wantErrContains: "carries credentials"},
		{name: "bare username", raw: "https://u@zensu.internal", wantErrContains: "carries credentials"},
		{name: "no scheme", raw: "zensu.internal", wantErrContains: "http:// or https:// scheme"},
		{name: "no host", raw: "https:///base", wantErrContains: "needs a host"},
		{name: "unparsable", raw: "http://[::1", wantErrContains: "not a valid URL"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateAPIURL(c.raw)
			if c.wantErrContains == "" {
				if err != nil {
					t.Fatalf("ValidateAPIURL(%q) = %v, want nil", c.raw, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q", c.wantErrContains)
			}
			if !strings.Contains(err.Error(), c.wantErrContains) {
				t.Errorf("error = %v, want it to name %q", err, c.wantErrContains)
			}
			if !strings.Contains(err.Error(), "ZENSU_API_URL") {
				t.Errorf("error = %v, want it to name the variable", err)
			}
			if strings.Contains(err.Error(), "zensu.internal") || strings.Contains(err.Error(), ":p@") {
				t.Errorf("the error must not echo the configured value, got %v", err)
			}
		})
	}
}

func TestNewMetricSourceModes(t *testing.T) {
	reader := NewClientsetLister(fake.NewSimpleClientset(), nil)

	cases := []struct {
		name            string
		cfg             SourceConfig
		wantName        string
		wantErrContains string
	}{
		{name: "none", cfg: SourceConfig{Mode: SourceNone}, wantName: SourceNone},
		{name: "metrics-server", cfg: SourceConfig{Mode: SourceMetricsServer}, wantName: SourceMetricsServer},
		{name: "empty mode defaults to auto", cfg: SourceConfig{}, wantName: SourceMetricsServer},
		{name: "auto without url stays on metrics-server", cfg: SourceConfig{Mode: SourceAuto}, wantName: SourceMetricsServer},
		{name: "auto with url", cfg: SourceConfig{Mode: SourceAuto, Exposition: ExpositionConfig{URL: "http://c:9090/metrics"}}, wantName: SourceMetricsServer},
		{name: "exposition", cfg: SourceConfig{Mode: SourceExposition, Exposition: ExpositionConfig{URL: "http://c:9090/metrics"}}, wantName: SourceExposition},
		{name: "exposition without url", cfg: SourceConfig{Mode: SourceExposition}, wantErrContains: "requires a scrape URL"},
		{name: "unknown mode", cfg: SourceConfig{Mode: "promql"}, wantErrContains: "unknown resource source"},
		{name: "url without scheme", cfg: SourceConfig{Mode: SourceExposition, Exposition: ExpositionConfig{URL: "c:9090/metrics"}}, wantErrContains: "http:// or https:// scheme"},
		{name: "url without host", cfg: SourceConfig{Mode: SourceAuto, Exposition: ExpositionConfig{URL: "http:///metrics"}}, wantErrContains: "needs a host"},
		{name: "url with credentials", cfg: SourceConfig{Mode: SourceExposition, Exposition: ExpositionConfig{URL: "http://u:p@c:9090/metrics"}}, wantErrContains: "carries credentials"},
		{name: "unparsable url", cfg: SourceConfig{Mode: SourceExposition, Exposition: ExpositionConfig{URL: "http://[::1"}}, wantErrContains: "not a valid URL"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src, err := NewMetricSource(c.cfg, reader, nil, nil)
			if c.wantErrContains != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q", c.wantErrContains)
				}
				if !strings.Contains(err.Error(), c.wantErrContains) {
					t.Errorf("error = %v, want it to name %q", err, c.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewMetricSource: %v", err)
			}
			if src.Name() != c.wantName {
				t.Errorf("Name() = %q, want %q", src.Name(), c.wantName)
			}
		})
	}
}

func TestNoneSourceYieldsNothing(t *testing.T) {
	got, err := noneSource{}.Samples(context.Background(), []ServiceTarget{{Slug: "api"}})
	if err != nil {
		t.Fatalf("noneSource must not error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want no samples, got %+v", got)
	}
}

// stubSource returns scripted samples per slug, so the Collect fold can be
// exercised with more than one service.
type stubSource struct {
	per map[string][]MetricSample
}

func (s *stubSource) Name() string { return "stub" }

func (s *stubSource) Samples(_ context.Context, _ []ServiceTarget) (map[string][]MetricSample, error) {
	return s.per, nil
}

// perSelectorReader scripts a distinct metrics result per label selector, so the
// per-target error handling can be exercised with divergent targets.
type perSelectorReader struct {
	inner   ClusterReader
	results map[string]struct {
		cpu, mem int64
		err      error
	}
}

func (p *perSelectorReader) ListDeployments(ctx context.Context, ns string) ([]appsv1.Deployment, error) {
	return p.inner.ListDeployments(ctx, ns)
}

func (p *perSelectorReader) ListPods(ctx context.Context, ns, sel string) ([]corev1.Pod, error) {
	return p.inner.ListPods(ctx, ns, sel)
}

func (p *perSelectorReader) PodMetricsForSelector(_ context.Context, _, selector string) (int64, int64, bool, error) {
	r, ok := p.results[selector]
	if !ok {
		return 0, 0, false, nil
	}
	if r.err != nil {
		return 0, 0, false, r.err
	}
	return r.cpu, r.mem, true, nil
}

// TestMetricsServerSourceTransientErrorSparesOtherTargets pins that a transient
// failure on one target does not cost the other targets their metrics.
func TestMetricsServerSourceTransientErrorSparesOtherTargets(t *testing.T) {
	reader := &perSelectorReader{
		inner: NewClientsetLister(fake.NewSimpleClientset(), nil),
		results: map[string]struct {
			cpu, mem int64
			err      error
		}{
			"app=broken":  {err: errors.New("metrics-server temporarily unavailable")},
			"app=healthy": {cpu: 400, mem: 530_000_000},
		},
	}
	src := newMetricsServerSource(reader, nil)

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "broken", Namespace: "default", Selector: "app=broken"},
		{Slug: "healthy", Namespace: "default", Selector: "app=healthy"},
	})
	if err != nil {
		t.Fatalf("a transient error on one target must not fail the batch: %v", err)
	}
	if len(got["broken"]) != 0 {
		t.Errorf("the failing target must carry no samples, got %+v", got["broken"])
	}
	healthy := map[string]float64{}
	for _, s := range got["healthy"] {
		healthy[s.Key] = s.Value
	}
	if healthy[MetricCPUMillicores] != 400 || healthy[MetricMemoryBytes] != 530_000_000 {
		t.Errorf("the healthy target must keep its exact values, got %+v", got["healthy"])
	}
}

type spySource struct {
	calls [][]ServiceTarget
}

func (s *spySource) Name() string { return "spy" }
func (s *spySource) Samples(_ context.Context, targets []ServiceTarget) (map[string][]MetricSample, error) {
	s.calls = append(s.calls, targets)
	return nil, nil
}
