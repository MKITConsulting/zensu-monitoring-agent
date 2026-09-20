package agent

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
)

func TestAutoSourceFallsThroughAndHoldsForTheCooldown(t *testing.T) {
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
		t.Errorf("metrics-server was probed again inside the cooldown (%d -> %d); re-probing every tick is what the cooldown exists to prevent",
			hitsAfterSwitch, reader.metricsHits)
	}
}

// TestAutoSourceReturnsToPrimaryAfterCooldown pins the half that makes the
// switch survivable: one transient NotFound during a metrics-server rollout
// must not strand the agent on the exposition for the process lifetime.
func TestAutoSourceReturnsToPrimaryAfterCooldown(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
`)
	reader := &fakeReader{
		inner:      NewClientsetLister(fake.NewSimpleClientset(), nil),
		metricsErr: ErrMetricsAPIUnavailable,
	}
	now := time.Now()
	src := &autoSource{
		primary:  newMetricsServerSource(reader, slog.Default()),
		fallback: newExpositionSource(ExpositionConfig{URL: srv.URL}, slog.Default(), nil),
		log:      slog.Default(),
		now:      func() time.Time { return now },
	}
	targets := []ServiceTarget{{Slug: "api", Namespace: "default", Selector: "app=api"}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	if src.Name() != SourceExposition {
		t.Fatalf("Name() = %q, want the fallback after the switch", src.Name())
	}

	reader.metricsErr = nil
	reader.metricsCPU = 400
	reader.metricsMem = 530_000_000
	reader.available = true

	now = now.Add(autoRetryInterval - time.Second)
	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("Samples inside the cooldown: %v", err)
	}
	if src.Name() != SourceExposition {
		t.Errorf("Name() = %q, want the fallback still — the cooldown has not elapsed", src.Name())
	}

	now = now.Add(2 * time.Second)
	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("Samples after the cooldown: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 400 {
		t.Errorf("cpu = %v, want 400 from metrics-server once it answers again", v)
	}
	if src.Name() != SourceMetricsServer {
		t.Errorf("Name() = %q, want %q after recovery", src.Name(), SourceMetricsServer)
	}
}

// TestMetricsServerSourceSkipsSelectorlessTarget pins that a target carrying no
// selector is skipped rather than passed on: an empty selector matches every pod
// in the namespace, so the service would report the whole namespace's usage.
func TestMetricsServerSourceSkipsSelectorlessTarget(t *testing.T) {
	reader := &fakeReader{
		inner:      NewClientsetLister(fake.NewSimpleClientset(), nil),
		metricsCPU: 400,
		metricsMem: 530_000_000,
		available:  true,
	}
	src := newMetricsServerSource(reader, slog.Default())

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "default"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %+v, want nothing — an empty selector must never be queried", got)
	}
	if reader.metricsHits != 0 {
		t.Errorf("metricsHits = %d, want 0 — the cluster must not be asked at all", reader.metricsHits)
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
		{name: "port but no host", raw: "http://:8080/metrics", wantErrContains: "needs a host"},
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
	hits    map[string]int
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
	if p.hits != nil {
		p.hits[selector]++
	}
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

// TestKnownSourceNamesAreMetricsLabels pins the invariant that holds the two
// packages together: every name a source can report must be a label the metrics
// package pre-initialises, or SetResourceSource files that tick under the
// unknown series. SourceAuto is deliberately excluded — it configures the
// composite, which always reports whichever of its two members is live.
//
// Go cannot enumerate a type's implementations, so sources is maintained by
// hand and every new MetricSource must be added to it. The set comparison is
// what makes that obligation fail loudly rather than silently: adding a label
// without a source, or a source without a label, breaks it. want is compacted,
// so two sources reporting the same name still require exactly one label.
func TestKnownSourceNamesAreMetricsLabels(t *testing.T) {
	sources := []MetricSource{
		newMetricsServerSource(nil, nil),
		noneSource{},
		newExpositionSource(ExpositionConfig{URL: "http://example.invalid/metrics"}, nil, nil),
	}
	want := []string{obs.SourceUnknown}
	for _, src := range sources {
		want = append(want, src.Name())
	}
	slices.Sort(want)
	want = slices.Compact(want)

	got := slices.Clone(obs.ResourceSources)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("obs.ResourceSources = %v, want %v — every reportable source name plus %q, and nothing else",
			got, want, obs.SourceUnknown)
	}
	if slices.Contains(obs.ResourceSources, SourceAuto) {
		t.Errorf("SourceAuto %q must NOT be a label: autoSource reports its live member, never itself", SourceAuto)
	}
}

// scriptedReader answers each PodMetricsForSelector call with the next entry of
// its script, holding on the last entry once the script runs out, so one test
// can drive an outage, a recovery and a second outage while another drives one
// uninterrupted outage across many calls. A nil entry answers successfully; set
// noData to make those successes report available=false, the shape an empty
// PodMetrics list produces.
type scriptedReader struct {
	inner  ClusterReader
	errs   []error
	noData bool
	calls  int
}

func (s *scriptedReader) ListDeployments(ctx context.Context, ns string) ([]appsv1.Deployment, error) {
	return s.inner.ListDeployments(ctx, ns)
}

func (s *scriptedReader) ListPods(ctx context.Context, ns, sel string) ([]corev1.Pod, error) {
	return s.inner.ListPods(ctx, ns, sel)
}

func (s *scriptedReader) PodMetricsForSelector(_ context.Context, _, _ string) (int64, int64, bool, error) {
	if len(s.errs) > 0 {
		i := min(s.calls, len(s.errs)-1)
		s.calls++
		if err := s.errs[i]; err != nil {
			return 0, 0, false, err
		}
	}
	if s.noData {
		return 0, 0, false, nil
	}
	return 400, 530_000_000, true, nil
}

// TestMetricsServerSourceWarnsAgainAfterRecovery pins that the warn latch is
// per outage rather than per process, as the latch's own doc comment on
// metricsServerSource promises.
func TestMetricsServerSourceWarnsAgainAfterRecovery(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	reader := &scriptedReader{
		inner: NewClientsetLister(fake.NewSimpleClientset(), nil),
		errs:  []error{ErrMetricsAPIUnavailable, nil, ErrMetricsAPIUnavailable},
	}
	src := newMetricsServerSource(reader, log)
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", Selector: "app=api"}}

	for tick := range 3 {
		out, err := src.Samples(context.Background(), targets)
		if err != nil && !errors.Is(err, ErrMetricsAPIUnavailable) {
			t.Fatalf("tick %d Samples: %v", tick, err)
		}
		if tick == 1 {
			if err != nil {
				t.Fatalf("tick 1 must be the recovery, got %v", err)
			}
			if v := samplesByKey(t, out, "api")[MetricCPUMillicores]; v != 400 {
				t.Errorf("recovery cpu = %v, want 400 — the middle tick must actually produce data", v)
			}
			if v := samplesByKey(t, out, "api")[MetricMemoryBytes]; v != 530_000_000 {
				t.Errorf("recovery mem = %v, want 530000000", v)
			}
		}
	}

	if got := strings.Count(buf.String(), "metrics-server (metrics.k8s.io) API not available"); got != 2 {
		t.Errorf("warnings = %d, want 2 — the first outage and the one after recovery", got)
	}
}

// TestMetricsServerSourceWarnsOncePerOutageAcrossTargets pins the latch against
// the tick shape that actually breaks it. Targets accumulate across namespaces,
// so one tick can read a healthy target before an unavailable one. If the reset
// runs per target, the healthy one re-arms the latch every tick and a single
// standing outage is logged forever — at the default interval roughly once a
// minute. The default configuration is the worst case, because NewMetricSource
// returns a bare metricsServerSource when no scrape URL is set, with none of the
// composite's cooldown.
func TestMetricsServerSourceWarnsOncePerOutageAcrossTargets(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	reader := &perSelectorReader{
		inner: NewClientsetLister(fake.NewSimpleClientset(), nil),
		hits:  map[string]int{},
		results: map[string]struct {
			cpu, mem int64
			err      error
		}{
			"app=healthy": {cpu: 400, mem: 530_000_000},
			"app=broken":  {err: ErrMetricsAPIUnavailable},
		},
	}
	src := newMetricsServerSource(reader, log)
	targets := []ServiceTarget{
		{Slug: "healthy", Namespace: "prod", Selector: "app=healthy"},
		{Slug: "broken", Namespace: "staging", Selector: "app=broken"},
	}

	const ticks = 3
	for range ticks {
		out, err := src.Samples(context.Background(), targets)
		if !errors.Is(err, ErrMetricsAPIUnavailable) {
			t.Fatalf("err = %v, want ErrMetricsAPIUnavailable", err)
		}
		if len(out) != 0 {
			t.Errorf("got %+v, want nothing — an absent API aborts the whole tick, including targets already read", out)
		}
	}

	if reader.hits["app=healthy"] != ticks {
		t.Fatalf("the healthy target was read %d times, want %d — this test only constrains the fix while a clean read precedes the failing one",
			reader.hits["app=healthy"], ticks)
	}
	if got := strings.Count(buf.String(), "metrics-server (metrics.k8s.io) API not available"); got != 1 {
		t.Errorf("warnings = %d, want 1 — one standing outage is one incident, however many targets read cleanly first", got)
	}
}

// TestAutoSourceFallsThroughWhenEveryReadFails pins the composite against the
// shape that silently disabled the fallback: a metrics-server the agent can
// reach but not read — an RBAC denial, or a broken APIService — fails every
// target with a non-NotFound error, which is not ErrMetricsAPIUnavailable. If
// the primary reports that as a successful empty tick, the composite never
// switches and the configured exposition is never consulted, from the very
// first tick and with no cooldown to recover from.
func TestAutoSourceFallsThroughWhenEveryReadFails(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
`)
	reader := &fakeReader{
		inner:      NewClientsetLister(fake.NewSimpleClientset(), nil),
		metricsErr: errors.New(`pods.metrics.k8s.io is forbidden: User "system:serviceaccount:zensu:agent" cannot list resource "pods"`),
	}
	src, err := NewMetricSource(SourceConfig{
		Mode:       SourceAuto,
		Exposition: ExpositionConfig{URL: srv.URL},
	}, reader, nil, nil)
	if err != nil {
		t.Fatalf("NewMetricSource: %v", err)
	}

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod", Selector: "app=api"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 250 {
		t.Errorf("cpu = %v, want 250 from the exposition — a primary that answered nothing must not hold the tick", v)
	}
	if src.Name() != SourceExposition {
		t.Errorf("Name() = %q, want %q — the composite must report the source it actually served from", src.Name(), SourceExposition)
	}

	// The sentinel's identity matters beyond the fall-through: reporting an
	// unreadable API as an absent one would tell an operator to install a
	// metrics-server that is already there, and would make the exported
	// sentinel dead for any caller matching on it.
	bare := newMetricsServerSource(reader, slog.Default())
	_, bareErr := bare.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod", Selector: "app=api"}})
	if !errors.Is(bareErr, ErrMetricsReadsAllFailed) {
		t.Errorf("err = %v, want ErrMetricsReadsAllFailed", bareErr)
	}
	if errors.Is(bareErr, ErrMetricsAPIUnavailable) {
		t.Errorf("err = %v, must NOT be ErrMetricsAPIUnavailable — the API answered, the reads were refused", bareErr)
	}
}

// TestAutoSourceStaysOnTheFallbackWhileThePrimaryIsStillDown pins the failed
// re-probe: once the cooldown elapses the composite probes the primary again,
// and a primary that is still down must keep serving from the fallback rather
// than returning its own empty tick. It does NOT reach the err == nil guard on
// the un-switch, which is unreachable while every error the primary returns
// wraps ErrSourceUnavailable.
func TestAutoSourceStaysOnTheFallbackWhileThePrimaryIsStillDown(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
`)
	reader := &fakeReader{
		inner:      NewClientsetLister(fake.NewSimpleClientset(), nil),
		metricsErr: ErrMetricsAPIUnavailable,
	}
	now := time.Now()
	src := &autoSource{
		primary:  newMetricsServerSource(reader, slog.Default()),
		fallback: newExpositionSource(ExpositionConfig{URL: srv.URL}, slog.Default(), nil),
		log:      slog.Default(),
		now:      func() time.Time { return now },
	}
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", Selector: "app=api"}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	if src.Name() != SourceExposition {
		t.Fatalf("Name() = %q, want the fallback after the switch", src.Name())
	}

	now = now.Add(autoRetryInterval + time.Second)
	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("Samples after the cooldown: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 250 {
		t.Errorf("cpu = %v, want 250 — the re-probe failed, so the fallback must still serve the tick", v)
	}
	if src.Name() != SourceExposition {
		t.Errorf("Name() = %q, want %q — a failed re-probe is not a recovery", src.Name(), SourceExposition)
	}
}

// TestMetricsServerSourceWarnsOncePerOutageAcrossTransientTicks pins the other
// half of the reset condition: a tick that completes without any clean read
// must NOT clear the latch. Without that, an outage interrupted by a tick of
// transient errors is logged as two incidents.
func TestMetricsServerSourceWarnsOncePerOutageAcrossTransientTicks(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	reader := &scriptedReader{
		inner: NewClientsetLister(fake.NewSimpleClientset(), nil),
		errs:  []error{ErrMetricsAPIUnavailable, errors.New("dial tcp: connection refused"), ErrMetricsAPIUnavailable},
	}
	src := newMetricsServerSource(reader, log)
	targets := []ServiceTarget{
		{Slug: "unselectable", Namespace: "prod"},
		{Slug: "api", Namespace: "prod", Selector: "app=api"},
	}

	for range 3 {
		// The middle tick's only read fails transiently, so it reports
		// ErrMetricsReadsAllFailed. This test is about the latch rather than
		// the return value, and both errors are expected shapes here.
		if _, err := src.Samples(context.Background(), targets); err != nil &&
			!errors.Is(err, ErrMetricsAPIUnavailable) && !errors.Is(err, ErrMetricsReadsAllFailed) {
			t.Fatalf("Samples: %v", err)
		}
	}

	if got := strings.Count(buf.String(), "metrics-server (metrics.k8s.io) API not available"); got != 1 {
		t.Errorf("warnings = %d, want 1 — neither a transient read nor a skipped target proves the API answered", got)
	}
}

// TestMetricsServerSourceClearsTheLatchOnAnEmptyRead pins that a read which
// answered but matched no pods still counts as the API being up. Since an empty
// PodMetrics list reports available=false with a nil error, keying the reset on
// data rather than on the read succeeding would leave a cluster whose pods are
// not yet sampled latched for the process lifetime.
func TestMetricsServerSourceClearsTheLatchOnAnEmptyRead(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	reader := &scriptedReader{
		inner:  NewClientsetLister(fake.NewSimpleClientset(), nil),
		errs:   []error{ErrMetricsAPIUnavailable, nil, ErrMetricsAPIUnavailable},
		noData: true,
	}
	src := newMetricsServerSource(reader, log)
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", Selector: "app=api"}}

	for range 3 {
		if _, err := src.Samples(context.Background(), targets); err != nil && !errors.Is(err, ErrMetricsAPIUnavailable) {
			t.Fatalf("Samples: %v", err)
		}
	}

	if got := strings.Count(buf.String(), "metrics-server (metrics.k8s.io) API not available"); got != 2 {
		t.Errorf("warnings = %d, want 2 — an empty read still proves the API answered", got)
	}
}

// TestMetricsServerSourceOmitsATargetWithNoData pins the consequence of an empty
// read one layer up from the adapter: the service is left out of the tick's
// samples entirely rather than carrying a fabricated zero.
func TestMetricsServerSourceOmitsATargetWithNoData(t *testing.T) {
	reader := &fakeReader{
		inner:     NewClientsetLister(fake.NewSimpleClientset(), nil),
		available: false,
	}
	src := newMetricsServerSource(reader, slog.Default())

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod", Selector: "app=api"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if _, ok := got["api"]; ok {
		t.Errorf("got %+v, want no entry for api — no data must not become a zero reading", got)
	}
}

// TestMetricsServerSourceWarnsOncePerOutage pins the other half of the same
// contract: the latch must still suppress the repeat within one outage, which
// is what it was introduced for.
func TestMetricsServerSourceWarnsOncePerOutage(t *testing.T) {
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	reader := &scriptedReader{
		inner: NewClientsetLister(fake.NewSimpleClientset(), nil),
		errs:  []error{ErrMetricsAPIUnavailable},
	}
	src := newMetricsServerSource(reader, log)
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", Selector: "app=api"}}

	for range 4 {
		if _, err := src.Samples(context.Background(), targets); !errors.Is(err, ErrMetricsAPIUnavailable) {
			t.Fatalf("err = %v, want ErrMetricsAPIUnavailable", err)
		}
	}

	if got := strings.Count(buf.String(), "metrics-server (metrics.k8s.io) API not available"); got != 1 {
		t.Errorf("warnings = %d, want 1 — one uninterrupted outage is one incident", got)
	}
}
