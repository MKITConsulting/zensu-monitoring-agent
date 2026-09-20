package agent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/kubernetes/fake"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
)

func scrapeMetrics(t *testing.T, m *obs.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	b, _ := io.ReadAll(rec.Result().Body)
	return string(b)
}

func TestReporterInstrumentsSuccess(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	m := obs.NewWithRegistry(prometheus.NewRegistry())
	r := &Reporter{BaseURL: backend.URL, APIKey: "k", Client: backend.Client(), Metrics: m}

	if err := r.Send(context.Background(), HeartbeatBatch{ProductID: "p"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `zensu_monitoring_agent_heartbeat_total{result="success"} 1`) {
		t.Errorf("missing success counter; body:\n%s", body)
	}
	if !strings.Contains(body, "zensu_monitoring_agent_post_duration_seconds_count 1") {
		t.Errorf("missing post_duration observation; body:\n%s", body)
	}
}

func TestReporterInstrumentsError(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()

	m := obs.NewWithRegistry(prometheus.NewRegistry())
	r := &Reporter{BaseURL: backend.URL, APIKey: "k", Client: backend.Client(), Metrics: m}

	if err := r.Send(context.Background(), HeartbeatBatch{ProductID: "p"}); err == nil {
		t.Fatal("expected error on HTTP 500")
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `zensu_monitoring_agent_heartbeat_total{result="error"} 1`) {
		t.Errorf("missing error counter; body:\n%s", body)
	}
	if !strings.Contains(body, `zensu_monitoring_agent_heartbeat_total{result="success"} 0`) {
		t.Errorf("success counter should remain 0; body:\n%s", body)
	}
}

func TestReporterInstrumentsTransportError(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	url := backend.URL
	backend.Close() // closed listener → transport-level failure, not an HTTP status

	m := obs.NewWithRegistry(prometheus.NewRegistry())
	r := &Reporter{BaseURL: url, APIKey: "k", Client: backend.Client(), Metrics: m}

	if err := r.Send(context.Background(), HeartbeatBatch{ProductID: "p"}); err == nil {
		t.Fatal("expected transport error against a closed backend")
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `zensu_monitoring_agent_heartbeat_total{result="error"} 1`) {
		t.Errorf("transport failure should count as error; body:\n%s", body)
	}
}

func TestReporterNilMetricsDoesNotPanic(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	r := &Reporter{BaseURL: backend.URL, APIKey: "k", Client: backend.Client()}
	if err := r.Send(context.Background(), HeartbeatBatch{ProductID: "p"}); err != nil {
		t.Fatalf("Send with nil Metrics: %v", err)
	}
}

func TestTickSetsServicesReported(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 2, 2))
	reader := &fakeReader{inner: NewClientsetLister(client, nil), metricsErr: ErrMetricsAPIUnavailable}
	m := obs.NewWithRegistry(prometheus.NewRegistry())

	a := New(Config{ProductID: "prod", Source: "test", Namespaces: []string{"default"}}, reader, &stubReporter{}, nil, newMetricsServerSource(reader, nil))
	a.Metrics = m

	if err := a.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "zensu_monitoring_agent_services_reported 1") {
		t.Errorf("services_reported should be 1 after a 1-service tick; body:\n%s", body)
	}
}

// roleSource serves a fixed sample set, so a tick can be made to deliver one
// role and not the other.
type roleSource struct {
	name    string
	samples []MetricSample
}

func (r *roleSource) Name() string { return r.name }

func (r *roleSource) Samples(_ context.Context, targets []ServiceTarget) (map[string][]MetricSample, error) {
	out := map[string][]MetricSample{}
	for _, t := range targets {
		out[t.Slug] = r.samples
	}
	return out, nil
}

// TestResourceSamplesAreRoleGranular pins the gauge that makes a per-role loss
// visible. Every failure mode this feature has is per role, and in each the other
// role still reports — so resource_services_mapped does not move, and a
// service-granular count reads healthy while half the data is gone.
func TestResourceSamplesAreRoleGranular(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 1, 1))
	m := obs.NewWithRegistry(prometheus.NewRegistry())
	src := &roleSource{name: SourceExposition, samples: []MetricSample{
		{Key: MetricMemoryBytes, Value: 1000},
	}}
	a := New(Config{ProductID: "p", Namespaces: []string{"default"}},
		NewClientsetLister(client, nil), &stubReporter{}, nil, src)
	a.Metrics = m

	if _, err := a.Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `zensu_monitoring_agent_resource_samples{role="memory"} 1`) {
		t.Errorf("memory role must report 1; body:\n%s", body)
	}
	if !strings.Contains(body, `zensu_monitoring_agent_resource_samples{role="cpu"} 0`) {
		t.Errorf("the missing CPU role must read 0, not be absent; body:\n%s", body)
	}
	// The service-granular gauge cannot see this loss, which is the point.
	if !strings.Contains(body, "zensu_monitoring_agent_resource_services_mapped 1") {
		t.Errorf("mapped must still read 1, showing why it is not enough; body:\n%s", body)
	}
}

// TestResourceSourceNamesTheActiveSource pins that the source in use is readable
// from one scrape. Without it a week-old pod cannot be asked which source it is
// on, and scrape_total cannot answer: it is pre-initialised in modes that never
// scrape, so a zero rate reads the same as a healthy tick and as a stopped
// exposition.
func TestResourceSourceNamesTheActiveSource(t *testing.T) {
	cases := []struct {
		active  string
		samples []MetricSample
	}{
		// The mapped row matters: without it every assertion here would run on a
		// tick that produced no samples, and a guard that reported the source
		// only on empty ticks would pass.
		{active: SourceExposition, samples: []MetricSample{{Key: MetricCPUMillicores, Value: 250}}},
		{active: SourceMetricsServer},
		{active: SourceNone},
	}
	for _, c := range cases {
		t.Run(c.active, func(t *testing.T) {
			active := c.active
			client := fake.NewSimpleClientset(deployment("default", "api", "api", 1, 1))
			m := obs.NewWithRegistry(prometheus.NewRegistry())
			a := New(Config{ProductID: "p", Namespaces: []string{"default"}},
				NewClientsetLister(client, nil), &stubReporter{}, nil,
				&roleSource{name: active, samples: c.samples})
			a.Metrics = m

			if _, err := a.Collect(context.Background()); err != nil {
				t.Fatalf("Collect: %v", err)
			}

			body := scrapeMetrics(t, m)
			if !strings.Contains(body, `zensu_monitoring_agent_resource_source{source="`+active+`"} 1`) {
				t.Errorf("the active source %q must read 1; body:\n%s", active, body)
			}
			for _, other := range obs.ResourceSources {
				if other == active {
					continue
				}
				if !strings.Contains(body, `zensu_monitoring_agent_resource_source{source="`+other+`"} 0`) {
					t.Errorf("inactive source %q must read 0 rather than be absent; body:\n%s", other, body)
				}
			}
		})
	}
}
