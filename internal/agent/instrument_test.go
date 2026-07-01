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

	a := New(Config{ProductID: "prod", Source: "test", Namespaces: []string{"default"}}, reader, &stubReporter{}, nil)
	a.Metrics = m

	if err := a.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, "zensu_monitoring_agent_services_reported 1") {
		t.Errorf("services_reported should be 1 after a 1-service tick; body:\n%s", body)
	}
}
