package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordHeartbeat(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())

	m.RecordHeartbeat(true, 100*time.Millisecond)
	m.RecordHeartbeat(false, 50*time.Millisecond)
	m.RecordHeartbeat(true, 75*time.Millisecond)

	if got := testutil.ToFloat64(m.heartbeats.WithLabelValues("success")); got != 2 {
		t.Errorf("heartbeat_total{success} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.heartbeats.WithLabelValues("error")); got != 1 {
		t.Errorf("heartbeat_total{error} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.lastSuccess); got <= 0 {
		t.Errorf("last_success_timestamp = %v, want > 0", got)
	}
}

func TestNewServesMetrics(t *testing.T) {
	m := New()
	m.RecordHeartbeat(true, 10*time.Millisecond)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `zensu_monitoring_agent_heartbeat_total{result="success"} 1`) {
		t.Errorf("New() registry missing recorded series; body:\n%s", string(body))
	}
}

func TestErrorDoesNotAdvanceLastSuccess(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	m.RecordHeartbeat(false, 10*time.Millisecond)

	if got := testutil.ToFloat64(m.lastSuccess); got != 0 {
		t.Errorf("last_success after error-only = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.heartbeats.WithLabelValues("error")); got != 1 {
		t.Errorf("error counter = %v, want 1", got)
	}
}

func TestPostDurationHistogramBuckets(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	m.RecordHeartbeat(true, 120*time.Millisecond)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, want := range []string{
		`zensu_monitoring_agent_post_duration_seconds_bucket{le="0.1"} 0`,
		`zensu_monitoring_agent_post_duration_seconds_bucket{le="0.25"} 1`,
		"zensu_monitoring_agent_post_duration_seconds_count 1",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("histogram missing %q (unit bug?); body:\n%s", want, text)
		}
	}
}

func TestSetServicesReported(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	m.SetServicesReported(4)
	if got := testutil.ToFloat64(m.servicesReported); got != 4 {
		t.Errorf("services_reported = %v, want 4", got)
	}
}

func TestNilMetricsAreNoOp(t *testing.T) {
	var m *Metrics
	m.RecordHeartbeat(true, time.Second)
	m.RecordHeartbeat(false, time.Second)
	m.SetServicesReported(3)
	if err := m.Serve(context.Background(), ":2112"); err != nil {
		t.Errorf("nil Serve = %v, want nil", err)
	}

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("nil Handler status = %d, want 404", rec.Code)
	}
}

func TestHandlerServesSeries(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	m.RecordHeartbeat(true, 120*time.Millisecond)
	m.SetServicesReported(2)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, want := range []string{
		"zensu_monitoring_agent_heartbeat_total",
		`zensu_monitoring_agent_heartbeat_total{result="success"} 1`,
		"zensu_monitoring_agent_last_success_timestamp_seconds",
		"zensu_monitoring_agent_post_duration_seconds_count 1",
		"zensu_monitoring_agent_services_reported 2",
		"go_goroutines",
		"process_start_time_seconds",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics body missing %q", want)
		}
	}
}

func TestServeGracefulShutdown(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- m.Serve(ctx, "127.0.0.1:0") }()

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Serve returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not shut down within 5s of ctx cancel")
	}
}
