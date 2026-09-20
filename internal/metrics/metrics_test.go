package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
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

// TestNilMetricsAreNoOp pins the contract that EVERY method on *Metrics is safe
// on a nil receiver, because metrics.enabled=false is a shipped configuration
// that leaves the pointer nil — a dropped guard panics a supported deployment
// rather than degrading it. The method set is walked by reflection rather than
// listed, so a method added without a guard fails here the day it lands; a
// hand-written list would only cover what someone remembered to add to it.
func TestNilMetricsAreNoOp(t *testing.T) {
	var m *Metrics
	nilValue := reflect.ValueOf(m)
	for i := range nilValue.NumMethod() {
		method := nilValue.Type().Method(i)
		args := make([]reflect.Value, method.Type.NumIn()-1)
		for j := range args {
			args[j] = reflect.New(method.Type.In(j + 1)).Elem()
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("nil %s panicked: %v — every method needs its own nil guard", method.Name, r)
				}
			}()
			nilValue.Method(i).Call(args)
		}()
	}

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

// TestRecordScrapeCountsByResult pins the scrape counters and the duration
// observation inside their own package, so an inverted label is caught here
// rather than only from the agent package.
func TestRecordScrapeCountsByResult(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	m.RecordScrape(true, 2*time.Second)
	m.RecordScrape(false, time.Second)
	m.RecordScrape(false, time.Second)

	if got := testutil.ToFloat64(m.scrapes.WithLabelValues("success")); got != 1 {
		t.Errorf("success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.scrapes.WithLabelValues("error")); got != 2 {
		t.Errorf("error = %v, want 2", got)
	}

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
		"zensu_monitoring_agent_scrape_duration_seconds_count 3",
		`zensu_monitoring_agent_scrape_duration_seconds_bucket{le="1"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("duration histogram missing %q (unit bug?); body:\n%s", want, text)
		}
	}
}

func TestSetResourceServicesMapped(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	m.SetResourceServicesMapped(4)
	if got := testutil.ToFloat64(m.resourceServicesMapped); got != 4 {
		t.Errorf("gauge = %v, want 4", got)
	}
	m.SetResourceServicesMapped(0)
	if got := testutil.ToFloat64(m.resourceServicesMapped); got != 0 {
		t.Errorf("gauge = %v, want 0 after reset", got)
	}
}

// TestEveryResourceLabelIsPreInitialised pins what ResourceRoles and
// ResourceSources exist for: a role or source that never reports must read 0
// rather than be absent, because a rate() or a threshold alert cannot tell an
// absent series from a healthy quiet one. Every other assertion in the suite
// writes its series with a setter first, so nothing else can observe absence.
func TestEveryResourceLabelIsPreInitialised(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	for _, role := range ResourceRoles {
		for _, name := range []string{"resource_samples", "resource_roles_withheld"} {
			want := "zensu_monitoring_agent_" + name + `{role="` + role + `"} 0` + "\n"
			if !strings.Contains(text, want) {
				t.Errorf("missing %q before any setter ran; body:\n%s", want, text)
			}
		}
	}
	for _, source := range ResourceSources {
		want := `zensu_monitoring_agent_resource_source{source="` + source + `"} 0` + "\n"
		if !strings.Contains(text, want) {
			t.Errorf("missing %q before any setter ran; body:\n%s", want, text)
		}
	}

	// Presence alone would let a loop pre-initialise the right vec plus a wrong
	// one. The counts bound each vec to exactly its own label set, which is the
	// cardinality guarantee SetResourceSource's fold exists to hold.
	for _, c := range []struct {
		metric string
		want   int
	}{
		{"resource_samples", len(ResourceRoles)},
		{"resource_roles_withheld", len(ResourceRoles)},
		{"resource_source", len(ResourceSources)},
	} {
		if got := strings.Count(text, "zensu_monitoring_agent_"+c.metric+"{"); got != c.want {
			t.Errorf("%s has %d series, want %d — no vec may carry a label from another set", c.metric, got, c.want)
		}
	}
}

// TestResourceSettersRecordTheirValues closes a verification gap rather than a
// behaviour gap: until it existed, the only assertions of these three setters
// lived in the agent package and read the rendered exposition, so this package's
// own suite passed over a broken setter and could not be refactored or re-homed
// without borrowing another package's tests as its spec.
func TestResourceSettersRecordTheirValues(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())

	m.SetResourceSamples("cpu", 3)
	m.SetResourceSamples("memory", 7)
	if got := testutil.ToFloat64(m.resourceSamples.WithLabelValues("cpu")); got != 3 {
		t.Errorf("resource_samples{cpu} = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.resourceSamples.WithLabelValues("memory")); got != 7 {
		t.Errorf("resource_samples{memory} = %v, want 7 — the role argument must reach the label", got)
	}
	m.SetResourceSamples("cpu", 0)
	if got := testutil.ToFloat64(m.resourceSamples.WithLabelValues("memory")); got != 7 {
		t.Errorf("resource_samples{memory} = %v, want 7 still — clearing one role must not clear the other", got)
	}

	m.SetRolesWithheld("cpu", 2)
	m.SetRolesWithheld("memory", 5)
	if got := testutil.ToFloat64(m.rolesWithheld.WithLabelValues("cpu")); got != 2 {
		t.Errorf("roles_withheld{cpu} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.rolesWithheld.WithLabelValues("memory")); got != 5 {
		t.Errorf("roles_withheld{memory} = %v, want 5 — the role argument must reach the label", got)
	}
	m.SetRolesWithheld("cpu", 0)
	if got := testutil.ToFloat64(m.rolesWithheld.WithLabelValues("cpu")); got != 0 {
		t.Errorf("roles_withheld{cpu} = %v, want 0 after the role stops being withheld", got)
	}
	if got := testutil.ToFloat64(m.rolesWithheld.WithLabelValues("memory")); got != 5 {
		t.Errorf("roles_withheld{memory} = %v, want 5 still — clearing one role must not clear the other", got)
	}

	m.SetResourceSource(SourceExposition)
	for _, source := range ResourceSources {
		want := 0.0
		if source == SourceExposition {
			want = 1
		}
		if got := testutil.ToFloat64(m.resourceSource.WithLabelValues(source)); got != want {
			t.Errorf("resource_source{%s} = %v, want %v — exactly one series is 1", source, got, want)
		}
	}
}

// TestSetResourceSourceFlagsAnUnlistedName pins that a name outside the
// canonical set is reported rather than swallowed, which is what SourceUnknown
// is declared for.
func TestSetResourceSourceFlagsAnUnlistedName(t *testing.T) {
	m := NewWithRegistry(prometheus.NewRegistry())
	m.SetResourceSource("auto")

	if got := testutil.ToFloat64(m.resourceSource.WithLabelValues(SourceUnknown)); got != 1 {
		t.Errorf("%s = %v, want 1 — an unlisted name must be visible", SourceUnknown, got)
	}
	for _, source := range ResourceSources {
		if source == SourceUnknown {
			continue
		}
		if got := testutil.ToFloat64(m.resourceSource.WithLabelValues(source)); got != 0 {
			t.Errorf("%s = %v, want 0 — only the unknown series reports an unlisted name", source, got)
		}
	}
}
