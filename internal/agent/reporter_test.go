package agent

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
	"github.com/MKITConsulting/zensu-monitoring-agent/internal/redact"
)

// TestReporterSend hands the captured request over through a buffered channel:
// the handler runs on the server's own goroutine, and a completed round trip is
// not a happens-before edge for a plain variable.
func TestReporterSend(t *testing.T) {
	type request struct {
		key  string
		body HeartbeatBatch
	}
	seen := make(chan request, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got request
		got.key = r.Header.Get("X-API-Key")
		_ = json.NewDecoder(r.Body).Decode(&got.body)
		seen <- got
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rep := NewReporter(srv.URL, "zsk_test", 5*time.Second)
	err := rep.Send(context.Background(), HeartbeatBatch{
		ProductID: "p",
		Services:  []ServiceHeartbeat{{Slug: "api", Status: StatusUp}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := <-seen
	gotKey, gotBody := got.key, got.body
	if gotKey != "zsk_test" {
		t.Errorf("X-API-Key = %q", gotKey)
	}
	if gotBody.ProductID != "p" || len(gotBody.Services) != 1 || gotBody.Services[0].Slug != "api" {
		t.Errorf("server received unexpected body: %+v", gotBody)
	}
}

func TestReporterSend_RejectsNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":"boom"}`))
	}))
	defer srv.Close()

	rep := NewReporter(srv.URL, "k", 5*time.Second)
	if err := rep.Send(context.Background(), HeartbeatBatch{ProductID: "p"}); err == nil {
		t.Error("expected error on 500 response")
	}
}

// TestReporterRefusesRedirect pins that the credential-carrying client will not
// follow a 3xx. A heartbeat POST has no legitimate redirect, and following one
// would send the API key to a destination the operator never configured.
func TestReporterRefusesRedirect(t *testing.T) {
	var elsewhereHits atomic.Int64
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		elsewhereHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer elsewhere.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, elsewhere.URL, http.StatusFound)
	}))
	defer redirector.Close()

	r := NewReporter(redirector.URL, "zsk_secret", 5*time.Second)
	if err := r.Send(context.Background(), HeartbeatBatch{ProductID: "p"}); err == nil {
		t.Fatal("a redirect must surface as an error, not be followed")
	}
	if n := elsewhereHits.Load(); n != 0 {
		t.Errorf("the redirect target was contacted %d times; the API key must never reach it", n)
	}
}

// TestReporterBoundsAndStripsPeerText pins that a rejecting endpoint cannot put
// unbounded or control bytes into the agent's log.
func TestReporterBoundsAndStripsPeerText(t *testing.T) {
	body := "rejected\n\x1b[31m\x00" + strings.Repeat("B", 8192)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	err := NewReporter(srv.URL, "k", 5*time.Second).Send(context.Background(), HeartbeatBatch{ProductID: "p"})
	if err == nil {
		t.Fatal("expected an error on HTTP 400")
	}
	msg := err.Error()
	if len(msg) > redact.SnippetBytes+64 {
		t.Errorf("error length %d is not bounded: %q", len(msg), msg)
	}
	for _, r := range msg {
		if r < 0x20 || r > 0x7e {
			t.Errorf("error carries a non-printable rune %q", r)
			break
		}
	}
}

// refusingTransport fails every request the way a refused dial does, so the
// test depends on neither a freed ephemeral port nor the platform's syscall
// wording.
type refusingTransport struct{}

func (refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connect: connection refused")
}

// TestReporterTransportErrorOmitsTheURL pins the same property on the heartbeat
// client as on the scrape client. Both call (*http.Client).Do, and *url.Error
// embeds the full URL in its Error().
func TestReporterTransportErrorOmitsTheURL(t *testing.T) {
	const base = "https://zensu.invalid"
	rep := NewReporter(base, "zsk_test", time.Second)
	rep.Client = &http.Client{Transport: refusingTransport{}}

	err := rep.Send(context.Background(), HeartbeatBatch{ProductID: "p"})
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if strings.Contains(err.Error(), base) {
		t.Errorf("the error must not carry the request URL, got %v", err)
	}
	if !strings.Contains(err.Error(), "connect") {
		t.Errorf("the error must still name a transport reason, got %v", err)
	}
}

func TestReporterTrimsTrailingSlashFromBaseURL(t *testing.T) {
	paths := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		paths <- req.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := NewReporter(srv.URL+"/", "k", 5*time.Second)
	if err := r.Send(context.Background(), HeartbeatBatch{ProductID: "p"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	gotPath := <-paths
	if gotPath != "/api/runtime/heartbeat" {
		t.Errorf("path = %q, want no doubled slash", gotPath)
	}
}

// mustNotDial fails the test if any request is attempted. It turns "this call
// never reaches the network" from a claim in a doc comment into an assertion.
type mustNotDial struct{ t *testing.T }

func (d mustNotDial) RoundTrip(req *http.Request) (*http.Response, error) {
	d.t.Errorf("no request may be issued, got %s %s", req.Method, req.URL)
	return nil, errors.New("mustNotDial")
}

// TestReporterSendRefusesANonFiniteMetric pins the last line of defence against a
// value JSON cannot carry. The exposition source already drops non-finite
// samples, so this guard sits behind that one: a value that still reaches Send
// must fail the whole batch before anything is written to the network, and the
// attempt must still be counted as a failed heartbeat — the deferred
// RecordHeartbeat has to fire even on a path that never builds a request.
func TestReporterSendRefusesANonFiniteMetric(t *testing.T) {
	cases := []struct {
		name  string
		value float64
	}{
		{"NaN", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := obs.NewWithRegistry(prometheus.NewRegistry())
			rep := NewReporter("http://zensu.example", "k", 5*time.Second)
			rep.Metrics = m
			rep.Client = &http.Client{Transport: mustNotDial{t: t}}

			err := rep.Send(context.Background(), HeartbeatBatch{ProductID: "p", Services: []ServiceHeartbeat{{
				Slug:    "api",
				Status:  StatusUp,
				Metrics: []MetricSample{{Key: MetricCPUMillicores, Value: c.value}},
			}}})
			if err == nil {
				t.Fatal("expected Send to refuse a batch JSON cannot encode")
			}
			if !strings.Contains(err.Error(), "unsupported value") {
				t.Errorf("err = %v, want it to name the encoding failure", err)
			}
			body := scrapeMetrics(t, m)
			if !strings.Contains(body, `zensu_monitoring_agent_heartbeat_total{result="error"} 1`) {
				t.Errorf("an unencodable batch must count as a failed heartbeat; body:\n%s", body)
			}
		})
	}
}

// TestReporterSendRejectsAnUnbuildableRequest pins that a base URL the HTTP
// package cannot turn into a request fails before anything reaches the network.
// Startup cannot produce such a URL — ValidateAPIURL refuses it through the same
// url.Parse that would fail here — so this guards a caller that sets the
// exported BaseURL field directly.
//
// Note what is deliberately NOT asserted: this branch returns the *url.Error
// bare, so it echoes the configured URL, unlike Send's transport branch
// (TestReporterTransportErrorOmitsTheURL) and unlike the identical call in the
// scrape client, which wraps it in redact.TransportReason. That asymmetry is
// reported as a defect rather than pinned here.
func TestReporterSendRejectsAnUnbuildableRequest(t *testing.T) {
	rep := NewReporter("http://zensu.example\x7f", "k", time.Second)
	rep.Client = &http.Client{Transport: mustNotDial{t: t}}

	err := rep.Send(context.Background(), HeartbeatBatch{ProductID: "p"})
	if err == nil {
		t.Fatal("expected Send to refuse a base URL it cannot build a request from")
	}
	if !strings.Contains(err.Error(), "invalid control character") {
		t.Errorf("err = %v, want the request-build failure rather than a transport or status error", err)
	}
}
