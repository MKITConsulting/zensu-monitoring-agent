package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MKITConsulting/zensu-monitoring-agent/internal/redact"
)

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

type refusingTransport struct{}

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
