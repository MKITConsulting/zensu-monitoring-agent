package scrape

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/MKITConsulting/zensu-monitoring-agent/internal/redact"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/protobuf/proto"
)

const sampleExposition = `# HELP k8s_pod_cpu_usage Pod CPU usage
# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{k8s_pod_name="api-1",k8s_namespace_name="prod"} 0.25
k8s_pod_cpu_usage{k8s_pod_name="api-2",k8s_namespace_name="prod"} 0.31
# HELP container_cpu_usage_seconds_total Cumulative CPU
# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 1234.5
# HELP bare_value No TYPE line follows the convention
# TYPE bare_value untyped
bare_value{pod="api-1"} 7
# HELP request_latency A histogram the agent cannot reduce
# TYPE request_latency histogram
request_latency_bucket{le="0.1"} 1
request_latency_bucket{le="+Inf"} 2
request_latency_sum 0.5
request_latency_count 2
`

func serve(t *testing.T, body, contentType string, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func findSeries(all []Series, name string, label, value string) (Series, bool) {
	for _, s := range all {
		if s.Name == name && s.Labels[label] == value {
			return s, true
		}
	}
	return Series{}, false
}

func TestFetchDecodesKindsFromTypeMetadata(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; version=0.0.4", http.StatusOK)

	got, err := NewClient(srv.URL, 0).Fetch(context.Background(), nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	cases := []struct {
		name      string
		label     string
		labelVal  string
		wantKind  Kind
		wantValue float64
	}{
		{"k8s_pod_cpu_usage", "k8s_pod_name", "api-1", KindGauge, 0.25},
		{"k8s_pod_cpu_usage", "k8s_pod_name", "api-2", KindGauge, 0.31},
		{"container_cpu_usage_seconds_total", "pod", "api-1", KindCounter, 1234.5},
		{"bare_value", "pod", "api-1", KindGauge, 7},
	}
	for _, c := range cases {
		t.Run(c.name+"/"+c.labelVal, func(t *testing.T) {
			s, ok := findSeries(got, c.name, c.label, c.labelVal)
			if !ok {
				t.Fatalf("series %s{%s=%s} missing", c.name, c.label, c.labelVal)
			}
			if s.Kind != c.wantKind {
				t.Errorf("kind = %v, want %v", s.Kind, c.wantKind)
			}
			if s.Value != c.wantValue {
				t.Errorf("value = %v, want %v", s.Value, c.wantValue)
			}
		})
	}

	for _, s := range got {
		if strings.HasPrefix(s.Name, "request_latency") {
			t.Errorf("histogram family must be skipped, got series %q", s.Name)
		}
	}
}

func TestFetchPreservesAllLabels(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; version=0.0.4", http.StatusOK)

	got, err := NewClient(srv.URL, 0).Fetch(context.Background(), nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	s, ok := findSeries(got, "k8s_pod_cpu_usage", "k8s_pod_name", "api-1")
	if !ok {
		t.Fatal("expected the api-1 CPU series")
	}
	if len(s.Labels) != 2 {
		t.Errorf("label count = %d, want 2 (%v)", len(s.Labels), s.Labels)
	}
	if s.Labels["k8s_namespace_name"] != "prod" {
		t.Errorf("namespace label = %q, want prod", s.Labels["k8s_namespace_name"])
	}
}

func TestFetchParsesWhenContentTypeIsUnrecognized(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; charset=utf-8", http.StatusOK)

	got, err := NewClient(srv.URL, 0).Fetch(context.Background(), nil)
	if err != nil {
		t.Fatalf("Fetch with bare text/plain: %v", err)
	}
	if _, ok := findSeries(got, "k8s_pod_cpu_usage", "k8s_pod_name", "api-1"); !ok {
		t.Error("an exposition served without a version parameter must still decode as text")
	}
}

func TestFetchRejectsErrorStatus(t *testing.T) {
	srv := serve(t, "collector on fire", "text/plain", http.StatusInternalServerError)

	_, err := NewClient(srv.URL, 0).Fetch(context.Background(), nil)
	if err == nil {
		t.Fatal("expected an error on HTTP 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should name the status, got %v", err)
	}
	if !strings.Contains(err.Error(), "collector on fire") {
		t.Errorf("error should carry a body snippet, got %v", err)
	}
}

func TestFetchRejectsOversizedBody(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; version=0.0.4", http.StatusOK)

	c := NewClient(srv.URL, 0)
	c.MaxBytes = 16

	_, err := c.Fetch(context.Background(), nil)
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("error = %v, want ErrBodyTooLarge", err)
	}
}

func TestFetchAcceptsBodyExactlyAtCap(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; version=0.0.4", http.StatusOK)

	c := NewClient(srv.URL, 0)
	c.MaxBytes = int64(len(sampleExposition))

	if _, err := c.Fetch(context.Background(), nil); err != nil {
		t.Fatalf("a body exactly at the cap must be accepted, got %v", err)
	}
}

// refusingTransport fails every request the way a refused dial does, without
// depending on a just-closed ephemeral port staying unbound or on the platform's
// wording for the syscall. http.Client still wraps it in *url.Error, which is
// the shape the redaction under test has to strip.
type refusingTransport struct{}

func (refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connect: connection refused")
}

// TestFetchTransportErrorOmitsTheURL pins that a failed request does not put the
// configured URL back into the log. *url.Error embeds it verbatim, query string
// included, which is exactly what startup validation refuses to print. Absence
// alone is not enough, so the reason itself is asserted too: dropping it would
// leave a leak-free but useless message.
func TestFetchTransportErrorOmitsTheURL(t *testing.T) {
	const host = "http://collector.invalid:9090"
	c := NewClient(host+"/metrics?token=s3cret", 0)
	c.HTTP = &http.Client{Transport: refusingTransport{}}

	_, err := c.Fetch(context.Background(), nil)
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if strings.Contains(err.Error(), "s3cret") || strings.Contains(err.Error(), host) {
		t.Errorf("the error must not carry the request URL, got %v", err)
	}
	reason := strings.TrimPrefix(err.Error(), "scrape: request failed: ")
	if reason == "" || reason == err.Error() || !strings.Contains(reason, "connect") {
		t.Errorf("the error must still name a transport reason, got %v", err)
	}
}

func TestFetchTransportErrorIsReturned(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; version=0.0.4", http.StatusOK)
	url := srv.URL
	srv.Close()

	if _, err := NewClient(url, 0).Fetch(context.Background(), nil); err == nil {
		t.Fatal("expected a transport error against a closed endpoint")
	}
}

func TestFingerprintIsOrderIndependentAndLabelSensitive(t *testing.T) {
	a := Series{Name: "m", Labels: map[string]string{"pod": "p1", "ns": "prod"}}
	b := Series{Name: "m", Labels: map[string]string{"ns": "prod", "pod": "p1"}}
	c := Series{Name: "m", Labels: map[string]string{"pod": "p2", "ns": "prod"}}
	d := Series{Name: "other", Labels: map[string]string{"pod": "p1", "ns": "prod"}}

	if a.Fingerprint() != b.Fingerprint() {
		t.Error("label map order must not change the fingerprint")
	}
	if a.Fingerprint() == c.Fingerprint() {
		t.Error("a different label value must change the fingerprint")
	}
	if a.Fingerprint() == d.Fingerprint() {
		t.Error("a different metric name must change the fingerprint")
	}
}

func TestKindString(t *testing.T) {
	cases := map[Kind]string{KindGauge: "gauge", KindCounter: "counter", KindUnsupported: "unsupported"}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Errorf("Kind(%d).String() = %q, want %q", k, got, want)
		}
	}
}

// TestFingerprintIsInjectiveAcrossLabelBoundaries pins the length-prefixed
// encoding. Under plain concatenation both series below render "1:m..."-free
// text "mabc" and would share one Rater slot, differencing two unrelated
// counters against each other.
// The second pair injects a key that sorts AFTER pod, with the delimiter embedded
// in the value exactly where the encoder would place it, so under a plain
// delimiter encoding both render "m\x1fpod\x1fa\x1fq\x1fb".
func TestFingerprintIsInjectiveAcrossLabelBoundaries(t *testing.T) {
	a := Series{Name: "m", Labels: map[string]string{"ab": "c"}}
	b := Series{Name: "m", Labels: map[string]string{"a": "bc"}}
	if a.Fingerprint() == b.Fingerprint() {
		t.Errorf("label-boundary collision: both render %q", a.Fingerprint())
	}

	c := Series{Name: "m", Labels: map[string]string{"pod": "a\x1fq\x1fb"}}
	d := Series{Name: "m", Labels: map[string]string{"pod": "a", "q": "b"}}
	if c.Fingerprint() == d.Fingerprint() {
		t.Errorf("a delimiter inside a label value must not forge a collision: %q", c.Fingerprint())
	}
}

// TestFetchKeepFilterDropsUnlistedFamilies pins that the pre-filter actually
// filters. Without it an exposition the agent mostly discards would still
// allocate a Series and a label map per row.
func TestFetchKeepFilterDropsUnlistedFamilies(t *testing.T) {
	body := `# TYPE wanted gauge
wanted{pod="p"} 1
# TYPE unwanted gauge
unwanted{pod="p"} 2
`
	srv := serve(t, body, "text/plain; version=0.0.4", http.StatusOK)

	got, err := NewClient(srv.URL, 0).Fetch(context.Background(), map[string]bool{"wanted": true})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(got) != 1 || got[0].Name != "wanted" {
		t.Fatalf("keep filter did not drop the unlisted family, got %+v", got)
	}
}

// TestFetchKeepFilterExemptsUnlistedFromBudget pins the load-bearing
// consequence: rows the filter drops must not count against MaxSamples, or a
// large irrelevant family would fail an otherwise fine scrape.
func TestFetchKeepFilterExemptsUnlistedFromBudget(t *testing.T) {
	var b strings.Builder
	b.WriteString("# TYPE unwanted gauge\n")
	for i := 0; i <= MaxSamples; i++ {
		fmt.Fprintf(&b, "unwanted{pod=\"p%d\"} 1\n", i)
	}
	b.WriteString("# TYPE wanted gauge\nwanted{pod=\"p\"} 7\n")

	got, err := decode(strings.NewReader(b.String()), expfmt.NewFormat(expfmt.TypeTextPlain), map[string]bool{"wanted": true})
	if err != nil {
		t.Fatalf("an oversized UNLISTED family must not fail the scrape: %v", err)
	}
	if len(got) != 1 || got[0].Value != 7 {
		t.Errorf("want only the kept row, got %+v", got)
	}
}

func TestFetchRejectsUnparsableExposition(t *testing.T) {
	srv := serve(t, "# TYPE broken gauge\nbroken{unterminated\n", "text/plain; version=0.0.4", http.StatusOK)

	_, err := NewClient(srv.URL, 0).Fetch(context.Background(), nil)
	if err == nil {
		t.Fatal("a malformed exposition must not decode silently")
	}
	if !strings.Contains(err.Error(), "decode exposition") {
		t.Errorf("error should name the decode step, got %v", err)
	}
}

func TestFetchRejectsUnbuildableRequest(t *testing.T) {
	if _, err := NewClient(":", 0).Fetch(context.Background(), nil); err == nil {
		t.Fatal("an unparsable URL must fail before any network call")
	}
}

func TestFetchUsesDefaultTimeoutAndCapWhenUnset(t *testing.T) {
	c := NewClient("http://example.invalid/metrics", 0)
	if c.HTTP.Timeout != DefaultTimeout {
		t.Errorf("timeout = %v, want the default %v", c.HTTP.Timeout, DefaultTimeout)
	}
	if c.MaxBytes != DefaultMaxBytes {
		t.Errorf("MaxBytes = %d, want the default %d", c.MaxBytes, DefaultMaxBytes)
	}
}

func TestFetchToleratesZeroedMaxBytes(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; version=0.0.4", http.StatusOK)
	c := NewClient(srv.URL, 0)
	c.MaxBytes = 0

	if _, err := c.Fetch(context.Background(), nil); err != nil {
		t.Fatalf("MaxBytes=0 must fall back to the default, got %v", err)
	}
}

func TestFetchToleratesNilHTTPClient(t *testing.T) {
	srv := serve(t, sampleExposition, "text/plain; version=0.0.4", http.StatusOK)
	c := &Client{URL: srv.URL}

	if _, err := c.Fetch(context.Background(), nil); err != nil {
		t.Fatalf("a zero-value Client must still fetch, got %v", err)
	}
}

// TestSeriesFromFamilySkipsMetricsWithoutTheirTypedValue covers the defensive
// nil guards on seriesFromFamily, which is reachable only by direct call: Fetch
// pins the decoder to the text format it requested, and the text parser cannot
// produce a family typed GAUGE whose metrics carry no Gauge. The guards stay
// because the function is package API, not because a scrape can hit them.
func TestSeriesFromFamilySkipsMetricsWithoutTheirTypedValue(t *testing.T) {
	cases := []struct {
		name   string
		family *dto.MetricFamily
	}{
		{
			name: "gauge family with no gauge value",
			family: &dto.MetricFamily{
				Name:   proto.String("g"),
				Type:   dto.MetricType_GAUGE.Enum(),
				Metric: []*dto.Metric{{}},
			},
		},
		{
			name: "counter family with no counter value",
			family: &dto.MetricFamily{
				Name:   proto.String("c"),
				Type:   dto.MetricType_COUNTER.Enum(),
				Metric: []*dto.Metric{{}},
			},
		},
		{
			name: "untyped family with no untyped value",
			family: &dto.MetricFamily{
				Name:   proto.String("u"),
				Type:   dto.MetricType_UNTYPED.Enum(),
				Metric: []*dto.Metric{{}},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := mustSeriesFromFamily(t, c.family); len(got) != 0 {
				t.Errorf("want no series from a value-less metric, got %+v", got)
			}
		})
	}
}

func TestSeriesFromFamilyReadsEachTypedValue(t *testing.T) {
	cases := []struct {
		name      string
		family    *dto.MetricFamily
		wantKind  Kind
		wantValue float64
	}{
		{
			name: "gauge",
			family: &dto.MetricFamily{
				Name:   proto.String("g"),
				Type:   dto.MetricType_GAUGE.Enum(),
				Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: proto.Float64(1.5)}}},
			},
			wantKind:  KindGauge,
			wantValue: 1.5,
		},
		{
			name: "counter",
			family: &dto.MetricFamily{
				Name:   proto.String("c"),
				Type:   dto.MetricType_COUNTER.Enum(),
				Metric: []*dto.Metric{{Counter: &dto.Counter{Value: proto.Float64(9)}}},
			},
			wantKind:  KindCounter,
			wantValue: 9,
		},
		{
			name: "untyped reads as a gauge",
			family: &dto.MetricFamily{
				Name:   proto.String("u"),
				Type:   dto.MetricType_UNTYPED.Enum(),
				Metric: []*dto.Metric{{Untyped: &dto.Untyped{Value: proto.Float64(3)}}},
			},
			wantKind:  KindGauge,
			wantValue: 3,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mustSeriesFromFamily(t, c.family)
			if len(got) != 1 {
				t.Fatalf("want 1 series, got %d", len(got))
			}
			if got[0].Kind != c.wantKind || got[0].Value != c.wantValue {
				t.Errorf("got kind=%v value=%v, want kind=%v value=%v",
					got[0].Kind, got[0].Value, c.wantKind, c.wantValue)
			}
		})
	}
}

func TestSeriesFromFamilySkipsSummaryAndHistogram(t *testing.T) {
	for _, mt := range []dto.MetricType{dto.MetricType_SUMMARY, dto.MetricType_HISTOGRAM} {
		family := &dto.MetricFamily{
			Name:   proto.String("agg"),
			Type:   mt.Enum(),
			Metric: []*dto.Metric{{}},
		}
		if got := mustSeriesFromFamily(t, family); len(got) != 0 {
			t.Errorf("%v must not reduce to a scalar, got %+v", mt, got)
		}
	}
}

// mustSeriesFromFamily converts a family with an ample budget, failing the test
// if the budget is what refused it.
func mustSeriesFromFamily(t *testing.T, mf *dto.MetricFamily) []Series {
	t.Helper()
	got, ok := seriesFromFamily(mf, MaxSamples)
	if !ok {
		t.Fatalf("family %q refused with a full budget", mf.GetName())
	}
	return got
}

// TestSeriesFromFamilyRefusesBeyondBudget pins that the sample budget stops a
// family MID-conversion. Checking only after a family is materialized would let
// a peer that puts its whole body into one accepted family pay the peak
// allocation before the guard fires.
func TestSeriesFromFamilyRefusesBeyondBudget(t *testing.T) {
	metrics := make([]*dto.Metric, 0, 5)
	for i := 0; i < 5; i++ {
		metrics = append(metrics, &dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(float64(i))}})
	}
	family := &dto.MetricFamily{
		Name:   proto.String("g"),
		Type:   dto.MetricType_GAUGE.Enum(),
		Metric: metrics,
	}

	if _, ok := seriesFromFamily(family, 4); ok {
		t.Error("a family of 5 rows must be refused with a budget of 4")
	}
	got, ok := seriesFromFamily(family, 5)
	if !ok || len(got) != 5 {
		t.Errorf("a budget of exactly 5 must succeed with 5 rows, got ok=%v len=%d", ok, len(got))
	}
	if _, ok := seriesFromFamily(family, 0); ok {
		t.Error("an exhausted budget must refuse")
	}
}

// TestDecodeRefusesTooManySamples pins the published limit end to end.
func TestDecodeRefusesTooManySamples(t *testing.T) {
	var b strings.Builder
	b.WriteString("# TYPE k8s_pod_cpu_usage gauge\n")
	for i := 0; i <= MaxSamples; i++ {
		fmt.Fprintf(&b, "k8s_pod_cpu_usage{pod=\"p%d\"} 1\n", i)
	}

	_, err := decode(strings.NewReader(b.String()), expfmt.NewFormat(expfmt.TypeTextPlain), nil)
	if !errors.Is(err, ErrTooManySamples) {
		t.Fatalf("err = %v, want ErrTooManySamples", err)
	}
}

func TestReadLimitResolution(t *testing.T) {
	cases := []struct {
		name string
		in   int64
		want int64
	}{
		{"negative falls back", -1, DefaultMaxBytes},
		{"unset falls back", 0, DefaultMaxBytes},
		{"one byte is honoured", 1, 1},
		{"the default is honoured", DefaultMaxBytes, DefaultMaxBytes},
		{"exactly at the ceiling", MaxConfigurableBytes, MaxConfigurableBytes},
		{"one past the ceiling falls back", MaxConfigurableBytes + 1, DefaultMaxBytes},
		{"MaxInt64 falls back rather than overflowing", math.MaxInt64, DefaultMaxBytes},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client := &Client{MaxBytes: c.in, MemLimit: math.MaxInt64}
			if got := client.readLimit(); got != c.want {
				t.Errorf("readLimit(%d) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

// TestCeilingDerivedFromMemLimit pins the rule that makes the documented "raise
// all three together" enforceable instead of advisory: a cap the pod's own heap
// limit cannot afford is refused, and the shipped default is never shrunk by a
// derivation the operator did not ask for.
func TestCeilingDerivedFromMemLimit(t *testing.T) {
	const mib = 1 << 20
	cases := []struct {
		name     string
		memLimit int64
		want     int64
	}{
		{"no runtime limit keeps the absolute ceiling", math.MaxInt64, MaxConfigurableBytes},
		{"the chart default cannot afford more than the default cap", 56 * mib, DefaultMaxBytes},
		{"a limit below the default still floors at the default", 8 * mib, DefaultMaxBytes},
		{"exactly ten times the default derives the default", DefaultMaxBytes * ExpansionFactor, DefaultMaxBytes},
		{"a raised limit derives a proportionally raised ceiling", 120 * mib, 12 * mib},
		{"a limit past the absolute ceiling is clamped", 1024 * mib, MaxConfigurableBytes},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := (&Client{MemLimit: c.memLimit}).ceiling(); got != c.want {
				t.Errorf("ceiling() at memLimit %d = %d, want %d", c.memLimit, got, c.want)
			}
		})
	}
}

// TestReadLimitRefusesCapTheMemLimitCannotAfford is the behavioural half: the
// operator raised scrapeMaxBytes to the absolute ceiling but left GOMEMLIMIT at
// the chart default, so the cap falls back rather than being honoured.
func TestReadLimitRefusesCapTheMemLimitCannotAfford(t *testing.T) {
	const mib = 1 << 20
	client := &Client{MaxBytes: MaxConfigurableBytes, MemLimit: 56 * mib}
	if got := client.readLimit(); got != DefaultMaxBytes {
		t.Errorf("readLimit() = %d, want the default %d", got, DefaultMaxBytes)
	}
	client.MemLimit = 160 * mib
	if got := client.readLimit(); got != MaxConfigurableBytes {
		t.Errorf("readLimit() with headroom = %d, want %d", got, MaxConfigurableBytes)
	}
}

// TestZeroMemLimitReadsTheRuntime pins that an unset MemLimit consults the
// process's own GOMEMLIMIT rather than silently behaving as unbounded.
func TestZeroMemLimitReadsTheRuntime(t *testing.T) {
	previous := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })

	debug.SetMemoryLimit(56 << 20)
	if got := (&Client{}).ceiling(); got != DefaultMaxBytes {
		t.Errorf("ceiling() under a 56MiB runtime limit = %d, want %d", got, DefaultMaxBytes)
	}

	debug.SetMemoryLimit(math.MaxInt64)
	if got := (&Client{}).ceiling(); got != MaxConfigurableBytes {
		t.Errorf("ceiling() under no runtime limit = %d, want %d", got, MaxConfigurableBytes)
	}
}

// TestFetchRefusesRedirect pins the documented egress guarantee: the peer cannot
// send the agent to a second destination.
func TestFetchRefusesRedirect(t *testing.T) {
	var elsewhereHits atomic.Int64
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		elsewhereHits.Add(1)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte("# TYPE leaked gauge\nleaked 1\n"))
	}))
	defer elsewhere.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, elsewhere.URL, http.StatusFound)
	}))
	defer redirector.Close()

	for _, c := range []*Client{NewClient(redirector.URL, 0), {URL: redirector.URL}} {
		got, err := c.Fetch(context.Background(), nil)
		if err == nil {
			t.Errorf("a redirect must surface as an error, not be followed; got %+v", got)
			continue
		}
		if !strings.Contains(err.Error(), "302") {
			t.Errorf("error should name the status, got %v", err)
		}
	}
	if n := elsewhereHits.Load(); n != 0 {
		t.Errorf("the redirect target was contacted %d times; it must never be reached", n)
	}
}

// TestFetchBoundsAndStripsPeerText pins that neither the reason phrase nor the
// body can put unbounded or control bytes into a log record.
func TestFetchBoundsAndStripsPeerText(t *testing.T) {
	body := "start\n\x1b[31mred\x00" + strings.Repeat("A", 4096) + "end"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, 0).Fetch(context.Background(), nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	msg := err.Error()
	if len(msg) > redact.SnippetBytes+redact.StatusPhraseBytes+64 {
		t.Errorf("error length %d is not bounded: %q", len(msg), msg)
	}
	for _, r := range msg {
		if r < 0x20 || r > 0x7e {
			t.Errorf("error carries a non-printable rune %q: %q", r, msg)
			break
		}
	}
}

// TestDecodeErrorIsBoundedAndStripped pins the same treatment on the parse path.
func TestDecodeErrorIsBoundedAndStripped(t *testing.T) {
	malformed := "# TYPE broken gauge\nbroken{" + strings.Repeat("x", 2048) + "\n"
	srv := serve(t, malformed, "text/plain; version=0.0.4", http.StatusOK)

	_, err := NewClient(srv.URL, 0).Fetch(context.Background(), nil)
	if err == nil {
		t.Fatal("expected a decode error")
	}
	msg := err.Error()
	if len(msg) > redact.SnippetBytes+64 {
		t.Errorf("decode error length %d is not bounded", len(msg))
	}
	for _, r := range msg {
		if r < 0x20 || r > 0x7e {
			t.Errorf("decode error carries a non-printable rune %q", r)
			break
		}
	}
}

func TestScalarOfRejectsAggregateTypes(t *testing.T) {
	if _, ok := scalarOf(&dto.Metric{}, dto.MetricType_HISTOGRAM); ok {
		t.Error("a histogram metric has no scalar value")
	}
}
