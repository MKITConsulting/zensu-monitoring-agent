// Package scrape reads a Prometheus exposition endpoint and decodes it into
// typed series, and resolves which of those series supply the resource metrics
// Zensu records. It has no Kubernetes imports, but it does carry domain
// knowledge — the Zensu metric roles, the kubeletstats and cAdvisor series
// names, and the unit factors — so it is exposition-side, not domain-free.
// Attribution to a service stays in internal/agent, which is the only part that
// needs cluster state.
//
// This is the read side of the collector-exposition resource-metric source and
// must not be confused with internal/metrics, which serves the agent's OWN
// /metrics endpoint.
package scrape

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MKITConsulting/zensu-monitoring-agent/internal/redact"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// DefaultMaxBytes caps how much of an exposition body is read. A collector
// scraping a large cluster can expose tens of megabytes; the agent only needs a
// handful of series and must not be pushed out of its container memory limit by
// an endpoint it does not control.
const DefaultMaxBytes int64 = 8 << 20

// MaxConfigurableBytes is a sanity bound on operator input, not a container-safety
// guarantee. It catches a typo, and math.MaxInt64, which would overflow the read
// limit into a silent empty success; such a value falls back to DefaultMaxBytes.
// It does NOT make every value below it safe to run: see MaxSamples for the peak
// model. Raising the cap is only safe together with the container memory limit and
// the runtime's soft ceiling.
const MaxConfigurableBytes int64 = 16 << 20

// MaxSamples caps how many Series one scrape may materialize, because a row
// costs far more as a Series plus its own label map than as ~40 bytes on the
// wire.
//
// It is NOT a peak-heap bound, and must not be read as one. expfmt hands back a
// whole decoded family at a time, so that family's protobuf values exist before
// this budget can refuse them; the budget bounds what the agent RETAINS, not
// what the parser transiently builds. Peak is governed by the body cap times the
// parser's expansion factor, and the deployment's GOMEMLIMIT gives the runtime a
// soft ceiling below the container limit so it collects under pressure instead of
// being OOM-killed.
const MaxSamples = 50_000

// ErrTooManySamples reports that an exposition exceeded MaxSamples. The scrape is
// abandoned rather than truncated, because a partial series set would silently
// under-report a service.
var ErrTooManySamples = errors.New("scrape: exposition exceeds the maximum number of series")

// DefaultTimeout is the per-scrape HTTP timeout applied when none is supplied.
const DefaultTimeout = 10 * time.Second

// Kind classifies a series by the exposition's own TYPE metadata rather than by
// configuration, so a pipeline that switches a metric from gauge to counter is
// followed automatically.
type Kind int

const (
	// KindUnsupported marks a family the agent cannot reduce to one scalar
	// (summary, histogram). Such families are dropped during decode.
	KindUnsupported Kind = iota
	// KindGauge is an instantaneous value. Prometheus UNTYPED families decode
	// as gauges: a bare value carries no accumulation semantics.
	KindGauge
	// KindCounter is a monotonic cumulative value that callers must rate.
	KindCounter
)

// String renders the kind as it appears in a Prometheus TYPE line.
func (k Kind) String() string {
	switch k {
	case KindGauge:
		return "gauge"
	case KindCounter:
		return "counter"
	default:
		return "unsupported"
	}
}

// ErrBodyTooLarge reports that the exposition exceeded the configured cap. The
// body is not parsed in that case, because a truncated exposition decodes into
// silently incomplete series.
var ErrBodyTooLarge = errors.New("scrape: exposition body exceeds the configured size cap")

// Series is one decoded sample from an exposition.
type Series struct {
	Name   string
	Labels map[string]string
	Value  float64
	Kind   Kind
}

// Fingerprint is a stable identity for a series across scrapes, used to line up
// consecutive observations of the same counter.
//
// Label names and values come from the scraped peer, so every part is written
// length-prefixed rather than delimiter-separated. A separator the peer could
// also place inside a value would make the encoding non-injective, and two
// distinct series sharing a key would difference against each other's previous
// value.
func (s Series) Fingerprint() string {
	keys := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	writeLenPrefixed(&b, s.Name)
	for _, k := range keys {
		writeLenPrefixed(&b, k)
		writeLenPrefixed(&b, s.Labels[k])
	}
	return b.String()
}

func writeLenPrefixed(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}

// Client fetches and decodes a Prometheus exposition over plain HTTP.
type Client struct {
	URL      string
	HTTP     *http.Client
	MaxBytes int64
}

// noRedirect refuses to follow a redirect, surfacing it as a normal response
// instead. The scrape URL is operator-configured and the README promises it is
// the only additional outbound destination; following a peer's 302 would let
// the peer choose where the agent connects next, including link-local metadata
// addresses.
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// NewClient builds a Client for url with the given per-request timeout. A
// non-positive timeout falls back to DefaultTimeout. The timeout covers the
// whole request, not just the connection, so a slow-drip body cannot pin a tick.
func NewClient(url string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Client{
		URL:      url,
		HTTP:     &http.Client{Timeout: timeout, CheckRedirect: noRedirect},
		MaxBytes: DefaultMaxBytes,
	}
}

// Fetch scrapes the endpoint once and returns every series it could reduce to a
// finite scalar. Summary and histogram families are skipped rather than
// erroring, so one unsupported family in a large exposition does not cost the
// whole scrape. When keep is non-empty only families whose normalized name is in
// it are materialized, which keeps an exposition the agent mostly discards from
// allocating a label map per sample.
//
// The decoder is pinned to the format the agent asked for rather than taken from
// the response Content-Type: letting the peer choose would let it steer the agent
// into a parser it never requested.
func (c *Client) Fetch(ctx context.Context, keep map[string]bool) ([]Series, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("scrape: build request: %s", redact.ForLog(redact.TransportReason(err)))
	}
	requested := expfmt.NewFormat(expfmt.TypeTextPlain)
	req.Header.Set("Accept", string(requested))

	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: DefaultTimeout, CheckRedirect: noRedirect}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scrape: request failed: %s", redact.ForLog(redact.TransportReason(err)))
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, redact.SnippetBytes))
		return nil, fmt.Errorf("scrape: endpoint returned %d %s: %s",
			resp.StatusCode, redact.Sanitize(redact.Truncate(resp.Status, redact.StatusPhraseBytes)), redact.Sanitize(string(snippet)))
	}

	limit := c.readLimit()
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("scrape: read body: %s", redact.ForLog(err.Error()))
	}
	if int64(len(body)) > limit {
		return nil, ErrBodyTooLarge
	}

	return decode(bytes.NewReader(body), requested, keep)
}

// readLimit resolves the effective body cap. A non-positive value means unset,
// and a value above MaxConfigurableBytes is refused rather than honoured — which
// also keeps limit+1 in Fetch from overflowing at math.MaxInt64, where a negative
// limit would make io.LimitReader return an empty body and a silent success.
func (c *Client) readLimit() int64 {
	if c.MaxBytes <= 0 || c.MaxBytes > MaxConfigurableBytes {
		return DefaultMaxBytes
	}
	return c.MaxBytes
}

// decode reads every family the keep set admits, bounded by MaxSamples. A parse
// error carries the parser's message, which quotes peer-controlled input, so it
// is bounded and stripped to printable ASCII before it can reach a log record.
func decode(r io.Reader, format expfmt.Format, keep map[string]bool) ([]Series, error) {
	dec := expfmt.NewDecoder(r, format)
	var out []Series
	for {
		var mf dto.MetricFamily
		switch err := dec.Decode(&mf); {
		case errors.Is(err, io.EOF):
			return out, nil
		case err != nil:
			return nil, fmt.Errorf("scrape: decode exposition: %s", redact.ForLog(err.Error()))
		}
		if len(keep) > 0 && !keep[NormalizeName(mf.GetName())] {
			continue
		}
		rows, ok := seriesFromFamily(&mf, MaxSamples-len(out))
		if !ok {
			return nil, ErrTooManySamples
		}
		out = append(out, rows...)
	}
}

// seriesFromFamily converts one family, refusing to materialize more than budget
// rows. The budget is checked per ROW, not per family: a peer can put its whole
// body into a single family the resolver accepts, so a between-families check
// would let the peak allocation happen before it fired. ok is false when the
// family would exceed the budget; nothing partial is returned, because a
// truncated series set would silently under-report a service.
//
// The output slice is sized against the budget rather than against the family's
// own length, so a peer-supplied metric count cannot drive one large allocation
// on its own.
func seriesFromFamily(mf *dto.MetricFamily, budget int) ([]Series, bool) {
	kind := kindOf(mf.GetType())
	if kind == KindUnsupported {
		return nil, true
	}
	size := len(mf.GetMetric())
	if size > budget {
		size = budget
	}
	out := make([]Series, 0, size)

	for _, m := range mf.GetMetric() {
		value, ok := scalarOf(m, mf.GetType())
		if !ok {
			continue
		}
		if len(out) >= budget {
			return nil, false
		}
		labels := make(map[string]string, len(m.GetLabel()))
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		out = append(out, Series{
			Name:   mf.GetName(),
			Labels: labels,
			Value:  value,
			Kind:   kind,
		})
	}
	return out, true
}

func kindOf(t dto.MetricType) Kind {
	switch t {
	case dto.MetricType_GAUGE, dto.MetricType_UNTYPED:
		return KindGauge
	case dto.MetricType_COUNTER:
		return KindCounter
	default:
		return KindUnsupported
	}
}

// scalarOf extracts the single value of a metric, rejecting NaN and infinities.
// Both are legal in the Prometheus text format and neither is representable in
// JSON, so a non-finite value that reached a MetricSample would make
// json.Marshal fail and take the whole heartbeat batch — status and restart
// counts included — down with it. Dropping the sample here keeps the tick's
// graceful-degrade promise.
func scalarOf(m *dto.Metric, t dto.MetricType) (float64, bool) {
	var v float64
	switch t {
	case dto.MetricType_GAUGE:
		if m.Gauge == nil {
			return 0, false
		}
		v = m.GetGauge().GetValue()
	case dto.MetricType_UNTYPED:
		if m.Untyped == nil {
			return 0, false
		}
		v = m.GetUntyped().GetValue()
	case dto.MetricType_COUNTER:
		if m.Counter == nil {
			return 0, false
		}
		v = m.GetCounter().GetValue()
	default:
		return 0, false
	}
	if !IsFinite(v) {
		return 0, false
	}
	return v, true
}

// IsFinite reports whether v can be represented in JSON. Callers that derive new
// values from a scalar — a rate, a sum, a unit conversion — must re-check, since
// arithmetic on finite inputs can still overflow to an infinity.
func IsFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
