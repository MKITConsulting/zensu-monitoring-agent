package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
	"github.com/MKITConsulting/zensu-monitoring-agent/internal/scrape"
)

type expositionClock struct{ t time.Time }

// expositionServer serves a body that can be swapped between scrapes, so a
// counter can advance across ticks.
type expositionServer struct {
	*httptest.Server

	mu   sync.RWMutex
	body string
}

func (c *expositionClock) now() time.Time { return c.t }

// setBody swaps the served exposition between scrapes. The handler runs on the
// server's own goroutine, so the field needs a lock rather than relying on
// request completion for ordering.
func (e *expositionServer) setBody(body string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.body = body
}

func (e *expositionServer) currentBody() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.body
}

func newExpositionServer(t *testing.T, body string) *expositionServer {
	t.Helper()
	es := &expositionServer{body: body}
	es.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(es.currentBody()))
	}))
	t.Cleanup(es.Close)
	return es
}

func newTestExpositionSource(t *testing.T, url string, cfg ExpositionConfig) (*expositionSource, *expositionClock) {
	t.Helper()
	cfg.URL = url
	src := newExpositionSource(cfg, nil, nil)
	clock := &expositionClock{t: time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)}
	src.rater.Now = clock.now
	return src, clock
}

// samplesByKey collapses duplicates, so a test that cares about the count
// asserts it separately.
func samplesByKey(t *testing.T, got map[string][]MetricSample, slug string) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	for _, s := range got[slug] {
		out[s.Key] = s.Value
	}
	return out
}

func TestExpositionSourceScalesGaugeCoresToMillicores(t *testing.T) {
	body := `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
# TYPE k8s_pod_memory_working_set_bytes gauge
k8s_pod_memory_working_set_bytes{zensu_service="api"} 5.36870912e+08
`
	srv := newExpositionServer(t, body)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}

	byKey := samplesByKey(t, got, "api")
	if byKey[MetricCPUMillicores] != 250 {
		t.Errorf("%s = %v, want 250 (0.25 cores x 1000)", MetricCPUMillicores, byKey[MetricCPUMillicores])
	}
	if byKey[MetricMemoryBytes] != 536870912 {
		t.Errorf("%s = %v, want 536870912 verbatim", MetricMemoryBytes, byKey[MetricMemoryBytes])
	}
	if len(got["api"]) != 2 {
		t.Errorf("want exactly one sample per key, got %+v", got["api"])
	}
}

func TestExpositionSourceRatesCounterAcrossTicks(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{zensu_service="api"} 100
`)
	src, clock := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod"}}

	first, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	if len(first["api"]) != 0 {
		t.Errorf("first tick must yield no CPU sample, got %+v", first["api"])
	}

	srv.setBody(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{zensu_service="api"} 130
`)
	clock.t = clock.t.Add(60 * time.Second)

	second, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	byKey := samplesByKey(t, second, "api")
	if byKey[MetricCPUMillicores] != 500 {
		t.Errorf("%s = %v, want 500 (30s over 60s = 0.5 cores x 1000)", MetricCPUMillicores, byKey[MetricCPUMillicores])
	}
}

// TestExpositionSourceRefusesCounterMemory pins that a cumulative memory family
// is NOT differenced. Rating it would yield bytes per second, which would then
// ship under the memory_bytes key as though it were a byte count.
func TestExpositionSourceRefusesCounterMemory(t *testing.T) {
	cpuAndMemCounters := func(cpu, mem string) string {
		return `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{zensu_service="api"} ` + cpu + `
# TYPE container_memory_working_set_bytes counter
container_memory_working_set_bytes{zensu_service="api"} ` + mem + `
`
	}
	srv := newExpositionServer(t, cpuAndMemCounters("100", "1000"))
	src, clock := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod"}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	srv.setBody(cpuAndMemCounters("130", "1600"))
	clock.t = clock.t.Add(60 * time.Second)

	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("second Samples: %v", err)
	}

	byKey := samplesByKey(t, got, "api")
	if byKey[MetricCPUMillicores] != 500 {
		t.Errorf("cpu = %v, want 500 (30s over 60s = 0.5 cores)", byKey[MetricCPUMillicores])
	}
	if _, present := byKey[MetricMemoryBytes]; present {
		t.Errorf("a cumulative memory counter must not be rated into memory_bytes, got %v", byKey[MetricMemoryBytes])
	}
	if len(got["api"]) != 1 {
		t.Errorf("want exactly the CPU sample, got %+v", got["api"])
	}
}

// TestExpositionSourceWarnsOnceOnUnrateableCounter pins the diagnostic behind
// the refusal above: a memory role resolved to a cumulative counter reports
// nothing, and the log is the only place that says why. It is a standing
// misconfiguration, so it is latched rather than repeated every tick.
func TestExpositionSourceWarnsOnceOnUnrateableCounter(t *testing.T) {
	body := `# TYPE container_memory_working_set_bytes counter
container_memory_working_set_bytes{zensu_service="api"} 1000
`
	srv := newExpositionServer(t, body)
	src, buf := loggedSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod"}}

	for i := 0; i < 2; i++ {
		if _, err := src.Samples(context.Background(), targets); err != nil {
			t.Fatalf("Samples %d: %v", i, err)
		}
	}
	if got := strings.Count(buf.String(), "cumulative counter this role cannot rate"); got != 1 {
		t.Errorf("unrateable warnings = %d, want exactly 1; log:\n%s", got, buf.String())
	}
}

// TestExpositionSourceWarnsWhenRaterIsFull pins the ceiling diagnostic and its
// re-arming. Rates simply go missing for refused series, so without the log an
// operator has no way to tell a full tracking table from a quiet exposition.
func TestExpositionSourceWarnsWhenRaterIsFull(t *testing.T) {
	two := `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 100
container_cpu_usage_seconds_total{pod="api-2",namespace="prod"} 100
`
	srv := newExpositionServer(t, two)
	src, buf := loggedSource(t, srv.URL, ExpositionConfig{})
	src.rater.MaxTracked = 1
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1", "api-2"}}}

	for i := 0; i < 2; i++ {
		if _, err := src.Samples(context.Background(), targets); err != nil {
			t.Fatalf("Samples %d: %v", i, err)
		}
	}
	if got := strings.Count(buf.String(), "more counter series than the agent will track"); got != 1 {
		t.Errorf("ceiling warnings = %d, want exactly 1; log:\n%s", got, buf.String())
	}

	srv.setBody(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 200
`)
	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("third Samples: %v", err)
	}
	srv.setBody(two)
	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("fourth Samples: %v", err)
	}
	if got := strings.Count(buf.String(), "more counter series than the agent will track"); got != 2 {
		t.Errorf("ceiling warnings = %d, want 2 — the latch must re-arm once the exposition fits again; log:\n%s", got, buf.String())
	}
}

// TestRateCountersSkipsRollupRows pins that a row the row policy drops does not
// consume a Rater slot. reduce never looks its rate up, so tracking it only
// spends the ceiling and withholds roles for services that would otherwise rate.
// The ceiling is left above the row count on purpose, so the count itself is
// the discriminator: feeding the raw series would track three. A ceiling of two
// would read 2 either way, because the rater refuses the third rather than
// growing.
func TestRateCountersSkipsRollupRows(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod",container="app"} 100
container_cpu_usage_seconds_total{pod="api-1",namespace="prod",container="sidecar"} 50
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 150
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if got := src.rater.Tracked(); got != 2 {
		t.Errorf("tracked = %d, want 2 — only the two container rows are rateable", got)
	}
	if src.rater.RefusedSeries() {
		t.Error("nothing was refused at the default ceiling")
	}
}

// TestRateCountersRollupRowsDoNotSpendTheCeiling is the same rule seen from the
// consequence: at a ceiling the rollup rows would otherwise push real rows out
// and withhold a service that could have rated.
func TestRateCountersRollupRowsDoNotSpendTheCeiling(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod",container="app"} 100
container_cpu_usage_seconds_total{pod="api-1",namespace="prod",container="sidecar"} 50
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 150
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	src.rater.MaxTracked = 2
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if src.rater.RefusedSeries() {
		t.Error("the rollup row must not push the two real rows against the ceiling")
	}
}

// TestExpositionSourceExcludesThePauseContainer drives the pause rule through
// Samples rather than the resolver alone. Rounds 1 to 4 each introduced their
// defect at a wiring seam, not in a rule, so the rule is pinned on both sides.
func TestExpositionSourceExcludesThePauseContainer(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="POD"} 900
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="app"} 100
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}}}

	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if mem := samplesByKey(t, got, "api")[MetricMemoryBytes]; mem != 100 {
		t.Errorf("memory = %v, want 100 — the pause container must not be summed", mem)
	}
}

// TestRateCountersKeepsStateWhenNothingIsRateable pins that a tick without a
// rateable role does not empty the Rater. Observe rebuilds its table from what
// it is handed, so handing it nothing would discard every predecessor and cost
// the service a SECOND tick of CPU once the counter family reappears.
func TestRateCountersKeepsStateWhenNothingIsRateable(t *testing.T) {
	const withCPU = `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 100
# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod"} 500
`
	srv := newExpositionServer(t, withCPU)
	src, clock := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	if src.rater.Tracked() != 1 {
		t.Fatalf("tracked = %d after the first tick, want 1", src.rater.Tracked())
	}

	// Tick 2: the CPU family is gone, so no selection is rateable at all.
	srv.setBody(`# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod"} 500
`)
	clock.t = clock.t.Add(60 * time.Second)
	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	if got := src.rater.Tracked(); got != 1 {
		t.Fatalf("tracked = %d, want the predecessor kept across a tick with no rateable role", got)
	}

	// Tick 3: CPU is back and must rate immediately, not after another tick.
	srv.setBody(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 220
# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod"} 500
`)
	clock.t = clock.t.Add(60 * time.Second)
	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("third Samples: %v", err)
	}
	if cpu := samplesByKey(t, got, "api")[MetricCPUMillicores]; cpu != 1000 {
		t.Errorf("cpu = %v, want 1000 (120 counter seconds over 120s = 1 core)", cpu)
	}
}

// TestExpositionSourceAppliesConfiguredTimeout pins that the configured timeout
// reaches the HTTP client. Without it the client would silently run on the
// package default and a slow collector could stall a tick past its interval.
func TestExpositionSourceAppliesConfiguredTimeout(t *testing.T) {
	src, _ := newTestExpositionSource(t, "http://c:9090/metrics", ExpositionConfig{Timeout: 3 * time.Second})

	if got := src.client.HTTP.Timeout; got != 3*time.Second {
		t.Errorf("client timeout = %v, want 3s", got)
	}
}

// TestExpositionSourceRatesBothRolesInOnePass guards the Rater single-call
// invariant with two CPU-role series: Observe rebuilds its state from the series
// it is handed, so rating in two calls would evict the first call's predecessors.
func TestExpositionSourceRatesBothRolesInOnePass(t *testing.T) {
	body := func(a, b string) string {
		return `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} ` + a + `
container_cpu_usage_seconds_total{pod="api-2",namespace="prod"} ` + b + `
`
	}
	srv := newExpositionServer(t, body("100", "1000"))
	src, clock := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1", "api-2"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	srv.setBody(body("130", "1060"))
	clock.t = clock.t.Add(60 * time.Second)

	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 1500 {
		t.Errorf("cpu = %v, want 1500 (0.5 + 1.0 cores); both series must rate in one pass", v)
	}
}

// TestExpositionSourceWithholdsPartiallyRatedCounter pins that a service whose
// pods did not all produce a rate reports nothing rather than a fraction of its
// real usage — a partial sum looks like a real measurement.
func TestExpositionSourceWithholdsPartiallyRatedCounter(t *testing.T) {
	first := `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 100
`
	srv := newExpositionServer(t, first)
	src, clock := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1", "api-2"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}

	srv.setBody(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 130
container_cpu_usage_seconds_total{pod="api-2",namespace="prod"} 900
`)
	clock.t = clock.t.Add(60 * time.Second)

	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	if len(got["api"]) != 0 {
		t.Errorf("api-2 has no predecessor, so the service must report no CPU, got %+v", got["api"])
	}
}

// TestExpositionSourceSkipsCadvisorRollupRow pins that the pod-level rollup row
// kubelet emits alongside the per-container rows is not counted twice. The
// rollup carries a value DIFFERENT from the container sum on purpose, so the
// inverse rule — keep the rollup, drop the containers — fails this test.
func TestExpositionSourceSkipsCadvisorRollupRow(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="app"} 300
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="sidecar"} 200
container_memory_working_set_bytes{pod="api-1",namespace="prod"} 900
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
	})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricMemoryBytes]; v != 500 {
		t.Errorf("memory = %v, want 500 (300 + 200); the container-less rollup row must not be added", v)
	}
}

// TestExpositionSourceRollupFilterAppliesToOverrides pins that the rollup rule
// follows the DATA, not the candidate's origin. An operator who pastes a
// container-scoped family name into the override — the exact string the README
// prints — must not get every service counted twice.
func TestExpositionSourceRollupFilterAppliesToOverrides(t *testing.T) {
	body := `# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="app"} 300
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="sidecar"} 200
container_memory_working_set_bytes{pod="api-1",namespace="prod"} 900
`
	srv := newExpositionServer(t, body)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{
		MemoryMetric: "container_memory_working_set_bytes",
	})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
	})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricMemoryBytes]; v != 500 {
		t.Errorf("memory = %v, want 500; an overridden container-scoped family must still drop its rollup row", v)
	}
}

// TestExpositionSourceKeepsPodOnlyRowsAlongsideRollups pins that a pod whose
// exposition carries only a container-less row still contributes. Treating every
// container-less row as a rollup would silently delete that service.
func TestExpositionSourceKeepsPodOnlyRowsAlongsideRollups(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="app"} 300
container_memory_working_set_bytes{pod="api-1",namespace="prod"} 900
container_memory_working_set_bytes{pod="worker-1",namespace="prod"} 700
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
		{Slug: "worker", Namespace: "prod", PodNames: []string{"worker-1"}},
	})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricMemoryBytes]; v != 300 {
		t.Errorf("api memory = %v, want 300 (the container row only)", v)
	}
	if v := samplesByKey(t, got, "worker")[MetricMemoryBytes]; v != 700 {
		t.Errorf("worker memory = %v, want 700; a pod with no container rows must still report", v)
	}
}

// TestExpositionSourceDropsSampleThatOverflowsScaling pins the post-scale
// finiteness re-check. The value is finite on the wire and only becomes
// infinite after the cores-to-millicores multiply, so the decode-boundary guard
// cannot catch it.
func TestExpositionSourceDropsSampleThatOverflowsScaling(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 1e308
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if len(got["api"]) != 0 {
		t.Fatalf("a value that overflows scaling must be dropped, got %+v", got["api"])
	}

	batch := HeartbeatBatch{ProductID: "p", Services: []ServiceHeartbeat{{
		Slug: "api", Status: StatusUp, Metrics: got["api"],
	}}}
	if _, err := json.Marshal(batch); err != nil {
		t.Errorf("the resulting batch must marshal: %v", err)
	}
}

// TestExpositionSourceWithholdsPerService pins that withholding a partially
// rated counter is scoped to the affected service and does not silence the rest.
func TestExpositionSourceWithholdsPerService(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 100
container_cpu_usage_seconds_total{pod="worker-1",namespace="prod"} 100
`)
	src, clock := newTestExpositionSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1", "api-2"}},
		{Slug: "worker", Namespace: "prod", PodNames: []string{"worker-1"}},
	}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	srv.setBody(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 130
container_cpu_usage_seconds_total{pod="api-2",namespace="prod"} 900
container_cpu_usage_seconds_total{pod="worker-1",namespace="prod"} 160
`)
	clock.t = clock.t.Add(60 * time.Second)

	got, err := src.Samples(context.Background(), targets)
	if err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	if len(got["api"]) != 0 {
		t.Errorf("api gained a pod with no predecessor and must report nothing, got %+v", got["api"])
	}
	if v := samplesByKey(t, got, "worker")[MetricCPUMillicores]; v != 1000 {
		t.Errorf("worker cpu = %v, want 1000; one service's gap must not silence another", v)
	}
}

// TestExpositionSourceHonoursConfiguredMaxBytes pins that the operator cap
// reaches the scrape client.
func TestExpositionSourceHonoursConfiguredMaxBytes(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{MaxBytes: 16})

	_, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api"}})
	if !errors.Is(err, scrape.ErrBodyTooLarge) {
		t.Fatalf("err = %v, want ErrBodyTooLarge", err)
	}
}

// TestExpositionSourceAppliesMetricOverrideEndToEnd pins the whole override
// chain: config to Resolver to the keep-filter to exact matching.
func TestExpositionSourceAppliesMetricOverrideEndToEnd(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE my_cpu gauge
my_cpu{zensu_service="api"} 0.4
# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 9
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{CPUMetric: "my_cpu"})

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 400 {
		t.Errorf("cpu = %v, want 400 from the override; the built-in candidate must not win", v)
	}
}

// TestExpositionSourceDropsNonFiniteSample pins the graceful-degrade guarantee:
// a NaN from the peer must cost the metric, never the heartbeat. json.Marshal
// rejects non-finite floats, so a NaN reaching a MetricSample would fail the
// whole batch.
func TestExpositionSourceDropsNonFiniteSample(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} NaN
# TYPE k8s_pod_memory_working_set_bytes gauge
k8s_pod_memory_working_set_bytes{zensu_service="api"} +Inf
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod"}})
	if err != nil {
		t.Fatalf("a non-finite sample must not error the scrape: %v", err)
	}
	if len(got["api"]) != 0 {
		t.Fatalf("want no samples, got %+v", got["api"])
	}

	batch := HeartbeatBatch{ProductID: "p", Services: []ServiceHeartbeat{{
		Slug: "api", Status: StatusUp, Metrics: got["api"],
	}}}
	if _, err := json.Marshal(batch); err != nil {
		t.Errorf("the resulting batch must marshal: %v", err)
	}
}

func TestExpositionSourceResolvesSlugFromPodLabels(t *testing.T) {
	cases := []struct {
		name   string
		labels string
	}{
		{"otel spelling", `k8s_pod_name="api-1",k8s_namespace_name="prod"`},
		{"cadvisor spelling", `pod="api-1",namespace="prod"`},
		{"pod only, unambiguous", `pod="api-1"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{`+c.labels+`} 0.5
`)
			src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

			got, err := src.Samples(context.Background(), []ServiceTarget{
				{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
			})
			if err != nil {
				t.Fatalf("Samples: %v", err)
			}
			if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 500 {
				t.Errorf("cpu = %v, want 500", v)
			}
		})
	}
}

func TestExpositionSourceSumsAcrossPodsOfOneService(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{pod="api-1",namespace="prod"} 0.25
k8s_pod_cpu_usage{pod="api-2",namespace="prod"} 0.30
k8s_pod_cpu_usage{pod="other-1",namespace="prod"} 9
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1", "api-2"}},
	})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 550 {
		t.Errorf("cpu = %v, want 550 (0.25 + 0.30 cores); the untracked pod must not contribute", v)
	}
	if len(got["api"]) != 1 {
		t.Errorf("want exactly one CPU sample per service, got %+v", got["api"])
	}
}

func TestExpositionSourceDropsUnattributableSamples(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="not-tracked"} 1
k8s_pod_cpu_usage{pod="unknown-pod",namespace="prod"} 2
k8s_pod_cpu_usage{} 3
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
	})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("no sample maps to a tracked service, want an empty result, got %+v", got)
	}
}

func TestExpositionSourceSlugLabelWinsOverPodMatching(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api",pod="worker-1",namespace="prod"} 0.4
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
		{Slug: "worker", Namespace: "prod", PodNames: []string{"worker-1"}},
	})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 400 {
		t.Errorf("api cpu = %v, want 400 from the explicit slug label", v)
	}
	if len(got["worker"]) != 0 {
		t.Errorf("pod matching must not also attribute the sample to worker: %+v", got["worker"])
	}
}

func TestExpositionSourceCustomSlugLabel(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{my_service="api"} 0.1
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{SlugLabel: "my_service"})

	got, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod"}})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 100 {
		t.Errorf("cpu = %v, want 100 via the custom slug label", v)
	}
}

func TestExpositionSourceAmbiguousPodNameNeedsNamespace(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{pod="api-1"} 0.5
k8s_pod_cpu_usage{pod="api-1",namespace="staging"} 0.7
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api-prod", Namespace: "prod", PodNames: []string{"api-1"}},
		{Slug: "api-staging", Namespace: "staging", PodNames: []string{"api-1"}},
	})
	if err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if len(got["api-prod"]) != 0 {
		t.Errorf("an ambiguous bare pod name must not be attributed, got %+v", got["api-prod"])
	}
	if v := samplesByKey(t, got, "api-staging")[MetricCPUMillicores]; v != 700 {
		t.Errorf("api-staging cpu = %v, want 700 — the namespaced row is unambiguous", v)
	}
}

func TestExpositionSourceUnknownMetricsYieldNothing(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE something_else gauge
something_else{pod="api-1"} 1
`)
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
	})
	if err != nil {
		t.Fatalf("an exposition without known metrics is not an error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("want no samples, got %+v", got)
	}
}

func TestExpositionSourceReturnsScrapeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	src, _ := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	if _, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api"}}); err == nil {
		t.Fatal("a failing scrape must surface as an error so the caller can log it")
	}
}

func TestExpositionSourceRecordsScrapeMetrics(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
`)
	m := obs.NewWithRegistry(prometheus.NewRegistry())
	src := newExpositionSource(ExpositionConfig{URL: srv.URL}, nil, m)

	if _, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api"}}); err != nil {
		t.Fatalf("Samples: %v", err)
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `zensu_monitoring_agent_scrape_total{result="success"} 1`) {
		t.Errorf("missing scrape success counter; body:\n%s", body)
	}
	if !strings.Contains(body, "zensu_monitoring_agent_scrape_duration_seconds_count 1") {
		t.Errorf("missing scrape duration observation; body:\n%s", body)
	}
}

func TestExpositionSourceNilMetricsDoesNotPanic(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 0.25
`)
	src := newExpositionSource(ExpositionConfig{URL: srv.URL}, nil, nil)

	if _, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api"}}); err != nil {
		t.Fatalf("Samples with nil metrics: %v", err)
	}
}

// TestExpositionSourceRecordsScrapeError is the error counterpart of
// TestExpositionSourceRecordsScrapeMetrics, matching the reporter's own
// success/error/transport symmetry.
func TestExpositionSourceRecordsScrapeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	m := obs.NewWithRegistry(prometheus.NewRegistry())
	src := newExpositionSource(ExpositionConfig{URL: srv.URL}, nil, m)

	if _, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api"}}); err == nil {
		t.Fatal("expected a scrape error")
	}

	body := scrapeMetrics(t, m)
	if !strings.Contains(body, `zensu_monitoring_agent_scrape_total{result="error"} 1`) {
		t.Errorf("missing scrape error counter; body:\n%s", body)
	}
	if !strings.Contains(body, `zensu_monitoring_agent_scrape_total{result="success"} 0`) {
		t.Errorf("success counter should remain 0; body:\n%s", body)
	}
	if !strings.Contains(body, "zensu_monitoring_agent_scrape_duration_seconds_count 1") {
		t.Errorf("a failed scrape should still be timed; body:\n%s", body)
	}
}

// debugSource builds an exposition source whose Debug records are captured too.
func debugSource(t *testing.T, url string, cfg ExpositionConfig) (*expositionSource, *strings.Builder) {
	t.Helper()
	cfg.URL = url
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return newExpositionSource(cfg, log, nil), &buf
}

// TestExpositionSourceReportsExcludedAndDroppedRows pins the two Debug lines
// that are the only signal a row went missing. Selection.Excluded has no other
// production consumer, so without this a service lost to the row policy is
// silent while the scrape still counts as a success.
func TestExpositionSourceReportsExcludedAndDroppedRows(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_memory_working_set_bytes gauge
container_memory_working_set_bytes{pod="api-1",namespace="prod",container="app"} 100
container_memory_working_set_bytes{pod="api-1",namespace="prod"} 999
container_memory_working_set_bytes{pod="ghost-1",namespace="prod"} 7
`)
	src, buf := debugSource(t, srv.URL, ExpositionConfig{})
	src.rater.Now = (&expositionClock{t: time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)}).now
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("Samples: %v", err)
	}
	logged := buf.String()
	if !strings.Contains(logged, "excluded exposition rows as pod-level rollups or pause containers") {
		t.Errorf("the excluded rows must be reported; log:\n%s", logged)
	}
	if !strings.Contains(logged, "dropped exposition rows that map to no tracked service") {
		t.Errorf("the unattributable row must be reported; log:\n%s", logged)
	}
	if !strings.Contains(logged, "rows=1") {
		t.Errorf("both diagnostics must carry their row count; log:\n%s", logged)
	}
}

// loggedSource builds an exposition source whose warnings are captured.
func loggedSource(t *testing.T, url string, cfg ExpositionConfig) (*expositionSource, *strings.Builder) {
	t.Helper()
	cfg.URL = url
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return newExpositionSource(cfg, log, nil), &buf
}

// TestExpositionSourceWarnsOnOverrideMismatch pins the diagnostic for the most
// likely operator misconfiguration: an override naming a family the exposition
// spells differently. Overrides match verbatim while built-in candidates
// tolerate unit suffixes, so this is easy to hit and otherwise silent.
func TestExpositionSourceWarnsOnOverrideMismatch(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total gauge
container_cpu_usage_seconds_total{zensu_service="api"} 1
`)
	src, buf := loggedSource(t, srv.URL, ExpositionConfig{CPUMetric: "container_cpu_usage"})

	if _, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api"}}); err != nil {
		t.Fatalf("Samples: %v", err)
	}
	if !strings.Contains(buf.String(), "override matched nothing") {
		t.Errorf("an override that resolves to nothing must be reported; log:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "container_cpu_usage") {
		t.Errorf("the warning must name the override; log:\n%s", buf.String())
	}
}

// TestExpositionSourceLatchesAndRearmsWarning pins both halves of the latch
// contract: a standing misconfiguration costs one line, and a pipeline that is
// repaired and later breaks again is reported afresh.
func TestExpositionSourceLatchesAndRearmsWarning(t *testing.T) {
	unmatched := `# TYPE something_else gauge
something_else{zensu_service="api"} 1
`
	matched := `# TYPE k8s_pod_cpu_usage gauge
k8s_pod_cpu_usage{zensu_service="api"} 1
`
	srv := newExpositionServer(t, unmatched)
	src, buf := loggedSource(t, srv.URL, ExpositionConfig{})
	targets := []ServiceTarget{{Slug: "api"}}
	const msg = "exposition carries none of the known CPU/memory metrics"

	for i := 0; i < 2; i++ {
		if _, err := src.Samples(context.Background(), targets); err != nil {
			t.Fatalf("unmatched Samples: %v", err)
		}
	}
	if n := strings.Count(buf.String(), msg); n != 1 {
		t.Fatalf("two unmatched ticks logged %d times, want 1; log:\n%s", n, buf.String())
	}

	srv.setBody(matched)
	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("matched Samples: %v", err)
	}

	srv.setBody(unmatched)
	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("re-broken Samples: %v", err)
	}
	if n := strings.Count(buf.String(), msg); n != 2 {
		t.Errorf("a repaired-then-rebroken pipeline logged %d times, want 2; log:\n%s", n, buf.String())
	}
}

// TestExpositionSourceWarnsOnWithheldService pins that withholding a partially
// rated service is not silent — the mapped gauge cannot show it, so the log is
// the only signal.
func TestExpositionSourceWarnsOnWithheldService(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 100
`)
	src, buf := loggedSource(t, srv.URL, ExpositionConfig{})
	clock := &expositionClock{t: time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)}
	src.rater.Now = clock.now
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1", "api-2"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	srv.setBody(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 130
container_cpu_usage_seconds_total{pod="api-2",namespace="prod"} 900
`)
	clock.t = clock.t.Add(60 * time.Second)

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	if got := strings.Count(buf.String(), "withholding a role"); got != 1 {
		t.Errorf("withheld count = %d, want exactly 1; log:\n%s", got, buf.String())
	}
}

// TestExpositionSourceSuppressesWithheldWarningOnFirstTick pins the other half
// of the rule: every counter series is incomplete on the tick that first
// observes it, so warning there would spend the latch on a startup artifact and
// hide the real case for the rest of the process's life.
func TestExpositionSourceSuppressesWithheldWarningOnFirstTick(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 100
`)
	src, buf := loggedSource(t, srv.URL, ExpositionConfig{})
	src.rater.Now = (&expositionClock{t: time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)}).now
	targets := []ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1", "api-2"}}}

	if _, err := src.Samples(context.Background(), targets); err != nil {
		t.Fatalf("first Samples: %v", err)
	}
	if strings.Contains(buf.String(), "withholding a role") {
		t.Errorf("the startup tick must not warn; log:\n%s", buf.String())
	}
}

// TestRateCountersKeepsUnattributedRows pins that attribution is NOT a filter on
// rater state. Dropping rows that momentarily attribute to nothing would cost a
// service two ticks of CPU instead of one when its pod listing blips.
func TestRateCountersKeepsUnattributedRows(t *testing.T) {
	srv := newExpositionServer(t, `# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 100
`)
	src, clock := newTestExpositionSource(t, srv.URL, ExpositionConfig{})

	// Tick 1: the pod list is empty, so nothing attributes.
	if _, err := src.Samples(context.Background(), []ServiceTarget{{Slug: "api", Namespace: "prod"}}); err != nil {
		t.Fatalf("first Samples: %v", err)
	}

	srv.setBody(`# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{pod="api-1",namespace="prod"} 130
`)
	clock.t = clock.t.Add(60 * time.Second)

	// Tick 2: the pod list recovers. The predecessor from tick 1 must still be
	// there, so CPU reports immediately rather than after another tick.
	got, err := src.Samples(context.Background(), []ServiceTarget{
		{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}},
	})
	if err != nil {
		t.Fatalf("second Samples: %v", err)
	}
	if v := samplesByKey(t, got, "api")[MetricCPUMillicores]; v != 500 {
		t.Errorf("cpu = %v, want 500; a tick of lost attribution must not cost a second tick", v)
	}
}

func TestSlugIndexResolvePrefersExplicitLabel(t *testing.T) {
	idx := newSlugIndex([]ServiceTarget{{Slug: "api", Namespace: "prod", PodNames: []string{"api-1"}}})

	cases := []struct {
		name     string
		labels   map[string]string
		wantSlug string
		wantOK   bool
	}{
		{"known slug label", map[string]string{DefaultSlugLabel: "api"}, "api", true},
		{"unknown slug label is not retried as a pod", map[string]string{DefaultSlugLabel: "ghost", "pod": "api-1"}, "", false},
		{"namespaced pod", map[string]string{"pod": "api-1", "namespace": "prod"}, "api", true},
		{"wrong namespace", map[string]string{"pod": "api-1", "namespace": "dev"}, "", false},
		{"no usable labels", map[string]string{"container": "app"}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			slug, ok := idx.resolve(scrape.Series{Name: "m", Labels: c.labels}, DefaultSlugLabel)
			if ok != c.wantOK || slug != c.wantSlug {
				t.Errorf("resolve = (%q, %v), want (%q, %v)", slug, ok, c.wantSlug, c.wantOK)
			}
		})
	}
}
