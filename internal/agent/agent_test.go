package agent

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func i32(v int32) *int32 { return &v }

func deployment(ns, name, svcSlug string, ready, desired int32) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: i32(desired),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: ready},
	}
	if svcSlug != "" {
		d.ObjectMeta.Annotations = map[string]string{AnnotationService: svcSlug}
	}
	return d
}

func pod(ns, name string, lbls map[string]string, restarts ...int32) *corev1.Pod {
	var cs []corev1.ContainerStatus
	for _, r := range restarts {
		cs = append(cs, corev1.ContainerStatus{RestartCount: r})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: lbls},
		Status:     corev1.PodStatus{ContainerStatuses: cs},
	}
}

// container builds one ContainerMetrics with the given CPU (millicores) and

// memory (bytes), used to assemble fake PodMetrics in tests.

func container(name string, cpuMilli, memBytes int64) metricsv1beta1.ContainerMetrics {
	return metricsv1beta1.ContainerMetrics{
		Name: name,
		Usage: corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(memBytes, resource.BinarySI),
		},
	}
}

func podMetrics(ns, name string, containers ...metricsv1beta1.ContainerMetrics) metricsv1beta1.PodMetrics {
	return metricsv1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Containers: containers,
	}
}

// fakeReader is a controllable ClusterReader: deployments/pods come from a fake

// clientset, while PodMetricsForSelector returns scripted CPU/memory (or a

// scripted error such as ErrMetricsAPIUnavailable) so metrics paths can be

// exercised deterministically.

type fakeReader struct {
	inner       ClusterReader
	metricsCPU  int64
	metricsMem  int64
	available   bool
	metricsErr  error
	metricsHits int
}

func (f *fakeReader) ListDeployments(ctx context.Context, ns string) ([]appsv1.Deployment, error) {
	return f.inner.ListDeployments(ctx, ns)
}

func (f *fakeReader) ListPods(ctx context.Context, ns, sel string) ([]corev1.Pod, error) {
	return f.inner.ListPods(ctx, ns, sel)
}

func (f *fakeReader) PodMetricsForSelector(_ context.Context, _, _ string) (int64, int64, bool, error) {
	f.metricsHits++
	if f.metricsErr != nil {
		return 0, 0, false, f.metricsErr
	}
	return f.metricsCPU, f.metricsMem, f.available, nil
}

func TestCollect_DedupAcrossNamespaces(t *testing.T) {
	client := fake.NewSimpleClientset(
		deployment("ns1", "auth-api", "auth", 3, 3),
		deployment("ns1", "worker", "worker", 1, 2),
		deployment("ns1", "db", "", 1, 1),             // unannotated, ignored
		deployment("ns2", "auth-api-2", "auth", 2, 2), // duplicate slug, deduped
	)
	a := New(Config{
		ProductID:  "prod",
		Namespaces: []string{"ns1", "ns2"},
		Interval:   30 * time.Second,
	}, NewClientsetLister(client, nil), &stubReporter{}, nil, newMetricsServerSource(NewClientsetLister(client, nil), nil))

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 services, got %d: %+v", len(got), got)
	}
	bySlug := map[string]ServiceHeartbeat{}
	for _, s := range got {
		bySlug[s.Slug] = s
	}
	if bySlug["auth"].Status != StatusUp {
		t.Errorf("auth status = %s", bySlug["auth"].Status)
	}
	if bySlug["worker"].Status != StatusDegraded {
		t.Errorf("worker status = %s", bySlug["worker"].Status)
	}
	if bySlug["auth"].IntervalSeconds != 30 {
		t.Errorf("interval not propagated: %d", bySlug["auth"].IntervalSeconds)
	}
}

func TestCollect_SumsRestartCounts(t *testing.T) {
	client := fake.NewSimpleClientset(
		deployment("default", "api", "api", 2, 2),
		pod("default", "api-1", map[string]string{"app": "api"}, 3),
		pod("default", "api-2", map[string]string{"app": "api"}, 1, 2),
		pod("default", "stray", map[string]string{"app": "other"}, 9),
	)
	a := New(Config{
		ProductID:  "prod",
		Namespaces: []string{"default"},
	}, NewClientsetLister(client, nil), &stubReporter{}, nil, newMetricsServerSource(NewClientsetLister(client, nil), nil))

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 service, got %d: %+v", len(got), got)
	}
	if got[0].RestartCount == nil {
		t.Fatal("RestartCount should be set when the deployment has a selector")
	}
	if *got[0].RestartCount != 6 {
		t.Errorf("RestartCount = %d, want 6 (3 + 1 + 2; stray pod with app=other excluded)", *got[0].RestartCount)
	}
}

// TestSumPodMetrics covers the raw summation across multiple containers and

// multiple pods, independent of the agent wiring.

// TestCollect_AttachesMetrics verifies CPU/memory are summed across multiple

// containers and multiple pods, then attached as exactly two MetricSamples with

// the correct keys and values.

func TestCollect_AttachesMetrics(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 2, 2))
	reader := &fakeReader{
		inner:      NewClientsetLister(client, nil),
		metricsCPU: 400,         // e.g. 100 + 50 + 250 across pods/containers
		metricsMem: 530_000_000, // e.g. 200M + 30M + 300M
		available:  true,
	}
	a := New(Config{ProductID: "prod", Namespaces: []string{"default"}}, reader, &stubReporter{}, nil, newMetricsServerSource(reader, nil))

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 service, got %d: %+v", len(got), got)
	}
	if reader.metricsHits != 1 {
		t.Errorf("PodMetricsForSelector called %d times, want 1", reader.metricsHits)
	}
	ms := got[0].Metrics
	if len(ms) != 2 {
		t.Fatalf("want exactly 2 metric samples, got %d: %+v", len(ms), ms)
	}
	byKey := map[string]float64{}
	for _, m := range ms {
		byKey[m.Key] = m.Value
	}
	if v, ok := byKey[MetricCPUMillicores]; !ok || v != 400 {
		t.Errorf("%s = %v (present=%v), want 400", MetricCPUMillicores, v, ok)
	}
	if v, ok := byKey[MetricMemoryBytes]; !ok || v != 530_000_000 {
		t.Errorf("%s = %v (present=%v), want 530000000", MetricMemoryBytes, v, ok)
	}
}

// TestCollect_GracefulDegradeNoMetricsServer verifies that when metrics-server

// is absent (sentinel error), the heartbeat is still produced with no Metrics

// and Collect returns no error — the tick must not fail.

func TestCollect_GracefulDegradeNoMetricsServer(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 2, 2))
	reader := &fakeReader{
		inner:      NewClientsetLister(client, nil),
		metricsErr: ErrMetricsAPIUnavailable,
	}
	a := New(Config{ProductID: "prod", Namespaces: []string{"default"}}, reader, &stubReporter{}, nil, newMetricsServerSource(reader, nil))

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect must not error on missing metrics-server: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 service, got %d: %+v", len(got), got)
	}
	if len(got[0].Metrics) != 0 {
		t.Errorf("Metrics must be empty when metrics-server is unavailable, got %+v", got[0].Metrics)
	}
	// Status/restart path still works.
	if got[0].Status != StatusUp {
		t.Errorf("status = %s, want up", got[0].Status)
	}
}

// TestTick_GracefulDegradeNoMetricsServer is the Tick-level counterpart: a

// heartbeat is still sent (no error) when metrics-server is missing.

func TestTick_GracefulDegradeNoMetricsServer(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 2, 2))
	reader := &fakeReader{
		inner:      NewClientsetLister(client, nil),
		metricsErr: ErrMetricsAPIUnavailable,
	}
	rep := &stubReporter{}
	a := New(Config{ProductID: "prod", Source: "test", Namespaces: []string{"default"}}, reader, rep, nil, newMetricsServerSource(reader, nil))

	if err := a.Tick(context.Background()); err != nil {
		t.Fatalf("Tick must not fail when metrics-server is missing: %v", err)
	}
	if rep.calls != 1 {
		t.Fatalf("heartbeat should still be sent once, got %d calls", rep.calls)
	}
	if len(rep.last.Services) != 1 || len(rep.last.Services[0].Metrics) != 0 {
		t.Errorf("sent batch should carry the service without metrics: %+v", rep.last.Services)
	}
}

// TestCollect_GracefulDegradeTransientError verifies a transient (non-sentinel)

// metrics error does not crash the tick and yields no metrics for that tick.

func TestCollect_GracefulDegradeTransientError(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 2, 2))
	reader := &fakeReader{
		inner:      NewClientsetLister(client, nil),
		metricsErr: errors.New("metrics-server temporarily unavailable"),
	}
	a := New(Config{ProductID: "prod", Namespaces: []string{"default"}}, reader, &stubReporter{}, nil, newMetricsServerSource(reader, nil))

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect must not error on transient metrics error: %v", err)
	}
	if len(got) != 1 || len(got[0].Metrics) != 0 {
		t.Errorf("transient metrics error should yield no metrics: %+v", got)
	}
}

// TestMetricsJSONMarshal verifies a batch carrying metrics marshals to the

// agreed contract shape: metrics is an array of {key,value} objects (camelCase).

func TestMetricsJSONMarshal(t *testing.T) {
	batch := HeartbeatBatch{
		ProductID: "p",
		Services: []ServiceHeartbeat{{
			Slug:   "api",
			Status: StatusUp,
			Metrics: []MetricSample{
				{Key: MetricCPUMillicores, Value: 1234},
				{Key: MetricMemoryBytes, Value: 530000000},
			},
		}},
	}
	b, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		`"metrics":[`,
		`"key":"cpu_millicores"`,
		`"value":1234`,
		`"key":"memory_bytes"`,
		`"value":530000000`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("marshalled batch missing %q\n got: %s", want, s)
		}
	}

	// Round-trip back to confirm exact values survive.
	var rt HeartbeatBatch
	if err := json.Unmarshal(b, &rt); err != nil {
		t.Fatal(err)
	}
	rms := rt.Services[0].Metrics
	if len(rms) != 2 || rms[0].Key != MetricCPUMillicores || rms[0].Value != 1234 ||
		rms[1].Key != MetricMemoryBytes || rms[1].Value != 530000000 {
		t.Errorf("round-tripped metrics wrong: %+v", rms)
	}
}

// TestServiceHeartbeat_OmitsEmptyMetrics confirms the metrics field is omitted

// entirely (omitempty) when no samples are present — so degraded heartbeats

// don't carry an empty "metrics" key.

func TestServiceHeartbeat_OmitsEmptyMetrics(t *testing.T) {
	b, err := json.Marshal(ServiceHeartbeat{Slug: "api", Status: StatusUp})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "metrics") {
		t.Errorf("empty metrics must be omitted, got: %s", b)
	}
}

// TestPodMetricsForSelector_NilMetricsClient verifies the real lister reports

// the metrics API as unavailable (graceful sentinel) when constructed without a

// metrics client.

type stubReporter struct {
	last   HeartbeatBatch
	err    error
	calls  int
	onCall func(n int)
}

func (s *stubReporter) Send(_ context.Context, b HeartbeatBatch) error {
	s.calls++
	s.last = b
	if s.onCall != nil {
		s.onCall(s.calls)
	}
	return s.err
}

func TestTick_SendsBatch(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 2, 2))
	rep := &stubReporter{}
	a := New(Config{
		ProductID:  "prod-x",
		Source:     "test",
		Namespaces: []string{"default"},
		Interval:   60 * time.Second,
	}, NewClientsetLister(client, nil), rep, nil, newMetricsServerSource(NewClientsetLister(client, nil), nil))

	if err := a.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rep.calls != 1 {
		t.Fatalf("calls = %d, want 1", rep.calls)
	}
	if rep.last.ProductID != "prod-x" || rep.last.Source != "test" || len(rep.last.Services) != 1 {
		t.Errorf("unexpected batch: %+v", rep.last)
	}
}

func TestTick_NoWorkloadsSkipsSend(t *testing.T) {
	rep := &stubReporter{}
	a := New(Config{Namespaces: []string{"default"}}, NewClientsetLister(fake.NewSimpleClientset(), nil), rep, nil, noneSource{})
	if err := a.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rep.calls != 0 {
		t.Error("must not send an empty batch")
	}
}

func TestRun_Once(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 1, 1))
	rep := &stubReporter{}
	a := New(Config{ProductID: "p", Namespaces: []string{"default"}, Interval: time.Second}, NewClientsetLister(client, nil), rep, nil, newMetricsServerSource(NewClientsetLister(client, nil), nil))
	if err := a.Run(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if rep.calls != 1 {
		t.Errorf("once mode should send exactly one batch, got %d", rep.calls)
	}
}

// TestRun_RejectsNonPositiveInterval pins that a misconfigured interval is an

// error rather than a panic. time.NewTicker panics on a non-positive duration,

// and both ZENSU_MONITORING_AGENT_INTERVAL and the chart's agent.intervalSeconds

// can produce one.

func TestRun_RejectsNonPositiveInterval(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		a := New(Config{ProductID: "p", Namespaces: []string{"default"}, Interval: d},
			NewClientsetLister(fake.NewSimpleClientset(), nil), &stubReporter{}, nil, noneSource{})

		err := a.Run(context.Background(), false)
		if err == nil {
			t.Fatalf("interval %v must be rejected, got nil", d)
		}
		if !strings.Contains(err.Error(), "interval must be positive") {
			t.Errorf("interval %v: error = %v, want it to name the interval", d, err)
		}
	}
}

// TestRun_LoopsUntilContextCancelled covers the ticker path and pins that a tick

// failing because the process is shutting down is not logged as a fault.

//

// The interval is far longer than a tick costs, so the ticker channel is empty

// when the second tick cancels. A one-millisecond interval would leave both

// select cases ready and Go would choose between them at random, admitting a

// third tick.

func TestRun_LoopsUntilContextCancelled(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 1, 1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	rep := &stubReporter{err: errors.New("backend down")}
	rep.onCall = func(n int) {
		if n == 2 {
			cancel()
		}
	}
	a := New(Config{ProductID: "p", Namespaces: []string{"default"}, Interval: 50 * time.Millisecond},
		NewClientsetLister(client, nil), rep, log, noneSource{})

	if err := a.Run(ctx, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if rep.calls < 2 {
		t.Errorf("calls = %d, want at least 2 — one immediate tick and one from the ticker", rep.calls)
	}
	if got := strings.Count(buf.String(), "heartbeat tick failed"); got != 1 {
		t.Errorf("logged %d tick failures, want 1 — the tick cancelled by shutdown is not a fault; log:\n%s", got, buf.String())
	}
}

// TestReporterSend hands the captured request over through a buffered channel:

// the handler runs on the server's own goroutine, and a completed round trip is

// not a happens-before edge for a plain variable.

// TestReporterRefusesRedirect pins that the credential-carrying client will not

// follow a 3xx. A heartbeat POST has no legitimate redirect, and following one

// would send the API key to a destination the operator never configured.

// TestReporterBoundsAndStripsPeerText pins that a rejecting endpoint cannot put

// unbounded or control bytes into the agent's log.

// refusingTransport fails every request the way a refused dial does, so the test

// depends on neither a freed ephemeral port nor the platform's syscall wording.

func (refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connect: connection refused")
}

// TestReporterTransportErrorOmitsTheURL pins the same property on the heartbeat

// client as on the scrape client. Both call (*http.Client).Do, and *url.Error

// embeds the full URL in its Error().

func TestCollect_SurvivesFailingExpositionSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := fake.NewSimpleClientset(
		deployment("default", "api", "api", 2, 2),
		pod("default", "api-1", map[string]string{"app": "api"}, 1),
	)
	a := New(Config{ProductID: "prod", Namespaces: []string{"default"}}, NewClientsetLister(client, nil), &stubReporter{}, nil,
		newExpositionSource(ExpositionConfig{URL: srv.URL}, nil, nil))

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatalf("a failing scrape must not fail the tick: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 service, got %d", len(got))
	}
	if len(got[0].Metrics) != 0 {
		t.Errorf("want no metrics after a failed scrape, got %+v", got[0].Metrics)
	}
	if got[0].Status != StatusUp {
		t.Errorf("status = %s, want up — uptime must survive a metrics failure", got[0].Status)
	}
	if got[0].RestartCount == nil || *got[0].RestartCount != 1 {
		t.Errorf("restartCount must survive a metrics failure, got %v", got[0].RestartCount)
	}
}

func TestCollect_PassesPodNamesToSource(t *testing.T) {
	client := fake.NewSimpleClientset(
		deployment("default", "api", "api", 2, 2),
		pod("default", "api-1", map[string]string{"app": "api"}, 0),
		pod("default", "api-2", map[string]string{"app": "api"}, 0),
	)
	spy := &spySource{}
	a := New(Config{ProductID: "prod", Namespaces: []string{"default"}}, NewClientsetLister(client, nil), &stubReporter{}, nil, spy)

	if _, err := a.Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(spy.calls) != 1 {
		t.Fatalf("source should be asked exactly once per tick, got %d calls", len(spy.calls))
	}
	targets := spy.calls[0]
	if len(targets) != 1 {
		t.Fatalf("want 1 target, got %d", len(targets))
	}
	if targets[0].Slug != "api" || targets[0].Namespace != "default" {
		t.Errorf("target identity wrong: %+v", targets[0])
	}
	if len(targets[0].PodNames) != 2 {
		t.Errorf("PodNames = %v, want both pods so label attribution can work", targets[0].PodNames)
	}
	if targets[0].Selector == "" {
		t.Error("Selector must be propagated for the metrics-server source")
	}
}

// TestCollect_FoldsSamplesToTheirOwnService pins that the two-phase fold matches

// samples to heartbeats by slug. With one service any indexing bug is invisible.

func TestCollect_FoldsSamplesToTheirOwnService(t *testing.T) {
	client := fake.NewSimpleClientset(
		deployment("default", "api", "api", 2, 2),
		deployment("default", "worker", "worker", 1, 1),
		deployment("default", "idle", "idle", 1, 1),
	)
	src := &stubSource{per: map[string][]MetricSample{
		"api":    {{Key: MetricCPUMillicores, Value: 250}},
		"worker": {{Key: MetricCPUMillicores, Value: 900}},
	}}
	m := obs.NewWithRegistry(prometheus.NewRegistry())
	a := New(Config{ProductID: "prod", Namespaces: []string{"default"}}, NewClientsetLister(client, nil), &stubReporter{}, nil, src)
	a.Metrics = m

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 services, got %d", len(got))
	}

	bySlug := map[string][]MetricSample{}
	for _, s := range got {
		bySlug[s.Slug] = s.Metrics
	}
	for slug, want := range map[string]float64{"api": 250, "worker": 900} {
		if len(bySlug[slug]) != 1 {
			t.Errorf("%s carries %d samples, want 1: %+v", slug, len(bySlug[slug]), bySlug[slug])
			continue
		}
		if bySlug[slug][0].Value != want {
			t.Errorf("%s cpu = %v, want %v — samples must not cross services", slug, bySlug[slug][0].Value, want)
		}
	}
	if len(bySlug["idle"]) != 0 {
		t.Errorf("idle had no samples and must carry none, got %+v", bySlug["idle"])
	}

	if body := scrapeMetrics(t, m); !strings.Contains(body, "zensu_monitoring_agent_resource_services_mapped 2") {
		t.Errorf("mapped gauge should read 2; body:\n%s", body)
	}
}

// TestCollect_ResetsMappedGaugeWhenNothingMaps pins the TRANSITION to zero, which

// is what the README tells operators to alert on. Asserting zero on a fresh

// registry would prove nothing: a Prometheus gauge already reads zero before

// anything sets it.

func TestCollect_ResetsMappedGaugeWhenNothingMaps(t *testing.T) {
	client := fake.NewSimpleClientset(deployment("default", "api", "api", 2, 2))
	m := obs.NewWithRegistry(prometheus.NewRegistry())
	populated := &stubSource{per: map[string][]MetricSample{
		"api": {{Key: MetricCPUMillicores, Value: 250}},
	}}
	a := New(Config{ProductID: "prod", Namespaces: []string{"default"}}, NewClientsetLister(client, nil), &stubReporter{}, nil, populated)
	a.Metrics = m

	if _, err := a.Collect(context.Background()); err != nil {
		t.Fatalf("first Collect: %v", err)
	}
	if body := scrapeMetrics(t, m); !strings.Contains(body, "zensu_monitoring_agent_resource_services_mapped 1") {
		t.Fatalf("gauge should read 1 after a mapping tick; body:\n%s", body)
	}

	a.source = &stubSource{}
	if _, err := a.Collect(context.Background()); err != nil {
		t.Fatalf("second Collect: %v", err)
	}
	if body := scrapeMetrics(t, m); !strings.Contains(body, "zensu_monitoring_agent_resource_services_mapped 0") {
		t.Errorf("gauge must fall back to 0 when nothing attributes; body:\n%s", body)
	}
}

// TestCollect_WarnsOnDuplicateSlug pins that a slug claimed twice is reported

// rather than silently deduplicated, because the exposition would attribute rows

// from both namespaces to the single surviving heartbeat.

// A standing misconfiguration costs one line, not one per tick, so the second
// Collect must not add a second warning.
func TestCollect_WarnsOnDuplicateSlug(t *testing.T) {
	client := fake.NewSimpleClientset(
		deployment("ns1", "api", "api", 2, 2),
		deployment("ns2", "api-copy", "api", 1, 1),
	)
	var logged strings.Builder
	a := New(Config{ProductID: "prod", Namespaces: []string{"ns1", "ns2"}},
		NewClientsetLister(client, nil), &stubReporter{},
		slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})), noneSource{})

	got, err := a.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want the slug reported once, got %d", len(got))
	}
	if got := strings.Count(logged.String(), "claimed by more than one Deployment"); got != 1 {
		t.Fatalf("warnings = %d, want 1; got:\n%s", got, logged.String())
	}

	if _, err := a.Collect(context.Background()); err != nil {
		t.Fatalf("second Collect: %v", err)
	}
	if got := strings.Count(logged.String(), "claimed by more than one Deployment"); got != 1 {
		t.Errorf("warnings = %d after a second tick, want the latch to hold at 1", got)
	}
}

// TestCollect_DuplicateSlugLatchIsPerPair pins the re-arm: the latch is keyed on

// the slug and the shadowed workload, so a THIRD Deployment later claiming the

// same slug is a new fact the operator has not been told about yet.

func TestCollect_DuplicateSlugLatchIsPerPair(t *testing.T) {
	client := fake.NewSimpleClientset(
		deployment("ns1", "api", "api", 2, 2),
		deployment("ns2", "api-copy", "api", 1, 1),
		deployment("ns3", "api-third", "api", 1, 1),
	)
	var logged strings.Builder
	a := New(Config{ProductID: "prod", Namespaces: []string{"ns1", "ns2", "ns3"}},
		NewClientsetLister(client, nil), &stubReporter{},
		slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})), noneSource{})

	if _, err := a.Collect(context.Background()); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if got := strings.Count(logged.String(), "claimed by more than one Deployment"); got != 2 {
		t.Errorf("warnings = %d, want one per shadowed Deployment; got:\n%s", got, logged.String())
	}
}
