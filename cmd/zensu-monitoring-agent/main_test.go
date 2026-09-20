package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/MKITConsulting/zensu-monitoring-agent/internal/agent"
)

func TestResourceSourceConfigDefaults(t *testing.T) {
	for _, key := range []string{
		"ZENSU_MONITORING_AGENT_RESOURCE_SOURCE",
		"ZENSU_MONITORING_AGENT_SCRAPE_URL",
		"ZENSU_MONITORING_AGENT_SCRAPE_SLUG_LABEL",
		"ZENSU_MONITORING_AGENT_SCRAPE_CPU_METRIC",
		"ZENSU_MONITORING_AGENT_SCRAPE_MEMORY_METRIC",
		"ZENSU_MONITORING_AGENT_SCRAPE_TIMEOUT",
		"ZENSU_MONITORING_AGENT_SCRAPE_MAX_BYTES",
	} {
		t.Setenv(key, "")
	}

	cfg := resourceSourceConfig(os.Getenv("ZENSU_MONITORING_AGENT_SCRAPE_URL"))
	if cfg.Mode != agent.SourceAuto {
		t.Errorf("Mode = %q, want %q", cfg.Mode, agent.SourceAuto)
	}
	if cfg.Exposition.URL != "" {
		t.Errorf("URL = %q, want empty so auto stays on metrics-server", cfg.Exposition.URL)
	}
	if cfg.Exposition.SlugLabel != agent.DefaultSlugLabel {
		t.Errorf("SlugLabel = %q, want %q", cfg.Exposition.SlugLabel, agent.DefaultSlugLabel)
	}
	if cfg.Exposition.Timeout != agent.DefaultScrapeTimeout {
		t.Errorf("Timeout = %v, want %v", cfg.Exposition.Timeout, agent.DefaultScrapeTimeout)
	}
	if cfg.Exposition.CPUMetric != "" || cfg.Exposition.MemoryMetric != "" {
		t.Errorf("metric overrides should be empty by default, got %q/%q",
			cfg.Exposition.CPUMetric, cfg.Exposition.MemoryMetric)
	}
	if cfg.Exposition.MaxBytes != 0 {
		t.Errorf("MaxBytes = %d, want 0 so the library default applies", cfg.Exposition.MaxBytes)
	}
}

// TestEnvInt64 exercises the default with a non-zero value as well, which proves
// the parameter is returned rather than a hard-coded zero.
func TestEnvInt64(t *testing.T) {
	cases := []struct {
		name string
		val  string
		want int64
	}{
		{"unset", "", 0},
		{"zero", "0", 0},
		{"negative", "-1", 0},
		{"unparsable", "notanumber", 0},
		{"a megabyte", "1048576", 1048576},
		{"blank", "  ", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("ZENSU_MONITORING_AGENT_TEST_INT", c.val)
			if got := envInt64("ZENSU_MONITORING_AGENT_TEST_INT", 0); got != c.want {
				t.Errorf("envInt64(%q, 0) = %d, want %d", c.val, got, c.want)
			}
			if c.want != 0 {
				return
			}
			if got := envInt64("ZENSU_MONITORING_AGENT_TEST_INT", 4096); got != 4096 {
				t.Errorf("envInt64(%q, 4096) = %d, want the default 4096", c.val, got)
			}
		})
	}
}

func TestExpositionReachable(t *testing.T) {
	cases := []struct {
		name string
		cfg  agent.SourceConfig
		want bool
	}{
		{"auto with url", agent.SourceConfig{Mode: agent.SourceAuto, Exposition: agent.ExpositionConfig{URL: "http://c/m"}}, true},
		{"empty mode with url", agent.SourceConfig{Exposition: agent.ExpositionConfig{URL: "http://c/m"}}, true},
		{"exposition with url", agent.SourceConfig{Mode: agent.SourceExposition, Exposition: agent.ExpositionConfig{URL: "http://c/m"}}, true},
		{"auto without url", agent.SourceConfig{Mode: agent.SourceAuto}, false},
		{"metrics-server with url", agent.SourceConfig{Mode: agent.SourceMetricsServer, Exposition: agent.ExpositionConfig{URL: "http://c/m"}}, false},
		{"none with url", agent.SourceConfig{Mode: agent.SourceNone, Exposition: agent.ExpositionConfig{URL: "http://c/m"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := expositionReachable(c.cfg); got != c.want {
				t.Errorf("expositionReachable = %v, want %v", got, c.want)
			}
		})
	}
}

func TestResourceSourceConfigReadsEnv(t *testing.T) {
	t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", agent.SourceExposition)
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_URL", "http://otel-collector.observability:8889/metrics")
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_SLUG_LABEL", "svc")
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_CPU_METRIC", "my_cpu")
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_MEMORY_METRIC", "my_mem")
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_TIMEOUT", "3s")
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_MAX_BYTES", "1048576")

	cfg := resourceSourceConfig(os.Getenv("ZENSU_MONITORING_AGENT_SCRAPE_URL"))
	if cfg.Mode != agent.SourceExposition {
		t.Errorf("Mode = %q, want %q", cfg.Mode, agent.SourceExposition)
	}
	if cfg.Exposition.URL != "http://otel-collector.observability:8889/metrics" {
		t.Errorf("URL = %q", cfg.Exposition.URL)
	}
	if cfg.Exposition.SlugLabel != "svc" {
		t.Errorf("SlugLabel = %q, want svc", cfg.Exposition.SlugLabel)
	}
	if cfg.Exposition.CPUMetric != "my_cpu" || cfg.Exposition.MemoryMetric != "my_mem" {
		t.Errorf("overrides = %q/%q", cfg.Exposition.CPUMetric, cfg.Exposition.MemoryMetric)
	}
	if cfg.Exposition.Timeout != 3*time.Second {
		t.Errorf("Timeout = %v, want 3s", cfg.Exposition.Timeout)
	}
	if cfg.Exposition.MaxBytes != 1048576 {
		t.Errorf("MaxBytes = %d, want 1048576", cfg.Exposition.MaxBytes)
	}
}

func TestResourceSourceConfigIgnoresUnparsableTimeout(t *testing.T) {
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_TIMEOUT", "soon")

	if got := resourceSourceConfig(os.Getenv("ZENSU_MONITORING_AGENT_SCRAPE_URL")).Exposition.Timeout; got != agent.DefaultScrapeTimeout {
		t.Errorf("Timeout = %v, want the default %v when the value does not parse", got, agent.DefaultScrapeTimeout)
	}
}

// TestRequiredConfig pins the startup refusals. They used to be inlined in main()
// where no test could reach them, so deleting them left every gate green while a
// userinfo-bearing URL was accepted again and rendered into the ConfigMap.
func TestRequiredConfig(t *testing.T) {
	const okURL = "https://api.zensu.dev"
	cases := []struct {
		name            string
		apiURL          string
		apiKey          string
		productID       string
		scrapeURL       string
		wantErrContains string
	}{
		{name: "all set", apiURL: okURL, apiKey: "zsk_x", productID: "p"},
		{name: "scrape url set and clean", apiURL: okURL, apiKey: "zsk_x", productID: "p", scrapeURL: "http://collector:8889/metrics"},
		{name: "missing api url", apiKey: "zsk_x", productID: "p", wantErrContains: "are all required"},
		{name: "missing api key", apiURL: okURL, productID: "p", wantErrContains: "are all required"},
		{name: "missing product id", apiURL: okURL, apiKey: "zsk_x", wantErrContains: "are all required"},
		{name: "api url with credentials", apiURL: "https://u:p@zensu.internal", apiKey: "zsk_x", productID: "p", wantErrContains: "carries credentials"},
		{name: "api url without scheme", apiURL: "zensu.internal", apiKey: "zsk_x", productID: "p", wantErrContains: "http:// or https:// scheme"},
		{name: "scrape url with credentials", apiURL: okURL, apiKey: "zsk_x", productID: "p", scrapeURL: "http://u:p@collector:8889/metrics", wantErrContains: "carries credentials"},
		{name: "scrape url without scheme", apiURL: okURL, apiKey: "zsk_x", productID: "p", scrapeURL: "collector:8889/metrics", wantErrContains: "http:// or https:// scheme"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := requiredConfig(c.apiURL, c.apiKey, c.productID, c.scrapeURL)
			if c.wantErrContains == "" {
				if err != nil {
					t.Fatalf("requiredConfig = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error containing %q", c.wantErrContains)
			}
			if !strings.Contains(err.Error(), c.wantErrContains) {
				t.Errorf("error = %v, want it to name %q", err, c.wantErrContains)
			}
			if strings.Contains(err.Error(), ":p@") || strings.Contains(err.Error(), "zensu.internal") {
				t.Errorf("the error must not echo the configured value, got %v", err)
			}
		})
	}
}

// fakeLister builds the cluster reader run() would otherwise get from the
// in-cluster config, which no test can have.
func fakeLister() (agent.ClusterReader, error) {
	return agent.NewClientsetLister(fake.NewSimpleClientset(), nil), nil
}

// startupEnv sets the three always-required variables plus a heartbeat URL that
// accepts anything, so a test can vary exactly the one knob it is about.
func startupEnv(t *testing.T) {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(backend.Close)
	t.Setenv("ZENSU_API_URL", backend.URL)
	t.Setenv("ZENSU_API_KEY", "zsk_x")
	t.Setenv("ZENSU_PRODUCT_ID", "p")
	t.Setenv("ZENSU_MONITORING_AGENT_NAMESPACES", "default")
	t.Setenv("ZENSU_MONITORING_AGENT_METRICS_ADDR", "127.0.0.1:0")
	t.Setenv("ZENSU_MONITORING_AGENT_METRICS_ENABLED", "true")
	t.Setenv("ZENSU_MONITORING_AGENT_INTERVAL", "")
	t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", "")
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_URL", "")
}

// runStartup drives run() to the point where startup either refuses or the agent
// begins, with a context already cancelled so the loop returns at once. The
// startup decisions are what these tests are about; the loop is covered in
// internal/agent.
func runStartup(t *testing.T, once bool) (string, error) {
	t.Helper()
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, log, once, fakeLister)
	return buf.String(), err
}

// TestRunRefusesExpositionWithoutURL pins the guard the chart does not duplicate:
// resourceMetrics.source=exposition with no scrapeUrl is a plausible
// misconfiguration, and this exit is its only defence. It lived inside main()
// where nothing could reach it.
func TestRunRefusesExpositionWithoutURL(t *testing.T) {
	startupEnv(t)
	t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", "exposition")
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_URL", "")

	_, err := runStartup(t, false)
	if err == nil {
		t.Fatal("expected startup to refuse exposition mode without a scrape URL")
	}
	if !strings.Contains(err.Error(), "resource metric source") {
		t.Errorf("err = %v, want it to name the stage that refused", err)
	}
	if !strings.Contains(err.Error(), "requires a scrape URL") {
		t.Errorf("err = %v, want it to name the missing value", err)
	}
}

// TestRunRefusesUnknownSource pins the other half of the same exit: a typo in the
// mode is refused at startup rather than silently degrading to no metrics.
func TestRunRefusesUnknownSource(t *testing.T) {
	startupEnv(t)
	t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", "prometheus")

	_, err := runStartup(t, false)
	if err == nil || !strings.Contains(err.Error(), "unknown resource source") {
		t.Fatalf("err = %v, want the unknown-source refusal", err)
	}
}

// TestRunRefusesTheScrapeURLInEveryMode pins that the scrape URL is refused
// whatever resourceMetrics.source says. The chart writes its ConfigMap key in
// every mode, so a credential in an unused scrape URL is exposed just the same as
// in a used one. It drives run() rather than requiredConfig, because the mode is
// only an input at this level: requiredConfig does not read it, so calling that
// per mode asserted the same thing five times and would survive someone
// reintroducing mode-conditional validation in the caller.
func TestRunRefusesTheScrapeURLInEveryMode(t *testing.T) {
	for _, mode := range []string{"none", "metrics-server", "exposition", "auto", ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			startupEnv(t)
			t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", mode)
			t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_URL", "http://u:p@collector:8889/metrics")

			_, err := runStartup(t, false)
			if err == nil || !strings.Contains(err.Error(), "carries credentials") {
				t.Fatalf("err = %v, want the credential refusal", err)
			}
			if strings.Contains(err.Error(), ":p@") || strings.Contains(err.Error(), "collector") {
				t.Errorf("the error must not echo the configured value, got %v", err)
			}
		})
	}
}

// TestRunWarnsOnceModeCannotRateCounters pins the caveat a CronJob operator has
// no other way to learn: one process per run means a counter never gets its
// second observation, so CPU is absent on every run while memory reports.
func TestRunWarnsOnceModeCannotRateCounters(t *testing.T) {
	startupEnv(t)
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_URL", "http://collector:8889/metrics")

	logged, err := runStartup(t, true)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(logged, "one-shot mode cannot rate cumulative counters") {
		t.Errorf("a cronjob with a scrape URL must carry the caveat; log:\n%s", logged)
	}
}

// TestRunSkipsTheOnceCaveatWithoutAnExposition is the negative half: the caveat
// is about scraping, so a cronjob that cannot scrape must not carry it.
func TestRunSkipsTheOnceCaveatWithoutAnExposition(t *testing.T) {
	startupEnv(t)
	t.Setenv("ZENSU_MONITORING_AGENT_SCRAPE_URL", "")

	logged, err := runStartup(t, true)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if strings.Contains(logged, "one-shot mode cannot rate cumulative counters") {
		t.Errorf("a cronjob with no exposition must not carry the caveat; log:\n%s", logged)
	}
}

// TestRunGatesMetricsOnLongRunningMode pins the hoisted metrics block: the
// agent's own endpoint is meaningless in a one-shot run, where nothing would ever
// scrape it before the process exits.
func TestRunGatesMetricsOnLongRunningMode(t *testing.T) {
	for _, c := range []struct {
		name string
		once bool
		want bool
	}{
		{name: "long-running serves metrics", once: false, want: true},
		{name: "one-shot does not", once: true, want: false},
	} {
		t.Run(c.name, func(t *testing.T) {
			startupEnv(t)
			t.Setenv("ZENSU_MONITORING_AGENT_METRICS_ENABLED", "true")

			logged, err := runStartup(t, c.once)
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if got := strings.Contains(logged, "metrics endpoint enabled"); got != c.want {
				t.Errorf("metrics endpoint enabled = %v, want %v; log:\n%s", got, c.want, logged)
			}
		})
	}
}

// TestEnvDuration pins the non-positive fallback. The chart renders
// ZENSU_MONITORING_AGENT_INTERVAL straight from agent.intervalSeconds, so
// intervalSeconds: 0 reaches the binary as "0s"; without the guard that
// misconfiguration crash-loops the pod instead of falling back.
func TestEnvDuration(t *testing.T) {
	cases := []struct {
		name string
		val  string
		want time.Duration
	}{
		{"unset", "", 30 * time.Second},
		{"zero", "0s", 30 * time.Second},
		{"negative", "-5s", 30 * time.Second},
		{"unparsable", "soon", 30 * time.Second},
		{"valid", "90s", 90 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("ZENSU_MONITORING_AGENT_TEST_DURATION", c.val)
			if got := envDuration("ZENSU_MONITORING_AGENT_TEST_DURATION", 30*time.Second); got != c.want {
				t.Errorf("envDuration(%q) = %v, want %v", c.val, got, c.want)
			}
		})
	}
}

func TestEnvBool(t *testing.T) {
	cases := []struct {
		name string
		val  string
		def  bool
		want bool
	}{
		{"unset keeps a true default", "", true, true},
		{"unset keeps a false default", "", false, false},
		{"one", "1", false, true},
		{"true", "true", false, true},
		{"uppercase", "TRUE", false, true},
		{"yes", "yes", false, true},
		{"on", "on", false, true},
		{"padded", "  true  ", false, true},
		{"zero", "0", true, false},
		{"false", "false", true, false},
		{"off", "off", true, false},
		{"garbage reads as off", "garbage", true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("ZENSU_MONITORING_AGENT_TEST_BOOL", c.val)
			if got := envBool("ZENSU_MONITORING_AGENT_TEST_BOOL", c.def); got != c.want {
				t.Errorf("envBool(val=%q, def=%v) = %v, want %v", c.val, c.def, got, c.want)
			}
		})
	}
}

// TestEnvLogLevel pins that the Debug diagnostics can be turned on at all, and
// that a typo does not crash-loop the pod over a logging preference.
func TestEnvLogLevel(t *testing.T) {
	cases := []struct {
		name string
		val  string
		want slog.Level
	}{
		{"unset", "", slog.LevelInfo},
		{"debug", "debug", slog.LevelDebug},
		{"upper case", "DEBUG", slog.LevelDebug},
		{"warn", "warn", slog.LevelWarn},
		{"error", "error", slog.LevelError},
		{"unreadable falls back rather than refusing to start", "chatty", slog.LevelInfo},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("ZENSU_MONITORING_AGENT_LOG_LEVEL", c.val)
			if got := envLogLevel(); got != c.want {
				t.Errorf("envLogLevel() = %v, want %v", got, c.want)
			}
		})
	}
}

// TestEnvList pins the fallback that keeps a blank value from narrowing the
// agent to no namespaces at all. A value holding only separators is ordinary
// chart output, and without the guard it would produce an empty list — which
// lists nothing rather than everything.
func TestEnvList(t *testing.T) {
	def := []string{"default"}
	cases := []struct {
		name string
		val  string
		want []string
	}{
		{"unset falls back", "", def},
		{"single value", "prod", []string{"prod"}},
		{"comma separated and trimmed", " a , b ,c ", []string{"a", "b", "c"}},
		{"blank entries are dropped", "a,,b", []string{"a", "b"}},
		{"only separators falls back", " , , ", def},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("ZENSU_MONITORING_AGENT_NAMESPACES", c.val)
			if got := envList("ZENSU_MONITORING_AGENT_NAMESPACES", def); !slices.Equal(got, c.want) {
				t.Errorf("envList() = %v, want %v", got, c.want)
			}
		})
	}
}

// annotatedLister serves one annotated Deployment, so a tick has something to
// report and the heartbeat path is actually reached.
func annotatedLister() (agent.ClusterReader, error) {
	desired := int32(1)
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "api",
			Annotations: map[string]string{agent.AnnotationService: "api"},
		},
		Spec:   appsv1.DeploymentSpec{Replicas: &desired},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 1},
	}
	return agent.NewClientsetLister(fake.NewSimpleClientset(d), nil), nil
}

// TestRunReportsAFailingKubernetesClient pins that a client the agent cannot
// build stops startup with the stage named. It is the first thing that fails
// when the ServiceAccount is not mounted, and an unnamed error there sends an
// operator looking at the API URL instead of at the pod spec.
func TestRunReportsAFailingKubernetesClient(t *testing.T) {
	startupEnv(t)
	refusal := errors.New("no service account token")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), false,
		func() (agent.ClusterReader, error) { return nil, refusal })
	if !errors.Is(err, refusal) {
		t.Fatalf("err = %v, want the constructor's refusal", err)
	}
	if !strings.Contains(err.Error(), "kubernetes client") {
		t.Errorf("err = %v, want it to name the stage that refused", err)
	}
}

// TestRunReturnsTheAgentError pins that a tick the backend refused reaches the
// caller, which is what makes a CronJob run exit non-zero. Swallowing it would
// leave a failed one-shot indistinguishable from a successful one, and the
// CronJob's own failure count is the only alarm a one-shot deployment has. The
// assertion names the backend's refusal rather than accepting any error, because
// every startup stage above this one also returns non-nil.
func TestRunReturnsTheAgentError(t *testing.T) {
	startupEnv(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()
	t.Setenv("ZENSU_API_URL", backend.URL)
	t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", agent.SourceNone)

	err := run(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), true, annotatedLister)
	if err == nil || !strings.Contains(err.Error(), "heartbeat rejected (500)") {
		t.Fatalf("err = %v, want the backend's refusal", err)
	}
}

// TestRunSwallowsTheAgentErrorOnShutdown pins the other half of the same guard:
// a tick that fails while the context is already cancelled must NOT fail the
// run. Without it a SIGTERM during a rollout would exit 1 and report an orderly
// shutdown as a crash.
func TestRunSwallowsTheAgentErrorOnShutdown(t *testing.T) {
	startupEnv(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()
	t.Setenv("ZENSU_API_URL", backend.URL)
	t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", agent.SourceNone)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), true, annotatedLister); err != nil {
		t.Fatalf("a failing tick under a cancelled context must not fail the run, got %v", err)
	}
}

// logRecord is one captured log line: the level an alert rule matches on, the
// message, and the attributes that carry the reason.
type logRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

// logSink publishes every record on a channel, so a test can wait for a line
// written by a goroutine instead of racing a shared buffer. A record dropped
// because the buffer was full would present as a timeout with no explanation, so
// drops are counted and asserted rather than swallowed silently.
type logSink struct {
	records chan logRecord
	dropped *atomic.Int32
}

func (s logSink) Enabled(context.Context, slog.Level) bool { return true }

func (s logSink) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{level: r.Level, msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	select {
	case s.records <- rec:
	default:
		s.dropped.Add(1)
	}
	return nil
}

func (s logSink) WithAttrs([]slog.Attr) slog.Handler { return s }

func (s logSink) WithGroup(string) slog.Handler { return s }

// TestRunLogsAMetricsServerThatCannotListen pins the only report an operator
// gets when the metrics port is already taken. The listener runs in its own
// goroutine and its failure never reaches run's return value, so without this
// line /metrics stays unreachable while the agent looks healthy. The level and
// the reason are asserted too: an alert rule matches on ERROR, and a message
// with no reason tells the operator nothing.
func TestRunLogsAMetricsServerThatCannotListen(t *testing.T) {
	startupEnv(t)
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	defer taken.Close()
	t.Setenv("ZENSU_MONITORING_AGENT_METRICS_ADDR", taken.Addr().String())
	t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", agent.SourceNone)

	var dropped atomic.Int32
	sink := logSink{records: make(chan logRecord, 256), dropped: &dropped}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, slog.New(sink), false, fakeLister) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("run did not return within 5s of ctx cancel")
		}
	})

	deadline := time.After(5 * time.Second)
	for {
		select {
		case rec := <-sink.records:
			if rec.msg != "metrics server stopped" {
				continue
			}
			if rec.level != slog.LevelError {
				t.Errorf("level = %v, want %v so an alert rule can match it", rec.level, slog.LevelError)
			}
			if rec.attrs["error"] == "" {
				t.Errorf("the line must carry the reason, got attrs %v", rec.attrs)
			}
			if got := dropped.Load(); got != 0 {
				t.Errorf("%d log records were dropped; the sink buffer is too small", got)
			}
			return
		case <-deadline:
			t.Fatal("the metrics listener failure was never logged")
		}
	}
}
