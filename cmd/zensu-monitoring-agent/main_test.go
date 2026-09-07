package main

import (
	"os"
	"strings"
	"testing"
	"time"

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

// TestRequiredConfigChecksTheScrapeURLInEveryMode pins that the scrape URL is
// refused whatever resourceMetrics.source says. The chart writes its ConfigMap
// key in every mode, so a credential in an unused scrape URL is exposed just the
// same as in a used one.
func TestRequiredConfigChecksTheScrapeURLInEveryMode(t *testing.T) {
	for _, mode := range []string{"none", "metrics-server", "exposition", "auto", ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			t.Setenv("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", mode)

			err := requiredConfig("https://api.zensu.dev", "zsk_x", "p", "http://u:p@collector:8889/metrics")
			if err == nil || !strings.Contains(err.Error(), "carries credentials") {
				t.Errorf("err = %v, want the credential refusal", err)
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
