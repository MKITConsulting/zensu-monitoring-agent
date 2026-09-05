// Command zensu-monitoring-agent reports the runtime status of annotated Kubernetes
// workloads to the Zensu API using an outbound push/heartbeat model.
//
// It reads (never mutates) Deployments carrying the `zensu.dev/service`
// annotation and POSTs their up/degraded/down status to
// ${ZENSU_API_URL}/api/runtime/heartbeat with an X-API-Key. All heartbeat
// traffic is outbound; in long-running deployment mode it may additionally
// expose a local Prometheus /metrics endpoint for scraping (opt-in).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/MKITConsulting/zensu-monitoring-agent/internal/agent"
	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
)

func main() {
	once := flag.Bool("once", false, "run a single heartbeat then exit (for a CronJob or host cron)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	apiURL := os.Getenv("ZENSU_API_URL")
	apiKey := os.Getenv("ZENSU_API_KEY")
	productID := os.Getenv("ZENSU_PRODUCT_ID")
	scrapeURL := os.Getenv("ZENSU_MONITORING_AGENT_SCRAPE_URL")
	if err := requiredConfig(apiURL, apiKey, productID, scrapeURL); err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	cfg := agent.Config{
		ProductID:  productID,
		Source:     envOr("ZENSU_MONITORING_AGENT_SOURCE", "k8s-agent"),
		Namespaces: envList("ZENSU_MONITORING_AGENT_NAMESPACES", []string{"default"}),
		Interval:   envDuration("ZENSU_MONITORING_AGENT_INTERVAL", 60*time.Second),
	}

	lister, err := agent.NewInClusterLister()
	if err != nil {
		log.Error("kubernetes client", "error", err)
		os.Exit(1)
	}
	reporter := agent.NewReporter(apiURL, apiKey, 15*time.Second)

	var m *obs.Metrics
	metricsEnabled := envBool("ZENSU_MONITORING_AGENT_METRICS_ENABLED", true) && !*once
	if metricsEnabled {
		m = obs.New()
		reporter.Metrics = m
	}

	sourceCfg := resourceSourceConfig(scrapeURL)
	source, err := agent.NewMetricSource(sourceCfg, lister, log, m)
	if err != nil {
		log.Error("resource metric source", "error", err)
		os.Exit(1)
	}
	log.Info("resource metric source configured", "source", source.Name())
	if *once && expositionReachable(sourceCfg) {
		log.Warn("one-shot mode cannot rate cumulative counters: a counter-based CPU metric needs two consecutive scrapes in one process, so CPU will be absent on every run while memory still reports",
			"mode", "cronjob", "source", source.Name())
	}

	a := agent.New(cfg, lister, reporter, log, source)
	a.Metrics = m

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if metricsEnabled {
		addr := envOr("ZENSU_MONITORING_AGENT_METRICS_ADDR", obs.DefaultAddr)
		log.Info("metrics endpoint enabled", "addr", addr, "path", "/metrics")
		go func() {
			if err := m.Serve(ctx, addr); err != nil && ctx.Err() == nil {
				log.Error("metrics server stopped", "error", err)
			}
		}()
	}

	if err := a.Run(ctx, *once); err != nil && ctx.Err() == nil {
		log.Error("agent stopped", "error", err)
		os.Exit(1)
	}
}

// requiredConfig holds the startup refusals main() would otherwise inline, where
// no test can reach them. The scrape URL is checked whenever it is set rather
// than only in the modes that scrape, because the chart writes its ConfigMap key
// in every mode and a credential sitting there unused is exposed just the same.
func requiredConfig(apiURL, apiKey, productID, scrapeURL string) error {
	if apiURL == "" || apiKey == "" || productID == "" {
		return errors.New("ZENSU_API_URL, ZENSU_API_KEY and ZENSU_PRODUCT_ID are all required")
	}
	if err := agent.ValidateAPIURL(apiURL); err != nil {
		return err
	}
	if scrapeURL == "" {
		return nil
	}
	return agent.ValidateScrapeURL(scrapeURL)
}

// resourceSourceConfig reads the resource-metric source settings. The knob is
// RESOURCE_SOURCE rather than METRICS_SOURCE because METRICS_ENABLED and
// METRICS_ADDR are already taken and mean the agent's OWN endpoint.
func resourceSourceConfig(scrapeURL string) agent.SourceConfig {
	return agent.SourceConfig{
		Mode: envOr("ZENSU_MONITORING_AGENT_RESOURCE_SOURCE", agent.SourceAuto),
		Exposition: agent.ExpositionConfig{
			URL:          scrapeURL,
			SlugLabel:    envOr("ZENSU_MONITORING_AGENT_SCRAPE_SLUG_LABEL", agent.DefaultSlugLabel),
			CPUMetric:    os.Getenv("ZENSU_MONITORING_AGENT_SCRAPE_CPU_METRIC"),
			MemoryMetric: os.Getenv("ZENSU_MONITORING_AGENT_SCRAPE_MEMORY_METRIC"),
			Timeout:      envDuration("ZENSU_MONITORING_AGENT_SCRAPE_TIMEOUT", agent.DefaultScrapeTimeout),
			MaxBytes:     envInt64("ZENSU_MONITORING_AGENT_SCRAPE_MAX_BYTES", 0),
		},
	}
}

// expositionReachable reports whether the configuration can ever scrape, which
// is what decides whether the one-shot counter caveat applies. It is gated on
// configuration rather than on the resolved source's name: in the default auto
// mode the composite reports its primary until a fallthrough happens, which by
// definition has not happened at startup, so a name check would silence the
// warning in exactly the setup it exists for.
func expositionReachable(cfg agent.SourceConfig) bool {
	if cfg.Exposition.URL == "" {
		return false
	}
	switch cfg.Mode {
	case agent.SourceExposition, agent.SourceAuto, "":
		return true
	default:
		return false
	}
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "":
		return def
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// envDuration falls back to def for a non-positive value as well as an
// unparsable one: every duration the agent reads is an interval or a timeout,
// and zero or negative makes neither usable.
func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

func envList(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}
