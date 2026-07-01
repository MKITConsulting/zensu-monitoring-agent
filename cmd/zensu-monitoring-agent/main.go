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
	"flag"
	"log/slog"
	"os"
	"os/signal"
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
	if apiURL == "" || apiKey == "" || productID == "" {
		log.Error("missing required config", "required", "ZENSU_API_URL, ZENSU_API_KEY, ZENSU_PRODUCT_ID")
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
	a := agent.New(cfg, lister, reporter, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if envBool("ZENSU_MONITORING_AGENT_METRICS_ENABLED", true) && !*once {
		m := obs.New()
		reporter.Metrics = m
		a.Metrics = m
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

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
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
