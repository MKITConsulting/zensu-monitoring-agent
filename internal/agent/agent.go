package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
)

// reporter is the subset of *Reporter the Agent depends on (eases testing).
type reporter interface {
	Send(ctx context.Context, batch HeartbeatBatch) error
}

// Config holds the agent's runtime configuration.
type Config struct {
	ProductID  string
	Source     string
	Namespaces []string
	Interval   time.Duration
}

// Agent collects annotated Deployment statuses and reports them to Zensu.
type Agent struct {
	cfg      Config
	lister   ClusterReader
	reporter reporter
	log      *slog.Logger
	Metrics  *obs.Metrics

	// metricsAPIWarned latches once metrics-server is found to be absent, so the
	// "no metrics-server" warning is logged a single time over the agent's
	// lifetime instead of once per service per tick.
	metricsAPIWarned atomic.Bool
}

// New builds an Agent.
func New(cfg Config, lister ClusterReader, r reporter, log *slog.Logger) *Agent {
	if log == nil {
		log = slog.Default()
	}
	return &Agent{cfg: cfg, lister: lister, reporter: r, log: log}
}

// Collect lists annotated workloads across the configured namespaces,
// deduplicated by service slug.
func (a *Agent) Collect(ctx context.Context) ([]ServiceHeartbeat, error) {
	seen := map[string]bool{}
	var out []ServiceHeartbeat
	for _, ns := range a.cfg.Namespaces {
		deps, err := a.lister.ListDeployments(ctx, ns)
		if err != nil {
			return nil, err
		}
		for _, d := range deps {
			entry, ok := MapDeployment(d)
			if !ok || seen[entry.Slug] {
				continue
			}
			if a.cfg.Interval > 0 {
				entry.IntervalSeconds = int32(a.cfg.Interval.Seconds())
			}
			if sel := deploymentSelector(d); sel != "" {
				if pods, err := a.lister.ListPods(ctx, ns, sel); err != nil {
					a.log.Warn("list pods for restartCount failed", "deployment", d.Name, "error", err)
				} else {
					rc := sumRestarts(pods)
					entry.RestartCount = &rc
				}
				a.attachMetrics(ctx, ns, sel, &entry)
			}
			seen[entry.Slug] = true
			out = append(out, entry)
		}
	}
	return out, nil
}

// attachMetrics reads CPU/memory from metrics-server for the given
// namespace+selector and appends them to entry. It NEVER fails the tick:
//   - missing metrics.k8s.io API (no metrics-server): log one Warn over the
//     agent's lifetime and continue without metrics (graceful degrade);
//   - any other (transient) error: log a Warn and skip metrics for this tick;
//   - success: append cpu_millicores and memory_bytes samples.
func (a *Agent) attachMetrics(ctx context.Context, namespace, selector string, entry *ServiceHeartbeat) {
	cpu, mem, available, err := a.lister.PodMetricsForSelector(ctx, namespace, selector)
	if err != nil {
		if errors.Is(err, ErrMetricsAPIUnavailable) {
			if a.metricsAPIWarned.CompareAndSwap(false, true) {
				a.log.Warn("metrics-server (metrics.k8s.io) not available; sending heartbeats without CPU/memory metrics")
			}
			return
		}
		a.log.Warn("read pod metrics failed; skipping metrics this tick", "service", entry.Slug, "error", err)
		return
	}
	if !available {
		return
	}
	entry.Metrics = append(entry.Metrics,
		MetricSample{Key: MetricCPUMillicores, Value: float64(cpu)},
		MetricSample{Key: MetricMemoryBytes, Value: float64(mem)},
	)
}

// Tick collects and reports a single heartbeat batch.
func (a *Agent) Tick(ctx context.Context) error {
	services, err := a.Collect(ctx)
	if err != nil {
		return err
	}
	if len(services) == 0 {
		a.Metrics.SetServicesReported(0)
		a.log.Info("no annotated workloads found", "annotation", AnnotationService)
		return nil
	}
	batch := HeartbeatBatch{ProductID: a.cfg.ProductID, Source: a.cfg.Source, Services: services}
	if err := a.reporter.Send(ctx, batch); err != nil {
		return err
	}
	a.Metrics.SetServicesReported(len(services))
	a.log.Info("heartbeat sent", "services", len(services))
	return nil
}

// Run executes a single Tick when once is true, otherwise loops on the
// configured interval until ctx is cancelled. Tick errors are logged, not fatal,
// so a transient API blip does not crash the agent.
func (a *Agent) Run(ctx context.Context, once bool) error {
	if once {
		return a.Tick(ctx)
	}
	if err := a.Tick(ctx); err != nil {
		a.log.Error("heartbeat tick failed", "error", err)
	}
	t := time.NewTicker(a.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := a.Tick(ctx); err != nil {
				a.log.Error("heartbeat tick failed", "error", err)
			}
		}
	}
}
