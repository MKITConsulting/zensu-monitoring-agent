package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
	// Metrics is assigned by the caller after New, because instrumentation is
	// optional; nil disables it and every method on it tolerates a nil receiver.
	Metrics *obs.Metrics

	// source supplies per-service resource metrics for the agent's lifetime.
	// NewMetricSource owns the selection policy; New normalizes nil to
	// noneSource rather than substituting a default of its own.
	source MetricSource

	// warnedSlugs latches the duplicate-slug warning per slug, so a standing
	// two-namespace misconfiguration costs one line rather than one per tick.
	warnedSlugs sync.Map
}

// New builds an Agent. source must come from NewMetricSource, which owns the
// selection policy; a nil source disables resource metrics, leaving uptime and
// restart counts unaffected.
func New(cfg Config, lister ClusterReader, r reporter, log *slog.Logger, source MetricSource) *Agent {
	if log == nil {
		log = slog.Default()
	}
	if source == nil {
		source = noneSource{}
	}
	return &Agent{
		cfg:      cfg,
		lister:   lister,
		reporter: r,
		log:      log,
		source:   source,
	}
}

// Collect lists annotated workloads across the configured namespaces,
// deduplicated by service slug, then attaches resource metrics.
//
// It runs in two phases: every target is gathered first, then the metric source
// is asked ONCE for the whole set. A per-service call would turn a single
// exposition scrape into one HTTP request per service.
func (a *Agent) Collect(ctx context.Context) ([]ServiceHeartbeat, error) {
	claimed := map[string]string{}
	var out []ServiceHeartbeat
	var targets []ServiceTarget

	for _, ns := range a.cfg.Namespaces {
		deps, err := a.lister.ListDeployments(ctx, ns)
		if err != nil {
			return nil, err
		}
		for _, d := range deps {
			entry, ok := MapDeployment(d)
			if !ok {
				continue
			}
			if owner, dup := claimed[entry.Slug]; dup {
				a.warnDuplicateSlug(entry.Slug, owner, ns+"/"+d.Name)
				continue
			}
			if a.cfg.Interval > 0 {
				entry.IntervalSeconds = int32(a.cfg.Interval.Seconds())
			}
			if sel := deploymentSelector(d); sel != "" {
				var names []string
				if pods, err := a.lister.ListPods(ctx, ns, sel); err != nil {
					a.log.Warn("list pods for restartCount failed", "deployment", d.Name, "error", err)
				} else {
					rc := sumRestarts(pods)
					entry.RestartCount = &rc
					names = podNames(pods)
				}
				targets = append(targets, ServiceTarget{
					Slug:      entry.Slug,
					Namespace: ns,
					Selector:  sel,
					PodNames:  names,
				})
			}
			claimed[entry.Slug] = ns + "/" + d.Name
			out = append(out, entry)
		}
	}

	samples := a.collectSamples(ctx, targets)
	mapped := 0
	for i := range out {
		ms, ok := samples[out[i].Slug]
		if !ok || len(ms) == 0 {
			continue
		}
		out[i].Metrics = append(out[i].Metrics, ms...)
		mapped++
	}
	a.Metrics.SetResourceServicesMapped(mapped)
	return out, nil
}

// warnDuplicateSlug reports one slug claimed by two Deployments, once per slug.
// The agent cannot resolve it: only the first Deployment's replicas and restarts
// are reported, while an exposition labelled by slug would contribute rows from
// both namespaces. Saying so beats deduplicating in silence — but the condition
// is a standing misconfiguration, so it is latched rather than repeated every
// tick. The latch is keyed on the pair, so a different workload later claiming
// the same slug is reported as the new fact it is.
func (a *Agent) warnDuplicateSlug(slug, reported, shadowed string) {
	key := slug + "\x1f" + shadowed
	if _, seen := a.warnedSlugs.Load(key); seen {
		return
	}
	a.warnedSlugs.Store(key, struct{}{})
	a.log.Warn("service slug claimed by more than one Deployment; only the first is reported",
		"slug", slug, "reported", reported, "shadowed", shadowed)
}

// collectSamples asks the configured source once for this tick's samples and
// returns them for Collect to fold in. It NEVER fails the tick: an unusable
// source costs the metrics only, while status and restartCount still ship.
// ErrSourceUnavailable is expected rather than exceptional — the source that
// raises it has already logged its one-time warning. A failure once ctx is done
// is the shutdown itself rather than a fault, and is not logged, matching
// tickOnce — Fetch flattens its errors into strings, so errors.Is cannot
// classify a cancellation here.
func (a *Agent) collectSamples(ctx context.Context, targets []ServiceTarget) map[string][]MetricSample {
	if len(targets) == 0 {
		return nil
	}

	samples, err := a.source.Samples(ctx, targets)
	if err != nil && !errors.Is(err, ErrSourceUnavailable) && ctx.Err() == nil {
		a.log.Warn("metric source failed; skipping metrics this tick", "source", a.source.Name(), "error", err)
	}
	return samples
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
// so a transient API blip does not crash the agent — except once ctx is done,
// where a failing in-flight request is the shutdown itself rather than a fault.
//
// A non-positive interval is rejected rather than passed to time.NewTicker,
// which panics on one; ZENSU_MONITORING_AGENT_INTERVAL and the chart's
// agent.intervalSeconds can both produce it.
func (a *Agent) Run(ctx context.Context, once bool) error {
	if once {
		return a.Tick(ctx)
	}
	if a.cfg.Interval <= 0 {
		return fmt.Errorf("interval must be positive, got %v", a.cfg.Interval)
	}
	a.tickOnce(ctx)
	t := time.NewTicker(a.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			a.tickOnce(ctx)
		}
	}
}

// tickOnce runs one Tick and logs a failure unless ctx is already done, so a
// rollout does not emit an ERROR for the request SIGTERM interrupted.
func (a *Agent) tickOnce(ctx context.Context) {
	if err := a.Tick(ctx); err != nil && ctx.Err() == nil {
		a.log.Error("heartbeat tick failed", "error", err)
	}
}
