package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync/atomic"
	"time"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
)

// Resource-metric source names, also the accepted values of
// ZENSU_MONITORING_AGENT_RESOURCE_SOURCE. Every name a Name() can return is an
// alias of the canonical constant in the metrics package, so the exported label
// set and the names reported into it cannot drift apart.
const (
	// SourceAuto tries metrics-server and falls through to the exposition only
	// when the cluster has no metrics.k8s.io API and a scrape URL is set. It is
	// a configuration value only: autoSource reports whichever of the two it is
	// currently on, never this name, which is why it is absent from
	// obs.ResourceSources.
	SourceAuto = "auto"
	// SourceMetricsServer reads metrics-server and never falls through.
	SourceMetricsServer = obs.SourceMetricsServer
	// SourceExposition scrapes a Prometheus exposition and never reads
	// metrics-server.
	SourceExposition = obs.SourceExposition
	// SourceNone disables resource metrics; uptime reporting is unaffected.
	SourceNone = obs.SourceNone
)

// ServiceTarget identifies one annotated workload to a MetricSource. PodNames
// is what Collect already listed for restartCount, reused here so a source that
// reads pod-labelled data does not have to query the cluster again.
type ServiceTarget struct {
	Slug      string
	Namespace string
	Selector  string
	PodNames  []string
}

// ErrSourceUnavailable reports that a source cannot serve this cluster at all,
// as opposed to failing one read. It is the seam-level signal the composite
// source falls through on; each implementation wraps it in its own concrete
// reason, so a future primary need not borrow another's vocabulary.
var ErrSourceUnavailable = errors.New("metric source cannot serve this cluster")

// ErrMetricsReadsAllFailed reports that every pod-metrics read attempted this
// tick failed for a reason other than an absent API. The condition is a count,
// not a reason: a standing RBAC denial or a broken APIService produces it, and
// so does a tick in which every read happened to fail transiently, which is
// indistinguishable here and is treated the same. It wraps ErrSourceUnavailable so the composite
// falls through, because a primary that answered nothing served nothing: the
// alternative is an empty map with a nil error, which reads as a healthy tick
// and leaves a configured exposition permanently unused.
var ErrMetricsReadsAllFailed = fmt.Errorf("every pod metrics read failed this tick: %w", ErrSourceUnavailable)

// MetricSource yields resource samples for one tick, keyed by service slug.
//
// The contract is per-TICK rather than per-service on purpose: an exposition is
// scraped once and answers for every service at once, so a per-service call
// would multiply one HTTP request into N.
//
// Implementations may be stateful across ticks — the exposition source
// remembers the previous observation of every counter — so a source must be
// built once and kept for the agent's lifetime, never per tick.
//
// Returning an error that wraps ErrSourceUnavailable is not a failure: it
// reports that this source cannot serve the cluster, which is what lets the
// composite source fall through to the next one.
type MetricSource interface {
	Name() string
	Samples(ctx context.Context, targets []ServiceTarget) (map[string][]MetricSample, error)
}

// metricsServerSource reads per-service CPU/memory from metrics-server. It owns
// the "no metrics-server" warn latch so the message is logged once per OUTAGE
// rather than once per tick; suppressing the repeat WITHIN a tick is the early
// return's doing, not the latch's. The latch clears only after a tick in which
// some target's read returned no error — data or not, since an empty result
// still proves the API answered. An API that goes away, returns and goes away
// again is two incidents, and silencing the second would leave an operator
// reading a log that says the problem happened once, hours ago; resetting per
// target instead would re-arm on any tick mixing a healthy target with an
// unavailable one, logging one standing outage forever.
type metricsServerSource struct {
	reader ClusterReader
	log    *slog.Logger
	warned atomic.Bool
}

func newMetricsServerSource(reader ClusterReader, log *slog.Logger) *metricsServerSource {
	if log == nil {
		log = slog.Default()
	}
	return &metricsServerSource{reader: reader, log: log}
}

func (s *metricsServerSource) Name() string { return SourceMetricsServer }

// Samples reads every target's pod metrics. A transient error skips that one
// target; an absent metrics.k8s.io API aborts the whole tick's metric read,
// because the API is missing cluster-wide and the remaining calls would each
// fail identically.
func (s *metricsServerSource) Samples(ctx context.Context, targets []ServiceTarget) (map[string][]MetricSample, error) {
	out := make(map[string][]MetricSample, len(targets))
	attempted, answered := false, false
	for _, t := range targets {
		if t.Selector == "" {
			continue
		}
		attempted = true
		cpu, mem, available, err := s.reader.PodMetricsForSelector(ctx, t.Namespace, t.Selector)
		if err != nil {
			if errors.Is(err, ErrMetricsAPIUnavailable) {
				if s.warned.CompareAndSwap(false, true) {
					s.log.Warn("metrics-server (metrics.k8s.io) API not available")
				}
				return nil, ErrMetricsAPIUnavailable
			}
			s.log.Warn("read pod metrics failed; skipping metrics this tick", "service", t.Slug, "error", err)
			continue
		}
		answered = true
		if !available {
			continue
		}
		out[t.Slug] = []MetricSample{
			{Key: MetricCPUMillicores, Value: float64(cpu)},
			{Key: MetricMemoryBytes, Value: float64(mem)},
		}
	}
	if answered {
		s.warned.Store(false)
	}
	if attempted && !answered {
		return nil, ErrMetricsReadsAllFailed
	}
	return out, nil
}

// noneSource disables resource metrics. Uptime, replica counts and restartCount
// are unaffected.
type noneSource struct{}

func (noneSource) Name() string { return SourceNone }

func (noneSource) Samples(context.Context, []ServiceTarget) (map[string][]MetricSample, error) {
	return nil, nil
}

// autoRetryInterval is how long the composite stays on the fallback before it
// re-probes the primary.
const autoRetryInterval = 5 * time.Minute

// autoSource reads metrics-server and switches to the exposition when the
// cluster reports no metrics.k8s.io API. The switch is reversible on a cooldown
// rather than permanent, because "unavailable" is not proof the cluster will
// never serve it: a metrics-server rollout answers NotFound for the length of
// one deployment, and an operator who installs metrics-server later expects the
// agent to pick it up without a restart. Re-probing on a cooldown keeps that
// recovery without probing — or logging — every tick.
type autoSource struct {
	primary    MetricSource
	fallback   MetricSource
	log        *slog.Logger
	now        func() time.Time
	switched   atomic.Bool
	switchedAt atomic.Int64
}

// clock reads the injected time source, defaulting to the wall clock.
func (s *autoSource) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// dueForRetry reports whether the cooldown since the last unavailable primary
// read has elapsed.
func (s *autoSource) dueForRetry() bool {
	last := s.switchedAt.Load()
	return last == 0 || s.clock().UnixNano()-last >= int64(autoRetryInterval)
}

func (s *autoSource) Name() string {
	if s.switched.Load() {
		return s.fallback.Name()
	}
	return s.primary.Name()
}

func (s *autoSource) Samples(ctx context.Context, targets []ServiceTarget) (map[string][]MetricSample, error) {
	if s.switched.Load() && !s.dueForRetry() {
		return s.fallback.Samples(ctx, targets)
	}

	out, err := s.primary.Samples(ctx, targets)
	if !errors.Is(err, ErrSourceUnavailable) {
		if err == nil && s.switched.CompareAndSwap(true, false) {
			s.log.Info("primary resource source is available again; reading CPU/memory from it",
				"primary", s.primary.Name(), "fallback", s.fallback.Name())
		}
		return out, err
	}

	s.switchedAt.Store(s.clock().UnixNano())
	if s.switched.CompareAndSwap(false, true) {
		s.log.Info("primary resource source unavailable; reading CPU/memory from the configured exposition instead",
			"primary", s.primary.Name(), "fallback", s.fallback.Name())
	}
	return s.fallback.Samples(ctx, targets)
}

// SourceConfig selects and configures the resource-metric source.
type SourceConfig struct {
	Mode       string
	Exposition ExpositionConfig
}

// NewMetricSource builds the resource-metric source for the configured mode.
// An unusable configuration is an error here rather than a silent degrade at
// runtime: an operator who sets exposition mode without a URL wants to know at
// startup, not by finding empty charts a day later.
func NewMetricSource(cfg SourceConfig, reader ClusterReader, log *slog.Logger, metrics *obs.Metrics) (MetricSource, error) {
	if log == nil {
		log = slog.Default()
	}

	switch mode := cfg.Mode; mode {
	case SourceNone:
		return noneSource{}, nil

	case SourceMetricsServer:
		return newMetricsServerSource(reader, log), nil

	case SourceExposition:
		if cfg.Exposition.URL == "" {
			return nil, fmt.Errorf("resource source %q requires a scrape URL", mode)
		}
		if err := ValidateScrapeURL(cfg.Exposition.URL); err != nil {
			return nil, err
		}
		return newExpositionSource(cfg.Exposition, log, metrics), nil

	case SourceAuto, "":
		if cfg.Exposition.URL == "" {
			return newMetricsServerSource(reader, log), nil
		}
		if err := ValidateScrapeURL(cfg.Exposition.URL); err != nil {
			return nil, err
		}
		return &autoSource{
			primary:  newMetricsServerSource(reader, log),
			fallback: newExpositionSource(cfg.Exposition, log, metrics),
			log:      log,
		}, nil

	default:
		return nil, fmt.Errorf("unknown resource source %q (want %s, %s, %s or %s)",
			mode, SourceAuto, SourceMetricsServer, SourceExposition, SourceNone)
	}
}

// ValidateScrapeURL rejects a URL the HTTP client could never use. Without it a
// value missing its scheme — an easy Helm mistake — passes startup and then
// fails inside every request, which is exactly the silent runtime degrade the
// source factory exists to prevent.
//
// It is exported so the caller can apply it whenever the value is set, not only
// in the modes that scrape: the chart writes the ConfigMap key in every mode, so
// a credential in an unused scrape URL is stored in clear text just the same.
func ValidateScrapeURL(raw string) error {
	if err := validateURL(raw); err != nil {
		return fmt.Errorf("%w; set ZENSU_MONITORING_AGENT_SCRAPE_URL to a plain http(s) endpoint", err)
	}
	return nil
}

// validateURL holds the shared rules. No message echoes the configured value:
// the error reaches the agent's log stream, which is usually shipped further
// than the ConfigMap these checks exist to keep secrets out of, and a
// credentialed URL can reach any branch — for "user:secret@host" url.Parse
// yields scheme "user" with User nil, so the credential branch does not see it
// and a redacted form would still print the password. The caller names the
// variable instead.
func validateURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return errors.New("not a valid URL")
	}
	if parsed.User != nil {
		return errors.New("carries credentials, which are unsupported and would be stored in a ConfigMap in clear text")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("needs an http:// or https:// scheme")
	}
	if parsed.Hostname() == "" {
		return errors.New("needs a host")
	}
	return nil
}
