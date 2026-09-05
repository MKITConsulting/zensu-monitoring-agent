package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync/atomic"

	obs "github.com/MKITConsulting/zensu-monitoring-agent/internal/metrics"
)

// Resource-metric source names, also the accepted values of
// ZENSU_MONITORING_AGENT_RESOURCE_SOURCE.
const (
	// SourceAuto tries metrics-server and falls through to the exposition only
	// when the cluster has no metrics.k8s.io API and a scrape URL is set.
	SourceAuto = "auto"
	// SourceMetricsServer reads metrics-server and never falls through.
	SourceMetricsServer = "metrics-server"
	// SourceExposition scrapes a Prometheus exposition and never reads
	// metrics-server.
	SourceExposition = "exposition"
	// SourceNone disables resource metrics; uptime reporting is unaffected.
	SourceNone = "none"
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
// the "no metrics-server" warn latch so the message is logged once over the
// agent's lifetime rather than once per service per tick.
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
	for _, t := range targets {
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
		if !available {
			continue
		}
		out[t.Slug] = []MetricSample{
			{Key: MetricCPUMillicores, Value: float64(cpu)},
			{Key: MetricMemoryBytes, Value: float64(mem)},
		}
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

// autoSource reads metrics-server and switches permanently to the exposition
// the first time the cluster reports no metrics.k8s.io API. The switch is
// one-way by design: a cluster does not grow a metrics-server mid-flight, and
// re-probing every tick would log a warning every tick.
type autoSource struct {
	primary  MetricSource
	fallback MetricSource
	log      *slog.Logger
	switched atomic.Bool
}

func (s *autoSource) Name() string {
	if s.switched.Load() {
		return s.fallback.Name()
	}
	return s.primary.Name()
}

func (s *autoSource) Samples(ctx context.Context, targets []ServiceTarget) (map[string][]MetricSample, error) {
	if s.switched.Load() {
		return s.fallback.Samples(ctx, targets)
	}

	out, err := s.primary.Samples(ctx, targets)
	if !errors.Is(err, ErrSourceUnavailable) {
		return out, err
	}

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
	if parsed.Host == "" {
		return errors.New("needs a host")
	}
	return nil
}
