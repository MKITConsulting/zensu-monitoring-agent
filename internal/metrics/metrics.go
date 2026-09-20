package metrics

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const DefaultAddr = ":2112"

// Metrics is the agent's own Prometheus registry and the counters it exports.
// Every method tolerates a nil receiver, so a caller that ran without
// ZENSU_MONITORING_AGENT_METRICS_ENABLED needs no branch of its own.
type Metrics struct {
	registry               *prometheus.Registry
	heartbeats             *prometheus.CounterVec
	lastSuccess            prometheus.Gauge
	postDuration           prometheus.Histogram
	servicesReported       prometheus.Gauge
	scrapes                *prometheus.CounterVec
	scrapeDuration         prometheus.Histogram
	resourceServicesMapped prometheus.Gauge
	rolesWithheld          *prometheus.GaugeVec
	resourceSamples        *prometheus.GaugeVec
	resourceSource         *prometheus.GaugeVec
}

// ResourceRoles are the per-role gauge labels. Both are pre-initialised so a
// role that never reports reads as 0 rather than as an absent series, which a
// rate() or a threshold alert cannot distinguish from a healthy quiet one.
var ResourceRoles = []string{"cpu", "memory"}

// The canonical resource-source names. They live here rather than beside the
// sources themselves because this package cannot import the one that implements
// them, while that one can import this: declaring them once is what keeps the
// exported label set and the names a source can report from drifting apart.
const (
	SourceMetricsServer = "metrics-server"
	SourceExposition    = "exposition"
	SourceNone          = "none"
	// SourceUnknown carries a name that is not one of the others. It exists so
	// drift is visible: without it an unrecognised name leaves every series at
	// 0, which a scrape cannot tell from one taken before the first tick.
	SourceUnknown = "unknown"
)

// ResourceSources are the source names resourceSource may carry. Listing them
// keeps every label pre-initialised, so "which source is this pod on" is
// answerable from one scrape rather than by waiting for the active one to
// appear.
var ResourceSources = []string{SourceMetricsServer, SourceExposition, SourceNone, SourceUnknown}

func New() *Metrics {
	return NewWithRegistry(prometheus.NewRegistry())
}

func NewWithRegistry(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		registry: reg,
		heartbeats: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "zensu_monitoring_agent_heartbeat_total",
			Help: "Total heartbeat POSTs by result (success|error).",
		}, []string{"result"}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "zensu_monitoring_agent_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful heartbeat POST.",
		}),
		postDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "zensu_monitoring_agent_post_duration_seconds",
			Help:    "Duration of heartbeat POSTs in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		servicesReported: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "zensu_monitoring_agent_services_reported",
			Help: "Number of services in the last successful heartbeat batch.",
		}),
		scrapes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "zensu_monitoring_agent_scrape_total",
			Help: "Total Prometheus-exposition scrapes by result (success|error).",
		}, []string{"result"}),
		scrapeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "zensu_monitoring_agent_scrape_duration_seconds",
			Help:    "Duration of Prometheus-exposition scrapes in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		resourceServicesMapped: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "zensu_monitoring_agent_resource_services_mapped",
			Help: "Number of services that received resource samples in the last tick, whatever the source.",
		}),
		rolesWithheld: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zensu_monitoring_agent_resource_roles_withheld",
			Help: "Number of services whose role was withheld in the last tick because not all of their rows produced a rate.",
		}, []string{"role"}),
		resourceSamples: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zensu_monitoring_agent_resource_samples",
			Help: "Number of services that received this resource role in the last tick.",
		}, []string{"role"}),
		resourceSource: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "zensu_monitoring_agent_resource_source",
			Help: "1 for the resource source in use in the last tick, 0 for the others.",
		}, []string{"source"}),
	}
	reg.MustRegister(m.heartbeats, m.lastSuccess, m.postDuration, m.servicesReported)
	reg.MustRegister(m.scrapes, m.scrapeDuration, m.resourceServicesMapped, m.rolesWithheld)
	reg.MustRegister(m.resourceSamples, m.resourceSource)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.heartbeats.WithLabelValues("success")
	m.heartbeats.WithLabelValues("error")
	m.scrapes.WithLabelValues("success")
	m.scrapes.WithLabelValues("error")
	for _, role := range ResourceRoles {
		m.rolesWithheld.WithLabelValues(role)
		m.resourceSamples.WithLabelValues(role)
	}
	for _, source := range ResourceSources {
		m.resourceSource.WithLabelValues(source)
	}
	return m
}

func (m *Metrics) RecordHeartbeat(success bool, d time.Duration) {
	if m == nil {
		return
	}
	if success {
		m.heartbeats.WithLabelValues("success").Inc()
		m.lastSuccess.SetToCurrentTime()
	} else {
		m.heartbeats.WithLabelValues("error").Inc()
	}
	m.postDuration.Observe(d.Seconds())
}

func (m *Metrics) SetServicesReported(n int) {
	if m == nil {
		return
	}
	m.servicesReported.Set(float64(n))
}

// RecordScrape counts one exposition scrape and observes its duration.
func (m *Metrics) RecordScrape(success bool, d time.Duration) {
	if m == nil {
		return
	}
	if success {
		m.scrapes.WithLabelValues("success").Inc()
	} else {
		m.scrapes.WithLabelValues("error").Inc()
	}
	m.scrapeDuration.Observe(d.Seconds())
}

// SetResourceServicesMapped records how many services received resource samples
// in the last tick. A healthy scrape with zero mapped services is the signature
// of a slug-resolution problem, which is otherwise invisible.
func (m *Metrics) SetResourceServicesMapped(n int) {
	if m == nil {
		return
	}
	m.resourceServicesMapped.Set(float64(n))
}

// SetResourceSamples records how many services received one resource role. It is
// per role because every failure mode this agent has is per role — a cumulative
// memory family that cannot be rated, a CPU role withheld for a service, a role
// that resolves to nothing — and in each the other role still reports, so a
// service-granular count reads as healthy while half the data is missing.
func (m *Metrics) SetResourceSamples(role string, n int) {
	if m == nil {
		return
	}
	m.resourceSamples.WithLabelValues(role).Set(float64(n))
}

// SetResourceSource records which source served the last tick, as 1 for that one
// and 0 for the rest. Without it the source in use is not observable at all on a
// pod that has been running for a week, and scrape_total cannot answer it: the
// counter is pre-initialised in modes that never scrape, so a zero rate reads the
// same as a healthy metrics-server tick and as an exposition that stopped. A name
// outside ResourceSources is recorded as SourceUnknown, so exactly one series is
// always 1 and an unrecognised caller shows up instead of disappearing.
func (m *Metrics) SetResourceSource(active string) {
	if m == nil {
		return
	}
	if !slices.Contains(ResourceSources, active) {
		active = SourceUnknown
	}
	for _, source := range ResourceSources {
		value := 0.0
		if source == active {
			value = 1
		}
		m.resourceSource.WithLabelValues(source).Set(value)
	}
}

// SetRolesWithheld records how many services had this role withheld. It is the
// counterpart to the withheld-role warning, which is latched and therefore
// cannot express a condition that persists or grows; this gauge can.
func (m *Metrics) SetRolesWithheld(role string, n int) {
	if m == nil {
		return
	}
	m.rolesWithheld.WithLabelValues(role).Set(float64(n))
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Serve(ctx context.Context, addr string) error {
	if m == nil {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
