package metrics

import (
	"context"
	"net/http"
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
}

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
	}
	reg.MustRegister(m.heartbeats, m.lastSuccess, m.postDuration, m.servicesReported)
	reg.MustRegister(m.scrapes, m.scrapeDuration, m.resourceServicesMapped)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.heartbeats.WithLabelValues("success")
	m.heartbeats.WithLabelValues("error")
	m.scrapes.WithLabelValues("success")
	m.scrapes.WithLabelValues("error")
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
