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

type Metrics struct {
	registry         *prometheus.Registry
	heartbeats       *prometheus.CounterVec
	lastSuccess      prometheus.Gauge
	postDuration     prometheus.Histogram
	servicesReported prometheus.Gauge
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
	}
	reg.MustRegister(m.heartbeats, m.lastSuccess, m.postDuration, m.servicesReported)
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m.heartbeats.WithLabelValues("success")
	m.heartbeats.WithLabelValues("error")
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
