package metrics

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type PrometheusCollector struct {
	registry   *prometheus.Registry
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
	summaries  map[string]*prometheus.SummaryVec
	mu         sync.Mutex
	namespace  string
}

func NewPrometheusCollector(namespace string) *PrometheusCollector {
	return &PrometheusCollector{
		registry:   prometheus.NewRegistry(),
		counters:   make(map[string]*prometheus.CounterVec),
		gauges:     make(map[string]*prometheus.GaugeVec),
		histograms: make(map[string]*prometheus.HistogramVec),
		summaries:  make(map[string]*prometheus.SummaryVec),
		namespace:  namespace,
	}
}

func (p *PrometheusCollector) IncrementCounter(ctx context.Context, name string, labels map[string]string, value float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	counter, exists := p.counters[name]
	if !exists {
		return
	}

	counter.With(labels).Add(value)
}

func (p *PrometheusCollector) SetGauge(ctx context.Context, name string, labels map[string]string, value float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	gauge, exists := p.gauges[name]
	if !exists {
		return
	}

	gauge.With(labels).Set(value)
}

func (p *PrometheusCollector) ObserveHistogram(ctx context.Context, name string, labels map[string]string, value float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	histogram, exists := p.histograms[name]
	if !exists {
		return
	}

	histogram.With(labels).Observe(value)
}

func (p *PrometheusCollector) ObserveSummary(ctx context.Context, name string, labels map[string]string, value float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	summary, exists := p.summaries[name]
	if !exists {
		return
	}

	summary.With(labels).Observe(value)
}

func (p *PrometheusCollector) RegisterCustomMetrics(metrics ...CustomMetric) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, metric := range metrics {
		switch metric.Type {
		case Counter:
			counter := prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Namespace: p.namespace,
					Name:      metric.Name,
					Help:      metric.Description,
				},
				metric.Labels,
			)
			if err := p.registry.Register(counter); err != nil {
				return err
			}
			p.counters[metric.Name] = counter

		case Gauge:
			gauge := prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: p.namespace,
					Name:      metric.Name,
					Help:      metric.Description,
				},
				metric.Labels,
			)
			if err := p.registry.Register(gauge); err != nil {
				return err
			}
			p.gauges[metric.Name] = gauge

		case Histogram:
			// Use custom buckets to reduce data volume
			var buckets []float64
			if metric.Name == "proxy_response_time_seconds" {
				// Custom buckets for response times (in seconds)
				buckets = []float64{0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0}
			} else if metric.Name == "health_check_duration_seconds" {
				// Custom buckets for health check duration
				buckets = []float64{1.0, 5.0, 10.0, 30.0, 60.0, 120.0}
			} else {
				// Default buckets for other histograms
				buckets = prometheus.DefBuckets
			}

			histogram := prometheus.NewHistogramVec(
				prometheus.HistogramOpts{
					Namespace: p.namespace,
					Name:      metric.Name,
					Help:      metric.Description,
					Buckets:   buckets,
				},
				metric.Labels,
			)
			if err := p.registry.Register(histogram); err != nil {
				return err
			}
			p.histograms[metric.Name] = histogram

		case Summary:
			summary := prometheus.NewSummaryVec(
				prometheus.SummaryOpts{
					Namespace: p.namespace,
					Name:      metric.Name,
					Help:      metric.Description,
				},
				metric.Labels,
			)
			if err := p.registry.Register(summary); err != nil {
				return err
			}
			p.summaries[metric.Name] = summary
		}
	}

	return nil
}

func (p *PrometheusCollector) GetMetricsHandler() interface{} {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{
		DisableCompression: true, // Disable gzip compression to avoid garbled output
	})
}
