package metrics

import "context"

// MetricType represents the type of Prometheus metric
type MetricType int

const (
	Counter MetricType = iota
	Gauge
	Histogram
	Summary
)

// CustomMetric represents a custom metric definition
type CustomMetric struct {
	Name        string
	Description string
	Type        MetricType
	Labels      []string
}

// Collector interface for metrics collection
type Collector interface {
	IncrementCounter(ctx context.Context, name string, labels map[string]string, value float64)
	SetGauge(ctx context.Context, name string, labels map[string]string, value float64)
	ObserveHistogram(ctx context.Context, name string, labels map[string]string, value float64)
	ObserveSummary(ctx context.Context, name string, labels map[string]string, value float64)
	RegisterCustomMetrics(metrics ...CustomMetric) error
	GetMetricsHandler() interface{}
}
