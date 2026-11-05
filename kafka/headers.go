package kafka

import (
	"context"
	"maps"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// injectTrace returns a copy of headers with ctx's trace context (traceparent,
// tracestate) added by the global OpenTelemetry propagator. Without a configured
// propagator it adds nothing.
func injectTrace(ctx context.Context, headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers)+2)
	maps.Copy(out, headers)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(out))
	return out
}

// extractTrace returns ctx with the trace context carried in headers, so a handler's
// spans continue the producer's trace.
func extractTrace(ctx context.Context, headers map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}

func toKgoHeaders(h map[string]string) []kgo.RecordHeader {
	if len(h) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(h))
	for k, v := range h {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return out
}

// fromKgoHeaders converts record headers to a map. Kafka allows repeated keys; the last
// one wins.
func fromKgoHeaders(h []kgo.RecordHeader) map[string]string {
	out := make(map[string]string, len(h))
	for _, rh := range h {
		out[rh.Key] = string(rh.Value)
	}
	return out
}
