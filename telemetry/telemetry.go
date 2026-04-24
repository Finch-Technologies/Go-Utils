package telemetry

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"runtime"
	runtimemetrics "runtime/metrics"
	"time"

	"github.com/finch-technologies/go-utils/env"
	"github.com/finch-technologies/go-utils/log"
	"github.com/finch-technologies/go-utils/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Options configures the OTEL telemetry providers. All fields are optional —
// zero values are filled from environment variables (see getOptions).
type Options struct {
	// Enabled controls whether telemetry is initialised. Maps to OTEL_ENABLED.
	Enabled bool
	// Host is the OTEL collector hostname. Maps to OTEL_HOST.
	Host string
	// Port is the OTEL collector port. Maps to OTEL_PORT.
	Port string
	// ServiceName is reported as the service.name resource attribute. Maps to OTEL_SERVICE_NAME.
	ServiceName string
	// Environment is reported as the deployment.environment attribute. Maps to ENVIRONMENT.
	Environment string
	// Protocol selects the export transport: "grpc" (port 4317) or "http" (port 4318).
	// Maps to OTEL_PROTOCOL.
	Protocol string
	// Insecure disables TLS on the collector connection. Maps to OTEL_INSECURE.
	Insecure bool
	// MetricInterval controls how often metrics are pushed to the collector.
	// Defaults to 30 seconds.
	MetricInterval time.Duration
}

// endpoint returns the host:port string used by OTLP exporters.
func (o Options) endpoint() string {
	return o.Host + ":" + o.Port
}

// Init initialises the global OTEL trace and metric providers and returns a
// shutdown function that flushes and closes them. Call the shutdown function
// before your process exits to ensure all pending telemetry is exported.
//
// When Enabled is false (the default unless OTEL_ENABLED=true), a no-op
// shutdown function is returned immediately.
//
// Usage — rely entirely on environment variables:
//
//	shutdown, err := telemetry.Init(ctx)
//
// Usage — override specific fields in code:
//
//	shutdown, err := telemetry.Init(ctx, telemetry.Options{ServiceName: "my-service"})
func Init(ctx context.Context, options ...Options) (shutdown func(context.Context) error, err error) {
	opts := getOptions(options...)

	if !opts.Enabled {
		log.Info("OTEL telemetry disabled — skipping initialisation")
		return func(context.Context) error { return nil }, nil
	}

	log.InfoFields("Initialising OTEL telemetry", map[string]any{
		"endpoint":     opts.endpoint(),
		"service_name": opts.ServiceName,
		"environment":  opts.Environment,
		"protocol":     opts.Protocol,
		"insecure":     opts.Insecure,
	})

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(opts.ServiceName),
			semconv.DeploymentEnvironment(opts.Environment),
		),
		resource.WithTelemetrySDK(), // sets telemetry.sdk.language=go → service.language.name in Elastic APM
		resource.WithProcess(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTEL resource: %w", err)
	}

	var (
		traceExp  sdktrace.SpanExporter
		metricExp sdkmetric.Exporter
	)

	switch opts.Protocol {
	case "http":
		traceExp, metricExp, err = buildHTTPExporters(ctx, opts)
	default: // "grpc"
		traceExp, metricExp, err = buildGRPCExporters(ctx, opts)
	}
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(traceExp)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(opts.MetricInterval),
		)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Error("OTEL exporter error: " + err.Error())
	}))

	if err := startRuntimeMetrics(); err != nil {
		log.Warning("OTEL runtime metrics: failed to register — " + err.Error())
	}

	log.Info("OTEL telemetry initialised — exporting to " + opts.endpoint() + " via " + opts.Protocol)

	return func(ctx context.Context) error {
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		var errs []error
		if err := tp.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("trace provider: %w", err))
		}
		if err := mp.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("metric provider: %w", err))
		}
		if len(errs) > 0 {
			return fmt.Errorf("telemetry shutdown errors: %v", errs)
		}
		log.Info("OTEL telemetry shut down — all pending data flushed")
		return nil
	}, nil
}

// getOptions merges caller-supplied options with environment-variable defaults.
// Follows the go-utils variadic options pattern.
func getOptions(options ...Options) Options {
	defaults := Options{
		Enabled:        env.GetOrDefault("OTEL_ENABLED", "false") == "true",
		Host:           env.GetOrDefault("OTEL_HOST", "localhost"),
		Port:           env.GetOrDefault("OTEL_PORT", "4317"),
		ServiceName:    env.GetOrDefault("OTEL_SERVICE_NAME", "service"),
		Environment:    env.GetOrDefault("ENVIRONMENT", "local"),
		Protocol:       env.GetOrDefault("OTEL_PROTOCOL", "http"),
		Insecure:       env.GetOrDefault("OTEL_INSECURE", "true") == "true",
		MetricInterval: 1 * time.Minute,
	}

	if len(options) == 0 {
		return defaults
	}

	opts := options[0]
	utils.MergeObjects(&opts, defaults)
	return opts
}

// buildGRPCExporters creates OTLP trace and metric exporters over gRPC (default port 4317).
func buildGRPCExporters(ctx context.Context, opts Options) (sdktrace.SpanExporter, sdkmetric.Exporter, error) {
	dialOpts := []grpc.DialOption{}
	if opts.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	probeGRPC(ctx, opts.endpoint(), dialOpts)

	// Use a shared pre-configured connection so the exporters cannot override
	// the transport credentials with their own defaults.
	conn, err := grpc.NewClient(opts.endpoint(), dialOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	traceExp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC trace exporter: %w", err)
	}

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithGRPCConn(conn),
		otlpmetricgrpc.WithTemporalitySelector(deltaTemporalitySelector),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC metric exporter: %w", err)
	}

	return traceExp, metricExp, nil
}

// buildHTTPExporters creates OTLP trace and metric exporters over HTTP/protobuf (default port 4318).
func buildHTTPExporters(ctx context.Context, opts Options) (sdktrace.SpanExporter, sdkmetric.Exporter, error) {
	scheme := "https"
	if opts.Insecure {
		scheme = "http"
	}
	baseURL := scheme + "://" + opts.endpoint()

	httpClient := &http.Client{Timeout: 10 * time.Second}
	if opts.Insecure {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		}
	}

	probeHTTP(ctx, baseURL, httpClient)

	insecureOpt := func() otlptracehttp.Option {
		if opts.Insecure {
			return otlptracehttp.WithInsecure()
		}
		return otlptracehttp.WithTLSClientConfig(nil)
	}()

	traceExp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(opts.endpoint()),
		otlptracehttp.WithHTTPClient(httpClient),
		insecureOpt,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create HTTP trace exporter: %w", err)
	}

	metricInsecureOpt := func() otlpmetrichttp.Option {
		if opts.Insecure {
			return otlpmetrichttp.WithInsecure()
		}
		return otlpmetrichttp.WithTLSClientConfig(nil)
	}()

	metricExp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(opts.endpoint()),
		otlpmetrichttp.WithHTTPClient(httpClient),
		metricInsecureOpt,
		otlpmetrichttp.WithTemporalitySelector(deltaTemporalitySelector),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create HTTP metric exporter: %w", err)
	}

	return traceExp, metricExp, nil
}

// probeGRPC dials the OTLP gRPC endpoint and waits up to 3 s for Ready or
// TransientFailure, then logs the outcome. Never blocks Init on failure.
func probeGRPC(ctx context.Context, endpoint string, dialOpts []grpc.DialOption) {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(endpoint, dialOpts...)
	if err != nil {
		log.Warning("OTEL gRPC probe: failed to create client for " + endpoint + ": " + err.Error())
		return
	}
	defer conn.Close()

	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			log.Info("OTEL gRPC probe: collector reachable at " + endpoint)
			return
		}
		if state == connectivity.TransientFailure {
			log.Warning("OTEL gRPC probe: collector unreachable at " + endpoint + " — will retry in background")
			return
		}
		if !conn.WaitForStateChange(probeCtx, state) {
			log.Warning("OTEL gRPC probe: timed out connecting to " + endpoint + " (state: " + state.String() + ") — will retry in background")
			return
		}
	}
}

// probeHTTP sends a HEAD request to the OTLP HTTP base URL and logs the outcome.
// Never blocks Init on failure.
func probeHTTP(ctx context.Context, baseURL string, client *http.Client) {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, http.MethodHead, baseURL, nil)
	if err != nil {
		log.Warning("OTEL HTTP probe: could not build request for " + baseURL + ": " + err.Error())
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Warning("OTEL HTTP probe: collector unreachable at " + baseURL + " (" + err.Error() + ") — will retry in background")
		return
	}
	resp.Body.Close()
	log.Info(fmt.Sprintf("OTEL HTTP probe: collector reachable at %s (HTTP %d)", baseURL, resp.StatusCode))
}

// TraceFields merges the active trace_id and span_id from ctx into a copy of
// fields so that structured log entries can be correlated with APM traces.
// Returns fields unchanged when ctx holds no valid span (e.g. telemetry disabled).
//
// Usage:
//
//	log.InfoFields("webhook delivered", telemetry.TraceFields(ctx, map[string]any{
//	    "url":    job.URL,
//	    "tenant": job.Tenant,
//	}))
func TraceFields(ctx context.Context, fields map[string]any) map[string]any {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return fields
	}
	sc := span.SpanContext()
	enriched := make(map[string]any, len(fields)+2)
	for k, v := range fields {
		enriched[k] = v
	}
	enriched["trace_id"] = sc.TraceID().String()
	enriched["span_id"] = sc.SpanID().String()
	return enriched
}

// deltaTemporalitySelector returns Delta temporality for counter and histogram
// instruments so that each periodic export contains only the per-interval
// increment rather than the running total since process start. Gauges keep
// Cumulative temporality since they represent a point-in-time snapshot.
//
// This allows Kibana / Elasticsearch to sum or rate individual exports
// directly, without needing to diff successive cumulative values.
func deltaTemporalitySelector(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	switch kind {
	case sdkmetric.InstrumentKindCounter,
		sdkmetric.InstrumentKindHistogram,
		sdkmetric.InstrumentKindObservableCounter:
		return metricdata.DeltaTemporality
	default:
		return metricdata.CumulativeTemporality
	}
}

// startRuntimeMetrics registers async observable instruments that sample Go
// runtime CPU and memory statistics on every metric collection cycle.
//
// Metrics emitted follow OpenTelemetry semantic conventions for Go runtime:
//   - go.memory.used        (gauge, bytes)   — heap memory in use by live objects
//   - go.memory.allocated   (gauge, bytes)   — heap memory allocated by the application
//   - go.memory.allocations (counter, count) — cumulative heap allocation count
//   - go.memory.gc.goal     (gauge, bytes)   — target heap size for the next GC cycle
//   - go.goroutine.count    (gauge, count)   — live goroutines
//   - go.processor.limit    (gauge, count)   — GOMAXPROCS (OS threads for Go code)
//   - go.cpu.time           (counter, seconds) — cumulative CPU time
func startRuntimeMetrics() error {
	m := otel.Meter("go.runtime")

	memUsed, err := m.Int64ObservableGauge("go.memory.used",
		metric.WithDescription("Heap memory in use by live objects"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return err
	}

	memAllocated, err := m.Int64ObservableGauge("go.memory.allocated",
		metric.WithDescription("Heap memory allocated by the application"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return err
	}

	memAllocations, err := m.Int64ObservableCounter("go.memory.allocations",
		metric.WithDescription("Cumulative count of heap allocations"),
	)
	if err != nil {
		return err
	}

	memGCGoal, err := m.Int64ObservableGauge("go.memory.gc.goal",
		metric.WithDescription("Target heap size for the next GC cycle"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return err
	}

	goroutines, err := m.Int64ObservableGauge("go.goroutine.count",
		metric.WithDescription("Number of live goroutines"),
	)
	if err != nil {
		return err
	}

	processorLimit, err := m.Int64ObservableGauge("go.processor.limit",
		metric.WithDescription("Number of OS threads that can execute user-level Go code simultaneously (GOMAXPROCS)"),
	)
	if err != nil {
		return err
	}

	cpuTime, err := m.Float64ObservableCounter("go.cpu.time",
		metric.WithDescription("Cumulative CPU time consumed by the Go process (user + system + GC)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	// Pre-allocate the runtime/metrics sample slice — reused on every callback.
	cpuSamples := []runtimemetrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		o.ObserveInt64(memUsed, int64(ms.HeapInuse))
		o.ObserveInt64(memAllocated, int64(ms.HeapAlloc))
		o.ObserveInt64(memAllocations, int64(ms.Mallocs))
		o.ObserveInt64(memGCGoal, int64(ms.NextGC))
		o.ObserveInt64(goroutines, int64(runtime.NumGoroutine()))
		o.ObserveInt64(processorLimit, int64(runtime.GOMAXPROCS(0)))

		runtimemetrics.Read(cpuSamples)
		if cpuSamples[0].Value.Kind() == runtimemetrics.KindFloat64 {
			o.ObserveFloat64(cpuTime, cpuSamples[0].Value.Float64())
		}

		return nil
	}, memUsed, memAllocated, memAllocations, memGCGoal, goroutines, processorLimit, cpuTime)

	return err
}
