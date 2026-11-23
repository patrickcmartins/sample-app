package main

// Telemetria com OpenTelemetry: traces e métricas saem por OTLP/HTTP para o Collector.
// O app não sabe se o destino é Tempo, Prometheus ou Datadog: isso é decisão do Collector.
//
// Configuração pelas env vars padrão do OTel (nada hardcoded):
//   OTEL_EXPORTER_OTLP_ENDPOINT  ex.: http://otel-collector.observability:4318
//   OTEL_SERVICE_NAME            ex.: frontend
//   OTEL_RESOURCE_ATTRIBUTES     ex.: deployment.environment=prod,service.version=sha-abc1234
// Sem OTEL_EXPORTER_OTLP_ENDPOINT a telemetria fica desligada (no-op): o app roda igual no kind-dev.

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func setupTelemetry(ctx context.Context) (shutdown func(context.Context) error, err error) {
	// W3C tracecontext: o mesmo formato que o Istio propaga, então os spans do sidecar
	// e os do código entram no mesmo trace.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.version", version),
	))
	if err != nil {
		return nil, err
	}

	traceExp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
		// Respeita a decisão de quem chamou (o Istio/gateway); na raiz, amostra tudo.
		// Em produção de verdade: amostragem no Collector (tail sampling), não aqui.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(15*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}

// traceHandler coloca trace_id/span_id em toda linha de log que tenha contexto.
// É isso que permite pular do log (Loki) para o trace (Tempo) no Grafana.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{h.Handler.WithAttrs(a)}
}
func (h traceHandler) WithGroup(n string) slog.Handler { return traceHandler{h.Handler.WithGroup(n)} }
