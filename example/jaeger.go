package main

import (
	"context"
	"fmt"
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
)

func JaegerInit(serviceName string) (opentracing.Tracer, io.Closer, error) {
	cfg := &config.Configuration{
		ServiceName: serviceName,
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans:           true,
			LocalAgentHostPort: "127.0.0.1:6831",
		},
	}
	tracer, closer, err := cfg.NewTracer()
	if err != nil {
		return nil, nil, err
	}

	opentracing.SetGlobalTracer(tracer)
	return tracer, closer, nil
}

func GetSpanInfo(span opentracing.Span) string {
	spanCtx := span.Context()
	if sp, ok := spanCtx.(jaeger.SpanContext); ok {
		return fmt.Sprintf("TraceID='%v' SpanID='%v' ParentID='%v'",
			sp.TraceID().String(), sp.SpanID().String(), sp.ParentID().String())
	}
	return "print span failed"
}

func GetSpanInfoFromContext(ctx context.Context) string {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return "context lack of span"
	}
	return GetSpanInfo(span)
}
