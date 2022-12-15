package jaegersarama

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
)

const (
	jaegerTraceparentHeader = "Uber-Trace-Id"
)

// Inject returns context contained span whose parent span comes from
// ctx. Do nothing if no parent span in ctx.
func Inject(ctx context.Context, msg *sarama.ProducerMessage) (context.Context, error) {
	var span opentracing.Span
	parentSpan := opentracing.SpanFromContext(ctx)
	if parentSpan == nil {
		return ctx, nil
	} else {
		span, _ = opentracing.StartSpanFromContext(ctx, msg.Topic)
	}
	defer span.Finish()
	ctx = opentracing.ContextWithSpan(ctx, span)
	span.SetTag("span.kind", "producer")
	span.SetTag("peer.service", "kafka")
	spanCtx := span.Context()

	if sp, ok := spanCtx.(jaeger.SpanContext); ok {
		value := fmt.Sprintf("%v:%v:%v:%v", sp.TraceID().String(), sp.SpanID().String(),
			sp.ParentID().String(), sp.Flags())
		msg.Headers = append(msg.Headers, sarama.RecordHeader{[]byte(jaegerTraceparentHeader), []byte(value)})
	}
	return ctx, nil
}

// Extract returns context contained span whose parent span comes from
// msg "Uber-Trace-Id" header. Do nothing if msg lack of "Uber-Trace-Id" header.
func Extract(ctx context.Context, msg *sarama.ConsumerMessage) (context.Context, error) {
	if !opentracing.IsGlobalTracerRegistered() {
		return ctx, nil
	}

	tracer := opentracing.GlobalTracer()
	for _, h := range msg.Headers {
		if string(h.Key) == jaegerTraceparentHeader {
			header := http.Header{jaegerTraceparentHeader: []string{string(h.Value)}}
			spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(header))
			span := tracer.StartSpan(msg.Topic, opentracing.ChildOf(spanCtx))
			defer span.Finish()
			ctx = opentracing.ContextWithSpan(ctx, span)
			span.SetTag("span.kind", "consumer")
			span.SetTag("peer.service", "kafka")
			return ctx, nil
		}
	}

	return ctx, nil
}
