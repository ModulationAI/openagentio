// Package otel bridges OpenAgentIO middleware to OpenTelemetry. Trace()
// rebuilds an upstream SpanContext from envelope.traceparent and starts a
// child span around handler execution; EnvelopePreparer() injects the active
// span back into outbound envelopes so cross-process traces stay linked.
//
// The package is opt-in: importing it pulls go.opentelemetry.io/otel into
// the dependency tree. pkg/bus itself stays OTel-free.
package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
)

// Trace returns a middleware that, on every handler invocation:
//
//  1. Extracts an upstream SpanContext from envelope.Traceparent via the
//     configured propagator.
//  2. Starts a Consumer-kind span named "acp.handle.<event_type>".
//  3. Sets messaging-semconv attributes plus acp.* extensions.
//  4. Records errors and marks span status accordingly when the inner
//     handler returns a non-nil error.
//
// If no TracerProvider is configured globally and no Option overrides it,
// OTel returns a Noop tracer and the middleware adds negligible overhead.
func Trace(opts ...Option) middleware.Middleware {
	cfg := newConfig(opts...)
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, e *event.Envelope) error {
			if e == nil {
				return next(ctx, e)
			}
			ctx = cfg.propagator.Extract(ctx, envelopeCarrier{e})

			ctx, span := cfg.tracer.Start(ctx, "acp.handle."+e.EventType,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String("acp"),
					semconv.MessagingDestinationName(e.EventType),
					semconv.MessagingMessageID(e.EventID),
					attribute.String("acp.event_type", e.EventType),
					attribute.String("acp.tenant_id", e.TenantID),
					attribute.String("acp.session_id", e.SessionID),
					attribute.String("acp.conversation_id", e.ConversationID),
				))
			defer span.End()

			err := next(ctx, e)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return err
		}
	}
}
