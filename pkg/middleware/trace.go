package middleware

import (
	"context"

	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/session"
)

// Trace injects the envelope into ctx so downstream handlers (and any nested
// Bus calls) can propagate trace_id / session_id without re-parsing the
// message. v0.2 will add an OTelTrace middleware that additionally bridges
// to OpenTelemetry SpanContext via the envelope's traceparent field.
func Trace() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, e *event.Envelope) error {
			ctx = session.Inject(ctx, e)
			return next(ctx, e)
		}
	}
}
