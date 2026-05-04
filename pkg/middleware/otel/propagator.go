package otel

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/ModulationAI/agentflowbus/pkg/bus"
	"github.com/ModulationAI/agentflowbus/pkg/event"
)

// EnvelopePreparer returns a bus.EnvelopePreparer that injects the current
// OpenTelemetry SpanContext into every outbound envelope (Publish / Invoke /
// StreamInvoke). Wire it via:
//
//	b, _ := bus.New(
//	    // ...
//	    bus.WithEnvelopePreparer(otel.EnvelopePreparer()),
//	)
//
// The preparer is a no-op when ctx carries no valid span — direct user
// calls outside any traced flow leave the envelope untouched.
func EnvelopePreparer(opts ...Option) bus.EnvelopePreparer {
	cfg := newConfig(opts...)
	return func(ctx context.Context, e *event.Envelope) {
		if e == nil {
			return
		}
		if !trace.SpanFromContext(ctx).SpanContext().IsValid() {
			return
		}
		cfg.propagator.Inject(ctx, envelopeCarrier{e})
	}
}
