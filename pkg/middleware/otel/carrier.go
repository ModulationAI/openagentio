package otel

import (
	"strings"

	"github.com/ModulationAI/openagentio/pkg/event"
)

// envelopeCarrier adapts an event.Envelope to OpenTelemetry's
// propagation.TextMapCarrier interface. Only the W3C `traceparent` header is
// surfaced for v0.2 — `tracestate` requires an envelope schema bump and is
// deferred (see plan §"Out of scope").
type envelopeCarrier struct{ e *event.Envelope }

func (c envelopeCarrier) Get(key string) string {
	if strings.EqualFold(key, "traceparent") {
		return c.e.Traceparent
	}
	return ""
}

func (c envelopeCarrier) Set(key, val string) {
	if strings.EqualFold(key, "traceparent") {
		c.e.Traceparent = val
	}
}

func (c envelopeCarrier) Keys() []string { return []string{"traceparent"} }
