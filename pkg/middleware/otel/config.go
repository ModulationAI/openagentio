package otel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation library identifier reported on every
// span. Following OTel convention, it matches the import path of the
// instrumentation package.
const tracerName = "github.com/ModulationAI/openagentio/pkg/middleware/otel"

// Option configures Trace and EnvelopePreparer. Sensible defaults are used
// when no Option is supplied: the global TracerProvider and the global
// TextMapPropagator (which Anthropic-style apps wire to W3C Trace Context
// during OTel SDK init).
type Option func(*config)

type config struct {
	tracer     trace.Tracer
	provider   trace.TracerProvider
	propagator propagation.TextMapPropagator
}

// WithTracerProvider overrides the TracerProvider. Useful for tests that
// want a SpanRecorder without touching the global provider.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) { c.provider = tp }
}

// WithPropagator overrides the TextMapPropagator. Default is
// otel.GetTextMapPropagator(), which standard OTel init wires to W3C Trace
// Context.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) { c.propagator = p }
}

func newConfig(opts ...Option) *config {
	c := &config{}
	for _, o := range opts {
		o(c)
	}
	if c.provider == nil {
		c.provider = otel.GetTracerProvider()
	}
	c.tracer = c.provider.Tracer(tracerName)
	if c.propagator == nil {
		c.propagator = otel.GetTextMapPropagator()
	}
	return c
}
