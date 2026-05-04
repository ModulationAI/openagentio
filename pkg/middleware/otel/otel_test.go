package otel_test

import (
	"context"
	"errors"
	"testing"

	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	otelapitrace "go.opentelemetry.io/otel/trace"

	"github.com/ModulationAI/agentflowbus/pkg/event"
	otelmw "github.com/ModulationAI/agentflowbus/pkg/middleware/otel"
)

// newRecorder builds a TracerProvider that captures every finished span in
// memory. Returns the provider and the recorder so tests can assert on
// span shape after the handler returns.
func newRecorder() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	return tp, rec
}

// makeEnvelope returns a minimal valid envelope with the given event_type
// plus identity fields the middleware should record on the span.
func makeEnvelope(eventType string) *event.Envelope {
	e := event.New(eventType)
	e.TenantID = "tenant-X"
	e.SessionID = "sess-X"
	e.ConversationID = "conv-X"
	return e
}

func TestTraceExtractsParentFromEnvelope(t *testing.T) {
	tp, rec := newRecorder()

	// 1. Open a parent span on the same provider so we know its trace id.
	parentCtx, parentSpan := tp.Tracer("test-parent").
		Start(context.Background(), "parent")
	wantTraceID := parentSpan.SpanContext().TraceID()
	parentSpan.End()

	// 2. Inject the parent's traceparent into a fresh envelope.
	prop := propagation.TraceContext{}
	env := makeEnvelope(event.MessageReceived)
	prop.Inject(parentCtx, &headerCarrier{e: env})

	// 3. Run the middleware against an empty ctx — the only link to the
	//    parent is via envelope.Traceparent.
	mw := otelmw.Trace(otelmw.WithTracerProvider(tp), otelmw.WithPropagator(prop))
	handler := mw(func(_ context.Context, _ *event.Envelope) error { return nil })
	if err := handler(context.Background(), env); err != nil {
		t.Fatalf("handler: %v", err)
	}

	// 4. Recorded child span must point at the parent trace.
	spans := rec.Ended()
	if len(spans) < 1 {
		t.Fatalf("no child span recorded")
	}
	child := lastNamed(spans, "acp.handle."+event.MessageReceived)
	if child == nil {
		t.Fatalf("missing acp.handle span; got %v", spanNames(spans))
	}
	if got := child.SpanContext().TraceID(); got != wantTraceID {
		t.Fatalf("trace id = %s want %s", got, wantTraceID)
	}
}

func TestTraceRecordsErrorOnHandlerFailure(t *testing.T) {
	tp, rec := newRecorder()
	mw := otelmw.Trace(
		otelmw.WithTracerProvider(tp),
		otelmw.WithPropagator(propagation.TraceContext{}),
	)

	boom := errors.New("kaboom")
	handler := mw(func(_ context.Context, _ *event.Envelope) error { return boom })
	if err := handler(context.Background(), makeEnvelope(event.ResponseError)); err != boom {
		t.Fatalf("middleware swallowed error: %v", err)
	}

	span := lastNamed(rec.Ended(), "acp.handle."+event.ResponseError)
	if span == nil {
		t.Fatal("no span recorded")
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("status = %v want Error", span.Status().Code)
	}
	if span.Status().Description != boom.Error() {
		t.Fatalf("status desc = %q want %q", span.Status().Description, boom.Error())
	}
	// RecordError adds an "exception" event.
	found := false
	for _, ev := range span.Events() {
		if ev.Name == "exception" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected exception event on span")
	}
}

func TestTraceAttributesSet(t *testing.T) {
	tp, rec := newRecorder()
	mw := otelmw.Trace(otelmw.WithTracerProvider(tp))
	handler := mw(func(_ context.Context, _ *event.Envelope) error { return nil })

	env := makeEnvelope(event.MessageReceived)
	if err := handler(context.Background(), env); err != nil {
		t.Fatalf("handler: %v", err)
	}

	span := lastNamed(rec.Ended(), "acp.handle."+event.MessageReceived)
	if span == nil {
		t.Fatal("no span recorded")
	}

	attrs := indexAttrs(span.Attributes())
	wantStrings := map[attribute.Key]string{
		semconv.MessagingSystemKey:               "acp",
		semconv.MessagingDestinationNameKey:      event.MessageReceived,
		semconv.MessagingMessageIDKey:            env.EventID,
		attribute.Key("acp.event_type"):          event.MessageReceived,
		attribute.Key("acp.tenant_id"):           "tenant-X",
		attribute.Key("acp.session_id"):          "sess-X",
		attribute.Key("acp.conversation_id"):     "conv-X",
	}
	for k, want := range wantStrings {
		if got := attrs[k]; got != want {
			t.Errorf("attr %q = %q want %q", k, got, want)
		}
	}
	if span.SpanKind() != otelapitrace.SpanKindConsumer {
		t.Errorf("span kind = %v want Consumer", span.SpanKind())
	}
}

func TestEnvelopePreparerInjectsTraceparent(t *testing.T) {
	tp, _ := newRecorder()
	otelapi.SetTextMapPropagator(propagation.TraceContext{})
	defer otelapi.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	prep := otelmw.EnvelopePreparer(otelmw.WithTracerProvider(tp))

	ctx, span := tp.Tracer("test").Start(context.Background(), "outbound")
	defer span.End()

	env := makeEnvelope(event.MessageReceived)
	prep(ctx, env)

	if env.Traceparent == "" {
		t.Fatal("preparer did not inject traceparent")
	}

	// Decode the traceparent and confirm it matches the active span's trace id.
	carrier := &headerCarrier{e: env}
	parsed := propagation.TraceContext{}.Extract(context.Background(), carrier)
	got := otelapitrace.SpanContextFromContext(parsed)
	if got.TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("traceparent trace id = %s want %s", got.TraceID(), span.SpanContext().TraceID())
	}
}

func TestEnvelopePreparerNoopWithoutSpan(t *testing.T) {
	prep := otelmw.EnvelopePreparer(otelmw.WithPropagator(propagation.TraceContext{}))
	env := makeEnvelope(event.MessageReceived)
	env.Traceparent = "preserved"
	prep(context.Background(), env)
	if env.Traceparent != "preserved" {
		t.Fatalf("preparer mutated envelope without active span: %q", env.Traceparent)
	}
}

// --- helpers --------------------------------------------------------------

// headerCarrier adapts an Envelope's Traceparent field to the propagation
// API; mirrors the package-internal carrier so tests don't need to import
// internal types.
type headerCarrier struct{ e *event.Envelope }

func (c *headerCarrier) Get(key string) string {
	if key == "traceparent" {
		return c.e.Traceparent
	}
	return ""
}

func (c *headerCarrier) Set(key, val string) {
	if key == "traceparent" {
		c.e.Traceparent = val
	}
}

func (c *headerCarrier) Keys() []string { return []string{"traceparent"} }

func lastNamed(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for i := len(spans) - 1; i >= 0; i-- {
		if spans[i].Name() == name {
			return spans[i]
		}
	}
	return nil
}

func spanNames(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.Name()
	}
	return out
}

func indexAttrs(kvs []attribute.KeyValue) map[attribute.Key]string {
	out := make(map[attribute.Key]string, len(kvs))
	for _, kv := range kvs {
		out[kv.Key] = kv.Value.AsString()
	}
	return out
}
