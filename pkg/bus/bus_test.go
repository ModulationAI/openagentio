package bus_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ModulationAI/agentflowbus/pkg/bus"
	"github.com/ModulationAI/agentflowbus/pkg/event"
	"github.com/ModulationAI/agentflowbus/pkg/transport/inmem"
)

// newTestBus boots a Bus on an in-memory transport. Returned cleanup must be
// deferred by callers; it closes the bus and the underlying driver.
func newTestBus(t *testing.T, agentID string, opts ...bus.Option) (bus.Bus, func()) {
	t.Helper()
	tr := inmem.New()
	allOpts := append([]bus.Option{
		bus.WithAgentID(agentID),
		bus.WithTransport(tr),
		bus.WithDefaultTimeout(2 * time.Second),
	}, opts...)
	b, err := bus.New(allOpts...)
	if err != nil {
		t.Fatalf("bus.New: %v", err)
	}
	return b, func() { _ = b.Close() }
}

func TestPublishSubscribeRoundTrip(t *testing.T) {
	b, done := newTestBus(t, "pubsub-agent")
	defer done()

	got := make(chan *event.Envelope, 1)
	sub, err := b.Subscribe(context.Background(), event.MessageReceived, func(_ context.Context, e *event.Envelope) error {
		got <- e
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	out := event.New(event.MessageReceived)
	out.From = "tester"
	out.Payload = json.RawMessage(`{"text":"hi"}`)
	if err := b.Publish(context.Background(), out); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case e := <-got:
		if e.EventType != event.MessageReceived {
			t.Fatalf("event_type = %q", e.EventType)
		}
		if string(e.Payload) != `{"text":"hi"}` {
			t.Fatalf("payload = %q", string(e.Payload))
		}
		if e.EventID != out.EventID {
			t.Fatalf("event_id changed: %q vs %q", e.EventID, out.EventID)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for delivery")
	}
}

func TestInvokeRoundTrip(t *testing.T) {
	b, done := newTestBus(t, "invoke-agent")
	defer done()

	if err := b.HandleInvoke("echo", func(_ context.Context, e *event.Envelope) (any, error) {
		return map[string]any{"echo": json.RawMessage(e.Payload)}, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	resp, err := b.Invoke(context.Background(), "echo", map[string]any{"msg": "ping"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.EventType != event.ResponseFinal {
		t.Fatalf("event_type = %q want %q", resp.EventType, event.ResponseFinal)
	}
	if !resp.IsFinal {
		t.Fatalf("is_final not set")
	}
	if resp.CorrelationID == "" {
		t.Fatal("correlation_id should be set to req.event_id")
	}
}

func TestInvokeHandlerErrorMapsToErrorEnvelope(t *testing.T) {
	b, done := newTestBus(t, "invoke-agent")
	defer done()

	if err := b.HandleInvoke("boom", func(_ context.Context, _ *event.Envelope) (any, error) {
		return nil, errBoom
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}

	resp, err := b.Invoke(context.Background(), "boom", nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.EventType != event.ResponseError {
		t.Fatalf("event_type = %q want %q", resp.EventType, event.ResponseError)
	}
	if !resp.IsFinal {
		t.Fatalf("is_final not set on error response")
	}

	var p event.ErrorPayload
	if err := json.Unmarshal(resp.Payload, &p); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if p.Code != event.CodeAgentUnavailable {
		t.Fatalf("code = %q", p.Code)
	}
	if p.Message != errBoom.Error() {
		t.Fatalf("message = %q", p.Message)
	}
}

func TestSubscribeQueueGroupBalances(t *testing.T) {
	b, done := newTestBus(t, "queue-agent")
	defer done()

	const N = 6
	hits := make([]int, 2)
	for i := range hits {
		idx := i
		_, err := b.Subscribe(context.Background(), event.MessageReceived, func(_ context.Context, _ *event.Envelope) error {
			hits[idx]++
			return nil
		}, bus.WithQueue("workers"))
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
	}

	for i := 0; i < N; i++ {
		out := event.New(event.MessageReceived)
		if err := b.Publish(context.Background(), out); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	total := hits[0] + hits[1]
	if total != N {
		t.Fatalf("queue total = %d want %d", total, N)
	}
	if hits[0] == 0 || hits[1] == 0 {
		t.Fatalf("queue not balanced: %v", hits)
	}
}

func TestInvokeMissingTarget(t *testing.T) {
	b, done := newTestBus(t, "noop-agent")
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, err := b.Invoke(ctx, "nobody-home", nil); err == nil {
		t.Fatal("expected timeout error when no handler is registered")
	}
}

type stringErr string

func (e stringErr) Error() string { return string(e) }

const errBoom stringErr = "boom"
