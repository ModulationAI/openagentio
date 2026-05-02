package nats_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"

	"github.com/ModulationAI/agentflowbus/pkg/bus"
	"github.com/ModulationAI/agentflowbus/pkg/event"
	"github.com/ModulationAI/agentflowbus/pkg/transport"
	natsdrv "github.com/ModulationAI/agentflowbus/pkg/transport/nats"
)

// runEmbeddedNATS spins up an in-process nats-server on a random port. The
// server is shut down via t.Cleanup so each test gets an isolated bus.
//
// Skipped under -short to keep the unit-test fast path lean.
func runEmbeddedNATS(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping NATS integration tests in -short mode")
	}
	opts := natsserver.DefaultTestOptions
	opts.Port = -1 // pick a free port
	s := natsserver.RunServer(&opts)
	if !s.ReadyForConnections(2 * time.Second) {
		s.Shutdown()
		t.Fatalf("embedded nats-server not ready within 2s")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}

// newDriver returns a Connect()'d driver with cleanup attached.
func newDriver(t *testing.T, url string) *natsdrv.Driver {
	t.Helper()
	d := natsdrv.New(natsdrv.URL(url), natsdrv.Name(t.Name()))
	if err := d.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// --- Driver-level tests ----------------------------------------------------

func TestNATS_ConnectCloseIdempotent(t *testing.T) {
	url := runEmbeddedNATS(t)
	d := natsdrv.New(natsdrv.URL(url))
	if err := d.Connect(context.Background()); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	if err := d.Connect(context.Background()); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestNATS_PublishSubscribePreservesHeaders(t *testing.T) {
	url := runEmbeddedNATS(t)
	d := newDriver(t, url)

	got := make(chan *transport.RawMessage, 1)
	sub, err := d.Subscribe(context.Background(), "afb.test.pubsub", "", func(_ context.Context, m *transport.RawMessage) error {
		got <- m
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	// Round-trip with the server so the SUB is registered before the PUB
	// arrives — without this the publish below could race the subscription.
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := d.Publish(context.Background(), &transport.RawMessage{
		Subject: "afb.test.pubsub",
		Data:    []byte("hello"),
		Headers: map[string]string{"x-trace": "abc", "x-tenant": "acme"},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case m := <-got:
		if string(m.Data) != "hello" {
			t.Fatalf("data = %q", m.Data)
		}
		if m.Headers["x-trace"] != "abc" || m.Headers["x-tenant"] != "acme" {
			t.Fatalf("headers lost: %v", m.Headers)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery")
	}
}

func TestNATS_QueueGroupBalances(t *testing.T) {
	url := runEmbeddedNATS(t)
	d := newDriver(t, url)

	const N = 12
	var hitsA, hitsB atomic.Int64
	var wg sync.WaitGroup
	wg.Add(N)

	subA, err := d.Subscribe(context.Background(), "afb.test.queue", "workers", func(_ context.Context, _ *transport.RawMessage) error {
		hitsA.Add(1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	defer subA.Unsubscribe()

	subB, err := d.Subscribe(context.Background(), "afb.test.queue", "workers", func(_ context.Context, _ *transport.RawMessage) error {
		hitsB.Add(1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}
	defer subB.Unsubscribe()
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	for i := 0; i < N; i++ {
		if err := d.Publish(context.Background(), &transport.RawMessage{
			Subject: "afb.test.queue",
			Data:    []byte("x"),
		}); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	waitDone(t, &wg, 2*time.Second)
	a, b := hitsA.Load(), hitsB.Load()
	if a+b != N {
		t.Fatalf("queue total = %d want %d (a=%d b=%d)", a+b, N, a, b)
	}
	if a == 0 || b == 0 {
		t.Fatalf("queue not balanced: a=%d b=%d", a, b)
	}
}

func TestNATS_RequestReplyEchoes(t *testing.T) {
	url := runEmbeddedNATS(t)
	d := newDriver(t, url)

	sub, err := d.Subscribe(context.Background(), "afb.test.rpc", "", func(ctx context.Context, m *transport.RawMessage) error {
		if m.ReplyTo == "" {
			return errors.New("missing reply subject")
		}
		return d.Publish(ctx, &transport.RawMessage{
			Subject: m.ReplyTo,
			Data:    append([]byte("echo:"), m.Data...),
		})
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := d.Request(ctx, &transport.RawMessage{
		Subject: "afb.test.rpc",
		Data:    []byte("ping"),
	})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(resp.Data) != "echo:ping" {
		t.Fatalf("resp = %q want echo:ping", resp.Data)
	}
}

func TestNATS_OpenInboxReceives(t *testing.T) {
	url := runEmbeddedNATS(t)
	d := newDriver(t, url)

	inbox, err := d.OpenInbox(context.Background())
	if err != nil {
		t.Fatalf("OpenInbox: %v", err)
	}
	defer inbox.Close()
	if inbox.Subject() == "" {
		t.Fatal("inbox subject empty")
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if err := d.Publish(context.Background(), &transport.RawMessage{
		Subject: inbox.Subject(),
		Data:    []byte("frame-1"),
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	m, err := inbox.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(m.Data) != "frame-1" {
		t.Fatalf("data = %q want frame-1", m.Data)
	}
}

func TestNATS_InboxRecvHonorsContextDeadline(t *testing.T) {
	url := runEmbeddedNATS(t)
	d := newDriver(t, url)

	inbox, err := d.OpenInbox(context.Background())
	if err != nil {
		t.Fatalf("OpenInbox: %v", err)
	}
	defer inbox.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := inbox.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv err = %v want DeadlineExceeded", err)
	}
}

func TestNATS_InboxCloseUnblocksRecv(t *testing.T) {
	url := runEmbeddedNATS(t)
	d := newDriver(t, url)

	inbox, err := d.OpenInbox(context.Background())
	if err != nil {
		t.Fatalf("OpenInbox: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := inbox.Recv(context.Background())
		errCh <- err
	}()
	// Give the goroutine a beat to park inside Recv before we Close.
	time.Sleep(20 * time.Millisecond)

	if err := inbox.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Recv did not unblock after Close")
	}
	if err := inbox.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// --- Bus-over-NATS tests ---------------------------------------------------

// busOverNATS wires a Bus on top of a NATS driver and returns both. Tests keep
// the driver handle so they can Flush() to make subscribe-then-publish on the
// same connection race-free — the Bus interface intentionally hides the
// underlying transport.
func busOverNATS(t *testing.T, url, agentID string) (bus.Bus, *natsdrv.Driver) {
	t.Helper()
	d := natsdrv.New(natsdrv.URL(url), natsdrv.Name(agentID))
	b, err := bus.New(
		bus.WithAgentID(agentID),
		bus.WithTransport(d),
		bus.WithDefaultTimeout(3*time.Second),
	)
	if err != nil {
		t.Fatalf("bus.New: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, d
}

func TestBusOverNATS_PubSub(t *testing.T) {
	url := runEmbeddedNATS(t)
	b, d := busOverNATS(t, url, "pubsub-agent")

	got := make(chan *event.Envelope, 1)
	sub, err := b.Subscribe(context.Background(), event.MessageReceived, func(_ context.Context, e *event.Envelope) error {
		got <- e
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	out := event.New(event.MessageReceived)
	out.From = "tester"
	out.Payload = json.RawMessage(`{"text":"hi-nats"}`)
	if err := b.Publish(context.Background(), out); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case e := <-got:
		if e.EventID != out.EventID {
			t.Fatalf("event_id changed: %q vs %q", e.EventID, out.EventID)
		}
		if string(e.Payload) != `{"text":"hi-nats"}` {
			t.Fatalf("payload = %q", e.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delivery")
	}
}

func TestBusOverNATS_InvokeRoundTrip(t *testing.T) {
	url := runEmbeddedNATS(t)
	b, d := busOverNATS(t, url, "invoke-agent")

	if err := b.HandleInvoke("echo", func(_ context.Context, e *event.Envelope) (any, error) {
		return map[string]any{"echo": json.RawMessage(e.Payload)}, nil
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	resp, err := b.Invoke(context.Background(), "echo", map[string]any{"msg": "ping"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.EventType != event.ResponseFinal {
		t.Fatalf("event_type = %q want %q", resp.EventType, event.ResponseFinal)
	}
	if !resp.IsFinal {
		t.Fatal("is_final should be true")
	}
	if resp.CorrelationID == "" {
		t.Fatal("correlation_id should be set on response")
	}
}

func TestBusOverNATS_InvokeHandlerErrorMapsToErrorEnvelope(t *testing.T) {
	url := runEmbeddedNATS(t)
	b, d := busOverNATS(t, url, "invoke-agent")

	if err := b.HandleInvoke("boom", func(_ context.Context, _ *event.Envelope) (any, error) {
		return nil, errors.New("kaboom")
	}); err != nil {
		t.Fatalf("HandleInvoke: %v", err)
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	resp, err := b.Invoke(context.Background(), "boom", nil)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if resp.EventType != event.ResponseError {
		t.Fatalf("event_type = %q want %q", resp.EventType, event.ResponseError)
	}
	if !resp.IsFinal {
		t.Fatal("is_final not set on error envelope")
	}
	var p event.ErrorPayload
	if err := json.Unmarshal(resp.Payload, &p); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if p.Code != event.CodeAgentUnavailable {
		t.Fatalf("code = %q", p.Code)
	}
	if p.Message != "kaboom" {
		t.Fatalf("message = %q", p.Message)
	}
}

func TestBusOverNATS_StreamInvokeOrdering(t *testing.T) {
	url := runEmbeddedNATS(t)
	b, d := busOverNATS(t, url, "stream-agent")

	if err := b.HandleStream("count", func(_ context.Context, _ *event.Envelope, w bus.StreamWriter) error {
		if err := w.Started(map[string]any{"model": "test"}); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			if err := w.Delta(map[string]any{"i": i}); err != nil {
				return err
			}
		}
		return w.Final(map[string]any{"count": 3})
	}); err != nil {
		t.Fatalf("HandleStream: %v", err)
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	s, err := b.StreamInvoke(context.Background(), "count", nil)
	if err != nil {
		t.Fatalf("StreamInvoke: %v", err)
	}
	defer s.Close()

	var (
		types []string
		seqs  []uint64
	)
	for env, err := range s.Events() {
		if err != nil {
			t.Fatalf("frame error: %v", err)
		}
		types = append(types, env.EventType)
		seqs = append(seqs, env.Seq)
	}

	wantTypes := []string{
		event.ResponseStarted,
		event.ResponseDelta,
		event.ResponseDelta,
		event.ResponseDelta,
		event.ResponseFinal,
	}
	if len(types) != len(wantTypes) {
		t.Fatalf("types = %v want %v", types, wantTypes)
	}
	for i, ty := range wantTypes {
		if types[i] != ty {
			t.Errorf("types[%d] = %q want %q", i, types[i], ty)
		}
		if seqs[i] != uint64(i) {
			t.Errorf("seqs[%d] = %d want %d", i, seqs[i], i)
		}
	}
}

// --- Helpers ---------------------------------------------------------------

func waitDone(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("waitgroup timed out")
	}
}
