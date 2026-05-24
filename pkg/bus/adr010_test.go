package bus

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/transport/inmem"
)

func TestBuildRequestEnvelopeWarnsOnNonRequestEventType(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	tr := inmem.New()
	b, err := New(
		WithAgentID("test-agent"),
		WithTransport(tr),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	db := b.(*defaultBus)

	// Passing a pub/sub style envelope into an invoke path should trigger a warn.
	env := event.NewEvent("goc.incident.created")
	_, err = db.buildRequestEnvelope("echo", env)
	if err != nil {
		t.Fatalf("buildRequestEnvelope: %v", err)
	}

	out := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("non-request event type")) {
		t.Fatalf("expected warn log for non-request event type, got:\n%s", out)
	}
	if !bytes.Contains(buf.Bytes(), []byte("goc.incident.created")) {
		t.Fatalf("expected warn log to contain event type, got:\n%s", out)
	}
}

func TestBuildRequestEnvelopeNoWarnForMessageReceived(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	tr := inmem.New()
	b, err := New(
		WithAgentID("test-agent"),
		WithTransport(tr),
		WithLogger(logger),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	db := b.(*defaultBus)

	// NewRequest uses MessageReceived — should not warn.
	env := event.NewRequest()
	_, err = db.buildRequestEnvelope("echo", env)
	if err != nil {
		t.Fatalf("buildRequestEnvelope: %v", err)
	}

	if buf.Len() != 0 {
		t.Fatalf("expected no warn log for MessageReceived, got:\n%s", buf.String())
	}
}

func TestBuildRequestEnvelopeDefaultsToNewRequest(t *testing.T) {
	tr := inmem.New()
	b, err := New(
		WithAgentID("test-agent"),
		WithTransport(tr),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	db := b.(*defaultBus)

	// Passing a plain map (not *event.Envelope) should create a request envelope.
	payload := map[string]any{"msg": "hi"}
	env, err := db.buildRequestEnvelope("echo", payload)
	if err != nil {
		t.Fatalf("buildRequestEnvelope: %v", err)
	}
	if env.EventType != event.MessageReceived {
		t.Fatalf("event_type = %q want %q", env.EventType, event.MessageReceived)
	}
}

func TestBuildRequestEnvelopeNilLoggerDoesNotPanic(t *testing.T) {
	// WithLogger(nil) must not panic even when the warn path is hit.
	tr := inmem.New()
	b, err := New(
		WithAgentID("test-agent"),
		WithTransport(tr),
		WithLogger(nil),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer b.Close()

	db := b.(*defaultBus)

	// Verify the nil guard fell back to a real logger.
	if db.opts.Logger == nil {
		t.Fatal("opts.Logger should have been defaulted when nil was passed")
	}

	// Trigger the warn path — must not panic.
	env := event.NewEvent("goc.incident.created")
	_, err = db.buildRequestEnvelope("echo", env)
	if err != nil {
		t.Fatalf("buildRequestEnvelope: %v", err)
	}
}
