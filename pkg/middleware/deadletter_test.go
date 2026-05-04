package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/ModulationAI/agentflowbus/pkg/event"
)

func TestDeadLetterCallsSinkOnError(t *testing.T) {
	handlerErr := errors.New("handler failed")
	var sinkEnv *event.Envelope
	var sinkErr error

	sink := func(_ context.Context, e *event.Envelope, err error) error {
		sinkEnv = e
		sinkErr = err
		return nil
	}

	h := func(_ context.Context, _ *event.Envelope) error {
		return handlerErr
	}

	chained := Chain(Handler(h), DeadLetter(sink))
	e := event.New("test")
	if err := chained(context.Background(), e); err != handlerErr {
		t.Fatalf("expected handlerErr, got %v", err)
	}
	if sinkEnv != e {
		t.Fatal("sink did not receive the envelope")
	}
	if sinkErr != handlerErr {
		t.Fatalf("sink did not receive the error: got %v", sinkErr)
	}
}

func TestDeadLetterNoopOnSuccess(t *testing.T) {
	called := false
	sink := func(_ context.Context, _ *event.Envelope, _ error) error {
		called = true
		return nil
	}

	h := func(_ context.Context, _ *event.Envelope) error { return nil }
	chained := Chain(Handler(h), DeadLetter(sink))
	if err := chained(context.Background(), event.New("test")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Fatal("sink should not be called on success")
	}
}

func TestDeadLetterWrapsSinkError(t *testing.T) {
	handlerErr := errors.New("handler failed")
	dlqErr := errors.New("dlq failed")

	sink := func(_ context.Context, _ *event.Envelope, _ error) error {
		return dlqErr
	}

	h := func(_ context.Context, _ *event.Envelope) error { return handlerErr }
	chained := Chain(Handler(h), DeadLetter(sink))
	err := chained(context.Background(), event.New("test"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected errors.Is(handlerErr), got %v", err)
	}
	if !errors.Is(err, dlqErr) {
		t.Fatalf("expected errors.Is(dlqErr), got %v", err)
	}
}

func TestDeadLetterRetriesThenDLQ(t *testing.T) {
	handlerErr := errors.New("transient")
	dlqCalls := 0

	sink := func(_ context.Context, _ *event.Envelope, err error) error {
		dlqCalls++
		if err != handlerErr {
			t.Fatalf("expected handlerErr in sink, got %v", err)
		}
		return nil
	}

	calls := 0
	h := func(_ context.Context, _ *event.Envelope) error {
		calls++
		return handlerErr
	}

	// DeadLetter outside Retry so exhaustion bubbles up to DLQ.
	chained := Chain(Handler(h), DeadLetter(sink), Retry(RetryPolicy{MaxAttempts: 3}))
	if err := chained(context.Background(), event.New("test")); err != handlerErr {
		t.Fatalf("expected handlerErr, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 handler calls, got %d", calls)
	}
	if dlqCalls != 1 {
		t.Fatalf("expected 1 dlq call, got %d", dlqCalls)
	}
}
