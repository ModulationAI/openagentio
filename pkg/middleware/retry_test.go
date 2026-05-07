package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ModulationAI/openagentio/pkg/event"
)

func TestRetrySucceedsOnFirstAttempt(t *testing.T) {
	calls := 0
	h := func(_ context.Context, _ *event.Envelope) error {
		calls++
		return nil
	}

	chained := Chain(Handler(h), Retry(RetryPolicy{MaxAttempts: 3}))
	e := event.New("test")
	if err := chained(context.Background(), e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
	if e.Metadata["acp.retry.attempt"] != 1 {
		t.Fatalf("expected attempt=1 metadata, got %v", e.Metadata["acp.retry.attempt"])
	}
}

func TestRetryRetriesUntilSuccess(t *testing.T) {
	calls := 0
	h := func(_ context.Context, _ *event.Envelope) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}

	chained := Chain(Handler(h), Retry(RetryPolicy{MaxAttempts: 5}))
	e := event.New("test")
	if err := chained(context.Background(), e); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if e.Metadata["acp.retry.attempt"] != 3 {
		t.Fatalf("expected attempt=3 metadata, got %v", e.Metadata["acp.retry.attempt"])
	}
}

func TestRetryExhaustsAndReturnsLastError(t *testing.T) {
	calls := 0
	lastErr := errors.New("permanent")
	h := func(_ context.Context, _ *event.Envelope) error {
		calls++
		return lastErr
	}

	chained := Chain(Handler(h), Retry(RetryPolicy{MaxAttempts: 3}))
	e := event.New("test")
	if err := chained(context.Background(), e); err != lastErr {
		t.Fatalf("expected lastErr, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryRespectsIsRetryable(t *testing.T) {
	calls := 0
	h := func(_ context.Context, _ *event.Envelope) error {
		calls++
		return errors.New("fatal")
	}

	policy := RetryPolicy{
		MaxAttempts: 5,
		IsRetryable: func(err error) bool { return false },
	}
	chained := Chain(Handler(h), Retry(policy))
	e := event.New("test")
	if err := chained(context.Background(), e); err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryBackoffDelays(t *testing.T) {
	calls := 0
	delays := []int{}
	h := func(_ context.Context, _ *event.Envelope) error {
		calls++
		return errors.New("transient")
	}

	policy := RetryPolicy{
		MaxAttempts: 3,
		Backoff: func(attempt int) time.Duration {
			delays = append(delays, attempt)
			return time.Millisecond
		},
	}
	chained := Chain(Handler(h), Retry(policy))
	start := time.Now()
	_ = chained(context.Background(), event.New("test"))
	elapsed := time.Since(start)

	if len(delays) != 2 {
		t.Fatalf("expected 2 backoff calls, got %v", delays)
	}
	if delays[0] != 1 || delays[1] != 2 {
		t.Fatalf("unexpected delay indices: %v", delays)
	}
	if elapsed < 2*time.Millisecond {
		t.Fatal("expected at least 2ms of backoff")
	}
}

func TestRetryStampsMetadata(t *testing.T) {
	calls := 0
	h := func(_ context.Context, e *event.Envelope) error {
		calls++
		if v, ok := e.Metadata["acp.retry.attempt"].(int); !ok || v != calls {
			t.Fatalf("attempt metadata mismatch on call %d: %v", calls, e.Metadata["acp.retry.attempt"])
		}
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	}

	chained := Chain(Handler(h), Retry(RetryPolicy{MaxAttempts: 3}))
	if err := chained(context.Background(), event.New("test")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	h := func(_ context.Context, _ *event.Envelope) error {
		calls++
		return errors.New("transient")
	}

	policy := RetryPolicy{
		MaxAttempts: 5,
		Backoff:     func(int) time.Duration { return time.Hour },
	}
	chained := Chain(Handler(h), Retry(policy))

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := chained(ctx, event.New("test"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetryableError(t *testing.T) {
	inner := errors.New("boom")
	wrapped := Retryable(inner)
	if !IsRetryableError(wrapped) {
		t.Fatal("expected IsRetryableError true")
	}
	if !errors.Is(wrapped, inner) {
		t.Fatal("expected errors.Is to unwrap")
	}
	if Retryable(nil) != nil {
		t.Fatal("Retryable(nil) should return nil")
	}
	if IsRetryableError(inner) {
		t.Fatal("plain error should not be retryable")
	}
}

func TestConstantBackoff(t *testing.T) {
	b := ConstantBackoff(5 * time.Millisecond)
	if b(1) != 5*time.Millisecond || b(99) != 5*time.Millisecond {
		t.Fatal("constant backoff mismatch")
	}
}

func TestExponentialBackoff(t *testing.T) {
	b := ExponentialBackoff(time.Millisecond, 10*time.Millisecond)
	if b(1) != time.Millisecond {
		t.Fatalf("expected 1ms, got %v", b(1))
	}
	if b(2) != 2*time.Millisecond {
		t.Fatalf("expected 2ms, got %v", b(2))
	}
	if b(3) != 4*time.Millisecond {
		t.Fatalf("expected 4ms, got %v", b(3))
	}
	if b(10) != 10*time.Millisecond {
		t.Fatalf("expected cap 10ms, got %v", b(10))
	}
}
