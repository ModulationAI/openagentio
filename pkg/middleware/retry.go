package middleware

import (
	"context"
	"errors"
	"time"

	"github.com/ModulationAI/agentflowbus/pkg/event"
)

// RetryPolicy configures how many times a handler may be retried and with
// what backoff strategy. A zero-value policy is usable: it defaults to one
// attempt (no retry) and zero backoff.
type RetryPolicy struct {
	// MaxAttempts is the total number of times the handler may be invoked.
	// Defaults to 1 if ≤ 0.
	MaxAttempts int
	// Backoff returns the delay to wait before attempt N (1-based).
	// If nil, retries happen immediately.
	Backoff func(attempt int) time.Duration
	// IsRetryable decides whether an error warrants a retry.
	// If nil, all errors are considered retryable.
	IsRetryable func(error) bool
}

// Retry wraps a handler so that transient failures are retried according to
// policy. On each attempt the envelope metadata key "acp.retry.attempt" is
// updated so downstream observers can see the current attempt count.
func Retry(policy RetryPolicy) Middleware {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 1
	}
	if policy.Backoff == nil {
		policy.Backoff = func(int) time.Duration { return 0 }
	}
	if policy.IsRetryable == nil {
		policy.IsRetryable = func(error) bool { return true }
	}

	return func(next Handler) Handler {
		return func(ctx context.Context, e *event.Envelope) error {
			var lastErr error
			for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
				if e.Metadata == nil {
					e.Metadata = make(map[string]any)
				}
				e.Metadata["acp.retry.attempt"] = attempt

				lastErr = next(ctx, e)
				if lastErr == nil {
					return nil
				}
				if !policy.IsRetryable(lastErr) {
					break
				}
				if attempt < policy.MaxAttempts {
					delay := policy.Backoff(attempt)
					if delay > 0 {
						select {
						case <-time.After(delay):
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
			}
			return lastErr
		}
	}
}

// ConstantBackoff returns a backoff function that always sleeps d.
func ConstantBackoff(d time.Duration) func(int) time.Duration {
	return func(int) time.Duration { return d }
}

// ExponentialBackoff returns a backoff function that doubles the delay on
// every attempt, capped at max.
func ExponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		d := base
		for i := 1; i < attempt; i++ {
			d *= 2
			if d > max {
				return max
			}
		}
		return d
	}
}

// retryableError is a sentinel wrapper that handlers may use to explicitly
// mark an error as retryable.
type retryableError struct{ error }

// Unwrap returns the wrapped error.
func (e *retryableError) Unwrap() error { return e.error }

// Retryable wraps err so that IsRetryableError returns true.
// Nil-safe: returns nil when err is nil.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{error: err}
}

// IsRetryableError reports whether err (or any error in its chain) was
// created with Retryable.
func IsRetryableError(err error) bool {
	var re *retryableError
	return errors.As(err, &re)
}
