package middleware

import (
	"context"
	"fmt"

	"github.com/ModulationAI/openagentio/pkg/event"
)

// DLQSink receives a failed envelope together with the last error. Implementations
// typically clone the envelope, stamp DLQ metadata, and publish it to a dead-letter
// subject. Returning an error from the sink does not swallow the original failure;
// the middleware wraps both errors so callers still observe the handler error.
type DLQSink func(ctx context.Context, e *event.Envelope, lastErr error) error

// DeadLetter wraps a handler so that any returned error is forwarded to sink
// before being propagated upward. If sink itself fails, the original error is
// preserved and wrapped with the DLQ publish error.
func DeadLetter(sink DLQSink) Middleware {
	if sink == nil {
		panic("DeadLetter: nil sink")
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, e *event.Envelope) error {
			err := next(ctx, e)
			if err == nil {
				return nil
			}
			if dlqErr := sink(ctx, e, err); dlqErr != nil {
				return fmt.Errorf("dlq publish failed: %w (original: %w)", dlqErr, err)
			}
			return err
		}
	}
}
