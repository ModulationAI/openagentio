package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/ModulationAI/openagentio/pkg/event"
)

// Logging emits a structured log line per handler invocation. nil logger
// falls back to slog.Default.
func Logging(logger *slog.Logger) Middleware {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, e *event.Envelope) error {
			start := time.Now()
			err := next(ctx, e)
			attrs := []any{
				"event_id", e.EventID,
				"event_type", e.EventType,
				"trace_id", e.TraceID,
				"session_id", e.SessionID,
				"duration_ms", time.Since(start).Milliseconds(),
			}
			if err != nil {
				logger.ErrorContext(ctx, "handler error", append(attrs, "err", err)...)
				return err
			}
			logger.DebugContext(ctx, "handler ok", attrs...)
			return nil
		}
	}
}
