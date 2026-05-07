package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/ModulationAI/openagentio/pkg/event"
)

// Recover catches panics in downstream handlers, converts them to errors, and
// logs the stack trace. It is the recommended outer-most middleware on every
// chain.
func Recover() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, e *event.Envelope) (err error) {
			defer func() {
				if r := recover(); r != nil {
					eventID := ""
					if e != nil {
						eventID = e.EventID
					}
					slog.ErrorContext(ctx, "handler panic",
						"recover", r,
						"event_id", eventID,
						"stack", string(debug.Stack()),
					)
					err = fmt.Errorf("handler panic: %v", r)
				}
			}()
			return next(ctx, e)
		}
	}
}
