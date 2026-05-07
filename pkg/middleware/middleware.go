// Package middleware provides composable handlers that wrap Bus.Subscribe and
// Bus.Invoke calls with cross-cutting concerns: panic recovery, structured
// logging, trace propagation, retry, etc.
package middleware

import (
	"context"

	"github.com/ModulationAI/openagentio/pkg/event"
)

// Handler is the inner handler type middleware operates on. It mirrors
// bus.Handler but is duplicated here to avoid an import cycle (bus imports
// middleware, not the other way around).
type Handler func(ctx context.Context, e *event.Envelope) error

// Middleware wraps a Handler with cross-cutting behavior. The outer-most
// middleware in a chain runs first.
type Middleware func(next Handler) Handler

// Chain composes mws around h. The returned Handler runs middlewares in the
// order they were supplied (mws[0] is outer-most).
func Chain(h Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
