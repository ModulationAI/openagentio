package http

import (
	"log/slog"
	"time"
)

// Option mutates an Adapter at construction time.
type Option func(*Adapter)

// WithAuth installs a request authenticator. When set, every request must
// pass the AuthFunc; non-nil error short-circuits with 401. A nil AuthFunc
// (or never calling WithAuth) leaves the adapter unauthenticated.
func WithAuth(fn AuthFunc) Option {
	return func(a *Adapter) { a.auth = fn }
}

// WithLogger overrides the default slog.Default() logger.
func WithLogger(l *slog.Logger) Option {
	return func(a *Adapter) {
		if l != nil {
			a.log = l
		}
	}
}

// WithTimeout caps the per-request overall deadline (invoke + stream).
// Passes through to bus.WithTimeout. Zero leaves the bus default in effect.
func WithTimeout(d time.Duration) Option {
	return func(a *Adapter) { a.timeout = d }
}

// WithIdleTimeout caps the gap between two streaming frames. Zero disables
// idle-timeout, leaving only the overall WithTimeout in effect.
func WithIdleTimeout(d time.Duration) Option {
	return func(a *Adapter) { a.idle = d }
}

// WithMiddleware appends one or more http.Handler middlewares. Outermost
// middleware (first in the list) wraps the others.
func WithMiddleware(mw ...Middleware) Option {
	return func(a *Adapter) { a.mws = append(a.mws, mw...) }
}

// WithSSERetry sets the retry interval sent to SSE clients in the retry:
// field (milliseconds). Zero disables the field. Default is 3s.
func WithSSERetry(d time.Duration) Option {
	return func(a *Adapter) { a.sseRetry = d }
}
