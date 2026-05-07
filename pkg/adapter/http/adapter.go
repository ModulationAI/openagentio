// Package http exposes a thin HTTP/SSE adapter that translates external REST
// traffic into bus.Bus calls. The adapter does not implement transport.Transport;
// it is a server-side wrapper that lets HTTP clients talk to an existing Bus.
//
// Endpoint surface (see prompts/design.md §10.1):
//
//	POST /v1/agents/{target}/invoke   -> bus.Invoke,       returns final payload JSON
//	POST /v1/agents/{target}/stream   -> bus.StreamInvoke, returns text/event-stream
//	POST /v1/events/{event_type}      -> bus.Publish,      returns 202 Accepted
//
// HTTP headers map onto envelope correlation/tenancy fields (see envelope.go),
// the request body becomes Envelope.Payload verbatim, and an optional
// AuthFunc can override tenant/user/session before dispatch.
package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/ModulationAI/openagentio/pkg/bus"
)

// Default per-request timeouts. Override via WithTimeout / WithIdleTimeout.
const (
	defaultTimeout     = 30 * time.Second
	defaultIdleTimeout = 0 * time.Second // 0 = no idle cap; rely on overall timeout
	defaultPublishWait = 5 * time.Second
)

// Middleware wraps an http.Handler — the standard func(http.Handler) http.Handler
// chain familiar from net/http. Outermost middleware runs first.
type Middleware func(http.Handler) http.Handler

// Adapter implements http.Handler. Construct with New, then mount on any
// http.Server: srv := &http.Server{Handler: New(b)}.
type Adapter struct {
	bus     bus.Bus
	log     *slog.Logger
	auth    AuthFunc
	timeout time.Duration
	idle    time.Duration
	mws     []Middleware
	handler http.Handler
}

// New constructs an Adapter wrapping the given Bus. The Adapter holds no
// resources of its own beyond the mux; closing the underlying Bus is the
// caller's responsibility.
func New(b bus.Bus, opts ...Option) *Adapter {
	a := &Adapter{
		bus:     b,
		log:     slog.Default(),
		timeout: defaultTimeout,
		idle:    defaultIdleTimeout,
	}
	for _, opt := range opts {
		opt(a)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/{target}/invoke", a.handleInvoke)
	mux.HandleFunc("POST /v1/agents/{target}/stream", a.handleStream)
	mux.HandleFunc("POST /v1/events/{event_type}", a.handlePublish)

	var h http.Handler = mux
	for i := len(a.mws) - 1; i >= 0; i-- {
		h = a.mws[i](h)
	}
	a.handler = h
	return a
}

// ServeHTTP makes Adapter satisfy http.Handler.
func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}
