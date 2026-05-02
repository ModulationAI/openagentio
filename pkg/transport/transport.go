// Package transport defines the wire-level abstraction the bus uses to talk to
// any underlying messaging system. v0.1 ships an in-memory driver and a NATS
// stub; JetStream and HTTP/SSE drivers land in v0.3 and v0.2 respectively.
package transport

import (
	"context"
	"errors"
)

// ErrNotImplemented is returned by drivers whose method bodies are reserved
// for future milestones.
var ErrNotImplemented = errors.New("transport: not implemented (v0.1 skeleton)")

// RawMessage is the codec-agnostic carrier between the bus runtime and the
// transport. Headers stay decoupled from the payload bytes so future
// transports (HTTP, JetStream) can map them to their native metadata channels.
type RawMessage struct {
	Subject string
	Data    []byte
	Headers map[string]string
	ReplyTo string
}

// Capabilities advertises optional features so the bus can pick the right code
// path (e.g. fall back to a simulated request/reply when Streaming is false).
type Capabilities struct {
	Streaming   bool
	Persistence bool
	QueueGroup  bool
	Headers     bool
}

// Handler is invoked once per delivered message. Implementations should treat
// errors as fatal for the message — drivers may log/metric them but should not
// retry without explicit middleware.
type Handler func(ctx context.Context, msg *RawMessage) error

// Subscription represents a live consumer registration. Closing it stops
// further deliveries; idempotent.
type Subscription interface {
	Unsubscribe() error
}

// Inbox is a single-consumer ephemeral subject used for streaming responses.
// Subject() should be embedded in the request envelope as ReplyTo so the
// callee can publish multiple messages back to the caller.
type Inbox interface {
	Subject() string
	Recv(ctx context.Context) (*RawMessage, error)
	Close() error
}

// Transport is the contract every wire driver implements.
type Transport interface {
	Connect(ctx context.Context) error
	Close() error
	Capabilities() Capabilities

	Publish(ctx context.Context, msg *RawMessage) error
	Subscribe(ctx context.Context, subject, queue string, h Handler) (Subscription, error)
	Request(ctx context.Context, msg *RawMessage) (*RawMessage, error)
	OpenInbox(ctx context.Context) (Inbox, error)
}
