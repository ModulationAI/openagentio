// Package bus exposes the application-facing API of OpenAgentIO: Publish,
// Subscribe, Invoke, StreamInvoke, and the corresponding handler registrations
// for service implementations.
//
// The runtime is backed by a transport.Transport and a codec.Codec; both are
// provided via functional options when constructing the Bus.
package bus

import (
	"context"
	"iter"

	"github.com/ModulationAI/openagentio/pkg/event"
)

// Handler consumes a delivered envelope. Returning an error is allowed but the
// runtime currently treats handler errors as terminal for the message; retry
// semantics live in middleware.
type Handler func(ctx context.Context, e *event.Envelope) error

// InvokeHandler is the server-side counterpart of Bus.Invoke: it returns a
// single value that the runtime wraps into a final response envelope.
type InvokeHandler func(ctx context.Context, e *event.Envelope) (any, error)

// StreamHandler is the server-side counterpart of Bus.StreamInvoke. It receives
// a StreamWriter that lets it emit started/delta/final/error frames in order.
type StreamHandler func(ctx context.Context, e *event.Envelope, w StreamWriter) error

// Subscription represents a live subscription. Closing it stops further
// deliveries; idempotent.
type Subscription interface {
	Unsubscribe() error
}

// StreamWriter is the server side of a streaming response. Started/Final/Error
// must each be called at most once per stream; Final and Error are mutually
// exclusive.
type StreamWriter interface {
	Started(meta any) error
	Delta(chunk any) error
	Final(result any) error
	Error(err error) error
}

// Stream is the client side of a streaming response. Range over Events() to
// consume incoming envelopes; Close cancels the underlying inbox.
type Stream interface {
	Events() iter.Seq2[*event.Envelope, error]
	Close() error
}

// Bus is the contract every runtime implementation satisfies.
type Bus interface {
	Publish(ctx context.Context, e *event.Envelope) error
	Subscribe(ctx context.Context, eventType string, h Handler, opts ...SubOption) (Subscription, error)

	Invoke(ctx context.Context, target string, payload any, opts ...InvokeOption) (*event.Envelope, error)
	StreamInvoke(ctx context.Context, target string, payload any, opts ...InvokeOption) (Stream, error)

	HandleInvoke(target string, h InvokeHandler, opts ...HandleOption) error
	HandleStream(target string, h StreamHandler, opts ...HandleOption) error

	Close() error
}
