package bus

import (
	"log/slog"
	"time"

	"github.com/ModulationAI/agentflowbus/pkg/codec"
	"github.com/ModulationAI/agentflowbus/pkg/middleware"
	"github.com/ModulationAI/agentflowbus/pkg/transport"
)

// DefaultSubjectPrefix is used when WithSubjectPrefix is not supplied.
// Override it via bus.WithSubjectPrefix("...") to coexist with legacy
// `agent.*` namespaces.
const DefaultSubjectPrefix = "acp.v1"

// Options bundles every Bus-level setting.
type Options struct {
	AgentID        string
	Tenant         string
	SubjectPrefix  string
	Codec          codec.Codec
	Transport      transport.Transport
	Logger         *slog.Logger
	Middleware     []middleware.Middleware
	DefaultTimeout time.Duration
}

// Option mutates Options.
type Option func(*Options)

func WithAgentID(id string) Option       { return func(o *Options) { o.AgentID = id } }
func WithTenant(t string) Option         { return func(o *Options) { o.Tenant = t } }
func WithSubjectPrefix(p string) Option  { return func(o *Options) { o.SubjectPrefix = p } }
func WithCodec(c codec.Codec) Option     { return func(o *Options) { o.Codec = c } }
func WithLogger(l *slog.Logger) Option   { return func(o *Options) { o.Logger = l } }

func WithTransport(t transport.Transport) Option {
	return func(o *Options) { o.Transport = t }
}

func WithMiddleware(mw ...middleware.Middleware) Option {
	return func(o *Options) { o.Middleware = append(o.Middleware, mw...) }
}

func WithDefaultTimeout(d time.Duration) Option {
	return func(o *Options) { o.DefaultTimeout = d }
}

// --- Per-call options ---------------------------------------------------------

// SubOption configures Bus.Subscribe.
type SubOption func(*subOpts)

type subOpts struct {
	Queue string
}

// WithQueue puts the subscriber into a queue group; messages on the subject
// are load-balanced across all members of the same queue.
func WithQueue(q string) SubOption { return func(o *subOpts) { o.Queue = q } }

// InvokeOption configures Bus.Invoke / Bus.StreamInvoke.
type InvokeOption func(*invokeOpts)

type invokeOpts struct {
	Timeout     time.Duration
	IdleTimeout time.Duration
}

// WithTimeout sets the overall deadline for the invocation.
func WithTimeout(d time.Duration) InvokeOption {
	return func(o *invokeOpts) { o.Timeout = d }
}

// WithIdleTimeout sets the maximum gap between two streaming frames.
func WithIdleTimeout(d time.Duration) InvokeOption {
	return func(o *invokeOpts) { o.IdleTimeout = d }
}

// HandleOption configures HandleInvoke / HandleStream.
type HandleOption func(*handleOpts)

type handleOpts struct {
	Queue string
}

// WithHandleQueue places the handler in a queue group so multiple replicas
// load-balance the work.
func WithHandleQueue(q string) HandleOption {
	return func(o *handleOpts) { o.Queue = q }
}

// --- Internal helpers ---------------------------------------------------------

func collectSubOpts(opts []SubOption) subOpts {
	var o subOpts
	for _, f := range opts {
		f(&o)
	}
	return o
}

func collectInvokeOpts(opts []InvokeOption) invokeOpts {
	var o invokeOpts
	for _, f := range opts {
		f(&o)
	}
	return o
}

func collectHandleOpts(opts []HandleOption) handleOpts {
	var o handleOpts
	for _, f := range opts {
		f(&o)
	}
	return o
}
