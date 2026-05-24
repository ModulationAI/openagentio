package bus

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

// StreamInvoke publishes a request to {prefix}.invoke.{target} with a fresh
// _INBOX as reply_to and returns a Stream over the received frames. Frames
// are reordered by Envelope.Seq; the iterator terminates after a frame with
// IsFinal=true, or earlier on idle/overall timeout.
func (b *defaultBus) StreamInvoke(ctx context.Context, target string, payload any, opts ...InvokeOption) (Stream, error) {
	if target == "" {
		return nil, errors.New("bus: empty invoke target")
	}
	o := collectInvokeOpts(opts)
	timeout := o.Timeout
	if timeout == 0 {
		timeout = b.opts.DefaultTimeout
	}

	streamCtx, cancel := context.WithCancel(ctx)
	if timeout > 0 {
		streamCtx, cancel = context.WithTimeout(ctx, timeout)
	}

	env, err := b.buildRequestEnvelope(target, payload)
	if err != nil {
		cancel()
		return nil, err
	}

	inbox, err := b.opts.Transport.OpenInbox(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	env.ReplyTo = inbox.Subject()

	b.prepareEnvelope(streamCtx, env)
	data, err := b.opts.Codec.EncodeEnvelope(env)
	if err != nil {
		_ = inbox.Close()
		cancel()
		return nil, err
	}
	if err := b.opts.Transport.Publish(streamCtx, &transport.RawMessage{
		Subject: b.invokeSubject(target, b.resolveTenant(env.TenantID)),
		Data:    data,
	}); err != nil {
		_ = inbox.Close()
		cancel()
		return nil, err
	}

	return &stream{
		ctx:    streamCtx,
		cancel: cancel,
		inbox:  inbox,
		codec:  b.opts.Codec,
		idle:   o.IdleTimeout,
	}, nil
}

// HandleStream subscribes to {prefix}.invoke.{target} and dispatches each
// request into a goroutine running the supplied handler. A StreamWriter is
// provided that publishes started/delta/final/error frames back to
// req.ReplyTo with monotonically increasing Seq numbers. If the handler
// returns without calling Final or Error, the runtime auto-emits one based on
// the returned error.
func (b *defaultBus) HandleStream(target string, h StreamHandler, opts ...HandleOption) error {
	if target == "" {
		return errors.New("bus: empty invoke target")
	}
	if h == nil {
		return errors.New("bus: nil stream handler")
	}
	o := collectHandleOpts(opts)
	if !o.QueueSet {
		o.Queue = target
	}
	subject := b.invokeSubject(target, b.opts.Tenant)

	dispatch := func(_ context.Context, msg *transport.RawMessage) error {
		req, err := b.opts.Codec.DecodeEnvelope(msg.Data)
		if err != nil {
			return err
		}
		if req.ReplyTo == "" {
			return errors.New("bus: stream request missing reply_to")
		}
		go b.handleStream(req, h)
		return nil
	}

	sub, err := b.opts.Transport.Subscribe(b.lifeCtx, subject, o.Queue, dispatch)
	if err != nil {
		return err
	}
	b.trackOwned(sub)
	return nil
}

func (b *defaultBus) handleStream(req *event.Envelope, h StreamHandler) {
	ctx, cancel := context.WithCancel(b.lifeCtx)
	defer cancel()

	w := &streamWriter{
		bus: b,
		ctx: ctx,
		req: req,
	}

	chained := middleware.Chain(middleware.Handler(func(c context.Context, e *event.Envelope) error {
		return h(c, e, w)
	}), b.opts.Middleware...)

	herr := chained(ctx, req)

	if w.isClosed() {
		return
	}
	if herr != nil {
		_ = w.Error(herr)
	} else {
		_ = w.Final(nil)
	}
}

// --- client-side stream ------------------------------------------------------

type stream struct {
	ctx    context.Context
	cancel context.CancelFunc
	inbox  transport.Inbox
	codec  codec.Codec
	idle   time.Duration

	closeOnce sync.Once
	closeErr  error
}

func (s *stream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.closeErr = s.inbox.Close()
	})
	return s.closeErr
}

func (s *stream) Events() iter.Seq2[*event.Envelope, error] {
	return func(yield func(*event.Envelope, error) bool) {
		var (
			expected uint64
			pending  = make(map[uint64]*event.Envelope)
		)

		for {
			recvCtx := s.ctx
			var recvCancel context.CancelFunc
			if s.idle > 0 {
				recvCtx, recvCancel = context.WithTimeout(s.ctx, s.idle)
			}
			msg, err := s.inbox.Recv(recvCtx)
			if recvCancel != nil {
				recvCancel()
			}

			if err != nil {
				switch {
				case s.ctx.Err() != nil:
					yield(nil, s.ctx.Err())
				case s.idle > 0 && errors.Is(err, context.DeadlineExceeded):
					yield(nil, ErrIdleTimeout)
				default:
					yield(nil, err)
				}
				return
			}

			env, err := s.codec.DecodeEnvelope(msg.Data)
			if err != nil {
				yield(nil, err)
				return
			}

			if env.Seq < expected {
				continue // duplicate / late frame, drop
			}
			if _, dup := pending[env.Seq]; dup {
				continue
			}
			pending[env.Seq] = env

			for {
				e, ok := pending[expected]
				if !ok {
					break
				}
				delete(pending, expected)
				expected++
				if !yield(e, nil) {
					return
				}
				if e.IsFinal {
					return
				}
			}
		}
	}
}

// --- server-side writer ------------------------------------------------------

type streamWriter struct {
	bus *defaultBus
	ctx context.Context
	req *event.Envelope

	mu      sync.Mutex
	seq     uint64
	started bool
	closed  bool
}

func (w *streamWriter) Started(meta any) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("stream: already closed")
	}
	if w.started {
		w.mu.Unlock()
		return errors.New("stream: started already emitted")
	}
	w.started = true
	seq := w.nextSeqLocked()
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseStarted)
	env.Seq = seq
	if meta != nil {
		data, err := w.bus.opts.Codec.EncodePayload(meta)
		if err != nil {
			return err
		}
		env.Payload = data
	}
	return w.publish(env)
}

func (w *streamWriter) Delta(chunk any) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("stream: already closed")
	}
	seq := w.nextSeqLocked()
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseDelta)
	env.Seq = seq
	if chunk != nil {
		data, err := w.bus.opts.Codec.EncodePayload(chunk)
		if err != nil {
			return err
		}
		env.Payload = data
	}
	return w.publish(env)
}

func (w *streamWriter) Final(result any) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("stream: already closed")
	}
	w.closed = true
	seq := w.nextSeqLocked()
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseFinal)
	env.Seq = seq
	env.IsFinal = true
	if result != nil {
		data, err := w.bus.opts.Codec.EncodePayload(result)
		if err != nil {
			return err
		}
		env.Payload = data
	}
	return w.publish(env)
}

func (w *streamWriter) Error(srcErr error) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("stream: already closed")
	}
	w.closed = true
	seq := w.nextSeqLocked()
	w.mu.Unlock()

	env := newReplyShell(w.bus.opts.AgentID, w.req, event.ResponseError)
	env.Seq = seq
	env.IsFinal = true
	payload := event.ErrorPayload{
		Code:    event.CodeAgentUnavailable,
		Message: srcErr.Error(),
	}
	data, _ := w.bus.opts.Codec.EncodePayload(payload)
	env.Payload = data
	return w.publish(env)
}

func (w *streamWriter) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (w *streamWriter) nextSeqLocked() uint64 {
	s := w.seq
	w.seq++
	return s
}

func (w *streamWriter) publish(env *event.Envelope) error {
	data, err := w.bus.opts.Codec.EncodeEnvelope(env)
	if err != nil {
		return err
	}
	return w.bus.opts.Transport.Publish(w.ctx, &transport.RawMessage{
		Subject: w.req.ReplyTo,
		Data:    data,
	})
}
