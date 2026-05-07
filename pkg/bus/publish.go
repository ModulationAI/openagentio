package bus

import (
	"context"
	"errors"

	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

func (b *defaultBus) Publish(ctx context.Context, e *event.Envelope) error {
	if e == nil {
		return errors.New("bus: nil envelope")
	}
	if e.EventType == "" {
		return errors.New("bus: envelope missing event_type")
	}
	subject := b.eventSubject(e.EventType, b.resolveTenant(e.TenantID))
	b.prepareEnvelope(ctx, e)
	data, err := b.opts.Codec.EncodeEnvelope(e)
	if err != nil {
		return err
	}
	return b.opts.Transport.Publish(ctx, &transport.RawMessage{
		Subject: subject,
		Data:    data,
		ReplyTo: e.ReplyTo,
	})
}

func (b *defaultBus) Subscribe(ctx context.Context, eventType string, h Handler, opts ...SubOption) (Subscription, error) {
	if h == nil {
		return nil, errors.New("bus: nil handler")
	}
	if eventType == "" {
		return nil, errors.New("bus: empty event_type")
	}
	o := collectSubOpts(opts)
	subject := b.eventSubject(eventType, b.opts.Tenant)

	chained := middleware.Chain(middleware.Handler(h), b.opts.Middleware...)

	sub, err := b.opts.Transport.Subscribe(ctx, subject, o.Queue, b.dispatch(chained))
	if err != nil {
		return nil, err
	}
	return &subscription{sub: sub}, nil
}

// dispatch decodes the raw transport message into an Envelope and runs the
// supplied handler. Decode errors are returned to the transport (which logs
// them via its own driver semantics); handler errors are owned by the
// middleware chain (Recover/Logging).
func (b *defaultBus) dispatch(h middleware.Handler) transport.Handler {
	return func(ctx context.Context, msg *transport.RawMessage) error {
		env, err := b.opts.Codec.DecodeEnvelope(msg.Data)
		if err != nil {
			return err
		}
		return h(ctx, env)
	}
}
