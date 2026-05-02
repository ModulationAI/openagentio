// Package nats is the NATS Core driver for AgentFlowBus.
//
// It maps the transport contract onto github.com/nats-io/nats.go: Publish,
// Subscribe (with optional queue groups), Request/Reply, and ephemeral
// _INBOX-backed Inbox for streaming responses.
package nats

import (
	"context"
	"errors"
	"sync"

	natsgo "github.com/nats-io/nats.go"

	"github.com/ModulationAI/agentflowbus/pkg/transport"
)

// Options configure the NATS driver.
type Options struct {
	URL  string
	Name string
}

// Option mutates Options.
type Option func(*Options)

// URL sets the NATS server URL (default "nats://localhost:4222").
func URL(u string) Option { return func(o *Options) { o.URL = u } }

// Name sets the client connection name reported to the server.
func Name(n string) Option { return func(o *Options) { o.Name = n } }

// Driver is the NATS Core transport. The zero value is unusable; construct
// with New and call Connect before any Publish/Subscribe/Request.
type Driver struct {
	opts Options

	mu     sync.Mutex
	conn   *natsgo.Conn
	ctx    context.Context
	cancel context.CancelFunc
}

// New constructs a Driver. Options are applied in order; later options win.
func New(opts ...Option) *Driver {
	o := Options{URL: natsgo.DefaultURL}
	for _, f := range opts {
		f(&o)
	}
	return &Driver{opts: o}
}

// Connect establishes the underlying NATS connection. Idempotent: a second
// call while already connected is a no-op.
func (d *Driver) Connect(_ context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn != nil {
		return nil
	}
	var natsOpts []natsgo.Option
	if d.opts.Name != "" {
		natsOpts = append(natsOpts, natsgo.Name(d.opts.Name))
	}
	nc, err := natsgo.Connect(d.opts.URL, natsOpts...)
	if err != nil {
		return err
	}
	d.conn = nc
	d.ctx, d.cancel = context.WithCancel(context.Background())
	return nil
}

// Close drains the connection so in-flight handlers complete, then releases
// the cancel context handed to subscription handlers. Idempotent.
func (d *Driver) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn == nil {
		return nil
	}
	if d.cancel != nil {
		d.cancel()
	}
	err := d.conn.Drain()
	d.conn = nil
	return err
}

// Capabilities advertises NATS Core features. Persistence is false; durable
// streams are reserved for the future JetStream driver (v0.3).
func (*Driver) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		Streaming:   true,
		Persistence: false,
		QueueGroup:  true,
		Headers:     true,
	}
}

func (d *Driver) Publish(_ context.Context, msg *transport.RawMessage) error {
	if msg == nil {
		return errors.New("nats: nil message")
	}
	nc, err := d.requireConn()
	if err != nil {
		return err
	}
	return nc.PublishMsg(toNats(msg))
}

func (d *Driver) Subscribe(_ context.Context, subject, queue string, h transport.Handler) (transport.Subscription, error) {
	if h == nil {
		return nil, errors.New("nats: nil handler")
	}
	nc, err := d.requireConn()
	if err != nil {
		return nil, err
	}

	driverCtx := d.ctx
	dispatch := func(m *natsgo.Msg) {
		// MsgHandler has no error return: handler errors are owned by the bus
		// middleware (recover/logging) layered above the transport. We pass
		// d.ctx so handlers learn about driver shutdown via cancellation.
		_ = h(driverCtx, fromNats(m))
	}

	var sub *natsgo.Subscription
	if queue == "" {
		sub, err = nc.Subscribe(subject, dispatch)
	} else {
		sub, err = nc.QueueSubscribe(subject, queue, dispatch)
	}
	if err != nil {
		return nil, err
	}
	return &natsSubscription{sub: sub}, nil
}

func (d *Driver) Request(ctx context.Context, msg *transport.RawMessage) (*transport.RawMessage, error) {
	if msg == nil {
		return nil, errors.New("nats: nil message")
	}
	nc, err := d.requireConn()
	if err != nil {
		return nil, err
	}
	resp, err := nc.RequestMsgWithContext(ctx, toNats(msg))
	if err != nil {
		return nil, err
	}
	return fromNats(resp), nil
}

func (d *Driver) OpenInbox(_ context.Context) (transport.Inbox, error) {
	nc, err := d.requireConn()
	if err != nil {
		return nil, err
	}
	subject := natsgo.NewInbox()
	box := &natsInbox{
		subject: subject,
		ch:      make(chan *transport.RawMessage, 64),
		done:    make(chan struct{}),
	}
	sub, err := nc.Subscribe(subject, func(m *natsgo.Msg) {
		select {
		case box.ch <- fromNats(m):
		case <-box.done:
			// Inbox closed mid-flight; drop the message rather than panic.
		}
	})
	if err != nil {
		return nil, err
	}
	box.sub = sub
	return box, nil
}

func (d *Driver) requireConn() (*natsgo.Conn, error) {
	d.mu.Lock()
	nc := d.conn
	d.mu.Unlock()
	if nc == nil {
		return nil, errors.New("nats: not connected")
	}
	return nc, nil
}

func toNats(msg *transport.RawMessage) *natsgo.Msg {
	nm := &natsgo.Msg{
		Subject: msg.Subject,
		Data:    msg.Data,
		Reply:   msg.ReplyTo,
	}
	if len(msg.Headers) > 0 {
		nm.Header = natsgo.Header{}
		for k, v := range msg.Headers {
			nm.Header.Set(k, v)
		}
	}
	return nm
}

func fromNats(m *natsgo.Msg) *transport.RawMessage {
	rm := &transport.RawMessage{
		Subject: m.Subject,
		Data:    m.Data,
		ReplyTo: m.Reply,
	}
	if len(m.Header) > 0 {
		rm.Headers = make(map[string]string, len(m.Header))
		for k := range m.Header {
			rm.Headers[k] = m.Header.Get(k)
		}
	}
	return rm
}

type natsSubscription struct {
	sub *natsgo.Subscription
}

func (s *natsSubscription) Unsubscribe() error { return s.sub.Unsubscribe() }

type natsInbox struct {
	subject string
	ch      chan *transport.RawMessage
	sub     *natsgo.Subscription
	done    chan struct{}
	once    sync.Once
}

func (i *natsInbox) Subject() string { return i.subject }

func (i *natsInbox) Recv(ctx context.Context) (*transport.RawMessage, error) {
	select {
	case m := <-i.ch:
		return m, nil
	case <-i.done:
		return nil, errors.New("nats inbox: closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (i *natsInbox) Close() error {
	var err error
	i.once.Do(func() {
		close(i.done)
		if i.sub != nil {
			err = i.sub.Unsubscribe()
		}
	})
	return err
}

// Compile-time interface check.
var _ transport.Transport = (*Driver)(nil)
