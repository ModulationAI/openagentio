// Package transportdial provides a convenience factory for creating a
// transport.Transport from environment variables.
//
// This is the recommended quick-start path for most applications.
// Advanced users who need TLS, custom connection pools, or other low-level
// NATS options should construct nats.New / inmem.New directly.
package transportdial

import (
	"context"
	"fmt"
	"os"

	"github.com/ModulationAI/openagentio/pkg/transport"
	"github.com/ModulationAI/openagentio/pkg/transport/inmem"
	"github.com/ModulationAI/openagentio/pkg/transport/nats"
)

type options struct {
	natsName string
}

// Option configures Dial.
type Option func(*options)

// WithNATSName sets the NATS connection name.
// Only used when OPENAGENTIO_TRANSPORT=nats (or default).
func WithNATSName(name string) Option {
	return func(o *options) {
		o.natsName = name
	}
}

// Dial creates a Transport from environment variables.
//
// Env vars:
//   - OPENAGENTIO_TRANSPORT: "nats" (default) or "inmem"
//   - NATS_URL: NATS server URL, default "nats://localhost:4222"
//
// Example:
//
//	tp, err := transportdial.Dial(ctx, transportdial.WithNATSName("echo-agent"))
func Dial(ctx context.Context, opts ...Option) (transport.Transport, error) {
	o := options{}
	for _, fn := range opts {
		fn(&o)
	}

	mode := os.Getenv("OPENAGENTIO_TRANSPORT")
	switch mode {
	case "inmem":
		return inmem.New(), nil
	case "", "nats":
		url := os.Getenv("NATS_URL")
		if url == "" {
			url = "nats://localhost:4222"
		}
		natsOpts := []nats.Option{nats.URL(url)}
		if o.natsName != "" {
			natsOpts = append(natsOpts, nats.Name(o.natsName))
		}
		tp := nats.New(natsOpts...)
		if err := tp.Connect(ctx); err != nil {
			return nil, fmt.Errorf("connect to NATS %s: %w", url, err)
		}
		return tp, nil
	default:
		return nil, fmt.Errorf("unsupported OPENAGENTIO_TRANSPORT=%q", mode)
	}
}
