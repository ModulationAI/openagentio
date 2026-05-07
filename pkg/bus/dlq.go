package bus

import (
	"context"
	"fmt"

	"github.com/ModulationAI/openagentio/pkg/codec"
	"github.com/ModulationAI/openagentio/pkg/event"
	"github.com/ModulationAI/openagentio/pkg/middleware"
	"github.com/ModulationAI/openagentio/pkg/transport"
)

// DLQSink returns a middleware.DLQSink that publishes a cloned envelope onto
// the dead-letter subject `{prefix}.dlq.{event_type}` using the supplied codec
// and transport. This is the canonical NATS Core–based DLQ implementation; it
// does not depend on JetStream.
//
// The sink stamps two metadata keys on the clone before publishing:
//   - acp.dlq.original_event_type — the original envelope.EventType
//   - acp.dlq.last_error            — the string representation of lastErr
func DLQSink(prefix string, codec codec.Codec, tr transport.Transport) middleware.DLQSink {
	return func(ctx context.Context, e *event.Envelope, lastErr error) error {
		cp := e.Clone()
		if cp.Metadata == nil {
			cp.Metadata = make(map[string]any)
		}
		cp.Metadata["acp.dlq.original_event_type"] = cp.EventType
		if lastErr != nil {
			cp.Metadata["acp.dlq.last_error"] = lastErr.Error()
		}
		data, err := codec.EncodeEnvelope(cp)
		if err != nil {
			return fmt.Errorf("dlq: encode envelope: %w", err)
		}
		subject := prefix + ".dlq." + cp.EventType
		return tr.Publish(ctx, &transport.RawMessage{
			Subject: subject,
			Data:    data,
		})
	}
}
