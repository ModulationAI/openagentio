// Package codec defines the serialization boundary between the bus runtime and
// the wire. v0.1 ships a JSON implementation; protobuf is reserved for v0.3.
package codec

import (
	"encoding/json"

	"github.com/ModulationAI/agentflowbus/pkg/event"
)

// Codec marshals/unmarshals envelopes and payloads. Implementations must be
// safe for concurrent use.
type Codec interface {
	// Name returns a stable identifier for the codec, e.g. "json" or "protobuf".
	// Used by adapters that negotiate Content-Type / Content-Encoding.
	Name() string

	EncodeEnvelope(e *event.Envelope) ([]byte, error)
	DecodeEnvelope(data []byte) (*event.Envelope, error)

	// Encode/DecodePayload handle the inner business value. Returning RawMessage
	// keeps the wire representation opaque to the bus runtime so callers can
	// embed pre-serialized blobs without double-encoding.
	EncodePayload(v any) (json.RawMessage, error)
	DecodePayload(raw json.RawMessage, v any) error
}
