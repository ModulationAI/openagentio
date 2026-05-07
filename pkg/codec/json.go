package codec

import (
	"encoding/json"

	"github.com/ModulationAI/openagentio/pkg/event"
)

type jsonCodec struct{}

// JSON returns the default JSON codec. The same instance is safe for
// concurrent use; callers may stash it in package-level variables.
func JSON() Codec { return jsonCodec{} }

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) EncodeEnvelope(e *event.Envelope) ([]byte, error) {
	return json.Marshal(e)
}

func (jsonCodec) DecodeEnvelope(data []byte) (*event.Envelope, error) {
	var out event.Envelope
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (jsonCodec) EncodePayload(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(v)
}

func (jsonCodec) DecodePayload(raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, v)
}
