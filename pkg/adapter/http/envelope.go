package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/ModulationAI/openagentio/pkg/event"
)

const maxBodyBytes = 4 << 20 // 4 MiB cap to keep gateway memory bounded

// readEnvelope builds a request envelope from the HTTP request: headers feed
// correlation/tenancy fields, body becomes Payload (raw JSON), and AuthContext
// (if present) overrides any header-derived values.
//
// eventType is empty for invoke/stream requests (Bus picks
// MessageReceived) and set for /v1/events publishes.
func readEnvelope(r *http.Request, eventType string, ac *AuthContext) (*event.Envelope, error) {
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	var env *event.Envelope
	if eventType != "" {
		env = event.New(eventType)
	} else {
		// Bus.buildRequestEnvelope will assign MessageReceived when payload is
		// not an envelope; passing an envelope through forces our headers to
		// flow. Use an empty event type here and let the Bus stamp one if it
		// wants — actually we must set one because Envelope requires it.
		env = event.New(event.MessageReceived)
	}

	// Body → Payload. Reject malformed JSON to fail fast (JSON Codec on the
	// other side would otherwise reject it inside Bus).
	if len(body) > 0 {
		if !json.Valid(body) {
			return nil, errors.New("body is not valid JSON")
		}
		env.Payload = json.RawMessage(body)
	}

	// Headers → envelope fields.
	h := r.Header
	if v := h.Get("X-Trace-Id"); v != "" {
		env.TraceID = v
	}
	if v := h.Get("X-Traceparent"); v != "" {
		env.Traceparent = v
	}
	if v := h.Get("X-Tenant-Id"); v != "" {
		env.TenantID = v
	}
	if v := h.Get("X-Session-Id"); v != "" {
		env.SessionID = v
	}
	if v := h.Get("X-Conversation-Id"); v != "" {
		env.ConversationID = v
	}
	if v := h.Get("X-User-Id"); v != "" {
		env.UserID = v
	}
	if v := h.Get("X-Channel"); v != "" {
		env.Channel = v
	}

	// AuthContext overrides headers — auth-derived identity wins.
	if ac != nil {
		if ac.TenantID != "" {
			env.TenantID = ac.TenantID
		}
		if ac.UserID != "" {
			env.UserID = ac.UserID
		}
		if ac.SessionID != "" {
			env.SessionID = ac.SessionID
		}
		if ac.ConversationID != "" {
			env.ConversationID = ac.ConversationID
		}
		if ac.Channel != "" {
			env.Channel = ac.Channel
		}
		if ac.TraceID != "" {
			env.TraceID = ac.TraceID
		}
	}

	return env, nil
}
