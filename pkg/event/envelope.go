// Package event defines the ACP-compatible Envelope and the standard set of
// event type identifiers exchanged across the bus.
package event

import (
	"encoding/json"
	"time"
)

// Envelope is the wire-level container for every message routed by the bus.
type Envelope struct {
	SpecVersion   string `json:"spec_version"`
	SchemaVersion int    `json:"schema_version"`

	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`

	TraceID        string `json:"trace_id,omitempty"`
	SpanID         string `json:"span_id,omitempty"`
	Traceparent    string `json:"traceparent,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	ConversationID string `json:"conversation_id,omitempty"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	ReplyTo        string `json:"reply_to,omitempty"`

	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`

	Channel  string `json:"channel,omitempty"`
	TenantID string `json:"tenant_id,omitempty"`
	UserID   string `json:"user_id,omitempty"`

	Seq     uint64 `json:"seq,omitempty"`
	IsFinal bool   `json:"is_final,omitempty"`

	Payload  json.RawMessage `json:"payload,omitempty"`
	Metadata map[string]any  `json:"metadata,omitempty"`
}

// New constructs an Envelope pre-populated with spec/schema version, a fresh
// UUIDv7 event_id, and the current UTC timestamp.
func New(eventType string) *Envelope {
	return &Envelope{
		SpecVersion:   SpecVersion,
		SchemaVersion: SchemaVersion,
		EventID:       NewID(),
		EventType:     eventType,
		OccurredAt:    time.Now().UTC(),
	}
}

// Clone returns a shallow copy suitable for in-pipeline mutation. Payload bytes
// are shared by reference; callers that mutate the payload should copy it.
func (e *Envelope) Clone() *Envelope {
	if e == nil {
		return nil
	}
	cp := *e
	if e.Metadata != nil {
		cp.Metadata = make(map[string]any, len(e.Metadata))
		for k, v := range e.Metadata {
			cp.Metadata[k] = v
		}
	}
	return &cp
}
