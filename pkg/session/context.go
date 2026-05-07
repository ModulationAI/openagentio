// Package session carries the active envelope (and the trace/session/conversation
// metadata it implies) on the context so downstream handlers and nested Bus
// calls can propagate it without re-parsing the message.
package session

import (
	"context"

	"github.com/ModulationAI/openagentio/pkg/event"
)

type ctxKey int

const keyEnvelope ctxKey = 1

// Inject stores the envelope in ctx. Passing a nil envelope returns ctx
// unchanged.
func Inject(ctx context.Context, e *event.Envelope) context.Context {
	if e == nil {
		return ctx
	}
	return context.WithValue(ctx, keyEnvelope, e)
}

// From returns the envelope previously stored via Inject, or nil if none.
func From(ctx context.Context) *event.Envelope {
	e, _ := ctx.Value(keyEnvelope).(*event.Envelope)
	return e
}

// Trace returns (trace_id, true) if an envelope with a non-empty TraceID is
// present on ctx.
func Trace(ctx context.Context) (string, bool) {
	if e := From(ctx); e != nil && e.TraceID != "" {
		return e.TraceID, true
	}
	return "", false
}

// Session returns (session_id, true) if an envelope with a non-empty
// SessionID is present on ctx.
func Session(ctx context.Context) (string, bool) {
	if e := From(ctx); e != nil && e.SessionID != "" {
		return e.SessionID, true
	}
	return "", false
}

// Conversation returns (conversation_id, true) if an envelope with a
// non-empty ConversationID is present on ctx.
func Conversation(ctx context.Context) (string, bool) {
	if e := From(ctx); e != nil && e.ConversationID != "" {
		return e.ConversationID, true
	}
	return "", false
}

// Tenant returns (tenant_id, true) if an envelope with a non-empty TenantID
// is present on ctx.
func Tenant(ctx context.Context) (string, bool) {
	if e := From(ctx); e != nil && e.TenantID != "" {
		return e.TenantID, true
	}
	return "", false
}
