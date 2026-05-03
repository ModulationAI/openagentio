package http

import (
	"errors"
	"net/http"
	"strings"
)

// AuthContext carries authentication-derived envelope overrides. Any non-empty
// field replaces the value derived from the corresponding HTTP header — e.g.
// the auth layer may pin TenantID even when the client sets X-Tenant-Id.
type AuthContext struct {
	TenantID       string
	UserID         string
	SessionID      string
	ConversationID string
	Channel        string
	TraceID        string
}

// AuthFunc validates a request and returns the authenticated identity. Returning
// a non-nil error short-circuits the request with 401. Returning (nil, nil) is
// equivalent to "no overrides" and lets the request proceed.
type AuthFunc func(r *http.Request) (*AuthContext, error)

// ErrUnauthorized is the canonical sentinel for AuthFunc to signal a rejected
// credential. Wrapping it preserves errors.Is matching for callers that want
// to distinguish auth failures from other errors.
var ErrUnauthorized = errors.New("http: unauthorized")

// BearerAuth builds an AuthFunc that extracts the bearer token from the
// Authorization header and delegates to validator. The validator returns the
// authenticated AuthContext or any error to reject the request.
//
// Header format: `Authorization: Bearer <token>` (case-insensitive scheme).
// Missing or malformed header → ErrUnauthorized.
func BearerAuth(validator func(token string) (*AuthContext, error)) AuthFunc {
	if validator == nil {
		panic("http: BearerAuth requires a non-nil validator")
	}
	return func(r *http.Request) (*AuthContext, error) {
		raw := r.Header.Get("Authorization")
		if raw == "" {
			return nil, ErrUnauthorized
		}
		const prefix = "bearer "
		if len(raw) <= len(prefix) || !strings.EqualFold(raw[:len(prefix)], prefix) {
			return nil, ErrUnauthorized
		}
		token := strings.TrimSpace(raw[len(prefix):])
		if token == "" {
			return nil, ErrUnauthorized
		}
		return validator(token)
	}
}
