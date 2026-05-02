package event

import "github.com/google/uuid"

// NewID returns a freshly minted event identifier. The format is UUIDv7
// (RFC 9562): time-ordered, 36 chars with hyphens, natively supported by every
// major language and database.
//
// On entropy failure NewID falls back to UUIDv4 so callers never see an empty
// ID. The fallback path is exercised by tests via uuid.SetRand.
func NewID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}
