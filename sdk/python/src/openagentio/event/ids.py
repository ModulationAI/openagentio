"""Event ID generation. Mirrors pkg/event/id.go: UUIDv7 (RFC 9562) with v4 fallback."""
from __future__ import annotations

import secrets
import time
import uuid


def new_id() -> str:
    """A freshly minted UUIDv7 event identifier.

    Time-ordered (sortable lexically), 36 chars with hyphens. Falls back to
    UUIDv4 if entropy generation fails for any reason so callers never see
    an empty ID.
    """
    try:
        ts_ms = int(time.time() * 1000) & 0xFFFFFFFFFFFF
        rand = secrets.token_bytes(10)
        b = bytearray(16)
        b[0] = (ts_ms >> 40) & 0xFF
        b[1] = (ts_ms >> 32) & 0xFF
        b[2] = (ts_ms >> 24) & 0xFF
        b[3] = (ts_ms >> 16) & 0xFF
        b[4] = (ts_ms >> 8) & 0xFF
        b[5] = ts_ms & 0xFF
        b[6] = 0x70 | (rand[0] & 0x0F)
        b[7] = rand[1]
        b[8] = 0x80 | (rand[2] & 0x3F)
        b[9:16] = rand[3:10]
        return str(uuid.UUID(bytes=bytes(b)))
    except Exception:
        return str(uuid.uuid4())
