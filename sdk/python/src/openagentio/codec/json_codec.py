"""JSON codec — wire-compatible with pkg/codec/json.go."""
from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, Protocol, runtime_checkable

from openagentio.event.envelope import Envelope


@runtime_checkable
class Codec(Protocol):
    """Marshals and unmarshals envelopes and payloads.

    Implementations must be safe for concurrent use.
    """

    name: str

    def encode_envelope(self, env: Envelope) -> bytes: ...
    def decode_envelope(self, data: bytes) -> Envelope: ...
    def encode_payload(self, value: Any) -> bytes | None: ...
    def decode_payload(self, raw: bytes | None) -> Any: ...


class JSONCodec:
    """Default codec. Same instance is safe for concurrent use."""

    name = "json"

    def encode_envelope(self, env: Envelope) -> bytes:
        return env.to_bytes()

    def decode_envelope(self, data: bytes) -> Envelope:
        return Envelope.from_bytes(data)

    def encode_payload(self, value: Any) -> bytes | None:
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if is_dataclass(value):
            value = asdict(value)
        return json.dumps(value, separators=(",", ":")).encode("utf-8")

    def decode_payload(self, raw: bytes | None) -> Any:
        if not raw:
            return None
        return json.loads(raw)
