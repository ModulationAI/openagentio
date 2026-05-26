"""Envelope → OTel TextMapCarrier adapter. Mirrors pkg/middleware/otel/carrier.go.

Only the W3C ``traceparent`` key is surfaced for v0.2 — ``tracestate``
requires an envelope schema bump and is deferred.

Provides :class:`EnvelopeCarrier`, :class:`EnvelopeGetter`, and
:class:`EnvelopeSetter` — custom implementations of OTel's
:class:`Getter` and :class:`Setter` protocols so the carrier does not
need to masquerade as a :class:`dict`.
"""
from __future__ import annotations

from typing import Optional

from opentelemetry.propagators.textmap import Getter, Setter

from openagentio.event.envelope import Envelope

_TRACEPARENT_KEY = "traceparent"


class EnvelopeCarrier:
    """Wraps an :class:`Envelope` for OTel context propagation.

    ``Get`` / ``Set`` match ``traceparent`` case-insensitively, mirroring
    Go's ``strings.EqualFold`` behaviour.  All other keys are silently ignored.
    """

    def __init__(self, envelope: Envelope) -> None:
        self._env = envelope

    def get(self, key: str) -> str:
        if key.lower() == _TRACEPARENT_KEY:
            return self._env.traceparent
        return ""

    def set(self, key: str, value: str) -> None:
        if key.lower() == _TRACEPARENT_KEY:
            self._env.traceparent = value

    def keys(self) -> list[str]:
        return [_TRACEPARENT_KEY]


class EnvelopeGetter(Getter[EnvelopeCarrier]):
    """OTel Getter that reads from an EnvelopeCarrier."""

    def get(self, carrier: EnvelopeCarrier, key: str) -> Optional[list[str]]:
        val = carrier.get(key)
        if val:
            return [val]
        return None

    def keys(self, carrier: EnvelopeCarrier) -> list[str]:
        return carrier.keys()


class EnvelopeSetter(Setter[EnvelopeCarrier]):
    """OTel Setter that writes to an EnvelopeCarrier."""

    def set(self, carrier: EnvelopeCarrier, key: str, value: str) -> None:
        carrier.set(key, value)