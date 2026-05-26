"""OTel EnvelopePreparer. Mirrors pkg/middleware/otel/propagator.go.

Injects the current OTel SpanContext into outbound envelopes (Publish /
Invoke / StreamInvoke) via the ``traceparent`` field.

Wire it via:

    bus = Bus.new(
        WithAgentID("agent"),
        WithTransport(InMemoryDriver()),
        WithEnvelopePreparer(envelope_preparer()),
    )

The preparer is a no-op when no valid span is active — direct user calls
outside any traced flow leave the envelope untouched.
"""
from __future__ import annotations

from opentelemetry.trace import get_current_span

from openagentio.event.envelope import Envelope

from openagentio.middleware.otel.carrier import EnvelopeCarrier, EnvelopeSetter
from openagentio.middleware.otel.config import Option, new_config


def envelope_preparer(*opts: Option):
    """Return a Bus EnvelopePreparer that injects the active span's
    traceparent into every outbound envelope.

    If no valid span is active, the envelope is left untouched (matching
    Go's ``!trace.SpanFromContext(ctx).SpanContext().IsValid()`` guard).

    In Python, the active span is retrieved via ``trace.get_current_span()``
    which uses contextvars — the span set by the Trace middleware is
    automatically available in nested Bus calls.
    """
    cfg = new_config(*opts)

    setter = EnvelopeSetter()

    def preparer(env: Envelope) -> None:
        if env is None:
            return
        span = get_current_span()
        if not span.get_span_context().is_valid:
            return
        carrier = EnvelopeCarrier(env)
        cfg.propagator.inject(carrier, setter=setter)

    return preparer