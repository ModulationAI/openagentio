"""OTel Bridge — optional OpenTelemetry integration. Mirrors pkg/middleware/otel/.

Importing this package requires ``opentelemetry-api`` to be installed.
Add ``openagentio[otel]`` to your dependencies to include it.

Public API:

- :func:`Trace` — inbound middleware: extracts parent from traceparent,
  starts Consumer span, sets semconv + acp attributes.
- :func:`envelope_preparer` — outbound hook: injects active span's
  traceparent into every outbound envelope.
- :class:`EnvelopeCarrier` — TextMapCarrier adapter for Envelope.traceparent.
- :class:`Config` — resolved configuration (tracer + propagator).
- :data:`Option`, :func:`WithTracerProvider`, :func:`WithPropagator` — functional
  option helpers.

Quickstart::

    from openagentio import Bus, InMemoryDriver, WithAgentID, WithTransport
    from openagentio import WithMiddleware, WithEnvelopePreparer
    from openagentio.middleware.otel import Trace, envelope_preparer

    bus = Bus.new(
        WithAgentID("agent"),
        WithTransport(InMemoryDriver()),
        WithMiddleware(Trace()),
        WithEnvelopePreparer(envelope_preparer()),
    )
"""
from __future__ import annotations

from openagentio.middleware.otel.config import Config, Option, WithTracerProvider, WithPropagator
from openagentio.middleware.otel.carrier import EnvelopeCarrier, EnvelopeGetter, EnvelopeSetter
from openagentio.middleware.otel.trace import Trace
from openagentio.middleware.otel.preparer import envelope_preparer

__all__ = [
    "Config",
    "Option",
    "WithTracerProvider",
    "WithPropagator",
    "EnvelopeCarrier",
    "EnvelopeGetter",
    "EnvelopeSetter",
    "Trace",
    "envelope_preparer",
]