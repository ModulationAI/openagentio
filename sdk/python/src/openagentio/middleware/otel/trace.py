"""OTel Trace middleware. Mirrors pkg/middleware/otel/otel.go.

On every inbound handler invocation the middleware:

1. Extracts an upstream SpanContext from ``envelope.traceparent``.
2. Starts a Consumer-kind span named ``acp.handle.<event_type>``.
3. Sets messaging-semconv attributes plus ``acp.*`` extensions.
4. Records errors and marks span status on handler failure.

When no TracerProvider is configured globally and no Option overrides it,
OTel returns a Noop tracer and the middleware adds negligible overhead.
"""
from __future__ import annotations

from opentelemetry.trace import SpanKind, use_span

from openagentio.event.envelope import Envelope
from openagentio.middleware import Handler, Middleware

from openagentio.middleware.otel.carrier import EnvelopeCarrier, EnvelopeGetter
from openagentio.middleware.otel.config import Option, new_config


def Trace(*opts: Option) -> Middleware:
    """Create OTel trace middleware.

    Each handler invocation extracts the parent span from ``traceparent``,
    starts a Consumer-kind child span, and records semconv + acp attributes.
    Errors are recorded on the span and re-raised.
    """
    cfg = new_config(*opts)

    getter = EnvelopeGetter()

    def wrap(next: Handler) -> Handler:
        async def handler(env: Envelope) -> None:
            if env is None:
                await next(env)
                return

            carrier = EnvelopeCarrier(env)
            parent_ctx = cfg.propagator.extract(carrier, getter=getter)

            span = cfg.tracer.start_span(
                name=f"acp.handle.{env.event_type}",
                context=parent_ctx,
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "acp",
                    "messaging.destination.name": env.event_type,
                    "messaging.message.id": env.event_id,
                    "acp.event_type": env.event_type,
                    "acp.tenant_id": env.tenant_id,
                    "acp.session_id": env.session_id,
                    "acp.conversation_id": env.conversation_id,
                },
            )

            with use_span(span, end_on_exit=True):
                await next(env)

        return handler

    return wrap