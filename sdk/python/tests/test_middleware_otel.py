"""OTel Bridge tests mirroring Go's pkg/middleware/otel/otel_test.go."""
from __future__ import annotations

import pytest

from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ALWAYS_OFF
from opentelemetry.trace import SpanKind, StatusCode, get_current_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from openagentio import (
    Bus,
    InMemoryDriver,
    WithAgentID,
    WithTransport,
    WithMiddleware,
    WithEnvelopePreparer,
)
from openagentio.event.envelope import Envelope
from openagentio.event.types import MessageReceived, ResponseError
from openagentio.middleware import Chain
from openagentio.middleware.otel import (
    Trace,
    envelope_preparer,
    EnvelopeCarrier,
    EnvelopeGetter,
    WithTracerProvider,
    WithPropagator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _new_recorder() -> tuple[TracerProvider, InMemorySpanExporter]:
    """Build a TracerProvider with in-memory span exporter for assertions."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider(sampler=ALWAYS_ON)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


def _make_envelope(event_type: str) -> Envelope:
    env = Envelope.new(event_type)
    env.tenant_id = "tenant-X"
    env.session_id = "sess-X"
    env.conversation_id = "conv-X"
    return env


def _last_named(spans, name: str):
    for s in reversed(spans):
        if s.name == name:
            return s
    return None


def _index_attrs(span) -> dict[str, str]:
    return dict(span.attributes)


# ---------------------------------------------------------------------------
# Tests — Trace middleware (Go parity: TestTrace*)
# ---------------------------------------------------------------------------


class TestTrace:
    @pytest.mark.asyncio
    async def test_trace_extracts_parent_from_envelope(self):
        """Child span belongs to the same trace as the parent injected via
        envelope.traceparent."""
        provider, exporter = _new_recorder()

        # 1. Create a parent span to inject its traceparent.
        parent_tracer = provider.get_tracer("test-parent")
        prop = TraceContextTextMapPropagator()
        parent_carrier = {}
        with parent_tracer.start_as_current_span("parent") as parent_span:
            prop.inject(carrier=parent_carrier)

        want_trace_id = parent_span.context.trace_id

        # 2. Put that traceparent into an envelope.
        env = _make_envelope(MessageReceived)
        env.traceparent = parent_carrier.get("traceparent", "")

        # 3. Run the middleware.
        mw = Trace(WithTracerProvider(provider), WithPropagator(prop))

        async def noop_handler(env: Envelope):
            pass

        handler = Chain(noop_handler, mw)
        await handler(env)

        # 4. Recorded child span must share the parent's trace id.
        spans = exporter.get_finished_spans()
        child = _last_named(spans, f"acp.handle.{MessageReceived}")
        assert child is not None
        assert child.context.trace_id == want_trace_id

    @pytest.mark.asyncio
    async def test_trace_records_error_on_handler_failure(self):
        """Handler exception: span status = ERROR, exception event recorded."""
        provider, exporter = _new_recorder()

        mw = Trace(
            WithTracerProvider(provider),
            WithPropagator(TraceContextTextMapPropagator()),
        )

        async def fail_handler(env: Envelope):
            raise RuntimeError("kaboom")

        handler = Chain(fail_handler, mw)

        with pytest.raises(RuntimeError, match="kaboom"):
            await handler(_make_envelope(ResponseError))

        span = _last_named(exporter.get_finished_spans(), f"acp.handle.{ResponseError}")
        assert span is not None
        assert span.status.status_code == StatusCode.ERROR
        assert "kaboom" in span.status.description
        # use_span auto-records an "exception" event.
        found = any(ev.name == "exception" for ev in span.events)
        assert found

    @pytest.mark.asyncio
    async def test_trace_attributes_set(self):
        """All 7 semconv + acp attributes + Consumer span kind."""
        provider, exporter = _new_recorder()

        mw = Trace(WithTracerProvider(provider))

        async def noop_handler(env: Envelope):
            pass

        handler = Chain(noop_handler, mw)
        env = _make_envelope(MessageReceived)
        await handler(env)

        span = _last_named(exporter.get_finished_spans(), f"acp.handle.{MessageReceived}")
        assert span is not None

        attrs = _index_attrs(span)
        assert attrs.get("messaging.system") == "acp"
        assert attrs.get("messaging.destination.name") == MessageReceived
        assert attrs.get("messaging.message.id") == env.event_id
        assert attrs.get("acp.event_type") == MessageReceived
        assert attrs.get("acp.tenant_id") == "tenant-X"
        assert attrs.get("acp.session_id") == "sess-X"
        assert attrs.get("acp.conversation_id") == "conv-X"

        assert span.kind == SpanKind.CONSUMER


# ---------------------------------------------------------------------------
# Tests — EnvelopePreparer (Go parity: TestEnvelopePreparer*)
# ---------------------------------------------------------------------------


class TestEnvelopePreparer:
    @pytest.mark.asyncio
    async def test_preparer_injects_traceparent(self):
        """Active span → traceparent injected into outbound envelope."""
        provider, _ = _new_recorder()

        prep = envelope_preparer(WithTracerProvider(provider))

        tracer = provider.get_tracer("test")
        with tracer.start_as_current_span("outbound") as active_span:
            env = _make_envelope(MessageReceived)
            prep(env)

        assert env.traceparent != ""

        # Decode the traceparent and confirm the trace id matches.
        carrier = EnvelopeCarrier(env)
        prop = TraceContextTextMapPropagator()
        ctx = prop.extract(carrier, getter=EnvelopeGetter())
        extracted_span = get_current_span(ctx)
        extracted_ctx = extracted_span.get_span_context()
        assert extracted_ctx.is_valid
        assert extracted_ctx.trace_id == active_span.context.trace_id

    def test_preparer_noop_without_span(self):
        """No active span → envelope.traceparent is NOT mutated."""
        prep = envelope_preparer(WithPropagator(TraceContextTextMapPropagator()))
        env = _make_envelope(MessageReceived)
        env.traceparent = "preserved"
        prep(env)
        assert env.traceparent == "preserved"

    def test_preparer_injects_traceparent_with_always_off_sampler(self):
        """ALWAYS_OFF sampler → span is NonRecordingSpan with valid SpanContext.
        The preparer should still inject traceparent because Go only checks
        SpanContext().IsValid(), not is_recording. Trace context must propagate
        even when the local span is not recorded."""
        provider_off = TracerProvider(sampler=ALWAYS_OFF)

        prep = envelope_preparer(
            WithTracerProvider(provider_off),
            WithPropagator(TraceContextTextMapPropagator()),
        )

        tracer_off = provider_off.get_tracer("test")
        with tracer_off.start_as_current_span("outbound"):
            env = _make_envelope(MessageReceived)
            prep(env)

        assert env.traceparent != ""
        # Decode and verify the traceparent carries a valid trace context.
        carrier = EnvelopeCarrier(env)
        prop = TraceContextTextMapPropagator()
        ctx = prop.extract(carrier, getter=EnvelopeGetter())
        extracted_span = get_current_span(ctx)
        assert extracted_span.get_span_context().is_valid


# ---------------------------------------------------------------------------
# Tests — Bus integration
# ---------------------------------------------------------------------------


class TestBusIntegration:
    @pytest.mark.asyncio
    async def test_otel_trace_middleware_with_bus(self):
        """OTel Trace middleware works end-to-end with Bus.invoke."""
        provider, exporter = _new_recorder()

        mw = Trace(WithTracerProvider(provider))

        bus = Bus.new(
            WithAgentID("otel-agent"),
            WithTransport(InMemoryDriver()),
            WithMiddleware(mw),
        )
        await bus.connect()

        async def echo(env: Envelope):
            return env.payload_json()

        await bus.handle_invoke("echo", echo)
        resp = await bus.invoke("echo", {"msg": "hello"})
        await bus.close()

        spans = exporter.get_finished_spans()
        # At least one acp.handle span should be recorded.
        found = any(s.name.startswith("acp.handle.") for s in spans)
        assert found