"""EnvelopePreparer integration tests. Mirrors pkg/bus/default.go prepareEnvelope."""
from __future__ import annotations

import asyncio

from openagentio import (
    Bus,
    Envelope,
    InMemoryDriver,
    MessageReceived,
    WithAgentID,
    WithEnvelopePreparer,
    WithTransport,
)


async def test_preparer_mutates_envelope() -> None:
    """Custom preparer sets a field on outbound envelopes."""
    stamp = "stamped-value"

    def stamp_preparer(env: Envelope) -> None:
        env.trace_id = stamp

    bus = Bus.new(
        WithAgentID("a"),
        WithTransport(InMemoryDriver()),
        WithEnvelopePreparer(stamp_preparer),
    )
    await bus.connect()

    received: asyncio.Future[Envelope] = asyncio.get_event_loop().create_future()

    async def handler(env: Envelope) -> None:
        if not received.done():
            received.set_result(env)

    sub = await bus.subscribe(MessageReceived, handler)
    try:
        await bus.publish(Envelope.new(MessageReceived))
        env = await asyncio.wait_for(received, 1.0)
        assert env.trace_id == stamp
    finally:
        await sub.unsubscribe()
        await bus.close()


async def test_multiple_preparers_applied_in_order() -> None:
    """Two preparers both mutate, order verified via metadata."""
    calls: list[str] = []

    def prep_a(env: Envelope) -> None:
        calls.append("a")

    def prep_b(env: Envelope) -> None:
        calls.append("b")

    bus = Bus.new(
        WithAgentID("a"),
        WithTransport(InMemoryDriver()),
        WithEnvelopePreparer(prep_a, prep_b),
    )
    await bus.connect()

    received = asyncio.Event()

    async def handler(_: Envelope) -> None:
        received.set()

    sub = await bus.subscribe(MessageReceived, handler)
    try:
        await bus.publish(Envelope.new(MessageReceived))
        await asyncio.wait_for(received.wait(), 1.0)
        assert calls == ["a", "b"]
    finally:
        await sub.unsubscribe()
        await bus.close()


async def test_invoke_applies_preparers() -> None:
    """bus.invoke() applies envelope_preparers to the request envelope."""
    seen_trace_id: list[str] = []

    async def invoke_handler(req: Envelope) -> dict:
        seen_trace_id.append(req.trace_id)
        return {"ok": True}

    def stamp_preparer(env: Envelope) -> None:
        env.trace_id = "invoked-trace"

    bus = Bus.new(
        WithAgentID("a"),
        WithTransport(InMemoryDriver()),
        WithEnvelopePreparer(stamp_preparer),
    )
    await bus.connect()
    try:
        await bus.handle_invoke("target", invoke_handler)
        await bus.invoke("target", {"msg": "hi"})
        assert seen_trace_id == ["invoked-trace"]
    finally:
        await bus.close()


async def test_publish_applies_preparers() -> None:
    """bus.publish() applies envelope_preparers to the published envelope."""
    received: asyncio.Future[Envelope] = asyncio.get_event_loop().create_future()

    async def handler(env: Envelope) -> None:
        if not received.done():
            received.set_result(env)

    def stamp_preparer(env: Envelope) -> None:
        env.trace_id = "published-trace"

    bus = Bus.new(
        WithAgentID("a"),
        WithTransport(InMemoryDriver()),
        WithEnvelopePreparer(stamp_preparer),
    )
    await bus.connect()

    sub = await bus.subscribe(MessageReceived, handler)
    try:
        await bus.publish(Envelope.new(MessageReceived))
        env = await asyncio.wait_for(received, 1.0)
        assert env.trace_id == "published-trace"
    finally:
        await sub.unsubscribe()
        await bus.close()


async def test_no_preparers_is_noop() -> None:
    """Bus without preparers works normally."""
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    await bus.connect()
    try:
        async def handler(req: Envelope) -> dict:
            return req.payload_json()

        await bus.handle_invoke("echo", handler)
        resp = await bus.invoke("echo", {"msg": "hello"})
        assert resp.payload_json() == {"msg": "hello"}
    finally:
        await bus.close()