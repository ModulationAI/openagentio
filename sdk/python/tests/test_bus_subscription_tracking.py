"""Bus subscription tracking and Bus.close() cleanup tests.

Mirrors Go's `pkg/bus/default.go` `trackOwned`/`Close` behavior: every
subscription created via `subscribe`/`handle_invoke`/`handle_stream` is
tracked, and Bus.close() unsubscribes them all without manual cleanup.
"""
from __future__ import annotations

import asyncio

from openagentio import (
    Bus,
    Envelope,
    InMemoryDriver,
    MessageReceived,
    StreamWriter,
    WithAgentID,
    WithTransport,
)


async def test_subscribe_is_tracked() -> None:
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    await bus.connect()
    try:
        async def handler(_: Envelope) -> None:
            pass

        before = len(bus._owned)
        await bus.subscribe(MessageReceived, handler)
        assert len(bus._owned) == before + 1
    finally:
        await bus.close()


async def test_close_unsubscribes_all_owned() -> None:
    """bus.close() unsubscribes all tracked subs without manual unsubscribe."""
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    await bus.connect()

    received = asyncio.Event()

    async def handler(_: Envelope) -> None:
        received.set()

    await bus.subscribe(MessageReceived, handler)
    await bus.publish(Envelope.new(MessageReceived))
    await asyncio.wait_for(received.wait(), 1.0)

    # Close without manual unsubscribe — _owned should be cleared.
    await bus.close()
    assert len(bus._owned) == 0


async def test_handle_invoke_subscription_is_tracked() -> None:
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    await bus.connect()
    try:
        async def handler(_: Envelope) -> dict:
            return {"ok": True}

        before = len(bus._owned)
        await bus.handle_invoke("target", handler)
        assert len(bus._owned) == before + 1
    finally:
        await bus.close()


async def test_handle_stream_subscription_is_tracked() -> None:
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    await bus.connect()
    try:
        async def handler(_: Envelope, w: StreamWriter) -> None:
            await w.final(None)

        before = len(bus._owned)
        await bus.handle_stream("target", handler)
        assert len(bus._owned) == before + 1
    finally:
        await bus.close()


async def test_close_clears_all_owned_subs() -> None:
    """Multiple subscriptions tracked across subscribe/handle_invoke/handle_stream."""
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    await bus.connect()

    async def event_handler(_: Envelope) -> None:
        pass

    async def invoke_handler(_: Envelope) -> dict:
        return {}

    async def stream_handler(_: Envelope, w: StreamWriter) -> None:
        await w.final(None)

    await bus.subscribe(MessageReceived, event_handler)
    await bus.handle_invoke("invoke-target", invoke_handler)
    await bus.handle_stream("stream-target", stream_handler)
    assert len(bus._owned) == 3

    await bus.close()
    assert len(bus._owned) == 0


async def test_close_is_idempotent() -> None:
    """Calling bus.close() twice does not raise."""
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    await bus.connect()

    async def handler(_: Envelope) -> None:
        pass

    await bus.subscribe(MessageReceived, handler)
    await bus.close()
    # Second close must not raise.
    await bus.close()