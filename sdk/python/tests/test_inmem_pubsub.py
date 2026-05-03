"""Pub/sub round-trips over the in-memory driver."""
from __future__ import annotations

import asyncio

import pytest

from agentflowbus import Bus, Envelope, InMemoryDriver, MessageReceived


async def test_publish_subscribe_round_trip(bus: Bus) -> None:
    received: asyncio.Future[Envelope] = asyncio.get_event_loop().create_future()

    async def handler(env: Envelope) -> None:
        if not received.done():
            received.set_result(env)

    sub = await bus.subscribe(MessageReceived, handler)
    try:
        out = Envelope.new(MessageReceived)
        out.from_ = "tester"
        out.payload = b'{"text":"hi"}'
        await bus.publish(out)

        env = await asyncio.wait_for(received, 2.0)
        assert env.event_id == out.event_id
        assert env.from_ == "tester"
        assert env.payload_json() == {"text": "hi"}
    finally:
        await sub.unsubscribe()


async def test_subscribe_requires_event_type(bus: Bus) -> None:
    async def handler(_: Envelope) -> None:
        return None

    with pytest.raises(ValueError):
        await bus.subscribe("", handler)


async def test_publish_requires_event_type(bus: Bus) -> None:
    env = Envelope()  # no event_type
    with pytest.raises(ValueError):
        await bus.publish(env)


async def test_subscribe_handler_error_does_not_kill_bus(bus: Bus) -> None:
    fired = asyncio.Event()

    async def crashing(_: Envelope) -> None:
        try:
            raise RuntimeError("boom")
        finally:
            fired.set()

    sub = await bus.subscribe(MessageReceived, crashing)
    try:
        out = Envelope.new(MessageReceived)
        out.from_ = "tester"
        await bus.publish(out)
        await asyncio.wait_for(fired.wait(), 1.0)

        # Bus is still operable after a handler exception.
        seen = asyncio.Event()

        async def healthy(_: Envelope) -> None:
            seen.set()

        sub2 = await bus.subscribe(MessageReceived, healthy)
        await bus.publish(Envelope.new(MessageReceived))
        await asyncio.wait_for(seen.wait(), 1.0)
        await sub2.unsubscribe()
    finally:
        await sub.unsubscribe()


async def test_queue_group_balances() -> None:
    """Two subscribers in the same queue group split deliveries."""
    transport = InMemoryDriver()
    b = Bus(agent_id="qa", transport=transport)
    await b.connect()
    try:
        a_hits = 0
        b_hits = 0

        async def a(_: Envelope) -> None:
            nonlocal a_hits
            a_hits += 1

        async def bb(_: Envelope) -> None:
            nonlocal b_hits
            b_hits += 1

        sub_a = await b.subscribe(MessageReceived, a, queue="workers")
        sub_b = await b.subscribe(MessageReceived, bb, queue="workers")
        try:
            for _ in range(12):
                await b.publish(Envelope.new(MessageReceived))
            assert a_hits + b_hits == 12
            assert a_hits > 0 and b_hits > 0
        finally:
            await sub_a.unsubscribe()
            await sub_b.unsubscribe()
    finally:
        await b.close()
