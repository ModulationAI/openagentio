"""NATS integration tests — exercised only when AFB_NATS_URL is set.

Start a broker locally, e.g. ``nats-server -p 4222``, then::

    AFB_NATS_URL=nats://localhost:4222 pytest tests/test_nats_integration.py

These tests cover both the NATSDriver in isolation and the Bus running on top
of it, mirroring pkg/transport/nats/integration_test.go on the Go side.
"""
from __future__ import annotations

import asyncio
import os

import pytest

from agentflowbus import (
    Bus,
    CodeAgentUnavailable,
    Envelope,
    MessageReceived,
    NATSDriver,
    RawMessage,
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    ResponseStarted,
    StreamWriter,
)

NATS_URL = os.getenv("AFB_NATS_URL", "")
pytestmark = pytest.mark.skipif(
    not NATS_URL,
    reason="AFB_NATS_URL not set — start nats-server and export the URL to enable",
)


# --- Driver-level tests -----------------------------------------------------


async def test_driver_connect_close_idempotent() -> None:
    d = NATSDriver(NATS_URL)
    await d.connect()
    await d.connect()  # no-op
    await d.close()
    await d.close()  # no-op


async def test_driver_publish_subscribe_preserves_headers() -> None:
    d = NATSDriver(NATS_URL, name="afb.py.test.pubsub")
    await d.connect()
    try:
        got: asyncio.Future[RawMessage] = asyncio.get_event_loop().create_future()

        async def handler(m: RawMessage) -> None:
            if not got.done():
                got.set_result(m)

        sub = await d.subscribe("afb.py.test.pubsub", "", handler)
        try:
            await d.flush()  # ensure SUB lands before PUB
            await d.publish(
                RawMessage(
                    subject="afb.py.test.pubsub",
                    data=b"hello",
                    headers={"x-trace": "abc", "x-tenant": "acme"},
                )
            )
            m = await asyncio.wait_for(got, 2.0)
            assert m.data == b"hello"
            assert m.headers is not None
            assert m.headers["x-trace"] == "abc"
            assert m.headers["x-tenant"] == "acme"
        finally:
            await sub.unsubscribe()
    finally:
        await d.close()


async def test_driver_queue_group_balances() -> None:
    d = NATSDriver(NATS_URL, name="afb.py.test.queue")
    await d.connect()
    try:
        N = 12
        a_hits = 0
        b_hits = 0
        done = asyncio.Event()
        seen = 0

        def make_handler(which: str):
            async def h(_: RawMessage) -> None:
                nonlocal a_hits, b_hits, seen
                if which == "a":
                    a_hits += 1
                else:
                    b_hits += 1
                seen += 1
                if seen == N:
                    done.set()
            return h

        sub_a = await d.subscribe("afb.py.test.queue", "workers", make_handler("a"))
        sub_b = await d.subscribe("afb.py.test.queue", "workers", make_handler("b"))
        try:
            await d.flush()
            for _ in range(N):
                await d.publish(RawMessage(subject="afb.py.test.queue", data=b"x"))
            await asyncio.wait_for(done.wait(), 2.0)
            assert a_hits + b_hits == N
            assert a_hits > 0 and b_hits > 0
        finally:
            await sub_a.unsubscribe()
            await sub_b.unsubscribe()
    finally:
        await d.close()


async def test_driver_open_inbox_receives() -> None:
    d = NATSDriver(NATS_URL, name="afb.py.test.inbox")
    await d.connect()
    try:
        inbox = await d.open_inbox()
        try:
            assert inbox.subject != ""
            await d.flush()
            await d.publish(RawMessage(subject=inbox.subject, data=b"frame-1"))
            m = await inbox.recv(timeout=2.0)
            assert m.data == b"frame-1"
        finally:
            await inbox.close()
    finally:
        await d.close()


async def test_driver_inbox_recv_honors_timeout() -> None:
    d = NATSDriver(NATS_URL, name="afb.py.test.timeout")
    await d.connect()
    try:
        inbox = await d.open_inbox()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await inbox.recv(timeout=0.05)
        finally:
            await inbox.close()
    finally:
        await d.close()


async def test_driver_inbox_close_unblocks_recv() -> None:
    d = NATSDriver(NATS_URL, name="afb.py.test.close")
    await d.connect()
    try:
        inbox = await d.open_inbox()
        err: list[BaseException] = []

        async def waiter() -> None:
            try:
                await inbox.recv()
            except BaseException as e:  # noqa: BLE001
                err.append(e)

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.02)  # park inside recv
        await inbox.close()
        await asyncio.wait_for(task, 1.0)
        assert err and isinstance(err[0], RuntimeError)
        await inbox.close()  # idempotent
    finally:
        await d.close()


# --- Bus-over-NATS tests ----------------------------------------------------


async def _new_bus(agent_id: str) -> tuple[Bus, NATSDriver]:
    d = NATSDriver(NATS_URL, name=agent_id)
    b = Bus(agent_id=agent_id, transport=d, default_timeout=3.0)
    await b.connect()
    return b, d


async def test_bus_over_nats_pubsub() -> None:
    b, d = await _new_bus("py-pubsub-agent")
    try:
        got: asyncio.Future[Envelope] = asyncio.get_event_loop().create_future()

        async def handler(env: Envelope) -> None:
            if not got.done():
                got.set_result(env)

        sub = await b.subscribe(MessageReceived, handler)
        try:
            await d.flush()

            out = Envelope.new(MessageReceived)
            out.from_ = "tester"
            out.payload = b'{"text":"hi-nats"}'
            await b.publish(out)

            env = await asyncio.wait_for(got, 2.0)
            assert env.event_id == out.event_id
            assert env.payload_json() == {"text": "hi-nats"}
        finally:
            await sub.unsubscribe()
    finally:
        await b.close()


async def test_bus_over_nats_invoke_round_trip() -> None:
    b, d = await _new_bus("py-invoke-agent")
    try:
        async def handler(req: Envelope) -> dict:
            return {"echo": req.payload_json()}

        await b.handle_invoke("echo", handler)
        await d.flush()

        resp = await b.invoke("echo", {"msg": "ping"})
        assert resp.event_type == ResponseFinal
        assert resp.is_final is True
        assert resp.correlation_id
        assert resp.payload_json() == {"echo": {"msg": "ping"}}
    finally:
        await b.close()


async def test_bus_over_nats_invoke_handler_error_maps_to_error_envelope() -> None:
    b, d = await _new_bus("py-error-agent")
    try:
        async def handler(_: Envelope) -> None:
            raise RuntimeError("kaboom")

        await b.handle_invoke("boom", handler)
        await d.flush()

        resp = await b.invoke("boom", None)
        assert resp.event_type == ResponseError
        assert resp.is_final is True
        err = resp.payload_json()
        assert err["code"] == CodeAgentUnavailable
        assert err["message"] == "kaboom"
    finally:
        await b.close()


async def test_bus_over_nats_stream_invoke_ordering() -> None:
    b, d = await _new_bus("py-stream-agent")
    try:
        async def handler(_: Envelope, w: StreamWriter) -> None:
            await w.started({"model": "test"})
            for i in range(3):
                await w.delta({"i": i})
            await w.final({"count": 3})

        await b.handle_stream("count", handler)
        await d.flush()

        s = await b.stream_invoke("count", None)
        types: list[str] = []
        seqs: list[int] = []
        try:
            async for env in s:
                types.append(env.event_type)
                seqs.append(env.seq)
        finally:
            await s.close()

        assert types == [
            ResponseStarted,
            ResponseDelta,
            ResponseDelta,
            ResponseDelta,
            ResponseFinal,
        ]
        assert seqs == [0, 1, 2, 3, 4]
    finally:
        await b.close()
