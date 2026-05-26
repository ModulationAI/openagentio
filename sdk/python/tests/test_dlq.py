"""DLQSink factory tests. Mirrors pkg/bus/dlq.go."""
from __future__ import annotations

import asyncio

from openagentio import Bus, Envelope, InMemoryDriver, MessageReceived, dlq_sink
from openagentio.codec.json_codec import JSONCodec
from openagentio.transport.base import RawMessage


async def test_dlq_sink_publishes_to_correct_subject() -> None:
    transport = InMemoryDriver()
    await transport.connect()

    received: asyncio.Future[RawMessage] = asyncio.get_event_loop().create_future()

    async def dlq_handler(msg: RawMessage) -> None:
        if not received.done():
            received.set_result(msg)

    await transport.subscribe("acp.v1.dlq.agent.message.received", "", dlq_handler)

    sink = dlq_sink(prefix="acp.v1", transport=transport)
    env = Envelope.new(MessageReceived)
    env.from_ = "agent"

    await sink(env, RuntimeError("test error"))

    msg = await asyncio.wait_for(received, 1.0)
    assert msg is not None
    codec = JSONCodec()
    dlq_env = codec.decode_envelope(msg.data)
    assert dlq_env.event_type == MessageReceived

    await transport.close()


async def test_dlq_sink_requires_transport() -> None:
    try:
        dlq_sink(transport=None)
        raise AssertionError("should have raised ValueError")
    except ValueError as e:
        assert "transport" in str(e)


async def test_dlq_sink_uses_default_codec() -> None:
    """When codec=None, JSONCodec is used implicitly."""
    transport = InMemoryDriver()
    await transport.connect()

    received: asyncio.Future[RawMessage] = asyncio.get_event_loop().create_future()

    async def dlq_handler(msg: RawMessage) -> None:
        if not received.done():
            received.set_result(msg)

    await transport.subscribe("acp.v1.dlq.test.event", "", dlq_handler)

    sink = dlq_sink(prefix="acp.v1", codec=None, transport=transport)
    env = Envelope.new("test.event")

    await sink(env, RuntimeError("fail"))

    msg = await asyncio.wait_for(received, 1.0)
    codec = JSONCodec()
    dlq_env = codec.decode_envelope(msg.data)
    assert dlq_env.event_type == "test.event"

    await transport.close()


async def test_dlq_sink_stamps_metadata_keys() -> None:
    transport = InMemoryDriver()
    await transport.connect()

    received: asyncio.Future[RawMessage] = asyncio.get_event_loop().create_future()

    async def dlq_handler(msg: RawMessage) -> None:
        if not received.done():
            received.set_result(msg)

    await transport.subscribe("acp.v1.dlq.custom.type", "", dlq_handler)

    sink = dlq_sink(prefix="acp.v1", transport=transport)
    env = Envelope.new("custom.type")

    await sink(env, ValueError("bad data"))

    msg = await asyncio.wait_for(received, 1.0)
    codec = JSONCodec()
    dlq_env = codec.decode_envelope(msg.data)

    assert dlq_env.metadata is not None
    assert dlq_env.metadata["acp.dlq.original_event_type"] == "custom.type"
    assert "bad data" in dlq_env.metadata["acp.dlq.last_error"]

    await transport.close()