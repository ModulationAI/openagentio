"""DeadLetter middleware + DLQSink factory."""
from __future__ import annotations

from openagentio import Bus, Envelope, InMemoryDriver, MessageReceived, dlq_sink, WithQueue
from openagentio.middleware import Handler
from openagentio.middleware.deadletter import DeadLetter, DLQError, DLQSink


async def test_deadletter_forwards_to_sink_on_error() -> None:
    """When handler fails, envelope is forwarded to sink before error propagates."""
    dlq_envelopes: list[Envelope] = []
    dlq_errors: list[Exception] = []

    async def sink(env: Envelope, last_err: Exception) -> None:
        dlq_envelopes.append(env)
        dlq_errors.append(last_err)

    async def handler(env: Envelope) -> None:
        raise RuntimeError("fail-dlq")

    wrapped = DeadLetter(sink)(handler)
    env = Envelope.new("test.dlq")
    try:
        await wrapped(env)
    except RuntimeError:
        pass

    assert len(dlq_envelopes) == 1
    assert len(dlq_errors) == 1
    assert str(dlq_errors[0]) == "fail-dlq"


async def test_deadletter_skips_sink_on_success() -> None:
    dlq_called: list[int] = []

    async def sink(env: Envelope, last_err: Exception) -> None:
        dlq_called.append(1)

    async def handler(env: Envelope) -> None:
        pass

    wrapped = DeadLetter(sink)(handler)
    await wrapped(Envelope.new("test.dlq.ok"))
    assert dlq_called == []


async def test_deadletter_nil_sink_panics() -> None:
    """Passing nil sink should raise ValueError."""
    try:
        DeadLetter(None)
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for nil sink")


async def test_deadletter_wraps_both_errors_if_sink_fails() -> None:
    """If sink itself fails, DLQError wraps both errors so they're inspectable."""
    async def sink(env: Envelope, last_err: Exception) -> None:
        raise RuntimeError("sink-fail")

    async def handler(env: Envelope) -> None:
        raise RuntimeError("original-fail")

    wrapped = DeadLetter(sink)(handler)
    env = Envelope.new("test.dlq.double")
    try:
        await wrapped(env)
    except DLQError as e:
        assert isinstance(e.dlq_error, RuntimeError)
        assert str(e.dlq_error) == "sink-fail"
        assert isinstance(e.original_error, RuntimeError)
        assert str(e.original_error) == "original-fail"


async def test_dlq_sink_factory_stamps_metadata() -> None:
    """dlq_sink() factory stamps acp.dlq.original_event_type and acp.dlq.last_error."""
    bus = Bus(agent_id="dlq-test", transport=InMemoryDriver())
    await bus.connect()

    received: list[Envelope] = []

    async def collector(env: Envelope) -> None:
        received.append(env)

    # Subscribe to the DLQ subject: acp.v1.dlq.agent.message.received
    sub = await bus.subscribe("agent.message.received", collector, WithQueue("dlq"))

    # Override the subject for the DLQ — dlq_sink publishes to acp.v1.dlq.{event_type}.
    # We need to subscribe to that exact subject. The bus.subscribe uses the event
    # subject layout: {prefix}.events.{event_type}, but dlq publishes to
    # {prefix}.dlq.{event_type} directly. We can't use bus.subscribe for this
    # because it wraps the subject. Let's use raw transport subscribe.
    sink = dlq_sink("acp.v1", bus._codec, bus._transport)

    # Subscribe directly via transport for the DLQ subject.
    dlq_received: list[Envelope] = []

    async def dlq_collector(msg) -> None:
        env = bus._codec.decode_envelope(msg.data)
        dlq_received.append(env)

    from openagentio.transport.base import RawMessage
    raw_sub = await bus._transport.subscribe("acp.v1.dlq.agent.message.received", "", dlq_collector)

    env = Envelope.new("agent.message.received")
    err = RuntimeError("test-error")
    await sink(env, err)

    await bus.close()

    assert len(dlq_received) == 1
    dlq_env = dlq_received[0]
    assert dlq_env.metadata is not None
    assert dlq_env.metadata.get("acp.dlq.original_event_type") == "agent.message.received"
    assert dlq_env.metadata.get("acp.dlq.last_error") == "test-error"