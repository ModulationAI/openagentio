"""Trace middleware — session inject/reset."""
from __future__ import annotations

from openagentio import Envelope, session
from openagentio.middleware import Handler
from openagentio.middleware.trace import Trace


async def test_trace_injects_envelope_into_session() -> None:
    saw_trace: list[str] = []

    async def handler(env: Envelope) -> None:
        saw_trace.append(session.trace_id() or "")

    wrapped = Trace()(handler)
    env = Envelope.new("test.trace")
    env.trace_id = "trace-abc"

    assert session.current() is None
    await wrapped(env)
    assert session.current() is None  # reset after handler
    assert saw_trace == ["trace-abc"]


async def test_trace_resets_on_exception() -> None:
    """Session must be reset even if the handler raises."""
    async def handler(env: Envelope) -> None:
        assert session.trace_id() == "trace-xyz"
        raise RuntimeError("fail")

    wrapped = Trace()(handler)
    env = Envelope.new("test.trace-reset")
    env.trace_id = "trace-xyz"

    try:
        await wrapped(env)
    except RuntimeError:
        pass

    assert session.current() is None