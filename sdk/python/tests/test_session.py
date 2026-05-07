"""Session/trace context propagation through the bus."""
from __future__ import annotations

import asyncio

from openagentio import Bus, Envelope, ResponseFinal, session


def test_inject_returns_envelope_via_current() -> None:
    env = Envelope.new("test.smoke")
    env.trace_id = "trace-smoke"
    env.session_id = "sess-smoke"
    env.conversation_id = "conv-smoke"
    env.tenant_id = "tenant-smoke"

    assert session.current() is None

    token = session.inject(env)
    try:
        assert session.current() is env
        assert session.trace_id() == "trace-smoke"
        assert session.session_id() == "sess-smoke"
        assert session.conversation_id() == "conv-smoke"
        assert session.tenant_id() == "tenant-smoke"
    finally:
        session.reset(token)

    assert session.current() is None


def test_helpers_return_none_when_fields_blank() -> None:
    env = Envelope.new("test.blank")
    token = session.inject(env)
    try:
        assert session.trace_id() is None
        assert session.session_id() is None
        assert session.conversation_id() is None
        assert session.tenant_id() is None
    finally:
        session.reset(token)


async def test_handler_can_read_session_helpers(bus: Bus) -> None:
    """Identity fields set on the request envelope should be readable via the
    session helpers from inside the handler. Tenant routing is exercised in
    the direct API test above; here we keep the envelope on the empty-tenant
    subject so subscribe/publish line up.
    """
    async def handler(_: Envelope) -> dict:
        return {
            "trace_id": session.trace_id(),
            "session_id": session.session_id(),
            "conversation_id": session.conversation_id(),
        }

    await bus.handle_invoke("ctx-echo", handler)

    req = Envelope.new("test.ctx")
    req.trace_id = "trace-A"
    req.session_id = "sess-A"
    req.conversation_id = "conv-A"

    resp = await bus.invoke("ctx-echo", req)
    assert resp.event_type == ResponseFinal
    assert resp.payload_json() == {
        "trace_id": "trace-A",
        "session_id": "sess-A",
        "conversation_id": "conv-A",
    }


async def test_concurrent_invokes_dont_bleed_session(bus: Bus) -> None:
    """Two concurrent invokes with distinct trace ids must each see only
    their own context, even when handlers interleave via ``asyncio.sleep(0)``.
    """
    async def handler(_: Envelope) -> dict:
        before = session.trace_id()
        # Force interleaving with the other handler's coroutine.
        await asyncio.sleep(0)
        after = session.trace_id()
        return {"before": before, "after": after}

    await bus.handle_invoke("isolated", handler)

    req_a = Envelope.new("test.iso")
    req_a.trace_id = "trace-A"
    req_b = Envelope.new("test.iso")
    req_b.trace_id = "trace-B"

    resp_a, resp_b = await asyncio.gather(
        bus.invoke("isolated", req_a),
        bus.invoke("isolated", req_b),
    )

    body_a = resp_a.payload_json()
    body_b = resp_b.payload_json()
    assert body_a == {"before": "trace-A", "after": "trace-A"}
    assert body_b == {"before": "trace-B", "after": "trace-B"}


async def test_session_clears_after_handler_returns(bus: Bus) -> None:
    """Once the handler returns, a fresh task must observe an empty session."""
    async def handler(_: Envelope) -> dict:
        return {"saw": session.trace_id()}

    await bus.handle_invoke("clear", handler)

    req = Envelope.new("test.clear")
    req.trace_id = "trace-C"
    resp = await bus.invoke("clear", req)
    assert resp.payload_json() == {"saw": "trace-C"}

    # A fresh asyncio.Task starts with no inherited binding from this scope.
    async def probe() -> object:
        return session.current()

    assert await asyncio.create_task(probe()) is None
    assert session.current() is None
