"""Invoke / reply round-trips over the in-memory driver."""
from __future__ import annotations

from agentflowbus import (
    Bus,
    CodeAgentUnavailable,
    Envelope,
    ResponseError,
    ResponseFinal,
)


async def test_invoke_round_trip(bus: Bus) -> None:
    async def handler(req: Envelope) -> dict:
        body = req.payload_json() or {}
        return {"echo": body}

    await bus.handle_invoke("echo", handler)
    resp = await bus.invoke("echo", {"msg": "ping"})
    assert resp.event_type == ResponseFinal
    assert resp.is_final is True
    assert resp.correlation_id  # set by the runtime
    assert resp.payload_json() == {"echo": {"msg": "ping"}}


async def test_invoke_handler_returning_envelope_is_adopted(bus: Bus) -> None:
    async def handler(req: Envelope) -> Envelope:
        out = Envelope.new("custom.reply")
        out.payload = b'{"ok":true}'
        return out

    await bus.handle_invoke("custom", handler)
    resp = await bus.invoke("custom", None)
    assert resp.event_type == "custom.reply"
    assert resp.correlation_id  # adopted from request
    assert resp.payload_json() == {"ok": True}


async def test_invoke_handler_error_maps_to_error_envelope(bus: Bus) -> None:
    async def handler(_: Envelope) -> None:
        raise RuntimeError("kaboom")

    await bus.handle_invoke("boom", handler)
    resp = await bus.invoke("boom", None)
    assert resp.event_type == ResponseError
    assert resp.is_final is True
    err = resp.payload_json()
    assert err["code"] == CodeAgentUnavailable
    assert err["message"] == "kaboom"


async def test_invoke_passes_through_envelope_payload(bus: Bus) -> None:
    """When payload is itself an Envelope, the bus uses it as the request."""
    seen_event_id: dict[str, str] = {}

    async def handler(req: Envelope) -> dict:
        seen_event_id["id"] = req.event_id
        return {"got": req.event_type}

    await bus.handle_invoke("passthru", handler)

    custom_req = Envelope.new("user.custom")
    custom_req.payload = b'{"raw":1}'
    resp = await bus.invoke("passthru", custom_req)

    assert seen_event_id["id"] == custom_req.event_id
    assert resp.correlation_id == custom_req.event_id
    assert resp.payload_json() == {"got": "user.custom"}
