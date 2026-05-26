"""Request-to-Envelope mapping mirroring pkg/adapter/http/envelope.go."""
from __future__ import annotations

import json

from starlette.requests import Request

from openagentio.event.envelope import Envelope
from openagentio.event.types import MessageReceived

from openagentio.adapter.http.auth import AuthContext

MAX_BODY_BYTES = 4 * 1024 * 1024  # 4 MiB


async def read_envelope(
    request: Request,
    event_type: str,
    ac: AuthContext | None,
) -> Envelope:
    """Build a request envelope from the HTTP request.

    Headers feed correlation/tenancy fields, body becomes ``payload`` (raw JSON),
    and *AuthContext* (if present) overrides any header-derived values.
    """
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_BODY_BYTES:
            raise ValueError("body exceeds 4 MiB limit")
        chunks.append(chunk)
    body = b"".join(chunks)

    if event_type:
        env = Envelope.new(event_type)
    else:
        env = Envelope.new(MessageReceived)

    if body:
        try:
            json.loads(body)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError("body is not valid JSON") from exc
        env.payload = body

    h = request.headers
    if v := h.get("x-trace-id"):
        env.trace_id = v
    if v := h.get("x-traceparent"):
        env.traceparent = v
    if v := h.get("x-tenant-id"):
        env.tenant_id = v
    if v := h.get("x-session-id"):
        env.session_id = v
    if v := h.get("x-conversation-id"):
        env.conversation_id = v
    if v := h.get("x-user-id"):
        env.user_id = v
    if v := h.get("x-channel"):
        env.channel = v

    if ac is not None:
        if ac.tenant_id:
            env.tenant_id = ac.tenant_id
        if ac.user_id:
            env.user_id = ac.user_id
        if ac.session_id:
            env.session_id = ac.session_id
        if ac.conversation_id:
            env.conversation_id = ac.conversation_id
        if ac.channel:
            env.channel = ac.channel
        if ac.trace_id:
            env.trace_id = ac.trace_id

    return env
