"""Authentication helpers mirroring pkg/adapter/http/auth.go."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from starlette.requests import Request


@dataclass
class AuthContext:
    tenant_id: str = ""
    user_id: str = ""
    session_id: str = ""
    conversation_id: str = ""
    channel: str = ""
    trace_id: str = ""


AuthFunc = Callable[[Request], Awaitable[AuthContext | None]]


class ErrUnauthorized(Exception):
    """Canonical sentinel for AuthFunc to signal a rejected credential."""


def BearerAuth(
    validator: Callable[[str], Awaitable[AuthContext | None]],
) -> AuthFunc:
    """Build an AuthFunc that extracts a Bearer token and delegates to *validator*.

    Missing or malformed ``Authorization`` header raises :class:`ErrUnauthorized`.
    """
    if validator is None:
        raise TypeError("http: BearerAuth requires a non-nil validator")

    async def auth_fn(request: Request) -> AuthContext | None:
        raw = request.headers.get("authorization", "")
        if not raw:
            raise ErrUnauthorized()
        prefix = "bearer "
        if len(raw) <= len(prefix) or not raw[: len(prefix)].lower() == prefix:
            raise ErrUnauthorized()
        token = raw[len(prefix) :].strip()
        if not token:
            raise ErrUnauthorized()
        return await validator(token)

    return auth_fn
