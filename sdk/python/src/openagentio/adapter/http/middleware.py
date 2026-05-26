"""Raw ASGI middleware mirroring pkg/adapter/http/middleware.go.

Uses raw ASGI rather than Starlette's BaseHTTPMiddleware to avoid buffering
streaming (SSE) responses.
"""
from __future__ import annotations

import logging
import time
import traceback
from typing import Any, Callable, Awaitable

from starlette.types import ASGIApp, Receive, Scope, Send

from openagentio.adapter.http.errors import write_error_json
from openagentio.event.payload import CodeCodecFailure

ASGIMiddleware = Callable[[ASGIApp], ASGIApp]


class Recover:
    """Guards downstream from unhandled exceptions.

    A recovered exception is logged and translated to a 500 JSON response.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or logging.getLogger("openagentio.http")

    def __call__(self, app: ASGIApp) -> ASGIApp:
        log = self._log

        async def middleware(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] not in ("http",):
                await app(scope, receive, send)
                return

            error_sent = False

            async def send_wrapper(message: dict[str, Any]) -> None:
                nonlocal error_sent
                if message.get("type") == "http.response.start":
                    error_sent = False
                await send(message)

            try:
                await app(scope, receive, send_wrapper)
            except Exception as exc:
                if error_sent:
                    return
                log.error(
                    "http: panic recovered",
                    extra={"path": scope.get("path", ""), "exc": exc},
                    exc_info=True,
                )
                resp = write_error_json(500, "INTERNAL_ERROR", "internal server error")
                error_sent = True
                await resp(scope, receive, send)

        return middleware


class Logging:
    """Emits a structured log line per request after the handler returns.

    Captures the status code via a thin ``send`` wrapper.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or logging.getLogger("openagentio.http")

    def __call__(self, app: ASGIApp) -> ASGIApp:
        log = self._log

        async def middleware(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["type"] not in ("http",):
                await app(scope, receive, send)
                return

            start = time.monotonic()
            status = 200

            async def send_wrapper(message: dict[str, Any]) -> None:
                nonlocal status
                if message.get("type") == "http.response.start":
                    raw_status = message.get("status", 200)
                    status = raw_status
                await send(message)

            await app(scope, receive, send_wrapper)
            duration_ms = (time.monotonic() - start) * 1000
            log.info(
                "http request",
                extra={
                    "method": scope.get("method", ""),
                    "path": scope.get("path", ""),
                    "status": status,
                    "duration_ms": round(duration_ms),
                },
            )

        return middleware
