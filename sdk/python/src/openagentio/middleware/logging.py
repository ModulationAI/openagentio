"""Logging middleware. Mirrors pkg/middleware/logging.go.

Emits a structured log line per handler invocation:
- success → DEBUG level
- error → ERROR level

Logged fields: event_id, event_type, trace_id, session_id, duration_ms.
"""
from __future__ import annotations

import logging
import time

from openagentio.event.envelope import Envelope
from openagentio.middleware import Handler, Middleware


def Logging(logger: logging.Logger | None = None) -> Middleware:
    """Emit a structured log line per handler invocation."""
    log = logger or logging.getLogger("openagentio.middleware")
    def wrap(next: Handler) -> Handler:
        async def handler(env: Envelope) -> None:
            start = time.monotonic()
            try:
                await next(env)
            except Exception as exc:
                duration_ms = (time.monotonic() - start) * 1000
                log.error(
                    "handler error",
                    extra={
                        "event_id": env.event_id,
                        "event_type": env.event_type,
                        "trace_id": env.trace_id,
                        "session_id": env.session_id,
                        "duration_ms": round(duration_ms, 2),
                        "err": exc,
                    },
                )
                raise
            else:
                duration_ms = (time.monotonic() - start) * 1000
                log.debug(
                    "handler ok",
                    extra={
                        "event_id": env.event_id,
                        "event_type": env.event_type,
                        "trace_id": env.trace_id,
                        "session_id": env.session_id,
                        "duration_ms": round(duration_ms, 2),
                    },
                )
        return handler
    return wrap