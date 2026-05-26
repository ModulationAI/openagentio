"""Recover middleware. Mirrors pkg/middleware/recover.go.

Catches all exceptions in downstream handlers, logs the traceback, and
re-raises so the bus can generate a proper error response.
Recommended as the outermost middleware on every chain.
"""
from __future__ import annotations

import logging
import traceback

from openagentio.event.envelope import Envelope
from openagentio.middleware import Handler, Middleware


def Recover(logger: logging.Logger | None = None) -> Middleware:
    """Catch exceptions in downstream handlers, log traceback, re-raise."""
    log = logger or logging.getLogger("openagentio.middleware")

    def wrap(next: Handler) -> Handler:
        async def handler(env: Envelope) -> None:
            try:
                await next(env)
            except Exception:
                event_id = env.event_id if env is not None else ""
                log.error(
                    "handler error",
                    extra={
                        "event_id": event_id,
                        "event_type": env.event_type,
                        "traceback": traceback.format_exc(),
                    },
                )
                raise
        return handler
    return wrap