"""Trace middleware — session inject/reset. Mirrors pkg/middleware/trace.go.

Injects the envelope into the per-task context (via :mod:`openagentio.session`)
so downstream handlers and nested Bus calls can read trace/session/conversation
metadata without re-passing the envelope. Resets the context token in a finally
block to prevent session bleed.
"""
from __future__ import annotations

from openagentio.event.envelope import Envelope
from openagentio.middleware import Handler, Middleware
from openagentio.session import inject as _inject, reset as _reset


def Trace() -> Middleware:
    """Inject envelope into context before downstream handler; reset after."""
    def wrap(next: Handler) -> Handler:
        async def handler(env: Envelope) -> None:
            token = _inject(env)
            try:
                await next(env)
            finally:
                _reset(token)
        return handler
    return wrap