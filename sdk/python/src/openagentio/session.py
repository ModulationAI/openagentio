"""Per-task session/trace context propagation.

Mirrors Go's ``pkg/session/context.go``. The bus dispatch loop calls
``inject(envelope)`` before awaiting a user handler so any deeper coroutine
can read the active envelope (and the trace/session/conversation/tenant ids
it carries) without re-passing it.

Implementation notes:

* Backed by ``contextvars.ContextVar``, which asyncio copies into each new
  ``Task`` automatically. Concurrent invokes therefore see isolated values.
* ``inject`` returns a ``Token`` that callers must hand back to ``reset`` in
  a ``finally:`` clause. Forgetting to reset is a leak — the next request
  dispatched on the same Task will observe the stale envelope.
"""
from __future__ import annotations

import contextvars
from typing import Optional

from openagentio.event.envelope import Envelope

_envelope: contextvars.ContextVar[Optional[Envelope]] = contextvars.ContextVar(
    "openagentio.envelope",
    default=None,
)


def inject(envelope: Envelope) -> contextvars.Token:
    """Bind ``envelope`` to the current asyncio Task. Pair with ``reset``."""
    return _envelope.set(envelope)


def reset(token: contextvars.Token) -> None:
    """Undo a previous ``inject`` using the returned token."""
    _envelope.reset(token)


def current() -> Optional[Envelope]:
    """Return the active envelope, or ``None`` outside a dispatched handler."""
    return _envelope.get()


def trace_id() -> Optional[str]:
    e = _envelope.get()
    if e is None or not e.trace_id:
        return None
    return e.trace_id


def session_id() -> Optional[str]:
    e = _envelope.get()
    if e is None or not e.session_id:
        return None
    return e.session_id


def tenant_id() -> Optional[str]:
    e = _envelope.get()
    if e is None or not e.tenant_id:
        return None
    return e.tenant_id


def conversation_id() -> Optional[str]:
    e = _envelope.get()
    if e is None or not e.conversation_id:
        return None
    return e.conversation_id


__all__ = [
    "inject",
    "reset",
    "current",
    "trace_id",
    "session_id",
    "tenant_id",
    "conversation_id",
]
