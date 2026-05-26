"""Composable middleware for Bus handlers. Mirrors pkg/middleware/middleware.go.

Middleware wraps a :data:`Handler` with cross-cutting behavior (panic recovery,
structured logging, trace propagation, retry, etc.). The outer-most middleware
in a chain runs first.

Handler and Middleware types are defined here (not in bus) to avoid circular
imports — bus imports middleware, not the other way around.
"""
from __future__ import annotations

from typing import Awaitable, Callable

from openagentio.event.envelope import Envelope

# Handler is the inner type middleware operates on. It mirrors bus.Handler
# but lives here to avoid an import cycle.
Handler = Callable[[Envelope], Awaitable[None]]

# Middleware wraps a Handler with cross-cutting behavior. The outer-most
# middleware in a chain runs first.
Middleware = Callable[[Handler], Handler]


def Chain(h: Handler, *mws: Middleware) -> Handler:
    """Compose *mws* around *h*. The returned Handler runs middlewares in the
    order they were supplied — ``mws[0]`` is outermost (runs first).

    Mirrors Go's ``middleware.Chain(h, mws...)``.
    """
    for mw in reversed(mws):
        h = mw(h)
    return h