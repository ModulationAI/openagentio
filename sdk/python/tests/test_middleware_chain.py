"""Middleware chain composition and ordering."""
from __future__ import annotations

from openagentio import Envelope, MessageReceived
from openagentio.middleware import Chain, Handler, Middleware


async def test_chain_runs_middlewares_in_order() -> None:
    """mws[0] is outermost (runs first), mws[-1] is innermost (runs last)."""
    order: list[str] = []

    def mw_a(next: Handler) -> Handler:
        async def h(env: Envelope) -> None:
            order.append("a-before")
            await next(env)
            order.append("a-after")
        return h

    def mw_b(next: Handler) -> Handler:
        async def h(env: Envelope) -> None:
            order.append("b-before")
            await next(env)
            order.append("b-after")
        return h

    async def base(env: Envelope) -> None:
        order.append("base")

    wrapped = Chain(base, mw_a, mw_b)
    await wrapped(Envelope.new(MessageReceived))

    assert order == ["a-before", "b-before", "base", "b-after", "a-after"]


async def test_chain_empty_mws_is_identity() -> None:
    """Chain with no middleware should return the original handler."""
    called: list[int] = []

    async def base(env: Envelope) -> None:
        called.append(1)

    wrapped = Chain(base)
    await wrapped(Envelope.new(MessageReceived))
    assert called == [1]


async def test_chain_single_mw() -> None:
    called: list[int] = []

    def mw(next: Handler) -> Handler:
        async def h(env: Envelope) -> None:
            await next(env)
            called.append(1)
        return h

    async def base(env: Envelope) -> None:
        pass

    wrapped = Chain(base, mw)
    await wrapped(Envelope.new(MessageReceived))
    assert called == [1]