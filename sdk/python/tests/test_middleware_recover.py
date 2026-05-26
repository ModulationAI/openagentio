"""Recover middleware — exception catching."""
from __future__ import annotations

from openagentio import Envelope, MessageReceived
from openagentio.middleware import Handler
from openagentio.middleware.recover import Recover


async def test_recover_catches_exception() -> None:
    """Recover middleware catches handler exception and re-raises it."""
    caught: Exception | None = None

    async def crashing(env: Envelope) -> None:
        raise RuntimeError("boom")

    wrapped = Recover()(crashing)
    try:
        await wrapped(Envelope.new(MessageReceived))
    except RuntimeError as e:
        caught = e

    assert caught is not None
    assert str(caught) == "boom"


async def test_recover_passes_through_success() -> None:
    saw: list[Envelope] = []

    async def ok(env: Envelope) -> None:
        saw.append(env)

    wrapped = Recover()(ok)
    env = Envelope.new(MessageReceived)
    await wrapped(env)
    assert saw[0] is env