"""Pytest fixtures shared across test modules."""
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio

from openagentio import Bus, InMemoryDriver


@pytest_asyncio.fixture
async def bus() -> AsyncIterator[Bus]:
    """Bus over a fresh InMemoryDriver. Each test gets an isolated broker."""
    b = Bus(agent_id="test-agent", transport=InMemoryDriver())
    await b.connect()
    try:
        yield b
    finally:
        await b.close()
