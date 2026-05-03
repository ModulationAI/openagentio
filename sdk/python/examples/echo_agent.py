"""Minimal AgentFlowBus example for Python: registers an echo invoke handler
on an in-memory transport and round-trips a single Invoke call against itself.

Mirrors examples/echo-agent/main.go from the Go SDK.
"""
from __future__ import annotations

import asyncio
import json
import logging

from agentflowbus import Bus, Envelope, InMemoryDriver


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    logger = logging.getLogger("echo-agent")

    bus = Bus(
        agent_id="echo",
        transport=InMemoryDriver(),
        logger=logger,
    )
    await bus.connect()
    try:
        async def echo(req: Envelope) -> object:
            return req.payload_json()

        await bus.handle_invoke("echo", echo)

        resp = await bus.invoke("echo", {"msg": "hello bus"})
        logger.info(
            "echo round-trip ok | event_type=%s correlation_id=%s is_final=%s payload=%s",
            resp.event_type,
            resp.correlation_id,
            resp.is_final,
            json.dumps(resp.payload_json(), separators=(",", ":")),
        )
    finally:
        await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
