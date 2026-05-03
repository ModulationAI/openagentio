"""Transport contract. Wire-level abstraction the bus uses to talk to any messaging system.

Mirrors pkg/transport/transport.go. Async-first; cancellation is via timeouts and
:py:class:`asyncio.CancelledError` rather than an explicit context object.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol, runtime_checkable


@dataclass
class RawMessage:
    """Codec-agnostic carrier between the bus runtime and the transport.

    Headers stay decoupled from the payload bytes so future transports
    (HTTP, JetStream) can map them to their native metadata channels.
    """

    subject: str
    data: bytes
    headers: dict[str, str] | None = None
    reply_to: str = ""


@dataclass
class Capabilities:
    """Optional features advertised by a driver so the bus can pick the right code path."""

    streaming: bool = False
    persistence: bool = False
    queue_group: bool = False
    headers: bool = False


# Handler invoked once per delivered message. Errors should be treated as fatal
# for the message — drivers may log/metric them but should not retry without
# explicit middleware.
TransportHandler = Callable[[RawMessage], Awaitable[None]]


@runtime_checkable
class Subscription(Protocol):
    """Live consumer registration. Idempotent unsubscribe."""

    async def unsubscribe(self) -> None: ...


@runtime_checkable
class Inbox(Protocol):
    """Single-consumer ephemeral subject used for streaming responses.

    The :py:attr:`subject` is embedded in the request envelope as ``reply_to``
    so the callee can publish multiple messages back to the caller.
    """

    @property
    def subject(self) -> str: ...

    async def recv(self, timeout: float | None = None) -> RawMessage: ...

    async def close(self) -> None: ...


@runtime_checkable
class Transport(Protocol):
    """Contract every wire driver implements."""

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    def capabilities(self) -> Capabilities: ...

    async def publish(self, msg: RawMessage) -> None: ...

    async def subscribe(
        self,
        subject: str,
        queue: str,
        handler: TransportHandler,
    ) -> Subscription: ...

    async def request(
        self,
        msg: RawMessage,
        timeout: float | None = None,
    ) -> RawMessage: ...

    async def open_inbox(self) -> Inbox: ...
