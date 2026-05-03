"""In-process pub/sub broker. Mirrors pkg/transport/inmem/inmem.go.

Intended for unit tests, examples, and single-binary deployments. Not safe for
cross-process communication.
"""
from __future__ import annotations

import asyncio
import uuid

from agentflowbus.transport.base import (
    Capabilities,
    Inbox,
    RawMessage,
    Subscription,
    Transport,
    TransportHandler,
)

# Module-level sentinel used to wake an inbox parked inside `recv` after `close`.
_CLOSED = object()


class InMemoryDriver:
    """Async in-memory pub/sub broker."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._subs: dict[str, list[_Subscription]] = {}
        self._rr: dict[str, int] = {}
        self._closed = False

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            self._subs.clear()
            self._rr.clear()

    def capabilities(self) -> Capabilities:
        return Capabilities(
            streaming=True,
            persistence=False,
            queue_group=True,
            headers=True,
        )

    async def publish(self, msg: RawMessage) -> None:
        if msg is None:
            raise ValueError("inmem: nil message")
        async with self._lock:
            if self._closed:
                raise RuntimeError("inmem: driver closed")
            subs = list(self._subs.get(msg.subject, ()))

        # Fan-out for empty queue, round-robin per non-empty queue group.
        fanout: list[_Subscription] = []
        groups: dict[str, list[_Subscription]] = {}
        for s in subs:
            if not s.queue:
                fanout.append(s)
            else:
                groups.setdefault(s.queue, []).append(s)

        for s in fanout:
            await s.handler(msg)
        for q, members in groups.items():
            key = msg.subject + "\x00" + q
            counter = self._rr.get(key, 0)
            idx = counter % len(members)
            self._rr[key] = (counter + 1) & 0x3FFFFFFF
            await members[idx].handler(msg)

    async def subscribe(
        self,
        subject: str,
        queue: str,
        handler: TransportHandler,
    ) -> Subscription:
        if handler is None:
            raise ValueError("inmem: nil handler")
        async with self._lock:
            if self._closed:
                raise RuntimeError("inmem: driver closed")
            sub = _Subscription(self, subject, queue or "", handler)
            self._subs.setdefault(subject, []).append(sub)
            return sub

    async def request(
        self,
        msg: RawMessage,
        timeout: float | None = None,
    ) -> RawMessage:
        # Mirror Go: bus uses open_inbox for both request/reply and streaming.
        raise NotImplementedError("inmem: request not implemented; use bus.invoke")

    async def open_inbox(self) -> Inbox:
        subject = "_INBOX.inmem." + uuid.uuid4().hex
        box = _InMemoryInbox(self, subject)

        async def handler(m: RawMessage) -> None:
            await box._enqueue(m)

        box._sub = await self.subscribe(subject, "", handler)
        return box


class _Subscription:
    def __init__(
        self,
        driver: "InMemoryDriver",
        subject: str,
        queue: str,
        handler: TransportHandler,
    ) -> None:
        self.driver = driver
        self.subject = subject
        self.queue = queue
        self.handler = handler
        self._closed = False

    async def unsubscribe(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self.driver._lock:
            lst = self.driver._subs.get(self.subject, [])
            try:
                lst.remove(self)
            except ValueError:
                pass


class _InMemoryInbox:
    def __init__(self, driver: "InMemoryDriver", subject: str) -> None:
        self._driver = driver
        self._subject = subject
        self._queue: asyncio.Queue = asyncio.Queue()
        self._sub: Subscription | None = None
        self._closed = False

    @property
    def subject(self) -> str:
        return self._subject

    async def _enqueue(self, msg: RawMessage) -> None:
        if not self._closed:
            await self._queue.put(msg)

    async def recv(self, timeout: float | None = None) -> RawMessage:
        getter = self._queue.get()
        if timeout is not None:
            getter = asyncio.wait_for(getter, timeout)
        item = await getter
        if item is _CLOSED:
            await self._queue.put(_CLOSED)  # keep the queue poisoned for any future recv
            raise RuntimeError("inmem inbox: closed")
        return item

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._sub is not None:
            try:
                await self._sub.unsubscribe()
            except Exception:
                pass
        await self._queue.put(_CLOSED)
