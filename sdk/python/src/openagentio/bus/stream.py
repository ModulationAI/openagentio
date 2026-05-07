"""Stream framing — server-side StreamWriter and client-side Stream iterator.

Mirrors pkg/bus/stream.go. A streaming response is a sequence of envelopes
sharing the same ``correlation_id``; the runtime tags each frame with a
monotonic ``seq`` and a terminal frame (``ResponseFinal`` or ``ResponseError``)
sets ``is_final = True``.
"""
from __future__ import annotations

import asyncio
from typing import Any

from openagentio.codec.json_codec import Codec
from openagentio.event.envelope import Envelope
from openagentio.event.payload import CodeAgentUnavailable, ErrorPayload
from openagentio.event.types import (
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    ResponseStarted,
)
from openagentio.transport.base import Inbox, RawMessage, Transport


class ErrIdleTimeout(Exception):
    """Raised when the gap between two streaming frames exceeds the idle timeout."""


class StreamWriter:
    """Server-side stream emitter.

    Each method publishes one frame back to the request's ``reply_to`` subject.
    ``started`` may be called at most once. ``final`` and ``error`` are
    mutually exclusive and terminal — calls after either raise.
    """

    def __init__(
        self,
        transport: Transport,
        codec: Codec,
        agent_id: str,
        request: Envelope,
    ) -> None:
        self._transport = transport
        self._codec = codec
        self._agent_id = agent_id
        self._req = request
        self._lock = asyncio.Lock()
        self._seq = 0
        self._started = False
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    async def started(self, meta: Any = None) -> None:
        async with self._lock:
            if self._closed:
                raise RuntimeError("stream: already closed")
            if self._started:
                raise RuntimeError("stream: started already emitted")
            self._started = True
            seq = self._next_seq_locked()

        env = new_reply_shell(self._agent_id, self._req, ResponseStarted)
        env.seq = seq
        env.payload = self._codec.encode_payload(meta)
        await self._publish(env)

    async def delta(self, chunk: Any = None) -> None:
        async with self._lock:
            if self._closed:
                raise RuntimeError("stream: already closed")
            seq = self._next_seq_locked()

        env = new_reply_shell(self._agent_id, self._req, ResponseDelta)
        env.seq = seq
        env.payload = self._codec.encode_payload(chunk)
        await self._publish(env)

    async def final(self, result: Any = None) -> None:
        async with self._lock:
            if self._closed:
                raise RuntimeError("stream: already closed")
            self._closed = True
            seq = self._next_seq_locked()

        env = new_reply_shell(self._agent_id, self._req, ResponseFinal)
        env.seq = seq
        env.is_final = True
        env.payload = self._codec.encode_payload(result)
        await self._publish(env)

    async def error(self, exc: BaseException) -> None:
        async with self._lock:
            if self._closed:
                raise RuntimeError("stream: already closed")
            self._closed = True
            seq = self._next_seq_locked()

        env = new_reply_shell(self._agent_id, self._req, ResponseError)
        env.seq = seq
        env.is_final = True
        payload = ErrorPayload(code=CodeAgentUnavailable, message=str(exc))
        env.payload = self._codec.encode_payload(payload)
        await self._publish(env)

    def _next_seq_locked(self) -> int:
        s = self._seq
        self._seq += 1
        return s

    async def _publish(self, env: Envelope) -> None:
        data = self._codec.encode_envelope(env)
        await self._transport.publish(
            RawMessage(subject=self._req.reply_to, data=data)
        )


class Stream:
    """Async iterator over a streaming response.

    Frames are reordered by :py:attr:`Envelope.seq`; the iterator stops after
    yielding a frame with ``is_final = True``.

    Two timeouts coexist (mirroring the Go SDK):

    * ``idle_timeout`` — maximum gap between two frames; expiring raises
      :class:`ErrIdleTimeout`.
    * ``deadline`` — absolute wall-clock deadline for the whole stream
      (in :py:meth:`asyncio.AbstractEventLoop.time` units); expiring raises
      :class:`asyncio.TimeoutError`. The overall deadline always wins when
      both timers would fire — the iterator checks the deadline before and
      after each ``recv`` call.
    """

    def __init__(
        self,
        inbox: Inbox,
        codec: Codec,
        idle_timeout: float | None = None,
        deadline: float | None = None,
    ) -> None:
        self._inbox = inbox
        self._codec = codec
        self._idle = idle_timeout
        self._deadline = deadline
        self._expected = 0
        self._pending: dict[int, Envelope] = {}
        self._exhausted = False

    def __aiter__(self) -> "Stream":
        return self

    async def __anext__(self) -> Envelope:
        if self._exhausted:
            raise StopAsyncIteration

        while True:
            ready = self._pending.pop(self._expected, None)
            if ready is not None:
                self._expected += 1
                if ready.is_final:
                    self._exhausted = True
                return ready

            wait = self._idle
            if self._deadline is not None:
                remaining = self._deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    self._exhausted = True
                    raise asyncio.TimeoutError("bus: stream overall timeout")
                wait = remaining if wait is None else min(wait, remaining)

            try:
                msg = await self._inbox.recv(timeout=wait)
            except asyncio.TimeoutError:
                self._exhausted = True
                if (
                    self._deadline is not None
                    and asyncio.get_running_loop().time() >= self._deadline
                ):
                    raise asyncio.TimeoutError(
                        "bus: stream overall timeout"
                    ) from None
                raise ErrIdleTimeout("bus: stream idle timeout") from None
            except RuntimeError:
                # inbox closed mid-iteration — clean exit.
                self._exhausted = True
                raise StopAsyncIteration from None

            env = self._codec.decode_envelope(msg.data)
            if env.seq < self._expected:
                continue  # late / duplicate frame
            if env.seq in self._pending:
                continue
            self._pending[env.seq] = env

    async def close(self) -> None:
        if self._exhausted:
            return
        self._exhausted = True
        try:
            await self._inbox.close()
        except Exception:
            pass


def new_reply_shell(agent_id: str, req: Envelope, event_type: str) -> Envelope:
    """Pre-populate a response envelope with correlation metadata copied from req."""
    resp = Envelope.new(event_type)
    resp.from_ = agent_id
    resp.to = req.from_
    resp.session_id = req.session_id
    resp.conversation_id = req.conversation_id
    resp.tenant_id = req.tenant_id
    resp.user_id = req.user_id
    resp.channel = req.channel
    resp.trace_id = req.trace_id
    resp.span_id = req.span_id
    resp.traceparent = req.traceparent
    resp.correlation_id = req.event_id
    return resp
