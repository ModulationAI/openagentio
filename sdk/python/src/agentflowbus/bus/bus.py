"""Application-facing bus over a Transport.

Async-first: every IO operation is a coroutine. Mirrors pkg/bus.

A Bus instance owns the subscriptions registered via :meth:`Bus.handle_invoke`
and :meth:`Bus.handle_stream`; closing the bus unsubscribes them and closes the
underlying transport.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from agentflowbus.bus.stream import Stream, StreamWriter, new_reply_shell
from agentflowbus.bus.subjects import (
    DEFAULT_SUBJECT_PREFIX,
    event_subject,
    invoke_subject,
)
from agentflowbus.codec.json_codec import Codec, JSONCodec
from agentflowbus.event.envelope import Envelope
from agentflowbus.event.payload import CodeAgentUnavailable, ErrorPayload
from agentflowbus.event.types import (
    MessageReceived,
    ResponseError,
    ResponseFinal,
    is_terminal,
)
from agentflowbus.transport.base import (
    RawMessage,
    Subscription as TransportSubscription,
    Transport,
)

# Handler signatures.
Handler = Callable[[Envelope], Awaitable[None]]
InvokeHandler = Callable[[Envelope], Awaitable[Any]]
StreamHandler = Callable[[Envelope, StreamWriter], Awaitable[None]]


class Bus:
    """Application-facing bus. Construct, ``await bus.connect()``, then publish/subscribe."""

    def __init__(
        self,
        *,
        agent_id: str,
        transport: Transport,
        tenant: str = "",
        subject_prefix: str = DEFAULT_SUBJECT_PREFIX,
        codec: Codec | None = None,
        logger: logging.Logger | None = None,
        default_timeout: float = 30.0,
    ) -> None:
        if not agent_id:
            raise ValueError("bus: agent_id is required")
        if transport is None:
            raise ValueError("bus: transport is required")
        self._agent_id = agent_id
        self._tenant = tenant
        self._prefix = subject_prefix
        self._codec = codec or JSONCodec()
        self._transport = transport
        self._logger = logger or logging.getLogger("agentflowbus")
        self._default_timeout = default_timeout

        self._owned: list[TransportSubscription] = []
        self._tasks: set[asyncio.Task] = set()
        self._closed = False
        self._lock = asyncio.Lock()

    # --- lifecycle -------------------------------------------------------

    async def connect(self) -> None:
        await self._transport.connect()

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            owned = list(self._owned)
            self._owned.clear()
            tasks = list(self._tasks)

        for t in tasks:
            t.cancel()
        for s in owned:
            try:
                await s.unsubscribe()
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: unsubscribe error: %s", e)
        await self._transport.close()

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def transport(self) -> Transport:
        return self._transport

    # --- pub / sub -------------------------------------------------------

    async def publish(self, env: Envelope) -> None:
        if env is None:
            raise ValueError("bus: nil envelope")
        if not env.event_type:
            raise ValueError("bus: envelope missing event_type")
        subject = event_subject(self._prefix, env.event_type, self._resolve_tenant(env.tenant_id))
        data = self._codec.encode_envelope(env)
        await self._transport.publish(
            RawMessage(subject=subject, data=data, reply_to=env.reply_to)
        )

    async def subscribe(
        self,
        event_type: str,
        handler: Handler,
        *,
        queue: str = "",
    ) -> TransportSubscription:
        if handler is None:
            raise ValueError("bus: nil handler")
        if not event_type:
            raise ValueError("bus: empty event_type")
        subject = event_subject(self._prefix, event_type, self._tenant)

        async def dispatch(msg: RawMessage) -> None:
            try:
                env = self._codec.decode_envelope(msg.data)
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: decode error: %s", e)
                return
            await self._safe_call(handler, env)

        return await self._transport.subscribe(subject, queue, dispatch)

    # --- invoke / reply --------------------------------------------------

    async def invoke(
        self,
        target: str,
        payload: Any = None,
        *,
        timeout: float | None = None,
    ) -> Envelope:
        if not target:
            raise ValueError("bus: empty invoke target")

        eff_timeout = timeout if timeout is not None else self._default_timeout
        env = self._build_request_envelope(target, payload)

        inbox = await self._transport.open_inbox()
        try:
            env.reply_to = inbox.subject
            data = self._codec.encode_envelope(env)
            await self._transport.publish(
                RawMessage(
                    subject=invoke_subject(
                        self._prefix, target, self._resolve_tenant(env.tenant_id)
                    ),
                    data=data,
                )
            )
            recv_timeout = eff_timeout if eff_timeout > 0 else None
            msg = await inbox.recv(timeout=recv_timeout)
            return self._codec.decode_envelope(msg.data)
        finally:
            await inbox.close()

    async def handle_invoke(
        self,
        target: str,
        handler: InvokeHandler,
        *,
        queue: str = "",
    ) -> None:
        if not target:
            raise ValueError("bus: empty invoke target")
        if handler is None:
            raise ValueError("bus: nil invoke handler")
        subject = invoke_subject(self._prefix, target, self._tenant)

        async def dispatch(msg: RawMessage) -> None:
            try:
                req = self._codec.decode_envelope(msg.data)
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: decode error: %s", e)
                return
            await self._handle_one(req, handler)

        sub = await self._transport.subscribe(subject, queue, dispatch)
        self._track_owned(sub)

    async def _handle_one(self, req: Envelope, handler: InvokeHandler) -> None:
        result: Any = None
        user_err: BaseException | None = None
        try:
            result = await handler(req)
        except BaseException as e:  # noqa: BLE001
            user_err = e

        if not req.reply_to:
            if user_err is not None:
                self._logger.warning(
                    "bus: invoke handler error (no reply_to): %s", user_err
                )
            return

        if user_err is not None:
            resp = self._error_response(req, user_err)
        elif isinstance(result, Envelope):
            resp = self._adopt_response(req, result)
        else:
            resp = self._final_response(req, result)

        try:
            data = self._codec.encode_envelope(resp)
            await self._transport.publish(
                RawMessage(subject=req.reply_to, data=data)
            )
        except Exception as e:  # noqa: BLE001
            self._logger.warning("bus: reply publish failed: %s", e)

    # --- stream invoke ---------------------------------------------------

    async def stream_invoke(
        self,
        target: str,
        payload: Any = None,
        *,
        timeout: float | None = None,
        idle_timeout: float | None = None,
    ) -> Stream:
        if not target:
            raise ValueError("bus: empty invoke target")

        eff_timeout = timeout if timeout is not None else self._default_timeout
        deadline: float | None
        if eff_timeout > 0:
            deadline = asyncio.get_running_loop().time() + eff_timeout
        else:
            deadline = None

        env = self._build_request_envelope(target, payload)
        inbox = await self._transport.open_inbox()
        env.reply_to = inbox.subject
        try:
            data = self._codec.encode_envelope(env)
            await self._transport.publish(
                RawMessage(
                    subject=invoke_subject(
                        self._prefix, target, self._resolve_tenant(env.tenant_id)
                    ),
                    data=data,
                )
            )
        except Exception:
            await inbox.close()
            raise

        return Stream(
            inbox=inbox,
            codec=self._codec,
            idle_timeout=idle_timeout,
            deadline=deadline,
        )

    async def handle_stream(
        self,
        target: str,
        handler: StreamHandler,
        *,
        queue: str = "",
    ) -> None:
        if not target:
            raise ValueError("bus: empty invoke target")
        if handler is None:
            raise ValueError("bus: nil stream handler")
        subject = invoke_subject(self._prefix, target, self._tenant)

        async def dispatch(msg: RawMessage) -> None:
            try:
                req = self._codec.decode_envelope(msg.data)
            except Exception as e:  # noqa: BLE001
                self._logger.warning("bus: decode error: %s", e)
                return
            if not req.reply_to:
                self._logger.warning("bus: stream request missing reply_to")
                return
            task = asyncio.create_task(self._run_stream_handler(req, handler))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        sub = await self._transport.subscribe(subject, queue, dispatch)
        self._track_owned(sub)

    async def _run_stream_handler(
        self, req: Envelope, handler: StreamHandler
    ) -> None:
        writer = StreamWriter(self._transport, self._codec, self._agent_id, req)
        herr: BaseException | None = None
        try:
            await handler(req, writer)
        except BaseException as e:  # noqa: BLE001
            herr = e

        if writer.closed:
            return
        try:
            if herr is not None:
                await writer.error(herr)
            else:
                await writer.final(None)
        except Exception as e:  # noqa: BLE001
            self._logger.warning("bus: stream auto-finalize failed: %s", e)

    # --- helpers ---------------------------------------------------------

    def _track_owned(self, sub: TransportSubscription) -> None:
        self._owned.append(sub)

    async def _safe_call(self, handler: Handler, env: Envelope) -> None:
        try:
            await handler(env)
        except BaseException as e:  # noqa: BLE001
            self._logger.warning("bus: handler error: %s", e)

    def _resolve_tenant(self, envelope_tenant: str) -> str:
        return envelope_tenant or self._tenant

    def _build_request_envelope(self, target: str, payload: Any) -> Envelope:
        if isinstance(payload, Envelope):
            env = payload.clone()
            if not env.from_:
                env.from_ = self._agent_id
            if not env.to:
                env.to = target
            if not env.tenant_id:
                env.tenant_id = self._tenant
            return env

        env = Envelope.new(MessageReceived)
        env.from_ = self._agent_id
        env.to = target
        env.tenant_id = self._tenant
        if payload is not None:
            env.payload = self._codec.encode_payload(payload)
        return env

    def _final_response(self, req: Envelope, payload: Any) -> Envelope:
        resp = new_reply_shell(self._agent_id, req, ResponseFinal)
        resp.is_final = True
        if payload is not None:
            resp.payload = self._codec.encode_payload(payload)
        return resp

    def _error_response(self, req: Envelope, exc: BaseException) -> Envelope:
        resp = new_reply_shell(self._agent_id, req, ResponseError)
        resp.is_final = True
        payload = ErrorPayload(code=CodeAgentUnavailable, message=str(exc))
        resp.payload = self._codec.encode_payload(payload)
        return resp

    def _adopt_response(self, req: Envelope, user: Envelope) -> Envelope:
        resp = user.clone()
        if not resp.from_:
            resp.from_ = self._agent_id
        if not resp.to:
            resp.to = req.from_
        if not resp.correlation_id:
            resp.correlation_id = req.event_id
        if not resp.session_id:
            resp.session_id = req.session_id
        if not resp.conversation_id:
            resp.conversation_id = req.conversation_id
        if not resp.tenant_id:
            resp.tenant_id = req.tenant_id
        if not resp.trace_id:
            resp.trace_id = req.trace_id
        if not resp.traceparent:
            resp.traceparent = req.traceparent
        if not resp.is_final and is_terminal(resp.event_type):
            resp.is_final = True
        return resp
