"""Error-to-HTTP mapping mirroring pkg/adapter/http/errors.go."""
from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from starlette.responses import JSONResponse, Response

from openagentio.bus.errors import BusError
from openagentio.bus.stream import ErrIdleTimeout
from openagentio.event.envelope import Envelope
from openagentio.event.payload import (
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeBackpressureDrop,
    CodeCodecFailure,
    CodeInvalidRequest,
    CodeNoHandler,
    CodeTransportFailure,
    ErrorPayload,
)
from openagentio.event.types import ResponseError

if TYPE_CHECKING:
    pass


_CODE_TO_STATUS = {
    CodeAuthFailure: 401,
    CodeInvalidRequest: 400,
    CodeNoHandler: 404,
    CodeAgentTimeout: 504,
    CodeAgentUnavailable: 502,
    CodeTransportFailure: 502,
    CodeBackpressureDrop: 429,
    CodeCodecFailure: 500,
}


def status_for_code(code: str) -> int:
    return _CODE_TO_STATUS.get(code, 500)


def status_for_bus_error(exc: BaseException) -> tuple[int, str]:
    if isinstance(exc, (asyncio.TimeoutError, ErrIdleTimeout)):
        return 504, CodeAgentTimeout
    if isinstance(exc, asyncio.CancelledError):
        return 499, CodeInvalidRequest
    return 502, CodeAgentUnavailable


def write_error_json(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"code": code, "message": message},
    )


def write_bus_error(exc: BaseException) -> JSONResponse:
    status, code = status_for_bus_error(exc)
    return write_error_json(status, code, str(exc))


def write_envelope_error(env: Envelope) -> JSONResponse:
    ep = ErrorPayload()
    if env.payload:
        try:
            data = json.loads(env.payload)
            ep = ErrorPayload(
                code=data.get("code", ""),
                message=data.get("message", ""),
                retryable=data.get("retryable", False),
                cause=data.get("cause", {}),
            )
        except (json.JSONDecodeError, ValueError):
            pass
    if not ep.code:
        ep.code = CodeAgentUnavailable
    if not ep.message:
        ep.message = "agent error"
    status = status_for_code(ep.code)
    content = {"code": ep.code, "message": ep.message}
    if ep.retryable:
        content["retryable"] = ep.retryable
    if ep.cause:
        content["cause"] = ep.cause
    return JSONResponse(status_code=status, content=content)
