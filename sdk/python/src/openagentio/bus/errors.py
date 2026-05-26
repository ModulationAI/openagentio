"""Structured bus errors with ACP error codes. Mirrors pkg/event/payload.go codes."""
from __future__ import annotations

from openagentio.event.payload import (
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeBackpressureDrop,
    CodeCodecFailure,
    CodeAuthFailure,
    CodeInvalidRequest,
    CodeNoHandler,
    CodeTransportFailure,
)


class BusError(Exception):
    """Base bus error carrying an ACP error code and retryable flag."""

    def __init__(
        self,
        code: str,
        message: str,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


class AgentTimeoutError(BusError):
    """Handler or invocation exceeded the deadline."""

    def __init__(self, message: str = "bus: deadline exceeded") -> None:
        super().__init__(CodeAgentTimeout, message, retryable=True)


class AgentUnavailableError(BusError):
    """Handler raised an unexpected exception."""

    def __init__(self, message: str = "bus: agent unavailable") -> None:
        super().__init__(CodeAgentUnavailable, message, retryable=False)


class NoHandlerError(BusError):
    """No handler registered for the target."""

    def __init__(self, message: str = "bus: no handler") -> None:
        super().__init__(CodeNoHandler, message, retryable=False)


class TransportFailureError(BusError):
    """Transport-level failure (connection lost, publish failed)."""

    def __init__(self, message: str = "bus: transport failure") -> None:
        super().__init__(CodeTransportFailure, message, retryable=True)


class AuthFailureError(BusError):
    """Authentication / authorization failure."""

    def __init__(self, message: str = "bus: auth failure") -> None:
        super().__init__(CodeAuthFailure, message, retryable=False)


class CodecFailureError(BusError):
    """Envelope encode/decode failure."""

    def __init__(self, message: str = "bus: codec failure") -> None:
        super().__init__(CodeCodecFailure, message, retryable=False)


class InvalidRequestError(BusError):
    """Malformed or semantically invalid request."""

    def __init__(self, message: str = "bus: invalid request") -> None:
        super().__init__(CodeInvalidRequest, message, retryable=False)


class BackpressureDropError(BusError):
    """Dropped due to back-pressure."""

    def __init__(self, message: str = "bus: backpressure drop") -> None:
        super().__init__(CodeBackpressureDrop, message, retryable=True)


def error_code_for(exc: BaseException) -> str:
    """Map an exception to the appropriate ACP error code for envelope responses."""
    if isinstance(exc, BusError):
        return exc.code
    if isinstance(exc, asyncio.TimeoutError):
        return CodeAgentTimeout
    from openagentio.bus.stream import ErrIdleTimeout
    if isinstance(exc, ErrIdleTimeout):
        return CodeAgentTimeout
    return CodeAgentUnavailable


def is_retryable_for(exc: BaseException) -> bool:
    """Determine retryable flag from exception type."""
    if isinstance(exc, BusError):
        return exc.retryable
    if isinstance(exc, (asyncio.TimeoutError,)):
        return True
    from openagentio.bus.stream import ErrIdleTimeout
    if isinstance(exc, ErrIdleTimeout):
        return True
    return False


# Lazy import to avoid circular dependency at module level.
import asyncio