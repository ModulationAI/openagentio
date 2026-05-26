"""BusError hierarchy, error_code_for() and is_retryable_for() tests.
Mirrors pkg/event/payload.go error codes and error mapping logic.
"""
from __future__ import annotations

import asyncio

from openagentio.bus.errors import (
    AgentTimeoutError,
    AgentUnavailableError,
    AuthFailureError,
    BackpressureDropError,
    BusError,
    CodecFailureError,
    InvalidRequestError,
    NoHandlerError,
    TransportFailureError,
    error_code_for,
    is_retryable_for,
)
from openagentio.bus.stream import ErrIdleTimeout
from openagentio.event.payload import (
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeBackpressureDrop,
    CodeCodecFailure,
    CodeInvalidRequest,
    CodeNoHandler,
    CodeTransportFailure,
)


def test_bus_error_base() -> None:
    err = BusError(code="CUSTOM", message="custom error", retryable=True)
    assert err.code == "CUSTOM"
    assert err.message == "custom error"
    assert err.retryable is True
    assert str(err) == "custom error"


def test_bus_error_base_defaults() -> None:
    err = BusError(code="CUSTOM", message="msg")
    assert err.retryable is False


def test_agent_timeout_error() -> None:
    err = AgentTimeoutError()
    assert err.code == CodeAgentTimeout
    assert err.retryable is True
    assert err.message == "bus: deadline exceeded"


def test_agent_timeout_error_custom_message() -> None:
    err = AgentTimeoutError("custom timeout msg")
    assert err.message == "custom timeout msg"


def test_agent_unavailable_error() -> None:
    err = AgentUnavailableError()
    assert err.code == CodeAgentUnavailable
    assert err.retryable is False
    assert err.message == "bus: agent unavailable"


def test_no_handler_error() -> None:
    err = NoHandlerError()
    assert err.code == CodeNoHandler
    assert err.retryable is False


def test_transport_failure_error() -> None:
    err = TransportFailureError()
    assert err.code == CodeTransportFailure
    assert err.retryable is True


def test_auth_failure_error() -> None:
    err = AuthFailureError()
    assert err.code == CodeAuthFailure
    assert err.retryable is False


def test_codec_failure_error() -> None:
    err = CodecFailureError()
    assert err.code == CodeCodecFailure
    assert err.retryable is False


def test_invalid_request_error() -> None:
    err = InvalidRequestError()
    assert err.code == CodeInvalidRequest
    assert err.retryable is False


def test_backpressure_drop_error() -> None:
    err = BackpressureDropError()
    assert err.code == CodeBackpressureDrop
    assert err.retryable is True


def test_error_code_for_bus_error_subclasses() -> None:
    """Each BusError subclass returns its own code."""
    assert error_code_for(AgentTimeoutError()) == CodeAgentTimeout
    assert error_code_for(AgentUnavailableError()) == CodeAgentUnavailable
    assert error_code_for(NoHandlerError()) == CodeNoHandler
    assert error_code_for(TransportFailureError()) == CodeTransportFailure
    assert error_code_for(AuthFailureError()) == CodeAuthFailure
    assert error_code_for(CodecFailureError()) == CodeCodecFailure
    assert error_code_for(InvalidRequestError()) == CodeInvalidRequest
    assert error_code_for(BackpressureDropError()) == CodeBackpressureDrop


def test_error_code_for_base_bus_error() -> None:
    err = BusError(code="MY_CODE", message="msg")
    assert error_code_for(err) == "MY_CODE"


def test_error_code_for_timeout_error() -> None:
    assert error_code_for(asyncio.TimeoutError()) == CodeAgentTimeout


def test_error_code_for_idle_timeout() -> None:
    assert error_code_for(ErrIdleTimeout()) == CodeAgentTimeout


def test_error_code_for_generic_exception() -> None:
    """Generic exceptions map to AGENT_UNAVAILABLE."""
    assert error_code_for(RuntimeError("boom")) == CodeAgentUnavailable
    assert error_code_for(ValueError("bad")) == CodeAgentUnavailable


def test_is_retryable_for_bus_error_subclasses() -> None:
    assert is_retryable_for(AgentTimeoutError()) is True
    assert is_retryable_for(AgentUnavailableError()) is False
    assert is_retryable_for(NoHandlerError()) is False
    assert is_retryable_for(TransportFailureError()) is True
    assert is_retryable_for(AuthFailureError()) is False
    assert is_retryable_for(CodecFailureError()) is False
    assert is_retryable_for(InvalidRequestError()) is False
    assert is_retryable_for(BackpressureDropError()) is True


def test_is_retryable_for_timeout_error() -> None:
    assert is_retryable_for(asyncio.TimeoutError()) is True


def test_is_retryable_for_idle_timeout() -> None:
    assert is_retryable_for(ErrIdleTimeout()) is True


def test_is_retryable_for_generic_exception() -> None:
    assert is_retryable_for(RuntimeError("boom")) is False
    assert is_retryable_for(ValueError("bad")) is False