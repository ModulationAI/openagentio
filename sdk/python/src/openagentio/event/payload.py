"""Standard payload shapes and error codes. Mirrors pkg/event/payload.go."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Standard error codes used by ErrorPayload.code. Wire-identical to pkg/event/payload.go.
CodeAgentTimeout = "AGENT_TIMEOUT"
CodeAgentUnavailable = "AGENT_UNAVAILABLE"
CodeBackpressureDrop = "BACKPRESSURE_DROP"
CodeTransportFailure = "TRANSPORT_FAILURE"
CodeCodecFailure = "CODEC_FAILURE"
CodeAuthFailure = "AUTH_FAILURE"
CodeInvalidRequest = "INVALID_REQUEST"
CodeNoHandler = "NO_HANDLER"


@dataclass
class StartedPayload:
    """Accompanies agent.response.started; carries optional upstream metadata."""
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class DeltaPayload:
    """Streamed increment for agent.response.delta."""
    delta: str = ""
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class FinalPayload:
    """Terminates a streaming response with the consolidated result."""
    result: dict[str, Any] = field(default_factory=dict)


@dataclass
class ErrorPayload:
    """Standardized failure shape for agent.response.error and failed tool results."""
    code: str = ""
    message: str = ""
    retryable: bool = False
    cause: dict[str, Any] = field(default_factory=dict)
