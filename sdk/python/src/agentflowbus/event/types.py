"""Event-type and protocol-version constants. Mirrors pkg/event/types.go."""

SPEC_VERSION = "acp/1.0"
SCHEMA_VERSION = 1

# User input.
MessageReceived = "agent.message.received"

# Response lifecycle.
ResponseStarted = "agent.response.started"
ResponseDelta = "agent.response.delta"
ResponseFinal = "agent.response.final"
ResponseError = "agent.response.error"

# Tool calls (reserved, enabled in v0.2+).
ToolCall = "agent.tool.call"
ToolResult = "agent.tool.result"

# Async tasks (reserved, enabled in v0.3+ with JetStream).
TaskCreated = "agent.task.created"
TaskCompleted = "agent.task.completed"

_TERMINAL = frozenset({ResponseFinal, ResponseError, ToolResult, TaskCompleted})


def is_terminal(event_type: str) -> bool:
    """True if this event type closes a streaming response on its correlation_id."""
    return event_type in _TERMINAL
