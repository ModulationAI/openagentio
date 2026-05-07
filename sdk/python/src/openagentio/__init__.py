"""OpenAgentIO Python SDK — ACP-compatible Envelope protocol on asyncio.

Quickstart::

    import asyncio
    from openagentio import Bus, InMemoryDriver

    async def main():
        bus = Bus(agent_id="echo", transport=InMemoryDriver())
        await bus.connect()

        async def echo(env):
            return env.payload_json()

        await bus.handle_invoke("echo", echo)
        resp = await bus.invoke("echo", {"msg": "hello"})
        print(resp.event_type, resp.payload_json())
        await bus.close()

    asyncio.run(main())
"""
from openagentio.bus import (
    DEFAULT_SUBJECT_PREFIX,
    Bus,
    ErrIdleTimeout,
    Handler,
    InvokeHandler,
    Stream,
    StreamHandler,
    StreamWriter,
)
from openagentio.codec import Codec, JSONCodec
from openagentio.event import (
    SCHEMA_VERSION,
    SPEC_VERSION,
    CodeAgentTimeout,
    CodeAgentUnavailable,
    CodeAuthFailure,
    CodeBackpressureDrop,
    CodeCodecFailure,
    CodeInvalidRequest,
    CodeNoHandler,
    CodeTransportFailure,
    DeltaPayload,
    Envelope,
    ErrorPayload,
    FinalPayload,
    MessageReceived,
    ResponseDelta,
    ResponseError,
    ResponseFinal,
    ResponseStarted,
    StartedPayload,
    TaskCompleted,
    TaskCreated,
    ToolCall,
    ToolResult,
    is_terminal,
    new_id,
)
from openagentio.transport import (
    Capabilities,
    Inbox,
    InMemoryDriver,
    NATSDriver,
    RawMessage,
    Subscription,
    Transport,
    TransportHandler,
)
from openagentio import session

__version__ = "0.2.0a0"

__all__ = [
    "__version__",
    # Bus.
    "Bus",
    "Stream",
    "StreamWriter",
    "ErrIdleTimeout",
    "Handler",
    "InvokeHandler",
    "StreamHandler",
    "DEFAULT_SUBJECT_PREFIX",
    # Envelope / events.
    "Envelope",
    "new_id",
    "is_terminal",
    "SPEC_VERSION",
    "SCHEMA_VERSION",
    "MessageReceived",
    "ResponseStarted",
    "ResponseDelta",
    "ResponseFinal",
    "ResponseError",
    "ToolCall",
    "ToolResult",
    "TaskCreated",
    "TaskCompleted",
    "StartedPayload",
    "DeltaPayload",
    "FinalPayload",
    "ErrorPayload",
    "CodeAgentTimeout",
    "CodeAgentUnavailable",
    "CodeBackpressureDrop",
    "CodeTransportFailure",
    "CodeCodecFailure",
    "CodeAuthFailure",
    "CodeInvalidRequest",
    "CodeNoHandler",
    # Codec.
    "Codec",
    "JSONCodec",
    # Transport.
    "Transport",
    "RawMessage",
    "Capabilities",
    "Subscription",
    "Inbox",
    "TransportHandler",
    "InMemoryDriver",
    "NATSDriver",
    # Session / trace context (see openagentio.session for full API).
    "session",
]
