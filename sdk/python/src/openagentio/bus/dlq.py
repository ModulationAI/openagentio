"""DLQSink factory. Mirrors pkg/bus/dlq.go.

Returns a :data:`DLQSink` that publishes a cloned envelope onto the
dead-letter subject ``{prefix}.dlq.{event_type}``. The sink stamps two
metadata keys on the clone:

- ``acp.dlq.original_event_type`` — the original EventType
- ``acp.dlq.last_error`` — the string representation of lastErr
"""
from __future__ import annotations

from openagentio.bus.subjects import DEFAULT_SUBJECT_PREFIX
from openagentio.codec.json_codec import Codec
from openagentio.event.envelope import Envelope
from openagentio.middleware.deadletter import DLQSink
from openagentio.transport.base import RawMessage, Transport


def dlq_sink(
    prefix: str = DEFAULT_SUBJECT_PREFIX,
    codec: Codec | None = None,
    transport: Transport | None = None,
) -> DLQSink:
    """Create a DLQSink that publishes to ``{prefix}.dlq.{event_type}``.

    The caller must supply *transport* (and optionally *codec*).
    """
    if transport is None:
        raise ValueError("dlq_sink: transport is required")
    c = codec or __import__("openagentio.codec.json_codec", fromlist=["JSONCodec"]).JSONCodec()

    async def sink(env: Envelope, last_err: Exception) -> None:
        cp = env.clone()
        if cp.metadata is None:
            cp.metadata = {}
        cp.metadata["acp.dlq.original_event_type"] = cp.event_type
        if last_err is not None:
            cp.metadata["acp.dlq.last_error"] = str(last_err)
        data = c.encode_envelope(cp)
        subject = f"{prefix}.dlq.{cp.event_type}"
        await transport.publish(RawMessage(subject=subject, data=data))

    return sink