"""Dead-letter middleware. Mirrors pkg/middleware/deadletter.go.

When a handler error occurs, forwards the envelope to a DLQSink before
propagating the error upward. If the sink itself fails, both errors are
preserved in a :class:`DLQError` wrapper so callers can inspect either one.
"""
from __future__ import annotations

from typing import Awaitable, Callable

from openagentio.event.envelope import Envelope
from openagentio.middleware import Handler, Middleware

# DLQSink receives a failed envelope together with the last error.
# Implementations typically clone the envelope, stamp DLQ metadata,
# and publish it to a dead-letter subject.
DLQSink = Callable[[Envelope, Exception], Awaitable[None]]


class DLQError(Exception):
    """Wraps both the DLQ publish failure and the original handler error.

    Mirrors Go's ``fmt.Errorf("dlq publish failed: %w (original: %w)", dlqErr, err)``.
    Both ``dlq_error`` and ``original_error`` are inspectable attributes.
    """

    def __init__(self, dlq_error: Exception, original_error: Exception) -> None:
        self.dlq_error = dlq_error
        self.original_error = original_error
        super().__init__(
            f"dlq publish failed: {dlq_error} (original: {original_error})"
        )

    def __str__(self) -> str:
        return f"dlq publish failed: {self.dlq_error} (original: {self.original_error})"


def DeadLetter(sink: DLQSink) -> Middleware:
    """Wrap handler so any error is forwarded to *sink* before propagation."""
    if sink is None:
        raise ValueError("DeadLetter: nil sink")

    def wrap(next: Handler) -> Handler:
        async def handler(env: Envelope) -> None:
            try:
                await next(env)
            except Exception as original:
                try:
                    await sink(env, original)
                except Exception as dlq_exc:
                    raise DLQError(dlq_exc, original) from original
                raise original
        return handler
    return wrap