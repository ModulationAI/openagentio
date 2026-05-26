"""Retry middleware. Mirrors pkg/middleware/retry.go.

Retries transient handler failures according to a :class:`RetryPolicy`.
On each attempt the envelope metadata key ``acp.retry.attempt`` is updated.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from openagentio.event.envelope import Envelope
from openagentio.middleware import Handler, Middleware


@dataclass
class RetryPolicy:
    """Configuration for the Retry middleware.

    A zero-value policy is usable: defaults to one attempt (no retry),
    zero backoff, and all errors retryable.
    """

    max_attempts: int = 1
    backoff: Callable[[int], float] = field(default_factory=lambda: lambda _attempt: 0.0)
    is_retryable: Callable[[Exception], bool] = field(default_factory=lambda: lambda _err: True)


def Retry(policy: RetryPolicy | None = None) -> Middleware:
    """Wrap handler so transient failures are retried according to *policy*."""
    p = policy or RetryPolicy()
    if p.max_attempts <= 0:
        p.max_attempts = 1

    def wrap(next: Handler) -> Handler:
        async def handler(env: Envelope) -> None:
            last_exc: Exception | None = None
            for attempt in range(1, p.max_attempts + 1):
                if env.metadata is None:
                    env.metadata = {}
                env.metadata["acp.retry.attempt"] = attempt

                try:
                    await next(env)
                    return
                except Exception as exc:
                    last_exc = exc
                    if not p.is_retryable(exc):
                        break
                    if attempt < p.max_attempts:
                        delay = p.backoff(attempt)
                        if delay > 0:
                            await asyncio.sleep(delay)
            if last_exc is not None:
                raise last_exc
        return handler
    return wrap


def ConstantBackoff(seconds: float) -> Callable[[int], float]:
    """Backoff function that always returns *seconds*."""
    return lambda _attempt: seconds


def ExponentialBackoff(base: float, max_seconds: float) -> Callable[[int], float]:
    """Backoff that doubles on each attempt, capped at *max_seconds*."""
    def _backoff(attempt: int) -> float:
        d = base
        for _ in range(1, attempt):
            d *= 2
            if d > max_seconds:
                return max_seconds
        return d
    return _backoff


class _RetryableError(Exception):
    """Sentinel wrapper that marks an error as explicitly retryable."""

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self._error = error


def Retryable(exc: Exception | None) -> Exception | None:
    """Wrap *exc* so :func:`IsRetryableError` returns True. Nil-safe."""
    if exc is None:
        return None
    return _RetryableError(exc)


def IsRetryableError(exc: Exception) -> bool:
    """Return True if *exc* (or any wrapped error in its chain) was created with :func:`Retryable`."""
    if isinstance(exc, _RetryableError):
        return True
    # Walk __cause__ chain.
    cause = exc.__cause__
    while cause is not None:
        if isinstance(cause, _RetryableError):
            return True
        cause = cause.__cause__
    return False