"""Retry middleware — RetryPolicy, backoff, acp.retry.attempt stamping."""
from __future__ import annotations

import asyncio

from openagentio import Envelope
from openagentio.middleware import Handler
from openagentio.middleware.retry import (
    ConstantBackoff,
    ExponentialBackoff,
    IsRetryableError,
    Retry,
    RetryPolicy,
    Retryable,
)


async def test_retry_succeeds_on_first_attempt() -> None:
    attempts = 0

    async def handler(env: Envelope) -> None:
        attempts += 1  # type: ignore[assignment]

    # Python closures need mutable container for nonlocal counter
    attempts_list: list[int] = []

    async def handler2(env: Envelope) -> None:
        attempts_list.append(1)

    wrapped = Retry(RetryPolicy(max_attempts=3))(handler2)
    env = Envelope.new("test.retry")
    await wrapped(env)
    assert attempts_list == [1]
    assert env.metadata.get("acp.retry.attempt") == 1


async def test_retry_retries_on_failure() -> None:
    """Handler fails first two calls, succeeds on third."""
    call_count = 0

    async def handler(env: Envelope) -> None:
        call_count += 1
        if call_count < 3:
            raise RuntimeError("transient")

    # Need mutable container
    counts: list[int] = []

    async def handler2(env: Envelope) -> None:
        counts.append(1)
        if len(counts) < 3:
            raise RuntimeError("transient")

    wrapped = Retry(RetryPolicy(max_attempts=3))(handler2)
    env = Envelope.new("test.retry")
    await wrapped(env)
    assert len(counts) == 3
    assert env.metadata.get("acp.retry.attempt") == 3


async def test_retry_stops_on_non_retryable() -> None:
    """is_retryable returning False should stop retries immediately."""
    counts: list[int] = []

    async def handler(env: Envelope) -> None:
        counts.append(1)
        raise RuntimeError("permanent")

    policy = RetryPolicy(max_attempts=5, is_retryable=lambda err: False)
    wrapped = Retry(policy)(handler)
    env = Envelope.new("test.retry.nonretryable")

    try:
        await wrapped(env)
    except RuntimeError as e:
        assert str(e) == "permanent"

    assert len(counts) == 1
    assert env.metadata.get("acp.retry.attempt") == 1


async def test_retry_stamps_acp_retry_attempt_metadata() -> None:
    """Each attempt stamps acp.retry.attempt on the envelope metadata."""
    seen_attempts: list[int] = []

    async def handler(env: Envelope) -> None:
        seen_attempts.append(env.metadata["acp.retry.attempt"])

    wrapped = Retry(RetryPolicy(max_attempts=1))(handler)
    env = Envelope.new("test.retry.stamp")
    await wrapped(env)
    assert seen_attempts == [1]


async def test_constant_backoff() -> None:
    """ConstantBackoff always returns the same delay."""
    bf = ConstantBackoff(0.5)
    assert bf(1) == 0.5
    assert bf(2) == 0.5
    assert bf(10) == 0.5


async def test_exponential_backoff() -> None:
    """ExponentialBackoff doubles delay, capped at max."""
    bf = ExponentialBackoff(0.1, 1.0)
    assert bf(1) == 0.1
    assert bf(2) == 0.2
    assert bf(3) == 0.4
    assert bf(4) == 0.8
    assert bf(5) == 1.0  # capped


async def test_retryable_and_is_retryable_error() -> None:
    """Retryable() wraps an error; IsRetryableError() detects it."""
    original = RuntimeError("retry-me")
    wrapped = Retryable(original)
    assert isinstance(wrapped, Exception)
    assert IsRetryableError(wrapped) is True
    assert IsRetryableError(original) is False


async def test_retryable_nil_safe() -> None:
    assert Retryable(None) is None


async def test_retry_exhausted_raises_last_error() -> None:
    """When all attempts fail, the last exception is raised."""
    counts: list[int] = []

    async def handler(env: Envelope) -> None:
        counts.append(1)
        raise ValueError("always-fail")

    wrapped = Retry(RetryPolicy(max_attempts=2))(handler)
    env = Envelope.new("test.retry.exhausted")

    try:
        await wrapped(env)
    except ValueError as e:
        assert str(e) == "always-fail"

    assert len(counts) == 2