"""Logging middleware — structured log lines."""
from __future__ import annotations

import logging

from openagentio import Envelope
from openagentio.middleware import Handler
from openagentio.middleware.logging import Logging


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def test_logging_emits_debug_on_success() -> None:
    logger = logging.Logger("test.logging")
    lh = _ListHandler()
    logger.addHandler(lh)
    logger.setLevel(logging.DEBUG)

    async def ok(env: Envelope) -> None:
        pass

    wrapped = Logging(logger)(ok)
    env = Envelope.new("test.log")
    env.trace_id = "trace-log"
    env.session_id = "sess-log"
    await wrapped(env)

    debug_records = [r for r in lh.records if r.levelno == logging.DEBUG]
    assert len(debug_records) >= 1
    assert debug_records[0].msg == "handler ok"


async def test_logging_emits_error_on_exception() -> None:
    logger = logging.Logger("test.logging.err")
    lh = _ListHandler()
    logger.addHandler(lh)
    logger.setLevel(logging.DEBUG)

    async def failing(env: Envelope) -> None:
        raise RuntimeError("log-fail")

    wrapped = Logging(logger)(failing)
    try:
        await wrapped(Envelope.new("test.log.err"))
    except RuntimeError:
        pass

    error_records = [r for r in lh.records if r.levelno == logging.ERROR]
    assert len(error_records) >= 1
    assert error_records[0].msg == "handler error"