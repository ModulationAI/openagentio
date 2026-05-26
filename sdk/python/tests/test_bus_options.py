"""Bus functional option pattern tests. Mirrors pkg/bus/options.go."""
from __future__ import annotations

import logging

from openagentio.bus.options import (
    Options,
    Option,
    SubOption,
    InvokeOption,
    HandleOption,
    _SubOpts,
    _InvokeOpts,
    _HandleOpts,
    collect_sub_opts,
    collect_invoke_opts,
    collect_handle_opts,
    WithAgentID,
    WithTransport,
    WithTenant,
    WithSubjectPrefix,
    WithCodec,
    WithLogger,
    WithDefaultTimeout,
    WithMiddleware,
    WithEnvelopePreparer,
    WithQueue,
    WithTimeout,
    WithIdleTimeout,
    WithHandleQueue,
)
from openagentio.codec.json_codec import JSONCodec
from openagentio.transport.inmem import InMemoryDriver


def test_options_defaults() -> None:
    opts = Options()
    assert opts.agent_id == ""
    assert opts.tenant == ""
    assert opts.subject_prefix == "acp.v1"
    assert opts.codec is None
    assert opts.transport is None
    assert opts.logger is None
    assert opts.middleware == []
    assert opts.envelope_preparers == []
    assert opts.default_timeout == 30.0


def test_with_agent_id() -> None:
    opts = Options()
    WithAgentID("my-agent")(opts)
    assert opts.agent_id == "my-agent"


def test_with_transport() -> None:
    opts = Options()
    t = InMemoryDriver()
    WithTransport(t)(opts)
    assert opts.transport is t


def test_with_tenant() -> None:
    opts = Options()
    WithTenant("tenant-X")(opts)
    assert opts.tenant == "tenant-X"


def test_with_subject_prefix() -> None:
    opts = Options()
    WithSubjectPrefix("custom.v2")(opts)
    assert opts.subject_prefix == "custom.v2"


def test_with_codec() -> None:
    opts = Options()
    c = JSONCodec()
    WithCodec(c)(opts)
    assert opts.codec is c


def test_with_logger() -> None:
    opts = Options()
    logger = logging.getLogger("test")
    WithLogger(logger)(opts)
    assert opts.logger is logger


def test_with_default_timeout() -> None:
    opts = Options()
    WithDefaultTimeout(5.0)(opts)
    assert opts.default_timeout == 5.0


def test_with_middleware() -> None:
    opts = Options()
    mw1 = lambda next: next
    mw2 = lambda next: next
    WithMiddleware(mw1, mw2)(opts)
    assert opts.middleware == [mw1, mw2]


def test_with_envelope_preparer() -> None:
    opts = Options()
    p1 = lambda env: None
    WithEnvelopePreparer(p1)(opts)
    assert opts.envelope_preparers == [p1]


def test_bus_new_with_options() -> None:
    from openagentio import Bus
    bus = Bus.new(WithAgentID("a"), WithTransport(InMemoryDriver()))
    assert bus.agent_id == "a"


def test_bus_new_rejects_empty_agent_id() -> None:
    from openagentio import Bus
    try:
        Bus.new(WithTransport(InMemoryDriver()))
        raise AssertionError("should have raised ValueError")
    except ValueError:
        pass


def test_bus_new_rejects_nil_transport() -> None:
    from openagentio import Bus
    try:
        Bus.new(WithAgentID("a"))
        raise AssertionError("should have raised ValueError")
    except ValueError:
        pass


def test_collect_sub_opts() -> None:
    so = collect_sub_opts([WithQueue("q")])
    assert so.queue == "q"


def test_collect_sub_opts_defaults() -> None:
    so = collect_sub_opts([])
    assert so.queue == ""


def test_collect_invoke_opts() -> None:
    io = collect_invoke_opts([WithTimeout(5.0), WithIdleTimeout(1.0)])
    assert io.timeout == 5.0
    assert io.idle_timeout == 1.0


def test_collect_invoke_opts_defaults() -> None:
    io = collect_invoke_opts([])
    assert io.timeout is None
    assert io.idle_timeout is None


def test_collect_handle_opts() -> None:
    ho = collect_handle_opts([WithHandleQueue("q")])
    assert ho.queue == "q"
    assert ho.queue_set is True


def test_collect_handle_opts_defaults() -> None:
    ho = collect_handle_opts([])
    assert ho.queue == ""
    assert ho.queue_set is False