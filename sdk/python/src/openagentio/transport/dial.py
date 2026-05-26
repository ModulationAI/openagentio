"""Transport factory keyed off ``OPENAGENTIO_TRANSPORT``.

Mirrors ``pkg/transport/dial/dial.go``. Reads the transport mode and (for NATS)
the URL from environment variables, constructs the matching driver, and
connects it before returning.

Environment variables:

* ``OPENAGENTIO_TRANSPORT`` -- ``"inmem"`` or ``"nats"``. Empty defaults to
  ``"nats"``. Any other value raises :class:`ValueError`.
* ``NATS_URL`` -- NATS server URL (default ``"nats://localhost:4222"``).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

from openagentio.transport.base import Transport
from openagentio.transport.inmem import InMemoryDriver
from openagentio.transport.nats import NATSDriver


_DEFAULT_NATS_URL = "nats://localhost:4222"


@dataclass
class _DialOpts:
    nats_name: str = ""


DialOption = Callable[[_DialOpts], None]


def WithNATSName(name: str) -> DialOption:
    """Set the NATS connection name. Ignored for non-NATS transports."""
    def apply(o: _DialOpts) -> None:
        o.nats_name = name
    return apply


async def dial(*options: DialOption) -> Transport:
    """Construct and connect a Transport based on ``OPENAGENTIO_TRANSPORT``."""
    opts = _DialOpts()
    for opt in options:
        opt(opts)

    mode = os.environ.get("OPENAGENTIO_TRANSPORT", "")
    if mode == "inmem":
        driver = InMemoryDriver()
        await driver.connect()
        return driver

    if mode == "" or mode == "nats":
        url = os.environ.get("NATS_URL", "") or _DEFAULT_NATS_URL
        driver = NATSDriver(url=url, name=opts.nats_name)
        try:
            await driver.connect()
        except Exception as e:
            raise RuntimeError(f"connect to NATS {url}: {e}") from e
        return driver

    raise ValueError(f"unsupported OPENAGENTIO_TRANSPORT={mode!r}")
