"""OTel Bridge configuration. Mirrors pkg/middleware/otel/config.go.

Provides :class:`Config`, the :data:`Option` type, and ``With*`` helpers.
Defaults use the global TracerProvider and TextMapPropagator when no overrides
are supplied — standard OTel SDK init wires both to W3C Trace Context.
"""
from __future__ import annotations

from typing import Callable

from opentelemetry.propagate import get_global_textmap
from opentelemetry.trace import Tracer, TracerProvider, get_tracer_provider

_TRACER_NAME = "openagentio.middleware.otel"


class Config:
    """Resolved configuration for Trace middleware and EnvelopePreparer."""

    tracer: Tracer
    propagator: object

    def __init__(self, tracer: Tracer, propagator: object) -> None:
        self.tracer = tracer
        self.propagator = propagator


Option = Callable[[Config], None]


def WithTracerProvider(tp: TracerProvider) -> Option:
    """Override the TracerProvider. Useful for tests that want a SpanRecorder."""
    def apply(c: Config) -> None:
        c.tracer = tp.get_tracer(_TRACER_NAME)
    return apply


def WithPropagator(p: object) -> Option:
    """Override the TextMapPropagator. Default is W3C Trace Context."""
    def apply(c: Config) -> None:
        c.propagator = p
    return apply


def new_config(*opts: Option) -> Config:
    """Resolve defaults: global TracerProvider → tracer, global propagator."""
    provider = get_tracer_provider()
    tracer = provider.get_tracer(_TRACER_NAME)
    propagator = get_global_textmap()

    cfg = Config(tracer=tracer, propagator=propagator)
    for o in opts:
        o(cfg)
    return cfg