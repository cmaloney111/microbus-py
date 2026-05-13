"""Shared OTel tracer-provider + exporter for trace-related tests.

OpenTelemetry's ``set_tracer_provider`` is a one-shot global on first call;
subsequent calls become no-ops. Multiple test modules each setting their
own provider would silently route spans to whichever fixture won the race.
A single session-scoped provider/exporter avoids that interference.
"""

from __future__ import annotations

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


@pytest.fixture(scope="session")
def _shared_trace_provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    provider = TracerProvider()
    exp = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exp))
    trace.set_tracer_provider(provider)
    return provider, exp


@pytest.fixture
def exporter(
    _shared_trace_provider: tuple[TracerProvider, InMemorySpanExporter],
) -> InMemorySpanExporter:
    _, exp = _shared_trace_provider
    exp.clear()
    return exp
