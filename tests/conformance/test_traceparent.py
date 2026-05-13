"""W3C trace context propagation across Python and Go Microbus peers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from microbus_py.wire.codec import decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

TRACEPARENT_RE = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")


@pytest.fixture(scope="module")
def _provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    provider = TracerProvider()
    exp = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exp))
    trace.set_tracer_provider(provider)
    return provider, exp


@pytest.fixture
def exporter(_provider: tuple[TracerProvider, InMemorySpanExporter]) -> InMemorySpanExporter:
    _, exp = _provider
    exp.clear()
    return exp


async def test_python_request_sends_traceparent_to_go_peer(
    conformance_env: ConformanceEnv,
    exporter: InMemorySpanExporter,
) -> None:
    svc = await connector_for(conformance_env, "py-trace.example")
    await svc.startup()
    try:
        wire = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"trace me",
            timeout=5.0,
        )
        resp = decode_response(wire)
        headers = dict(resp.headers)
        traceparent = headers.get("Traceparent")
        assert traceparent is not None
        assert TRACEPARENT_RE.match(traceparent)
        assert [s for s in exporter.get_finished_spans() if s.kind == trace.SpanKind.CLIENT]
    finally:
        await svc.shutdown()
