"""Tests for OTel span emission around ``Connector.request`` and dispatch.

A request from caller to handler must produce a *client* span on the caller
side and a *server* span on the responder side, with the server span's
parent being the client span (W3C ``traceparent`` header propagation).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from opentelemetry import trace

from microbus_py import Connector, InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse

if TYPE_CHECKING:
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


@pytest.mark.asyncio
async def test_request_emits_client_span(exporter: InMemorySpanExporter) -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def handler(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        await client.request(method="POST", url="https://srv.example:443/x", timeout=2.0)
    finally:
        await client.shutdown()
        await server.shutdown()

    spans = exporter.get_finished_spans()
    client_spans = [s for s in spans if s.kind == trace.SpanKind.CLIENT]
    assert client_spans, "expected at least one client span"
    span = client_spans[0]
    assert span.attributes is not None
    assert span.attributes.get("http.method") == "POST"
    assert span.attributes.get("microbus.target") == "srv.example"


@pytest.mark.asyncio
async def test_handler_span_parents_to_caller(exporter: InMemorySpanExporter) -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv2.example", transport=transport)
    client = Connector(hostname="cli2.example", transport=transport)

    async def handler(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        await client.request(method="POST", url="https://srv2.example:443/x", timeout=2.0)
    finally:
        await client.shutdown()
        await server.shutdown()

    spans = exporter.get_finished_spans()
    by_kind = {s.kind: s for s in spans}
    client_span = by_kind.get(trace.SpanKind.CLIENT)
    server_span = by_kind.get(trace.SpanKind.SERVER)
    assert client_span is not None
    assert server_span is not None
    assert server_span.context.trace_id == client_span.context.trace_id
    assert server_span.parent is not None
    assert server_span.parent.span_id == client_span.context.span_id
