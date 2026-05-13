"""Tests for client-span body-peek enrichment.

When a JSON request body contains a string ``run_id`` field, the active
CLIENT span MUST be enriched with the ``microbus.body.run_id`` attribute.
This is the framework hook that lets workflow correlation flow across
client+server spans without callers manually setting attributes.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from opentelemetry import trace

from microbus_py import Connector, InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse

if TYPE_CHECKING:
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


async def _roundtrip(*, body: bytes, server_host: str, client_host: str) -> None:
    transport = InMemoryBroker()
    server = Connector(hostname=server_host, transport=transport)
    client = Connector(hostname=client_host, transport=transport)

    async def handler(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        await client.request(
            method="POST",
            url=f"https://{server_host}:443/x",
            body=body,
            timeout=2.0,
        )
    finally:
        await client.shutdown()
        await server.shutdown()


def _client_span(exporter: InMemorySpanExporter):  # type: ignore[no-untyped-def]
    spans = [s for s in exporter.get_finished_spans() if s.kind == trace.SpanKind.CLIENT]
    assert spans, "expected at least one client span"
    return spans[0]


@pytest.mark.asyncio
async def test_request_body_with_run_id_emits_microbus_body_run_id_attribute(
    exporter: InMemorySpanExporter,
) -> None:
    body = json.dumps({"run_id": "abc-123", "extra": True}).encode()
    await _roundtrip(body=body, server_host="srvbp1.example", client_host="clibp1.example")
    span = _client_span(exporter)
    assert span.attributes is not None
    assert span.attributes.get("microbus.body.run_id") == "abc-123"


@pytest.mark.asyncio
async def test_request_body_without_run_id_emits_no_attribute(
    exporter: InMemorySpanExporter,
) -> None:
    body = json.dumps({"other_field": "x"}).encode()
    await _roundtrip(body=body, server_host="srvbp2.example", client_host="clibp2.example")
    span = _client_span(exporter)
    assert span.attributes is not None
    assert "microbus.body.run_id" not in span.attributes


@pytest.mark.asyncio
async def test_request_body_non_dict_skips_extraction(
    exporter: InMemorySpanExporter,
) -> None:
    body = json.dumps(["array", "instead"]).encode()
    await _roundtrip(body=body, server_host="srvbp3.example", client_host="clibp3.example")
    span = _client_span(exporter)
    assert span.attributes is not None
    assert "microbus.body.run_id" not in span.attributes


@pytest.mark.asyncio
async def test_request_body_run_id_non_string_skips_extraction(
    exporter: InMemorySpanExporter,
) -> None:
    body = json.dumps({"run_id": 12345}).encode()
    await _roundtrip(body=body, server_host="srvbp4.example", client_host="clibp4.example")
    span = _client_span(exporter)
    assert span.attributes is not None
    assert "microbus.body.run_id" not in span.attributes


@pytest.mark.asyncio
async def test_request_body_size_cap_64kib_skips_extraction(
    exporter: InMemorySpanExporter,
) -> None:
    padding = "x" * (65 * 1024)
    body = json.dumps({"run_id": "should-not-appear", "pad": padding}).encode()
    assert len(body) > (64 << 10)
    await _roundtrip(body=body, server_host="srvbp5.example", client_host="clibp5.example")
    span = _client_span(exporter)
    assert span.attributes is not None
    assert "microbus.body.run_id" not in span.attributes


@pytest.mark.asyncio
async def test_request_body_non_json_skips_extraction(
    exporter: InMemorySpanExporter,
) -> None:
    body = b"plain string not json"
    await _roundtrip(body=body, server_host="srvbp6.example", client_host="clibp6.example")
    span = _client_span(exporter)
    assert span.attributes is not None
    assert "microbus.body.run_id" not in span.attributes
