"""OTLP trace export conformance for Python connectors."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from microbus_py.wire.codec import decode_response
from tests.fixtures.go_sidecar import connector_for
from tests.fixtures.otlp_collector import OtlpCollector, Span

if TYPE_CHECKING:
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

_SERVICE_NAME = "py-otlp.example"


@pytest.fixture
def otlp_conformance_env(
    otlp_collector: OtlpCollector,
    conformance_env: ConformanceEnv,
) -> ConformanceEnv:
    _ = otlp_collector
    return conformance_env


async def test_python_connector_exports_client_span_to_otlp_collector(
    otlp_collector: OtlpCollector,
    otlp_conformance_env: ConformanceEnv,
) -> None:
    svc = await connector_for(otlp_conformance_env, _SERVICE_NAME)
    await svc.startup()
    try:
        wire = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"otlp",
            timeout=5.0,
        )
        resp = decode_response(wire)
        assert resp.status_code == 200
    finally:
        await svc.shutdown()

    span = await otlp_collector.wait_for_span(
        lambda item: (
            item.kind == Span.SPAN_KIND_CLIENT
            and item.resource.get("service.name") == _SERVICE_NAME
        )
    )
    assert span.name == "POST echo.example/echo"
    assert span.resource["service.namespace"] == otlp_conformance_env.plane
    assert span.resource["service.instance.id"] == svc.instance_id
    assert span.attributes["http.method"] == "POST"
    assert span.attributes["microbus.target"] == "echo.example"
