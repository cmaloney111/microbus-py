"""Tests for connector-owned OpenTelemetry runtime setup."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import pytest
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

from microbus_py import trace as trace_mod
from microbus_py.trace import create_trace_runtime

if TYPE_CHECKING:
    from collections.abc import Sequence

    from opentelemetry.sdk.trace import ReadableSpan

_ENDPOINT = "http://127.0.0.1:4317"


class FakeExporter(SpanExporter):
    created: ClassVar[list[FakeExporter]] = []

    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.shutdown_called = False
        self.created.append(self)

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        _ = spans
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.fixture(autouse=True)
def _clear_exporter_state() -> None:
    FakeExporter.created.clear()


def test_runtime_uses_global_tracer_when_otlp_endpoint_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    runtime = create_trace_runtime(
        hostname="svc.example",
        version=7,
        instance_id="abc",
        plane="microbus",
        deployment="TESTING",
    )

    assert runtime.provider is None
    runtime.shutdown()


def test_runtime_configures_grpc_exporter_and_resource_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", _ENDPOINT)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")
    monkeypatch.setattr(trace_mod, "GrpcOTLPSpanExporter", FakeExporter)

    runtime = create_trace_runtime(
        hostname="svc.example",
        version=7,
        instance_id="abc",
        plane="microbus",
        deployment="TESTING",
    )
    try:
        assert runtime.provider is not None
        assert FakeExporter.created[-1].endpoint == _ENDPOINT
        attrs = runtime.provider.resource.attributes
        assert attrs["service.namespace"] == "microbus"
        assert attrs["service.name"] == "svc.example"
        assert attrs["service.version"] == "7"
        assert attrs["service.instance.id"] == "abc"
        assert attrs["deployment.environment"] == "TESTING"
    finally:
        runtime.shutdown()

    assert FakeExporter.created[-1].shutdown_called


def test_runtime_adds_trace_path_for_http_exporter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    monkeypatch.setattr(trace_mod, "HttpOTLPSpanExporter", FakeExporter)

    runtime = create_trace_runtime(
        hostname="svc.example",
        version=0,
        instance_id="abc",
        plane="microbus",
        deployment="LAB",
    )
    try:
        assert FakeExporter.created[-1].endpoint == "http://collector:4318/v1/traces"
    finally:
        runtime.shutdown()


def test_runtime_keeps_explicit_http_trace_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://collector:4318/v1/traces",
    )
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/protobuf")
    monkeypatch.setattr(trace_mod, "HttpOTLPSpanExporter", FakeExporter)

    runtime = create_trace_runtime(
        hostname="svc.example",
        version=0,
        instance_id="abc",
        plane="microbus",
        deployment="LAB",
    )
    try:
        assert FakeExporter.created[-1].endpoint == "http://collector:4318/v1/traces"
    finally:
        runtime.shutdown()
