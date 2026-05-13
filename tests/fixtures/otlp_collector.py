"""In-process OTLP trace collector for conformance tests."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import grpc  # type: ignore[import-untyped]
import pytest
from opentelemetry.proto.collector.trace.v1 import (
    trace_service_pb2,
    trace_service_pb2_grpc,
)
from opentelemetry.proto.trace.v1.trace_pb2 import Span

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue

_EXPORTER_PROTOCOL = "grpc"
_EXPORT_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class CollectedSpan:
    name: str
    kind: int
    trace_id: str
    span_id: str
    parent_span_id: str
    resource: dict[str, object]
    attributes: dict[str, object]


class _TraceService(trace_service_pb2_grpc.TraceServiceServicer):
    def __init__(self, collector: OtlpCollector) -> None:
        self._collector = collector

    async def Export(
        self,
        request: trace_service_pb2.ExportTraceServiceRequest,
        context: object,
    ) -> trace_service_pb2.ExportTraceServiceResponse:
        _ = context
        self._collector.record(request)
        return trace_service_pb2.ExportTraceServiceResponse()


class OtlpCollector:
    def __init__(self, server: Any, endpoint: str) -> None:
        self._server = server
        self.endpoint = endpoint
        self._spans: list[CollectedSpan] = []
        self._changed = asyncio.Event()

    @property
    def spans(self) -> tuple[CollectedSpan, ...]:
        return tuple(self._spans)

    @classmethod
    async def start(cls) -> OtlpCollector:
        server = grpc.aio.server()
        port = server.add_insecure_port("127.0.0.1:0")
        collector = cls(server, f"http://127.0.0.1:{port}")
        trace_service_pb2_grpc.add_TraceServiceServicer_to_server(  # type: ignore[no-untyped-call]
            _TraceService(collector),
            server,
        )
        await server.start()
        return collector

    async def close(self) -> None:
        await self._server.stop(grace=0)

    def record(self, request: trace_service_pb2.ExportTraceServiceRequest) -> None:
        for resource_spans in request.resource_spans:
            resource = _key_values(resource_spans.resource.attributes)
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    self._spans.append(
                        CollectedSpan(
                            name=span.name,
                            kind=span.kind,
                            trace_id=span.trace_id.hex(),
                            span_id=span.span_id.hex(),
                            parent_span_id=span.parent_span_id.hex(),
                            resource=resource,
                            attributes=_key_values(span.attributes),
                        )
                    )
        self._changed.set()

    async def wait_for_span(
        self,
        predicate: Callable[[CollectedSpan], bool],
        *,
        timeout: float = _EXPORT_TIMEOUT_SECONDS,
    ) -> CollectedSpan:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            for span in self._spans:
                if predicate(span):
                    return span
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for exported OTLP span")
            self._changed.clear()
            await asyncio.wait_for(self._changed.wait(), timeout=remaining)


@pytest.fixture
async def otlp_collector(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[OtlpCollector]:
    collector = await OtlpCollector.start()
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.endpoint)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", _EXPORTER_PROTOCOL)
    try:
        yield collector
    finally:
        await collector.close()


def _key_values(values: list[KeyValue]) -> dict[str, object]:
    return {item.key: _any_value(item.value) for item in values}


def _any_value(value: AnyValue) -> object:
    field = value.WhichOneof("value")
    if field is None:
        return None
    selected: Any = getattr(value, field)
    if field == "array_value":
        return tuple(_any_value(item) for item in selected.values)
    if field == "kvlist_value":
        return _key_values(selected.values)
    return selected


__all__ = ["CollectedSpan", "OtlpCollector", "Span", "otlp_collector"]
