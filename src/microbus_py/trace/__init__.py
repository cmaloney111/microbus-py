"""OpenTelemetry span lifecycle for Microbus connectors.

Caller wraps :meth:`Connector.request` in a CLIENT span and injects W3C
``traceparent`` into the request headers. Responder dispatcher extracts the
header, attaches the SERVER span as child, and propagates the context to the
handler. Tracer name: ``microbus_py``.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
    OTLPSpanExporter as GrpcOTLPSpanExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter as HttpOTLPSpanExporter,
)
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter

if TYPE_CHECKING:
    from collections.abc import Iterator

__all__ = [
    "TRACER",
    "TraceRuntime",
    "client_span",
    "create_trace_runtime",
    "extract_remote_context",
    "inject_traceparent",
    "server_span",
]


TRACER = trace.get_tracer("microbus_py")
_GRPC_PROTOCOL = "grpc"
_HTTP_TRACE_SUFFIX = "/v1/traces"


@dataclass(slots=True)
class TraceRuntime:
    """Connector-owned tracing runtime.

    When OTLP env vars are absent, ``provider`` is ``None`` and the runtime
    delegates to the process-global tracer provider. When OTLP is configured,
    each connector owns a provider so exported spans carry connector-specific
    service resource attributes.
    """

    tracer: trace.Tracer
    provider: TracerProvider | None = None

    def shutdown(self) -> None:
        if self.provider is not None:
            self.provider.shutdown()


def create_trace_runtime(
    *,
    hostname: str,
    version: int,
    instance_id: str,
    plane: str,
    deployment: str,
) -> TraceRuntime:
    endpoint = _trace_endpoint()
    if not endpoint:
        return TraceRuntime(tracer=trace.get_tracer("microbus_py"))

    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.namespace": plane,
                "service.name": hostname,
                "service.version": str(version),
                "service.instance.id": instance_id,
                "deployment.environment": deployment,
            }
        ),
        shutdown_on_exit=False,
    )
    provider.add_span_processor(BatchSpanProcessor(_span_exporter(endpoint)))
    return TraceRuntime(tracer=provider.get_tracer("microbus_py"), provider=provider)


@contextmanager
def client_span(
    *,
    target_host: str,
    method: str,
    path: str,
    tracer: trace.Tracer | None = None,
) -> Iterator[trace.Span]:
    active_tracer = tracer or TRACER
    with active_tracer.start_as_current_span(
        f"{method} {target_host}{path}",
        kind=trace.SpanKind.CLIENT,
        attributes={
            "http.method": method,
            "http.target": path,
            "microbus.target": target_host,
        },
    ) as span:
        yield span


@contextmanager
def server_span(
    *,
    method: str,
    path: str,
    headers: list[tuple[str, str]],
    tracer: trace.Tracer | None = None,
) -> Iterator[trace.Span]:
    parent = extract_remote_context(headers)
    active_tracer = tracer or TRACER
    with active_tracer.start_as_current_span(
        f"{method} {path}",
        kind=trace.SpanKind.SERVER,
        context=parent,
        attributes={"http.method": method, "http.target": path},
    ) as span:
        yield span


def inject_traceparent(headers: list[tuple[str, str]]) -> list[tuple[str, str]]:
    carrier: dict[str, str] = {}
    inject(carrier)
    return [*headers, *carrier.items()]


def extract_remote_context(headers: list[tuple[str, str]]) -> Any:
    carrier = {k.lower(): v for k, v in headers}
    return extract(carrier)


def _trace_endpoint() -> str:
    return os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "",
    )


def _trace_protocol(endpoint: str) -> str:
    protocol = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL") or os.environ.get(
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "",
    )
    if not protocol and ":4317" in endpoint:
        protocol = _GRPC_PROTOCOL
    return protocol


def _span_exporter(endpoint: str) -> SpanExporter:
    protocol = _trace_protocol(endpoint)
    if protocol == _GRPC_PROTOCOL:
        return GrpcOTLPSpanExporter(endpoint=endpoint)

    return HttpOTLPSpanExporter(endpoint=_http_trace_endpoint(endpoint))


def _http_trace_endpoint(endpoint: str) -> str:
    clean = endpoint.rstrip("/")
    if clean.endswith(_HTTP_TRACE_SUFFIX):
        return clean
    return f"{clean}{_HTTP_TRACE_SUFFIX}"
