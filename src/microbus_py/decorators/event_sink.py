"""``@event_sink`` decorator — subscribe to a source service's outbound event.

The decorated handler is wired up as a normal subscription on the source
service's hostname/port/path, so the wire path is identical to a regular
function endpoint, but the bound subject lives under the source's namespace.
"""

from __future__ import annotations

import inspect
import json
import typing
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from microbus_py.connector.route import parse_route
from microbus_py.errors.http import HTTP_REASONS, json_error_body
from microbus_py.errors.traced import TracedError
from microbus_py.wire.codec import HTTPRequest, HTTPResponse

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector

__all__ = ["EventSinkFeature", "event_sink_decorator"]


SinkHandler = Callable[..., Awaitable[BaseModel]]


@dataclass(slots=True, frozen=True)
class EventSinkFeature:
    name: str
    source: str
    route: str
    method: str
    port: int
    queue: str | None
    required_claims: str | None
    input_model: type[BaseModel]
    output_model: type[BaseModel]


def event_sink_decorator(svc: Connector) -> Callable[..., Callable[[SinkHandler], SinkHandler]]:
    def deco(
        *,
        source: str,
        route: str,
        name: str,
        method: str = "POST",
        queue: str | None = None,
        required_claims: str | None = None,
    ) -> Callable[[SinkHandler], SinkHandler]:
        port, path, _ = parse_route(route)

        def wrap(handler: SinkHandler) -> SinkHandler:
            input_model, output_model = _resolve_models(handler, name)
            wrapped = _wrap_handler(handler, input_model)
            absolute = f"https://{source}:{port}{path}"
            effective_queue = queue if queue is not None else svc.hostname
            svc.subscribe(
                name=name,
                method=method,
                path=absolute,
                handler=wrapped,
                queue=effective_queue,
                required_claims=required_claims,
                description="",
            )
            feature = EventSinkFeature(
                name=name,
                source=source,
                route=path,
                method=method.upper(),
                port=port,
                queue=queue,
                required_claims=required_claims,
                input_model=input_model,
                output_model=output_model,
            )
            features = svc.__dict__.setdefault("_event_sink_features", [])
            features.append(feature)
            return handler

        return wrap

    return deco


def _resolve_models(
    handler: SinkHandler, sink_name: str
) -> tuple[type[BaseModel], type[BaseModel]]:
    hints = typing.get_type_hints(handler)
    sig = inspect.signature(handler)
    params = [p for p in sig.parameters.values() if p.kind != inspect.Parameter.VAR_KEYWORD]
    if not params:
        raise TypeError(f"event_sink '{sink_name}' handler requires one Pydantic input parameter")
    input_type = hints.get(params[0].name)
    if not (isinstance(input_type, type) and issubclass(input_type, BaseModel)):
        raise TypeError(f"event_sink '{sink_name}' input must be a Pydantic BaseModel subclass")
    return_type = hints.get("return")
    if not (isinstance(return_type, type) and issubclass(return_type, BaseModel)):
        raise TypeError(f"event_sink '{sink_name}' must annotate a Pydantic BaseModel return type")
    return input_type, return_type


def _wrap_handler(
    handler: SinkHandler, input_model: type[BaseModel]
) -> Callable[[HTTPRequest], Awaitable[HTTPResponse]]:
    async def runner(req: HTTPRequest) -> HTTPResponse:
        try:
            payload: Any = json.loads(req.body) if req.body else {}
            inp = input_model.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            err = TracedError(f"invalid event payload: {exc}", status_code=400)
            return HTTPResponse(
                status_code=400,
                reason=HTTP_REASONS[400],
                headers=[("Content-Type", "application/json")],
                body=json_error_body(err),
            )
        result = await handler(inp)
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "application/json")],
            body=result.model_dump_json().encode("utf-8"),
        )

    return runner
