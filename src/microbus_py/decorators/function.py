"""``@function`` decorator — typed Pydantic-modelled RPC endpoints."""

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

__all__ = ["FunctionFeature", "function_decorator"]


@dataclass(slots=True, frozen=True)
class FunctionFeature:
    name: str
    route: str
    method: str
    description: str
    input_model: type[BaseModel]
    output_model: type[BaseModel]
    required_claims: str | None
    port: int
    host: str | None = None
    load_balancing: str = "default"


TypedHandler = Callable[..., Awaitable[BaseModel]]


def function_decorator(svc: Connector) -> Callable[..., Callable[[TypedHandler], TypedHandler]]:
    def deco(
        *,
        route: str,
        method: str = "POST",
        name: str | None = None,
        description: str = "",
        required_claims: str | None = None,
        queue: str | None = None,
        port: int | None = None,
        load_balancing: str = "default",
    ) -> Callable[[TypedHandler], TypedHandler]:
        def wrap(handler: TypedHandler) -> TypedHandler:
            input_model, output_model = _resolve_models(handler)
            wrapped = _wrap_handler(handler, input_model)
            feature_name = name or handler.__name__
            sub_queue = _queue_for(queue, load_balancing)
            svc.subscribe(
                name=feature_name,
                method=method,
                path=route,
                handler=wrapped,
                queue=sub_queue,
                required_claims=required_claims,
                description=description,
                port=port,
            )
            eff_port, eff_path, host_override = _path_port(route, port)
            svc.register_function_feature(
                FunctionFeature(
                    name=feature_name,
                    route=eff_path,
                    method=method.upper(),
                    description=description,
                    input_model=input_model,
                    output_model=output_model,
                    required_claims=required_claims,
                    port=eff_port,
                    host=host_override,
                    load_balancing=load_balancing,
                )
            )
            return handler

        return deco_inner(wrap)

    return deco


def _queue_for(queue: str | None, load_balancing: str) -> str | None:
    if queue is not None:
        return queue
    if load_balancing == "none":
        return ""
    if load_balancing == "default":
        return None
    return load_balancing


def deco_inner(
    wrap: Callable[[TypedHandler], TypedHandler],
) -> Callable[[TypedHandler], TypedHandler]:
    return wrap


def _resolve_models(handler: TypedHandler) -> tuple[type[BaseModel], type[BaseModel]]:
    hints = typing.get_type_hints(handler)
    sig = inspect.signature(handler)
    params = [p for p in sig.parameters.values() if p.kind != inspect.Parameter.VAR_KEYWORD]
    if not params:
        raise TypeError(
            f"@function handler {handler.__name__} requires one Pydantic input parameter"
        )
    input_type = hints.get(params[0].name)
    if not (isinstance(input_type, type) and issubclass(input_type, BaseModel)):
        raise TypeError(
            f"@function handler {handler.__name__} input must be a Pydantic BaseModel subclass"
        )
    return_type = hints.get("return")
    if not (isinstance(return_type, type) and issubclass(return_type, BaseModel)):
        raise TypeError(
            f"@function handler {handler.__name__} must annotate a Pydantic BaseModel return type"
        )
    return input_type, return_type


def _wrap_handler(
    handler: TypedHandler, input_model: type[BaseModel]
) -> Callable[[HTTPRequest], Awaitable[HTTPResponse]]:
    async def runner(req: HTTPRequest) -> HTTPResponse:
        try:
            payload: Any = json.loads(req.body) if req.body else {}
            inp = input_model.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            err = TracedError(f"invalid request body: {exc}", status_code=400)
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


def _path_port(route: str, port: int | None) -> tuple[int, str, str | None]:
    eff_port, eff_path, host_override = parse_route(route)
    if port is not None:
        eff_port = port
    return eff_port, eff_path, host_override
