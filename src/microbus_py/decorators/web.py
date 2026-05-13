"""``@web`` decorator — raw HTTPRequest/HTTPResponse handlers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from microbus_py.connector.route import parse_route
from microbus_py.wire.codec import HTTPRequest, HTTPResponse

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector

__all__ = ["WebFeature", "web_decorator"]


WebHandler = Callable[[HTTPRequest], Awaitable[HTTPResponse]]


@dataclass(slots=True, frozen=True)
class WebFeature:
    name: str
    route: str
    method: str
    description: str
    required_claims: str | None
    port: int
    host: str | None = None
    load_balancing: str = "default"


def web_decorator(svc: Connector) -> Callable[..., Callable[[WebHandler], WebHandler]]:
    def deco(
        *,
        route: str,
        method: str = "GET",
        name: str | None = None,
        description: str = "",
        required_claims: str | None = None,
        queue: str | None = None,
        port: int | None = None,
        load_balancing: str = "default",
    ) -> Callable[[WebHandler], WebHandler]:
        def wrap(handler: WebHandler) -> WebHandler:
            feature_name = name or handler.__name__
            sub_queue = _queue_for(queue, load_balancing)
            svc.subscribe(
                name=feature_name,
                method=method,
                path=route,
                handler=handler,
                queue=sub_queue,
                required_claims=required_claims,
                description=description,
                port=port,
            )
            eff_port, eff_path, host_override = parse_route(route)
            if port is not None:
                eff_port = port
            svc.register_web_feature(
                WebFeature(
                    name=feature_name,
                    route=eff_path,
                    method=method.upper(),
                    description=description,
                    required_claims=required_claims,
                    port=eff_port,
                    host=host_override,
                    load_balancing=load_balancing,
                )
            )
            return handler

        return wrap

    return deco


def _queue_for(queue: str | None, load_balancing: str) -> str | None:
    if queue is not None:
        return queue
    if load_balancing == "none":
        return ""
    if load_balancing == "default":
        return None
    return load_balancing
