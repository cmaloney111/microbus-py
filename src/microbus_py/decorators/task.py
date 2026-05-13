"""``@task`` decorator — workflow task endpoints on port 428."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from microbus_py.connector.route import parse_route
from microbus_py.errors.http import HTTP_REASONS, json_error_body
from microbus_py.errors.traced import TracedError
from microbus_py.wire.codec import HTTPRequest, HTTPResponse
from microbus_py.workflow.flow import Flow
from microbus_py.workflow.registry import TaskFeature, register_task

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector

__all__ = ["task_decorator"]


TaskHandler = Callable[[Flow], Awaitable[Flow]]


def task_decorator(svc: Connector) -> Callable[..., Callable[[TaskHandler], TaskHandler]]:
    def deco(
        *,
        route: str,
        name: str | None = None,
        description: str = "",
        required_claims: str | None = None,
        queue: str | None = None,
    ) -> Callable[[TaskHandler], TaskHandler]:
        def wrap(handler: TaskHandler) -> TaskHandler:
            feature_name = name or handler.__name__
            wrapped = _wrap_handler(handler)
            port, path, host_override = parse_route(route)
            if not _route_has_explicit_port(route):
                port = 428
            svc.subscribe(
                name=feature_name,
                method="POST",
                path=route,
                handler=wrapped,
                queue=queue,
                required_claims=required_claims,
                description=description,
                port=port,
            )
            register_task(
                svc,
                TaskFeature(
                    name=feature_name,
                    route=path,
                    description=description,
                    required_claims=required_claims,
                    port=port,
                    host=host_override,
                ),
            )
            return handler

        return wrap

    return deco


def _route_has_explicit_port(route: str) -> bool:
    if route.startswith(":"):
        return True
    if route.startswith(("http://", "https://")):
        _, _, rest = route.partition("://")
        host_part, _, _ = rest.partition("/")
        return ":" in host_part
    if route.startswith("//"):
        rest = route[2:]
        host_part, _, _ = rest.partition("/")
        return ":" in host_part
    return False


def _wrap_handler(handler: TaskHandler) -> Callable[[HTTPRequest], Awaitable[HTTPResponse]]:
    async def runner(req: HTTPRequest) -> HTTPResponse:
        try:
            doc = json.loads(req.body) if req.body else {}
            flow = Flow.from_json(doc)
        except (json.JSONDecodeError, ValueError) as exc:
            err = TracedError(f"invalid flow body: {exc}", status_code=400)
            return HTTPResponse(
                status_code=400,
                reason=HTTP_REASONS[400],
                headers=[("Content-Type", "application/json")],
                body=json_error_body(err),
            )
        result = await handler(flow)
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "application/json")],
            body=json.dumps(result.to_json()).encode("utf-8"),
        )

    return runner
