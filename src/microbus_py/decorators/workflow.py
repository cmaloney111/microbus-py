"""``@workflow`` decorator — workflow graph definition endpoints."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from microbus_py.connector.route import parse_route
from microbus_py.decorators.task import _route_has_explicit_port
from microbus_py.wire.codec import HTTPRequest, HTTPResponse
from microbus_py.workflow.graph import Graph
from microbus_py.workflow.registry import WorkflowFeature, register_workflow

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector

__all__ = ["workflow_decorator"]


WorkflowFn = Callable[[], Graph]


def workflow_decorator(svc: Connector) -> Callable[..., Callable[[WorkflowFn], WorkflowFn]]:
    def deco(
        *,
        route: str,
        name: str | None = None,
        description: str = "",
        required_claims: str | None = None,
        queue: str | None = None,
    ) -> Callable[[WorkflowFn], WorkflowFn]:
        def wrap(fn: WorkflowFn) -> WorkflowFn:
            feature_name = name or fn.__name__
            handler = _wrap_handler(fn)
            port, path, host_override = parse_route(route)
            if not _route_has_explicit_port(route):
                port = 428
            svc.subscribe(
                name=feature_name,
                method="GET",
                path=route,
                handler=handler,
                queue=queue,
                required_claims=required_claims,
                description=description,
                port=port,
            )
            register_workflow(
                svc,
                WorkflowFeature(
                    name=feature_name,
                    route=path,
                    description=description,
                    required_claims=required_claims,
                    port=port,
                    host=host_override,
                ),
            )
            return fn

        return wrap

    return deco


def _wrap_handler(fn: WorkflowFn) -> Callable[[HTTPRequest], Awaitable[HTTPResponse]]:
    async def runner(_req: HTTPRequest) -> HTTPResponse:
        graph = fn()
        body = json.dumps({"graph": graph.to_json()}).encode("utf-8")
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "application/json")],
            body=body,
        )

    return runner
