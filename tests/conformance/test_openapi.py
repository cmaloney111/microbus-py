"""OpenAPI control endpoint parity for Python connectors."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import httpx
import jwt as pyjwt
import pytest
from pydantic import BaseModel

from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]
JsonObject = dict[str, object]


class EchoIn(BaseModel):
    name: str


class EchoOut(BaseModel):
    message: str


async def test_python_connector_serves_go_style_openapi_control_document(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await connector_for(
        conformance_env,
        "py-openapi.example",
        version=7,
        description="Python OpenAPI conformance service",
    )

    @svc.function(route="/echo", method="POST", description="Echo a name")
    async def echo(inp: EchoIn) -> EchoOut:
        return EchoOut(message=inp.name)

    @svc.web(route="/admin", method="GET", required_claims="roles.admin")
    async def admin(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    await svc.startup()
    try:
        public_doc = await _fetch_doc(svc)
        assert public_doc["info"] == {
            "title": "py-openapi.example",
            "description": "Python OpenAPI conformance service",
            "version": "7",
        }
        public_paths = _as_object(public_doc["paths"])
        assert "/py-openapi.example:443/echo" in public_paths
        assert "/py-openapi.example:888/openapi.json" in public_paths
        assert "/py-openapi.example:443/admin" not in public_paths
        echo_path = _as_object(public_paths["/py-openapi.example:443/echo"])
        echo_op = _as_object(echo_path["post"])
        assert echo_op["x-feature-type"] == "function"
        assert echo_op["x-name"] == "echo"

        token = pyjwt.encode(
            {"sub": "u", "roles": {"admin": True}},
            key="",
            algorithm="none",
        )
        admin_doc = await _fetch_doc(svc, actor_token=token)
        admin_paths = _as_object(admin_doc["paths"])
        assert "/py-openapi.example:443/admin" in admin_paths
        admin_path = _as_object(admin_paths["/py-openapi.example:443/admin"])
        admin_op = _as_object(admin_path["get"])
        assert admin_op["security"] == [{"http_bearer_jwt": []}]
    finally:
        await svc.shutdown()


async def test_go_openapi_portal_proxies_python_control_document(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await connector_for(
        conformance_env,
        "py-openapi-portal.example",
        version=3,
        description="Portal aggregation target",
    )

    @svc.function(route="/echo", method="POST", description="Echo a name")
    async def echo(inp: EchoIn) -> EchoOut:
        return EchoOut(message=inp.name)

    await svc.startup()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{conformance_env.sidecar_url}/openapi.json",
                params={"hostname": "py-openapi-portal.example"},
            )
        assert resp.status_code == 200
        doc = cast("JsonObject", resp.json())
        info = _as_object(doc["info"])
        paths = _as_object(doc["paths"])
        assert info["title"] == "py-openapi-portal.example"
        assert info["version"] == "3"
        assert "/py-openapi-portal.example:443/echo" in paths
    finally:
        await svc.shutdown()


async def test_go_openapi_portal_aggregate_includes_python_control_document(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await connector_for(
        conformance_env,
        "py-openapi-aggregate.example",
        version=5,
        description="Portal aggregate target",
    )

    @svc.function(route="/echo", method="POST", description="Echo a name")
    async def echo(inp: EchoIn) -> EchoOut:
        return EchoOut(message=inp.name)

    await svc.startup()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{conformance_env.sidecar_url}/openapi.json")
        assert resp.status_code == 200
        doc = cast("JsonObject", resp.json())
        paths = _as_object(doc["paths"])
        assert "/py-openapi-aggregate.example:443/echo" in paths
        tags = doc["tags"]
        assert isinstance(tags, list)
        assert {
            "name": "py-openapi-aggregate.example",
            "description": "Portal aggregate target",
        } in tags
    finally:
        await svc.shutdown()


async def _fetch_doc(
    svc: Connector,
    *,
    actor_token: str | None = None,
) -> JsonObject:
    wire = await svc.request(
        method="GET",
        url="https://py-openapi.example:888/openapi.json",
        actor_token=actor_token,
        timeout=5.0,
    )
    resp = decode_response(wire)
    assert resp.status_code == 200
    assert dict(resp.headers)["Cache-Control"] == "private, no-store"
    doc = json.loads(resp.body)
    assert isinstance(doc, dict)
    return cast("JsonObject", doc)


def _as_object(value: object) -> JsonObject:
    assert isinstance(value, dict)
    return cast("JsonObject", value)
