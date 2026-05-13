"""Tests for openapi/builder.py — OpenAPI 3.1 doc emission."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from microbus_py import Connector, InMemoryBroker
from microbus_py.openapi.builder import build_openapi
from microbus_py.workflow import Graph

if TYPE_CHECKING:
    from microbus_py.wire.codec import HTTPRequest, HTTPResponse


class EchoIn(BaseModel):
    name: str


class EchoOut(BaseModel):
    message: str


class InnerPayload(BaseModel):
    value: int


class OuterPayload(BaseModel):
    inner: InnerPayload


def _make_service() -> Connector:
    transport = InMemoryBroker()
    svc = Connector(hostname="echo.example", transport=transport, version=3)

    @svc.function(route="/echo", method="POST", description="Echo a name")
    async def echo(inp: EchoIn) -> EchoOut:
        return EchoOut(message=inp.name)

    return svc


class TestOpenAPIDocument:
    def test_top_level_metadata(self) -> None:
        doc = build_openapi(_make_service())
        assert doc["openapi"].startswith("3.1")
        assert doc["info"]["title"] == "echo.example"
        assert doc["info"]["version"] == "3"

    def test_paths_include_decorated_function(self) -> None:
        doc = build_openapi(_make_service())
        assert "/echo.example:443/echo" in doc["paths"]
        assert "/echo.example:888/openapi.json" in doc["paths"]
        op = doc["paths"]["/echo.example:443/echo"]["post"]
        assert op["description"] == "Echo a name"
        assert op["x-feature-type"] == "function"
        assert op["x-name"] == "echo"

    def test_host_overridden_function_uses_route_host(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="base.example", transport=transport)

        @svc.function(route="//external.example:7443/echo", method="POST")
        async def echo(inp: EchoIn) -> EchoOut:
            return EchoOut(message=inp.name)

        doc = build_openapi(svc)
        assert "/external.example:7443/echo" in doc["paths"]
        assert "/base.example:7443/echo" not in doc["paths"]

    def test_host_overridden_web_endpoint_uses_route_host(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="base.example", transport=transport)

        @svc.web(route="//external.example/admin", method="GET")
        async def admin(_req: HTTPRequest) -> HTTPResponse:
            raise AssertionError("OpenAPI generation should not call handlers")

        doc = build_openapi(svc)
        assert "/external.example:443/admin" in doc["paths"]
        assert "/base.example:443/admin" not in doc["paths"]

    def test_request_body_schema_references_pydantic_model(self) -> None:
        doc = build_openapi(_make_service())
        op = doc["paths"]["/echo.example:443/echo"]["post"]
        body = op["requestBody"]["content"]["application/json"]["schema"]
        resolved = _resolve(doc, body)
        props = resolved.get("properties", {}) if isinstance(resolved, dict) else {}
        assert isinstance(props, dict)
        assert "name" in props

    def test_response_schema_references_pydantic_model(self) -> None:
        doc = build_openapi(_make_service())
        op = doc["paths"]["/echo.example:443/echo"]["post"]
        resp = op["responses"]["2XX"]["content"]["application/json"]["schema"]
        resolved = _resolve(doc, resp)
        props = resolved.get("properties", {}) if isinstance(resolved, dict) else {}
        assert isinstance(props, dict)
        assert "message" in props

    def test_nested_pydantic_defs_are_promoted_to_components(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="nested.example", transport=transport)

        @svc.function(route="/nested", method="POST")
        async def nested(inp: OuterPayload) -> OuterPayload:
            return inp

        doc = build_openapi(svc)

        schemas = doc["components"]["schemas"]
        assert "error_ErrorResponse" in schemas
        assert "nested_example__nested_IN" in schemas
        assert "nested_example__nested_IN_InnerPayload" in schemas
        assert "OuterPayload" not in schemas
        assert "InnerPayload" not in schemas

    def test_component_names_are_endpoint_scoped(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="schema.example", transport=transport)

        @svc.function(route="/one", method="POST")
        async def one(inp: EchoIn) -> EchoOut:
            return EchoOut(message=inp.name)

        @svc.function(route="/two", method="POST")
        async def two(inp: EchoIn) -> EchoOut:
            return EchoOut(message=inp.name)

        doc = build_openapi(svc)
        schemas = doc["components"]["schemas"]
        assert "schema_example__one_IN" in schemas
        assert "schema_example__one_OUT" in schemas
        assert "schema_example__two_IN" in schemas
        assert "schema_example__two_OUT" in schemas
        assert "EchoIn" not in schemas
        assert "EchoOut" not in schemas

        one_schema = doc["paths"]["/schema.example:443/one"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        two_schema = doc["paths"]["/schema.example:443/two"]["post"]["requestBody"]["content"][
            "application/json"
        ]["schema"]
        assert one_schema == {"$ref": "#/components/schemas/schema_example__one_IN"}
        assert two_schema == {"$ref": "#/components/schemas/schema_example__two_IN"}

    def test_web_endpoint_is_included_without_json_body_schema(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="web.example", transport=transport)

        @svc.web(route="/health", method="GET", description="Health check")
        async def health(_req: HTTPRequest) -> HTTPResponse:
            raise AssertionError("OpenAPI generation should not call handlers")

        doc = build_openapi(svc)
        op = doc["paths"]["/web.example:443/health"]["get"]
        assert op["summary"] == "health"
        assert op["description"] == "Health check"
        assert op["x-feature-type"] == "web"
        assert op["x-name"] == "health"
        assert op["responses"]["2XX"] == {"description": "OK"}

    def test_workflow_endpoint_is_included_as_graph_feature(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="workflow.example", transport=transport)

        @svc.workflow(route="/graphs/demo", description="Demo graph")
        def demo() -> Graph:
            return Graph("demo")

        doc = build_openapi(svc)
        op = doc["paths"]["/workflow.example:428/graphs/demo"]["get"]
        assert op["summary"] == "demo"
        assert op["description"] == "Demo graph"
        assert op["x-feature-type"] == "workflow"
        assert op["x-name"] == "demo"


class TestClaimFiltering:
    def test_admin_only_endpoints_hidden_from_unprivileged_caller(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="api.example", transport=transport)

        @svc.function(route="/public", method="POST")
        async def public(inp: EchoIn) -> EchoOut:
            return EchoOut(message=inp.name)

        @svc.function(route="/admin", method="POST", required_claims="roles.admin")
        async def admin(inp: EchoIn) -> EchoOut:
            return EchoOut(message=inp.name)

        public_doc = build_openapi(svc, claims={"sub": "u"})
        assert "/api.example:443/public" in public_doc["paths"]
        assert "/api.example:443/admin" not in public_doc["paths"]

        admin_doc = build_openapi(svc, claims={"sub": "u", "roles": ["admin"]})
        assert "/api.example:443/admin" in admin_doc["paths"]

    def test_claim_filtering_applies_to_web_endpoints_and_marks_security(self) -> None:
        transport = InMemoryBroker()
        svc = Connector(hostname="api.example", transport=transport)

        @svc.web(route="/admin-page", method="GET", required_claims="roles.admin")
        async def admin_page(_req: HTTPRequest) -> HTTPResponse:
            raise AssertionError("OpenAPI generation should not call handlers")

        public_doc = build_openapi(svc, claims={"sub": "u"})
        assert "/api.example:443/admin-page" not in public_doc["paths"]

        admin_doc = build_openapi(svc, claims={"sub": "u", "roles": ["admin"]})
        op = admin_doc["paths"]["/api.example:443/admin-page"]["get"]
        assert op["security"] == [{"http_bearer_jwt": []}]
        schemes = admin_doc["components"]["securitySchemes"]
        assert schemes["http_bearer_jwt"]["scheme"] == "bearer"


def _resolve(doc: dict[str, object], ref: dict[str, object]) -> dict[str, object]:
    """Resolve a ``$ref`` JSON pointer into the embedded definition."""
    pointer_value = ref.get("$ref", "")
    pointer = pointer_value if isinstance(pointer_value, str) else ""
    if not pointer.startswith("#/"):
        return ref
    cur: object = doc
    for seg in pointer[2:].split("/"):
        assert isinstance(cur, dict)
        cur = cur[seg]
    assert isinstance(cur, dict)
    return cur
