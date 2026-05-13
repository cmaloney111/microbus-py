"""Build an OpenAPI 3.1 document from a Connector's registered features.

Pydantic v2's ``model_json_schema`` is used directly so the schemas are
authored where the types are. Endpoints whose ``required_claims`` do not
match the optional ``claims`` argument are filtered out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from microbus_py.claims.evaluator import compile_expr

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic import BaseModel

    from microbus_py.connector.connector import Connector

__all__ = ["build_openapi"]


def build_openapi(
    svc: Connector,
    *,
    claims: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    paths: dict[str, dict[str, Any]] = {}
    components: dict[str, Any] = {"schemas": {}, "securitySchemes": {}}

    for feature in svc.list_function_features():
        if not _claims_satisfy(feature.required_claims, claims):
            continue
        in_schema = _embed_schema(
            feature.input_model,
            components,
            _schema_key(svc, feature.name, "IN"),
        )
        out_schema = _embed_schema(
            feature.output_model,
            components,
            _schema_key(svc, feature.name, "OUT"),
        )
        operation: dict[str, Any] = {
            "summary": feature.name,
            "description": feature.description,
            "x-feature-type": "function",
            "x-name": feature.name,
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": in_schema}},
            },
            "responses": {
                "2XX": {
                    "description": "OK",
                    "content": {"application/json": {"schema": out_schema}},
                },
            },
        }
        _add_error_responses(operation, feature.required_claims, components)
        paths.setdefault(_path_key(svc, feature.port, feature.route, feature.host), {})[
            feature.method.lower()
        ] = operation

    for web in svc.list_web_features():
        if not _claims_satisfy(web.required_claims, claims):
            continue
        web_operation: dict[str, Any] = {
            "summary": web.name,
            "description": web.description,
            "x-feature-type": "web",
            "x-name": web.name,
            "responses": {"2XX": {"description": "OK"}},
        }
        _add_error_responses(web_operation, web.required_claims, components)
        paths.setdefault(_path_key(svc, web.port, web.route, web.host), {})[web.method.lower()] = (
            web_operation
        )

    for workflow in svc.list_workflow_features():
        if not _claims_satisfy(workflow.required_claims, claims):
            continue
        workflow_operation: dict[str, Any] = {
            "summary": workflow.name,
            "description": workflow.description,
            "x-feature-type": "workflow",
            "x-name": workflow.name,
            "responses": {"2XX": {"description": "OK"}},
        }
        _add_error_responses(workflow_operation, workflow.required_claims, components)
        paths.setdefault(_path_key(svc, workflow.port, workflow.route, workflow.host), {})[
            "get"
        ] = workflow_operation

    control_operation: dict[str, Any] = {
        "summary": "OpenAPI",
        "description": "OpenAPI returns the OpenAPI 3.1 document of the microservice.",
        "x-feature-type": "function",
        "x-name": "OpenAPI",
        "responses": {"2XX": {"description": "OK"}},
    }
    _add_error_responses(control_operation, None, components)
    paths.setdefault(_path_key(svc, 888, "/openapi.json", None), {})["get"] = control_operation

    return {
        "openapi": "3.1.0",
        "info": {
            "title": svc.hostname,
            "description": svc.description,
            "version": str(svc.version),
        },
        "servers": [{"url": "https://localhost/"}],
        "paths": paths,
        "components": components,
    }


def _embed_schema(
    model: type[BaseModel],
    components: dict[str, Any],
    key: str,
) -> dict[str, Any]:
    schema = model.model_json_schema(ref_template=f"#/components/schemas/{key}_{{model}}")
    nested = schema.pop("$defs", {})
    for name, sub in nested.items():
        components["schemas"][f"{key}_{name}"] = sub
    components["schemas"][key] = schema
    return {"$ref": f"#/components/schemas/{key}"}


def _schema_key(svc: Connector, name: str, suffix: str) -> str:
    return f"{svc.hostname.replace('.', '_')}__{name}_{suffix}"


def _claims_satisfy(required: str | None, claims: Mapping[str, Any] | None) -> bool:
    if not required:
        return True
    expr = compile_expr(required)
    return expr.evaluate(claims or {})


def _path_key(svc: Connector, port: int, route: str, host: str | None) -> str:
    path = route if route.startswith("/") else f"/{route}"
    return f"/{host or svc.hostname}:{port}{path}"


def _add_error_responses(
    operation: dict[str, Any],
    required_claims: str | None,
    components: dict[str, Any],
) -> None:
    error_ref = _ensure_error_schema(components)
    if required_claims:
        components["securitySchemes"]["http_bearer_jwt"] = {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        }
        operation["security"] = [{"http_bearer_jwt": []}]
        operation["responses"]["401"] = _error_response("Unauthorized", error_ref)
        operation["responses"]["403"] = _error_response(
            f"Forbidden; token required with: {required_claims}",
            error_ref,
        )
    operation["responses"]["4XX"] = _error_response("User error", error_ref)
    operation["responses"]["5XX"] = _error_response("Server error", error_ref)


def _ensure_error_schema(components: dict[str, Any]) -> str:
    name = "error_ErrorResponse"
    components["schemas"].setdefault(
        name,
        {
            "type": "object",
            "properties": {
                "err": {
                    "type": "object",
                    "properties": {
                        "msg": {"type": "string"},
                        "code": {"type": "string"},
                        "stack": {"type": "array", "items": {"type": "object"}},
                    },
                }
            },
        },
    )
    return f"#/components/schemas/{name}"


def _error_response(description: str, schema_ref: str) -> dict[str, Any]:
    return {
        "description": description,
        "content": {"application/json": {"schema": {"$ref": schema_ref}}},
    }
