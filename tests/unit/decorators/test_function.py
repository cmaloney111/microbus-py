"""Tests for the ``@svc.function`` decorator.

Decorated coroutines accept a Pydantic input model and return a Pydantic
output model. The decorator wraps them as HTTPRequest→HTTPResponse
handlers and registers a subscription on the connector. Argument
serialisation is JSON in both directions.
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel

from microbus_py import Connector, InMemoryBroker
from microbus_py.wire.codec import decode_response


class GreetIn(BaseModel):
    name: str


class GreetOut(BaseModel):
    message: str


@pytest.mark.asyncio
async def test_function_decorator_registers_and_round_trips() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="hello.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    @server.function(route="/greet", method="POST")
    async def greet(inp: GreetIn) -> GreetOut:
        return GreetOut(message=f"Hello {inp.name}")

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://hello.example:443/greet",
            body=GreetIn(name="World").model_dump_json().encode("utf-8"),
            headers=[("Content-Type", "application/json")],
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body == {"message": "Hello World"}
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_function_propagates_handler_exception_as_500() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="bad.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    @server.function(route="/boom", method="POST")
    async def boom(_inp: GreetIn) -> GreetOut:
        raise RuntimeError("nope")

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://bad.example:443/boom",
            body=GreetIn(name="X").model_dump_json().encode("utf-8"),
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 500
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_function_rejects_malformed_json_with_400() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    @server.function(route="/x", method="POST")
    async def x(_inp: GreetIn) -> GreetOut:
        return GreetOut(message="ok")

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://srv.example:443/x",
            body=b"{not json",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 400
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_function_collects_metadata_for_openapi() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="api.example", transport=transport)

    @server.function(route="/greet", method="POST", description="Says hello")
    async def greet(inp: GreetIn) -> GreetOut:
        return GreetOut(message=f"Hi {inp.name}")

    features = server.list_function_features()
    assert any(
        f.route == "/greet"
        and f.method == "POST"
        and f.input_model is GreetIn
        and f.output_model is GreetOut
        and f.description == "Says hello"
        for f in features
    )
