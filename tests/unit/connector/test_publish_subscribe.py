"""Tests for connector subscribe + publish/request round-trip."""

from __future__ import annotations

import jwt as pyjwt
import pytest

from microbus_py.connector.connector import Connector
from microbus_py.transport.inmemory import InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


@pytest.mark.asyncio
async def test_subscribe_then_request_round_trip() -> None:
    transport = InMemoryBroker()

    server = Connector(hostname="echo.example", transport=transport)
    client = Connector(hostname="client.example", instance_id="cli-1", transport=transport)

    async def echo_handler(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "text/plain")],
            body=req.body,
        )

    server.subscribe(name="Echo", method="POST", path="/echo", handler=echo_handler)

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"ping",
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b"ping"
    finally:
        await client.shutdown()
        await server.shutdown()


# Auth status-code coverage moved to test_auth_status_codes.py
# (missing/invalid actor → 401, insufficient claims → 403).


@pytest.mark.asyncio
async def test_required_claims_satisfied_returns_200() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="adm.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")

    server.subscribe(
        name="Admin",
        method="GET",
        path="/admin",
        handler=handler,
        required_claims="roles.admin",
    )
    await server.startup()
    await client.startup()
    token = pyjwt.encode({"sub": "u", "roles": ["admin", "user"]}, key="", algorithm="none")
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://adm.example:443/admin",
            actor_token=token,
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b"ok"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_subscribe_with_port_prefix_in_route() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="alt.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"alt")

    # Port baked into the route as `:444/path`
    server.subscribe(name="Alt", method="GET", path=":444/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://alt.example:444/x",
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b"alt"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_handler_exception_returns_500() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="bad.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def crashing(_req: HTTPRequest) -> HTTPResponse:
        raise RuntimeError("kaboom")

    server.subscribe(name="Crash", method="POST", path="/crash", handler=crashing)

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://bad.example:443/crash",
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 500
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_non_runtime_handler_exception_returns_500() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="bad.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def crashing(_req: HTTPRequest) -> HTTPResponse:
        raise ValueError("kaboom")

    server.subscribe(name="Crash", method="POST", path="/crash", handler=crashing)

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://bad.example:443/crash",
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 500
    finally:
        await client.shutdown()
        await server.shutdown()
