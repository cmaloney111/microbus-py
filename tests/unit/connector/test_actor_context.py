"""Handler-side actor accessor: ``Connector.actor_for(req)`` reads parsed claims."""

from __future__ import annotations

import jwt as pyjwt
import pytest

from microbus_py import Connector, InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


@pytest.mark.asyncio
async def test_actor_claims_visible_to_handler() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    seen: dict[str, object] = {}

    async def handler(req: HTTPRequest) -> HTTPResponse:
        actor = server.actor_for(req)
        seen["claims"] = dict(actor.claims) if actor else None
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        token = pyjwt.encode({"sub": "u", "role": "admin"}, key="", algorithm="none")
        wire = await client.request(
            method="POST",
            url="https://srv.example:443/x",
            actor_token=token,
            timeout=2.0,
        )
        resp = decode_response(wire)
        assert resp.status_code == 200
        assert seen["claims"] == {"sub": "u", "role": "admin"}
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_actor_for_returns_none_when_no_actor_header() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv2.example", transport=transport)
    client = Connector(hostname="cli2.example", transport=transport)

    seen: dict[str, object] = {}

    async def handler(req: HTTPRequest) -> HTTPResponse:
        seen["actor"] = server.actor_for(req)
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        await client.request(method="POST", url="https://srv2.example:443/x", timeout=2.0)
        assert seen["actor"] is None
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_current_actor_visible_to_handler_without_req_arg() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv3.example", transport=transport)
    client = Connector(hostname="cli3.example", transport=transport)

    seen: dict[str, object] = {}

    async def handler(req: HTTPRequest) -> HTTPResponse:
        actor = server.current_actor()
        seen["claims"] = dict(actor.claims) if actor else None
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        token = pyjwt.encode({"sub": "u-current", "role": "admin"}, key="", algorithm="none")
        await client.request(
            method="POST",
            url="https://srv3.example:443/x",
            actor_token=token,
            timeout=2.0,
        )
        assert seen["claims"] == {"sub": "u-current", "role": "admin"}
    finally:
        await client.shutdown()
        await server.shutdown()


def test_current_actor_returns_none_outside_request_context() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv4.example", transport=transport)
    assert server.current_actor() is None
