"""``Connector.require_actor_sub`` + ``Connector.current_actor_sub`` helpers.

These wrap the canonical ``actor.claims.get("sub")`` extraction so consumers
no longer hand-roll it. ``current_actor_sub`` is non-raising and returns
``None`` for missing/empty sub. ``require_actor_sub`` raises ``Forbidden``
by default, ``Unauthorized`` when ``status=401`` is passed.
"""

from __future__ import annotations

import base64
import json

import pytest

from microbus_py import Connector, InMemoryBroker
from microbus_py.errors.types import Forbidden, Unauthorized
from microbus_py.testing import mint_unsigned_actor
from microbus_py.wire.codec import HTTPRequest, HTTPResponse


def _mint_no_sub() -> str:
    header = (
        base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode())
        .rstrip(b"=")
        .decode()
    )
    payload = (
        base64.urlsafe_b64encode(
            json.dumps({"iss": "auth.example", "iat": 0, "exp": 9999999999}).encode()
        )
        .rstrip(b"=")
        .decode()
    )
    return f"{header}.{payload}."


def test_current_actor_sub_returns_none_when_no_actor_in_context() -> None:
    transport = InMemoryBroker()
    c = Connector(hostname="srv.example", transport=transport)
    assert c.current_actor_sub() is None


@pytest.mark.asyncio
async def test_current_actor_sub_returns_sub_from_actor() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv2.example", transport=transport)
    client = Connector(hostname="cli2.example", transport=transport)

    seen: dict[str, object] = {}

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        seen["sub"] = server.current_actor_sub()
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        token = mint_unsigned_actor(sub="alice")
        await client.request(
            method="POST",
            url="https://srv2.example:443/x",
            actor_token=token,
            timeout=2.0,
        )
        assert seen["sub"] == "alice"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_current_actor_sub_returns_none_when_sub_missing() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv3.example", transport=transport)
    client = Connector(hostname="cli3.example", transport=transport)

    seen: dict[str, object] = {}

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        seen["sub"] = server.current_actor_sub()
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        await client.request(
            method="POST",
            url="https://srv3.example:443/x",
            actor_token=_mint_no_sub(),
            timeout=2.0,
        )
        assert seen["sub"] is None
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_current_actor_sub_returns_none_when_sub_empty_string() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv4.example", transport=transport)
    client = Connector(hostname="cli4.example", transport=transport)

    seen: dict[str, object] = {}

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        seen["sub"] = server.current_actor_sub()
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        token = mint_unsigned_actor(sub="")
        await client.request(
            method="POST",
            url="https://srv4.example:443/x",
            actor_token=token,
            timeout=2.0,
        )
        assert seen["sub"] is None
    finally:
        await client.shutdown()
        await server.shutdown()


def test_require_actor_sub_raises_forbidden_on_no_actor() -> None:
    transport = InMemoryBroker()
    c = Connector(hostname="srv5.example", transport=transport)
    with pytest.raises(Forbidden) as excinfo:
        c.require_actor_sub()
    assert excinfo.value.status_code == 403
    assert "actor" in str(excinfo.value)


@pytest.mark.asyncio
async def test_require_actor_sub_raises_forbidden_on_empty_sub() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv6.example", transport=transport)
    client = Connector(hostname="cli6.example", transport=transport)

    captured: dict[str, BaseException] = {}

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        try:
            server.require_actor_sub()
        except Forbidden as exc:
            captured["err"] = exc
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        token = mint_unsigned_actor(sub="")
        await client.request(
            method="POST",
            url="https://srv6.example:443/x",
            actor_token=token,
            timeout=2.0,
        )
        err = captured["err"]
        assert isinstance(err, Forbidden)
        assert err.status_code == 403
        assert "actor" in str(err)
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_require_actor_sub_returns_sub_on_valid_actor() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv7.example", transport=transport)
    client = Connector(hostname="cli7.example", transport=transport)

    seen: dict[str, object] = {}

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        seen["sub"] = server.require_actor_sub()
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="A", method="POST", path="/x", handler=handler)
    await server.startup()
    await client.startup()
    try:
        token = mint_unsigned_actor(sub="bob")
        await client.request(
            method="POST",
            url="https://srv7.example:443/x",
            actor_token=token,
            timeout=2.0,
        )
        assert seen["sub"] == "bob"
    finally:
        await client.shutdown()
        await server.shutdown()


def test_require_actor_sub_custom_status_raises_unauthorized() -> None:
    transport = InMemoryBroker()
    c = Connector(hostname="srv8.example", transport=transport)
    with pytest.raises(Unauthorized) as excinfo:
        c.require_actor_sub(status=401)
    assert excinfo.value.status_code == 401
    assert not isinstance(excinfo.value, Forbidden)
    assert "actor" in str(excinfo.value)
