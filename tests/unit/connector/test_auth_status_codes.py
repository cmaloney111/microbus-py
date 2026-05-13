"""Tests for 401-vs-403 split in the dispatcher.

Per fabric semantics:
* Missing or invalid actor JWT → 401 Unauthorized.
* Valid actor whose claims do not satisfy ``required_claims`` → 403 Forbidden.
"""

from __future__ import annotations

import base64
import json

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from microbus_py.connector.connector import Connector
from microbus_py.transport.inmemory import InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


async def _handler(_req: HTTPRequest) -> HTTPResponse:
    return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")


def _build_signed_token(
    claims: dict[str, object],
    *,
    issuer: str = "https://access.token.core",
) -> tuple[str, Ed25519PublicKey]:
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    merged_claims = {
        "iss": issuer,
        "microbus": "1",
        **claims,
    }
    token = pyjwt.encode(
        merged_claims,
        key=private_pem,
        algorithm="EdDSA",
        headers={"kid": "default"},
    )
    return token, private_key.public_key()


async def _make_pair() -> tuple[Connector, Connector]:
    transport = InMemoryBroker()
    server = Connector(hostname="adm.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)
    server.subscribe(
        name="Admin",
        method="GET",
        path="/admin",
        handler=_handler,
        required_claims="roles.admin",
    )
    await server.startup()
    await client.startup()
    return server, client


@pytest.mark.asyncio
async def test_missing_actor_returns_401() -> None:
    server, client = await _make_pair()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://adm.example:443/admin",
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 401
        assert resp.reason == "Unauthorized"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_invalid_actor_returns_401() -> None:
    server, client = await _make_pair()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://adm.example:443/admin",
            actor_token="not.a.valid.jwt",
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 401
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_insufficient_claims_returns_403() -> None:
    server, client = await _make_pair()
    try:
        token = pyjwt.encode({"sub": "u", "roles": ["user"]}, key="", algorithm="none")
        resp_bytes = await client.request(
            method="GET",
            url="https://adm.example:443/admin",
            actor_token=token,
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 403
        assert resp.reason == "Forbidden"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_satisfied_claims_returns_200() -> None:
    server, client = await _make_pair()
    try:
        token = pyjwt.encode(
            {"sub": "u", "roles": ["admin", "user"]},
            key="",
            algorithm="none",
        )
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
async def test_signed_actor_satisfied_claims_returns_200_in_verified_deployment() -> None:
    transport = InMemoryBroker()
    token, public_key = _build_signed_token({"sub": "u", "roles": ["admin", "user"]})
    server = Connector(
        hostname="signed.example",
        transport=transport,
        deployment="LOCAL",
        actor_public_keys={"default": public_key},
    )
    client = Connector(hostname="cli.example", transport=transport)
    server.subscribe(
        name="Admin",
        method="GET",
        path="/admin",
        handler=_handler,
        required_claims="roles.admin",
    )
    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://signed.example:443/admin",
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
async def test_signed_actor_with_wrong_key_returns_401_in_verified_deployment() -> None:
    transport = InMemoryBroker()
    token, _public_key = _build_signed_token({"sub": "u", "roles": ["admin", "user"]})
    wrong_public_key = Ed25519PrivateKey.generate().public_key()
    server = Connector(
        hostname="wrong-key.example",
        transport=transport,
        deployment="LOCAL",
        actor_public_keys={"default": wrong_public_key},
    )
    client = Connector(hostname="cli.example", transport=transport)
    server.subscribe(
        name="Admin",
        method="GET",
        path="/admin",
        handler=_handler,
        required_claims="roles.admin",
    )
    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://wrong-key.example:443/admin",
            actor_token=token,
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 401
        assert resp.reason == "Unauthorized"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_signed_actor_unknown_key_refreshes_jwks() -> None:
    transport = InMemoryBroker()
    token, public_key = _build_signed_token({"sub": "u", "roles": ["admin", "user"]})
    issuer = Connector(hostname="access.token.core", transport=transport)
    server = Connector(
        hostname="refresh-key.example",
        transport=transport,
        deployment="LOCAL",
        trusted_issuer_hosts=["access.token.core"],
    )
    client = Connector(hostname="cli.example", transport=transport)
    encoded_key = (
        base64.urlsafe_b64encode(public_key.public_bytes_raw()).decode("ascii").rstrip("=")
    )
    jwk = {
        "kty": "OKP",
        "crv": "Ed25519",
        "kid": "default",
        "x": encoded_key,
    }

    async def jwks(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "application/json")],
            body=json.dumps({"keys": [jwk]}, separators=(",", ":")).encode("utf-8"),
        )

    issuer.subscribe(name="JWKS", method="GET", path="/jwks", handler=jwks, port=888)
    server.subscribe(
        name="Admin",
        method="GET",
        path="/admin",
        handler=_handler,
        required_claims="roles.admin",
    )
    await issuer.startup()
    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://refresh-key.example:443/admin",
            actor_token=token,
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b"ok"
    finally:
        await client.shutdown()
        await server.shutdown()
        await issuer.shutdown()


@pytest.mark.asyncio
async def test_signed_actor_bad_known_kid_does_not_refresh_jwks() -> None:
    transport = InMemoryBroker()
    good_token, good_public_key = _build_signed_token({"sub": "u", "roles": ["admin", "user"]})
    evil_token, evil_public_key = _build_signed_token(
        {"sub": "attacker", "roles": ["admin", "user"]},
        issuer="https://evil.issuer",
    )
    issuer = Connector(hostname="evil.issuer", transport=transport)
    server = Connector(
        hostname="poison-key.example",
        transport=transport,
        deployment="LOCAL",
        actor_public_keys={"default": good_public_key},
    )
    client = Connector(hostname="cli.example", transport=transport)
    jwks_hits = 0
    encoded_evil_key = (
        base64.urlsafe_b64encode(evil_public_key.public_bytes_raw()).decode("ascii").rstrip("=")
    )
    evil_jwk = {
        "kty": "OKP",
        "crv": "Ed25519",
        "kid": "default",
        "x": encoded_evil_key,
    }

    async def jwks(_req: HTTPRequest) -> HTTPResponse:
        nonlocal jwks_hits
        jwks_hits += 1
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "application/json")],
            body=json.dumps({"keys": [evil_jwk]}, separators=(",", ":")).encode("utf-8"),
        )

    issuer.subscribe(name="JWKS", method="GET", path="/jwks", handler=jwks, port=888)
    server.subscribe(
        name="Admin",
        method="GET",
        path="/admin",
        handler=_handler,
        required_claims="roles.admin",
    )
    await issuer.startup()
    await server.startup()
    await client.startup()
    try:
        bad_resp_bytes = await client.request(
            method="GET",
            url="https://poison-key.example:443/admin",
            actor_token=evil_token,
            timeout=1.0,
        )
        bad_resp = decode_response(bad_resp_bytes)
        assert bad_resp.status_code == 401
        assert jwks_hits == 0

        good_resp_bytes = await client.request(
            method="GET",
            url="https://poison-key.example:443/admin",
            actor_token=good_token,
            timeout=1.0,
        )
        good_resp = decode_response(good_resp_bytes)
        assert good_resp.status_code == 200
        assert good_resp.body == b"ok"
    finally:
        await client.shutdown()
        await server.shutdown()
        await issuer.shutdown()
