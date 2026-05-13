"""Auth conformance across Go and Python services."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import jwt as pyjwt
import pytest
from pydantic import BaseModel

from microbus_py.wire.codec import decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


class SecureIn(BaseModel):
    value: str = ""


class SecureOut(BaseModel):
    ok: bool
    value: str = ""


async def _client(env: ConformanceEnv) -> Connector:
    svc = await connector_for(env, "py-auth.example")
    await svc.startup()
    return svc


async def test_unauth_request_to_go_secured_returns_401(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        resp_bytes = await svc.request(
            method="POST",
            url="https://secured.example:443/admin",
            body=b"{}",
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 401
    finally:
        await svc.shutdown()


async def test_invalid_role_to_go_secured_returns_403(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        token = pyjwt.encode({"sub": "u", "role": "user"}, key="", algorithm="none")
        resp_bytes = await svc.request(
            method="POST",
            url="https://secured.example:443/admin",
            body=b"{}",
            actor_token=token,
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 403
    finally:
        await svc.shutdown()


async def test_admin_role_to_go_secured_returns_200(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        token = pyjwt.encode({"sub": "u", "role": "admin"}, key="", algorithm="none")
        resp_bytes = await svc.request(
            method="POST",
            url="https://secured.example:443/admin",
            body=b"{}",
            actor_token=token,
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert b"ok" in resp.body
    finally:
        await svc.shutdown()


async def test_go_access_token_jwks_authorizes_python_required_claims(
    conformance_env: ConformanceEnv,
) -> None:
    caller = await connector_for(conformance_env, "py-access-token-client.example")
    secured = await connector_for(
        conformance_env,
        "py-access-token-secured.example",
        deployment="PROD",
        trusted_issuer_hosts=["access.token.core"],
    )

    @secured.function(route="/admin", method="POST", required_claims='role=="admin"')
    async def admin(inp: SecureIn) -> SecureOut:
        return SecureOut(ok=True, value=inp.value)

    await caller.startup()
    await secured.startup()
    try:
        mint_wire = await caller.request(
            method="POST",
            url="https://access.token.core:444/mint",
            body=json.dumps({"claims": {"sub": "u", "role": "admin"}}).encode("utf-8"),
            headers=[("Content-Type", "application/json")],
            timeout=5.0,
        )
        mint_resp = decode_response(mint_wire)
        assert mint_resp.status_code == 200
        token = json.loads(mint_resp.body)["token"]
        assert isinstance(token, str)

        resp_wire = await caller.request(
            method="POST",
            url="https://py-access-token-secured.example:443/admin",
            body=json.dumps({"value": "signed"}).encode("utf-8"),
            headers=[("Content-Type", "application/json")],
            actor_token=token,
            timeout=5.0,
        )
        resp = decode_response(resp_wire)
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"ok": True, "value": "signed"}
    finally:
        await secured.shutdown()
        await caller.shutdown()
