"""Go connector publishes to a Python ``@function`` over the shared NATS bus."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import httpx
import pytest
from pydantic import BaseModel

from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


class ReverseIn(BaseModel):
    text: str


class ReverseOut(BaseModel):
    text: str


async def test_go_peer_calls_python_handler(conformance_env: ConformanceEnv) -> None:
    svc = await connector_for(conformance_env, "py-target.example")

    @svc.function(route="/reverse", method="POST")
    async def reverse(inp: ReverseIn) -> ReverseOut:
        return ReverseOut(text=inp.text[::-1])

    await svc.startup()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{conformance_env.sidecar_url}/caller.example/call-python",
                json={"text": "abcdef"},
            )
        assert resp.status_code == 200
        assert json.loads(resp.content) == {"text": "fedcba"}
    finally:
        await svc.shutdown()


async def test_go_peer_propagates_actor_to_python_required_claims(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await connector_for(conformance_env, "py-target.example")

    @svc.function(route="/reverse", method="POST", required_claims="roles.admin")
    async def reverse(inp: ReverseIn) -> ReverseOut:
        return ReverseOut(text=inp.text[::-1])

    await svc.startup()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            missing = await client.post(
                f"{conformance_env.sidecar_url}/caller.example/call-python",
                json={"text": "abcdef"},
            )
            assert missing.status_code == 401

            viewer = await client.post(
                f"{conformance_env.sidecar_url}/caller.example/call-python-viewer",
                json={"text": "abcdef"},
            )
            assert viewer.status_code == 403

            admin = await client.post(
                f"{conformance_env.sidecar_url}/caller.example/call-python-admin",
                json={"text": "abcdef"},
            )
            assert admin.status_code == 200
            assert json.loads(admin.content) == {"text": "fedcba"}
    finally:
        await svc.shutdown()
