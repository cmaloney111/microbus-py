"""Distributed cache interop against the Go sidecar."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx
import pytest

from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

_CACHE_HOST = "cache-peer.example"


async def _cache_peer(env: ConformanceEnv) -> Connector:
    svc = await connector_for(env, _CACHE_HOST)
    await svc.startup()
    return svc


async def test_python_and_go_distrib_cache_load_each_other(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _cache_peer(conformance_env)
    try:
        await svc.distrib_cache.store("from-python", b"py-value")

        async with httpx.AsyncClient(timeout=10.0) as client:
            loaded = await client.get(
                f"{conformance_env.sidecar_url}/{_CACHE_HOST}/load",
                params={"key": "from-python"},
            )
            stored = await client.put(
                f"{conformance_env.sidecar_url}/{_CACHE_HOST}/store",
                params={"key": "from-go"},
                content=b"go-value",
            )

        assert loaded.status_code == 200
        assert loaded.content == b"py-value"
        assert stored.status_code == 204
        assert await svc.distrib_cache.load("from-go") == b"go-value"
    finally:
        await svc.shutdown()


async def test_python_shutdown_rescues_cache_entries_to_go_peer(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _cache_peer(conformance_env)
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            await svc.distrib_cache.store("rescued", b"saved")

            await svc.shutdown()

            loaded = await client.get(
                f"{conformance_env.sidecar_url}/{_CACHE_HOST}/load",
                params={"key": "rescued"},
            )
            assert loaded.status_code == 200
            assert loaded.content == b"saved"
        finally:
            await svc.shutdown()
