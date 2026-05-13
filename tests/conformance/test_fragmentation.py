"""Large-body cross-language round-trip — exercises fragmentation across Go and Python."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from microbus_py.wire.codec import decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


def _payload(size: int) -> bytes:
    seed = b"python-go-fragmentation:"
    return (seed * ((size // len(seed)) + 1))[:size]


async def _client(env: ConformanceEnv) -> Connector:
    svc = await connector_for(env, "py-frag.example")
    await svc.startup()
    return svc


async def test_python_request_larger_than_one_mib_reassembles_on_go_side(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        body = _payload(16 << 20)
        resp_bytes = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=body,
            timeout=10.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == body
    finally:
        await svc.shutdown()


async def test_go_response_padded_to_one_mib_reassembles_on_python_side(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        resp_bytes = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"hello",
            headers=[("Microbus-Test-Pad", str(16 << 20))],
            timeout=10.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body.startswith(b"hello")
        assert len(resp.body) >= 16 << 20
    finally:
        await svc.shutdown()
