"""Cross-language ``TracedError`` JSON envelope parity."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from microbus_py.errors.traced import TracedError
from microbus_py.wire.codec import decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


async def _client(env: ConformanceEnv) -> Connector:
    svc = await connector_for(env, "py-err.example")
    await svc.startup()
    return svc


async def test_go_emitted_traced_error_parses_on_python_side(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        resp_bytes = await svc.request(
            method="GET",
            url="https://echo.example:443/force-error",
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 500
        envelope = json.loads(resp.body)
        err = TracedError.from_json(envelope.get("err", envelope))
        assert err.status_code == 500
        assert err.message
        assert err.stack
    finally:
        await svc.shutdown()
