"""Baggage propagation across the language boundary.

Python publishes a ``Microbus-Baggage-*`` header with the request; Go sidecar
echoes any baggage header verbatim into the response; Python decodes and
asserts identical key/value pairs survive the round-trip. This is the
correlation-context primitive this SDK relies on for tracing fan-out.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


async def _client(env: ConformanceEnv) -> Connector:
    svc = await connector_for(env, "py-bag.example")
    await svc.startup()
    return svc


async def test_baggage_round_trips_through_go_echo(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        wire = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"x",
            headers=[
                ("Microbus-Baggage-Tenant", "acme"),
                ("Microbus-Baggage-Trace-Id", "00000000000000000000000000000001"),
            ],
            timeout=5.0,
        )
        resp = decode_response(wire)
        assert resp.status_code == 200
        out = {k: v for k, v in resp.headers if k.startswith("Microbus-Baggage-")}
        assert out["Microbus-Baggage-Tenant"] == "acme"
        assert out["Microbus-Baggage-Trace-Id"] == "00000000000000000000000000000001"
    finally:
        await svc.shutdown()


async def test_baggage_with_long_value_round_trips(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        long_val = "a" * 512
        wire = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"y",
            headers=[("Microbus-Baggage-Long", long_val)],
            timeout=5.0,
        )
        resp = decode_response(wire)
        baggage = {k: v for k, v in resp.headers if k.startswith("Microbus-Baggage-")}
        assert baggage["Microbus-Baggage-Long"] == long_val
    finally:
        await svc.shutdown()


async def test_baggage_survives_go_python_go_request_chain(
    conformance_env: ConformanceEnv,
) -> None:
    target = await connector_for(conformance_env, "py-bag-target.example")
    caller = await connector_for(conformance_env, "py-bag-chain.example")

    async def relay(req: HTTPRequest) -> HTTPResponse:
        wire = await target.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=req.body,
            timeout=5.0,
        )
        return decode_response(wire)

    target.subscribe(name="Relay", method="POST", path="/relay", handler=relay)
    await target.startup()
    await caller.startup()
    try:
        wire = await caller.request(
            method="POST",
            url="https://caller.example:443/call-python-baggage",
            body=b"chain",
            headers=[("Microbus-Baggage-Suitcase", "Clothes")],
            timeout=10.0,
        )
        resp = decode_response(wire)
        baggage = {k: v for k, v in resp.headers if k.startswith("Microbus-Baggage-")}
        assert resp.status_code == 200
        assert resp.body == b"chain"
        assert baggage["Microbus-Baggage-Suitcase"] == "Clothes"
    finally:
        await caller.shutdown()
        await target.shutdown()
