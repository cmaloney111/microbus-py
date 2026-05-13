"""Cross-language request/response round-trips against the Go sidecar."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from microbus_py.wire.codec import HTTPResponse, decode_response
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


async def _client(env: ConformanceEnv) -> Connector:
    svc = await connector_for(env, "py-tester.example")
    await svc.startup()
    return svc


async def test_python_caller_to_go_echo_responds_with_body_round_trip(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        resp_bytes = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"alpha",
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b"alpha"
    finally:
        await svc.shutdown()


async def test_python_caller_to_go_echo_handles_empty_body(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        resp_bytes = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"",
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b""
    finally:
        await svc.shutdown()


async def test_python_caller_to_go_echo_round_trips_unicode_body(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        body = "héllo 🌍".encode()
        resp_bytes = await svc.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=body,
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.body == body
    finally:
        await svc.shutdown()


async def test_python_caller_to_unknown_go_host_times_out_via_ack(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            await svc.request(
                method="POST",
                url="https://nonexistent.example:443/anything",
                body=b"",
                timeout=5.0,
            )
        _ = HTTPResponse  # ruff TC001 anchor
    finally:
        await svc.shutdown()
