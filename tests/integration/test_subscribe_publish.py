"""Integration tests — Connector pair over a real NATS server.

Requires: docker-compose nats-server reachable at $NATS_URL (default
nats://127.0.0.1:4222). Marked ``integration`` so they're skipped in the
default unit-only CI job.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from microbus_py.connector.connector import Connector
from microbus_py.transport.nats import NATSTransport
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

pytestmark = pytest.mark.integration


def _payload(size: int) -> bytes:
    seed = b"microbus-py-fragmentation:"
    return (seed * ((size // len(seed)) + 1))[:size]


@pytest.mark.asyncio
async def test_python_to_python_function_call_over_nats(nats_url: str) -> None:
    server_transport = NATSTransport()
    client_transport = NATSTransport()

    server = Connector(hostname="echo.example", transport=server_transport)
    client = Connector(hostname="cli.example", transport=client_transport)

    async def echo(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "text/plain")],
            body=req.body,
        )

    server.subscribe(name="Echo", method="POST", path="/echo", handler=echo)

    await server_transport.connect(nats_url, name="echo.example")
    await client_transport.connect(nats_url, name="cli.example")
    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://echo.example:443/echo",
            body=b"hello",
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b"hello"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_queue_group_load_balances_across_replicas(nats_url: str) -> None:
    """Two replicas in the same queue group → only one handles each request."""
    counts = {"a": 0, "b": 0}

    server_a = Connector(hostname="worker.example", transport=NATSTransport())
    server_b = Connector(hostname="worker.example", transport=NATSTransport())
    client = Connector(hostname="cli.example", transport=NATSTransport())

    def make_handler(label: str) -> Callable[[HTTPRequest], Awaitable[HTTPResponse]]:
        async def handler(_req: HTTPRequest) -> HTTPResponse:
            counts[label] += 1
            return HTTPResponse(status_code=200, reason="OK", headers=[], body=label.encode())

        return handler

    server_a.subscribe(name="Work", method="POST", path="/work", handler=make_handler("a"))
    server_b.subscribe(name="Work", method="POST", path="/work", handler=make_handler("b"))

    await server_a.transport.connect(nats_url, name="worker-a")
    await server_b.transport.connect(nats_url, name="worker-b")
    await client.transport.connect(nats_url, name="cli")
    await server_a.startup()
    await server_b.startup()
    await client.startup()
    try:
        for _ in range(20):
            await client.request(
                method="POST",
                url="https://worker.example:443/work",
                timeout=5.0,
            )
        assert counts["a"] + counts["b"] == 20
        # Either replica may be quicker, but both should handle some
        assert counts["a"] >= 1
        assert counts["b"] >= 1
    finally:
        await client.shutdown()
        await server_a.shutdown()
        await server_b.shutdown()


@pytest.mark.asyncio
async def test_request_to_unknown_host_times_out(nats_url: str) -> None:
    client = Connector(hostname="cli.example", transport=NATSTransport())
    await client.transport.connect(nats_url, name="cli")
    await client.startup()
    try:
        with pytest.raises((TimeoutError, Exception)):
            await client.request(
                method="GET",
                url="https://nonexistent.example:443/x",
                timeout=0.5,
            )
    finally:
        await client.shutdown()


@pytest.mark.asyncio
async def test_required_claims_enforced_over_real_nats(nats_url: str) -> None:
    server = Connector(hostname="adm.example", transport=NATSTransport())
    client = Connector(hostname="cli.example", transport=NATSTransport())

    async def handler(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")

    server.subscribe(
        name="Admin",
        method="GET",
        path="/admin",
        handler=handler,
        required_claims="roles.admin",
    )

    await server.transport.connect(nats_url, name="adm")
    await client.transport.connect(nats_url, name="cli")
    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://adm.example:443/admin",
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        # Missing actor → 401 (per fabric semantics).
        assert resp.status_code == 401
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.parametrize("size", [1 << 20, 4 << 20, 16 << 20])
@pytest.mark.asyncio
async def test_large_python_to_python_body_round_trips_over_real_nats(
    nats_url: str,
    size: int,
) -> None:
    server = Connector(hostname="large.example", transport=NATSTransport())
    client = Connector(hostname="cli.example", transport=NATSTransport())
    received: list[int] = []

    async def echo(req: HTTPRequest) -> HTTPResponse:
        received.append(len(req.body))
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=req.body)

    server.subscribe(name="Large", method="POST", path="/large", handler=echo)

    await server.transport.connect(nats_url, name="large")
    await client.transport.connect(nats_url, name="cli")
    await server.startup()
    await client.startup()
    try:
        body = _payload(size)
        resp_bytes = await client.request(
            method="POST",
            url="https://large.example:443/large",
            body=body,
            timeout=20.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert received == [size]
        assert resp.body == body
    finally:
        await client.shutdown()
        await server.shutdown()
