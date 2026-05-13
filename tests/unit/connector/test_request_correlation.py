"""Tests for the fabric-style request/response correlation pattern.

The connector subscribes to its own ``subjectOfResponses(plane, hostname,
instance_id)`` at startup. Requests carry ``Microbus-From-Host`` and
``Microbus-From-Id`` headers; responders publish their ACK and final
response to the caller's response subject. Correlation happens via the
``Microbus-Msg-Id`` header.
"""

from __future__ import annotations

import asyncio

import pytest

from microbus_py.connector.connector import Connector
from microbus_py.frame.headers import H, OpCode
from microbus_py.transport.inmemory import InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


@pytest.mark.asyncio
async def test_ack_arrives_before_final_response() -> None:
    """Subscriber must publish a 202 ACK before the handler runs."""
    transport = InMemoryBroker()
    server = Connector(hostname="slow.example", transport=transport)
    client = Connector(
        hostname="cli.example",
        instance_id="cli-1",
        transport=transport,
    )

    handler_started = asyncio.Event()
    handler_release = asyncio.Event()

    async def slow_handler(req: HTTPRequest) -> HTTPResponse:
        handler_started.set()
        await handler_release.wait()
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"done")

    server.subscribe(name="Slow", method="POST", path="/slow", handler=slow_handler)

    await server.startup()
    await client.startup()
    try:
        request_task = asyncio.create_task(
            client.request(
                method="POST",
                url="https://slow.example:443/slow",
                timeout=5.0,
            )
        )
        # By the time the handler has started, the ACK must already have
        # arrived at the client (dispatcher sends ACK before invoking the
        # handler).
        await asyncio.wait_for(handler_started.wait(), timeout=1.0)
        assert client.acks_received >= 1
        # Release the handler and confirm we get the response.
        handler_release.set()
        resp = decode_response(await request_task)
        assert resp.status_code == 200
        assert resp.body == b"done"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_no_responder_returns_404_ack_timeout() -> None:
    """Request to a host that has no subscribers raises 404 ACK timeout."""
    transport = InMemoryBroker()
    client = Connector(hostname="cli.example", transport=transport)
    await client.startup()
    try:
        with pytest.raises(TimeoutError):
            await client.request(
                method="GET",
                url="https://nobody.example:443/x",
                timeout=0.1,
            )
    finally:
        await client.shutdown()


@pytest.mark.asyncio
async def test_concurrent_requests_correlate_by_msg_id() -> None:
    """Two requests in flight at once must not cross-correlate responses."""
    transport = InMemoryBroker()
    server = Connector(hostname="echo.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def echo(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[],
            body=req.body,
        )

    server.subscribe(name="Echo", method="POST", path="/echo", handler=echo)
    await server.startup()
    await client.startup()
    try:
        results = await asyncio.gather(
            client.request(
                method="POST",
                url="https://echo.example:443/echo",
                body=b"alpha",
                timeout=2.0,
            ),
            client.request(
                method="POST",
                url="https://echo.example:443/echo",
                body=b"beta",
                timeout=2.0,
            ),
            client.request(
                method="POST",
                url="https://echo.example:443/echo",
                body=b"gamma",
                timeout=2.0,
            ),
        )
        bodies = sorted(decode_response(r).body for r in results)
        assert bodies == [b"alpha", b"beta", b"gamma"]
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_response_carries_op_code_res_not_ack() -> None:
    """The final response frame's OpCode is RES; the ACK frame had OpCode ACK."""
    transport = InMemoryBroker()
    server = Connector(hostname="echo.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def echo(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    server.subscribe(name="Echo", method="GET", path="/echo", handler=echo)
    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://echo.example:443/echo",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        op = next((v for n, v in resp.headers if n == H.OP_CODE), None)
        assert op == OpCode.RES.value
    finally:
        await client.shutdown()
        await server.shutdown()
