"""Tests for the ``@svc.web`` decorator (raw HTTP handlers)."""

from __future__ import annotations

import pytest

from microbus_py import Connector, InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


@pytest.mark.asyncio
async def test_web_decorator_round_trip() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    @server.web(route="/raw", method="GET")
    async def raw(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "text/plain")],
            body=b"raw " + req.method.encode(),
        )

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://srv.example:443/raw",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert resp.body == b"raw GET"
    finally:
        await client.shutdown()
        await server.shutdown()
