"""Tests for ``Application`` aggregator — multi-connector lifecycle."""

from __future__ import annotations

import pytest

from microbus_py import Connector, InMemoryBroker
from microbus_py.application import Application
from microbus_py.wire.codec import HTTPRequest, HTTPResponse


@pytest.mark.asyncio
async def test_application_starts_and_stops_all_connectors() -> None:
    transport = InMemoryBroker()
    a = Connector(hostname="a.example", transport=transport)
    b = Connector(hostname="b.example", transport=transport)
    app = Application().add(a).add(b)

    await app.startup()
    try:
        assert a.is_started
        assert b.is_started
    finally:
        await app.shutdown()
    assert not a.is_started
    assert not b.is_started


@pytest.mark.asyncio
async def test_application_stops_all_when_one_fails_to_start() -> None:
    transport = InMemoryBroker()
    a = Connector(hostname="a2.example", transport=transport)
    b = Connector(hostname="b2.example", transport=transport)

    @a.on_startup
    async def boom() -> None:
        raise RuntimeError("startup boom")

    app = Application().add(a).add(b)
    with pytest.raises(RuntimeError, match="startup boom"):
        await app.startup()
    assert not a.is_started
    assert not b.is_started


@pytest.mark.asyncio
async def test_application_stops_started_connectors_when_later_startup_fails() -> None:
    transport = InMemoryBroker()
    a = Connector(hostname="a3.example", transport=transport)
    b = Connector(hostname="b3.example", transport=transport)

    @b.on_startup
    async def boom() -> None:
        raise ValueError("startup boom")

    app = Application().add(a).add(b)
    with pytest.raises(ValueError, match="startup boom"):
        await app.startup()
    assert not a.is_started
    assert not b.is_started


@pytest.mark.asyncio
async def test_application_run_in_test_provides_async_context() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    async def handler(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=req.body)

    server.subscribe(name="Echo", method="POST", path="/echo", handler=handler)

    async with Application().add(server).add(client).run_in_test():
        wire = await client.request(
            method="POST",
            url="https://srv.example:443/echo",
            body=b"hi",
            timeout=2.0,
        )
        assert wire  # responded
    assert not server.is_started
    assert not client.is_started
