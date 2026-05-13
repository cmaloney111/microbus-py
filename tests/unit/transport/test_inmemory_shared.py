"""Multiple connectors must share an InMemoryBroker without breaking each other.

Bug being fixed: ``InMemoryBroker.close()`` previously cleared ALL subs +
all per-subject indexes. That meant two connectors sharing a broker would
break the second one's subscriptions when the first one shut down. Each
connector already calls ``unsubscribe()`` for its own subs in
``_unbind_all``; ``close()`` should be a no-op on shared state.
"""

from __future__ import annotations

import pytest

from microbus_py import Connector, InMemoryBroker
from microbus_py.application import Application
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


@pytest.mark.asyncio
async def test_shutdown_one_connector_does_not_break_peer() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv.example", transport=transport)
    client_a = Connector(hostname="cli-a.example", transport=transport)
    client_b = Connector(hostname="cli-b.example", transport=transport)

    async def echo(req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=req.body)

    server.subscribe(name="Echo", method="POST", path="/echo", handler=echo)

    async with Application().add(server).add(client_a).add(client_b).run_in_test():
        wire_a = await client_a.request(
            method="POST",
            url="https://srv.example:443/echo",
            body=b"alpha",
            timeout=2.0,
        )
        assert decode_response(wire_a).body == b"alpha"

        await client_a.shutdown()

        wire_b = await client_b.request(
            method="POST",
            url="https://srv.example:443/echo",
            body=b"beta",
            timeout=2.0,
        )
        assert decode_response(wire_b).body == b"beta"


@pytest.mark.asyncio
async def test_close_is_idempotent() -> None:
    transport = InMemoryBroker()
    await transport.close()
    await transport.close()
    await transport.close()
