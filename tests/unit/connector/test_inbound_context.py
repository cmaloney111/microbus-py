"""Inbound request context propagated to downstream publishes."""

from __future__ import annotations

import jwt as pyjwt
import pytest

from microbus_py.connector.connector import Connector
from microbus_py.frame.headers import H
from microbus_py.transport.inmemory import InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


@pytest.mark.asyncio
async def test_handler_downstream_request_propagates_inbound_context_headers() -> None:
    transport = InMemoryBroker()
    client = Connector(hostname="client.example", transport=transport)
    middle = Connector(hostname="middle.example", transport=transport)
    downstream = Connector(hostname="downstream.example", transport=transport)
    seen: dict[str, str] = {}

    async def middle_handler(_req: HTTPRequest) -> HTTPResponse:
        wire = await middle.request(
            method="POST",
            url="https://downstream.example:443/seen",
            body=b"relay",
            timeout=1.0,
        )
        return decode_response(wire)

    async def downstream_handler(req: HTTPRequest) -> HTTPResponse:
        seen.update(dict(req.headers))
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=req.body)

    middle.subscribe(name="Middle", method="POST", path="/relay", handler=middle_handler)
    downstream.subscribe(name="Seen", method="POST", path="/seen", handler=downstream_handler)
    await downstream.startup()
    await middle.startup()
    await client.startup()
    actor_token = pyjwt.encode({"sub": "u"}, key="", algorithm="none")
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://middle.example:443/relay",
            body=b"start",
            headers=[
                ("Microbus-Baggage-Tenant", "acme"),
                ("X-Forwarded-For", "203.0.113.9"),
                ("Accept-Language", "en-US"),
                (H.CLOCK_SHIFT, "125ms"),
            ],
            actor_token=actor_token,
            timeout=1.0,
        )
        resp = decode_response(resp_bytes)
        frame_headers = [name for name, _value in resp.headers if name == H.MSG_ID]
        assert resp.status_code == 200
        assert resp.body == b"relay"
        assert len(frame_headers) == 1
        assert seen["Microbus-Baggage-Tenant"] == "acme"
        assert seen["X-Forwarded-For"] == "203.0.113.9"
        assert seen["Accept-Language"] == "en-US"
        assert seen[H.CLOCK_SHIFT] == "125ms"
        assert seen[H.ACTOR] == actor_token
    finally:
        await client.shutdown()
        await middle.shutdown()
        await downstream.shutdown()


@pytest.mark.asyncio
async def test_explicit_downstream_headers_override_propagated_context() -> None:
    transport = InMemoryBroker()
    client = Connector(hostname="client.example", transport=transport)
    middle = Connector(hostname="override.example", transport=transport)
    downstream = Connector(hostname="downstream.example", transport=transport)
    seen: list[tuple[str, str]] = []

    async def middle_handler(_req: HTTPRequest) -> HTTPResponse:
        override_actor = pyjwt.encode({"sub": "override"}, key="", algorithm="none")
        wire = await middle.request(
            method="POST",
            url="https://downstream.example:443/seen",
            headers=[
                ("Microbus-Baggage-Tenant", "override"),
                ("Accept-Language", "fr-FR"),
            ],
            actor_token=override_actor,
            timeout=1.0,
        )
        return decode_response(wire)

    async def downstream_handler(req: HTTPRequest) -> HTTPResponse:
        seen.extend(req.headers)
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"ok")

    middle.subscribe(name="Middle", method="POST", path="/relay", handler=middle_handler)
    downstream.subscribe(name="Seen", method="POST", path="/seen", handler=downstream_handler)
    await downstream.startup()
    await middle.startup()
    await client.startup()
    inbound_actor = pyjwt.encode({"sub": "inbound"}, key="", algorithm="none")
    try:
        await client.request(
            method="POST",
            url="https://override.example:443/relay",
            headers=[
                ("Microbus-Baggage-Tenant", "inbound"),
                ("Accept-Language", "en-US"),
            ],
            actor_token=inbound_actor,
            timeout=1.0,
        )
        names = [name for name, _value in seen]
        headers = dict(seen)
        assert names.count("Microbus-Baggage-Tenant") == 1
        assert names.count("Accept-Language") == 1
        assert names.count(H.ACTOR) == 1
        assert headers["Microbus-Baggage-Tenant"] == "override"
        assert headers["Accept-Language"] == "fr-FR"
        assert pyjwt.decode(headers[H.ACTOR], options={"verify_signature": False}) == {
            "sub": "override"
        }
    finally:
        await client.shutdown()
        await middle.shutdown()
        await downstream.shutdown()
