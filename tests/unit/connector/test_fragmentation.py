"""Connector-level request and response fragmentation."""

from __future__ import annotations

import time

import pytest

from microbus_py.connector.connector import (
    Connector,
    _fragment_request,
    _fragment_response,
    _integrated_headers,
    _RequestFragments,
    _ResponseFragments,
)
from microbus_py.frame.headers import H
from microbus_py.transport.inmemory import InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


def _payload(size: int) -> bytes:
    seed = b"fragmented-connector:"
    return (seed * ((size // len(seed)) + 1))[:size]


@pytest.mark.asyncio
async def test_large_request_and_response_round_trip_in_memory() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="frag.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)
    received: list[bytes] = []

    async def echo(req: HTTPRequest) -> HTTPResponse:
        received.append(req.body)
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=req.body)

    server.subscribe(name="Echo", method="POST", path="/echo", handler=echo)
    await server.startup()
    await client.startup()
    try:
        body = _payload((2 << 20) + 17)
        resp_bytes = await client.request(
            method="POST",
            url="https://frag.example:443/echo",
            body=body,
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert received == [body]
        assert resp.body == body
        assert client.acks_received >= 1
    finally:
        await client.shutdown()
        await server.shutdown()


class _TinyPayloadBroker(InMemoryBroker):
    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    def max_payload(self) -> int:
        return 1024

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_startup_rejects_transport_payload_too_small_for_headers() -> None:
    transport = _TinyPayloadBroker()
    svc = Connector(hostname="tiny.example", transport=transport)

    with pytest.raises(RuntimeError, match="payload"):
        await svc.startup()
    assert transport.closed


def test_single_fragment_helpers_return_original_message() -> None:
    req = HTTPRequest(method="GET", url="https://x.example/a", headers=[], body=b"abc")
    resp = HTTPResponse(status_code=204, reason="No Content", headers=[], body=b"")

    assert _fragment_request(req, max_size=1024) == [req]
    assert _fragment_response(resp, max_size=1024) == [resp]
    assert _integrated_headers([(H.FRAGMENT, "1/2"), ("Content-Length", "3")], 0) == []


def test_fragment_collectors_reject_total_mismatch() -> None:
    req_state = _RequestFragments(total=2)
    resp_state = _ResponseFragments(total=2)

    with pytest.raises(ValueError, match="request fragment"):
        req_state.add(
            HTTPRequest(
                method="POST",
                url="https://x.example/a",
                headers=[(H.FRAGMENT, "1/3")],
                body=b"a",
            )
        )

    with pytest.raises(ValueError, match="response fragment"):
        resp_state.add(
            HTTPResponse(
                status_code=200,
                reason="OK",
                headers=[(H.FRAGMENT, "1/3")],
                body=b"a",
            )
        )


def test_fragment_collectors_expire_after_deadline() -> None:
    expired_start = time.monotonic() - 1.0
    req_state = _RequestFragments(
        total=2,
        deadline_seconds=0.01,
        started_at=expired_start,
    )
    resp_state = _ResponseFragments(
        total=2,
        deadline_seconds=0.01,
        started_at=expired_start,
    )

    req_state.add(
        HTTPRequest(
            method="POST",
            url="https://x.example/a",
            headers=[(H.FRAGMENT, "1/2")],
            body=b"a",
        )
    )
    resp_state.add(
        HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[(H.FRAGMENT, "1/2")],
            body=b"a",
        )
    )

    assert req_state.expired()
    assert resp_state.expired()


def test_connector_prunes_expired_fragment_collectors() -> None:
    svc = Connector(hostname="frag.example", transport=InMemoryBroker())
    fresh_req = _RequestFragments(total=2, deadline_seconds=60.0)
    stale_req = _RequestFragments(total=2, deadline_seconds=0.0)
    fresh_resp = _ResponseFragments(total=2, deadline_seconds=60.0)
    stale_resp = _ResponseFragments(total=2, deadline_seconds=0.0)

    svc._request_fragments["fresh"] = fresh_req
    svc._request_fragments["stale"] = stale_req
    svc._response_fragments["fresh"] = fresh_resp
    svc._response_fragments["stale"] = stale_resp

    svc._drop_expired_fragments()

    assert svc._request_fragments == {"fresh": fresh_req}
    assert svc._response_fragments == {"fresh": fresh_resp}
