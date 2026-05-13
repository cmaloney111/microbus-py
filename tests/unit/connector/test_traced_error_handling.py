"""Handler-raised TracedError must surface with its declared status_code.

Today the dispatcher catches every Exception and rewrites the response as
500. Handlers that want to return 400/401/403/404/etc. from a typed
``@function`` are forced to use ``@web`` and emit the wire bytes manually.
This test pins the desired behaviour: a handler raising
``TracedError(status_code=N, message="...")`` should produce a response with
``status_code=N`` and the standard ``{"err":{...}}`` envelope.
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel

from microbus_py import Connector, InMemoryBroker
from microbus_py.application import Application
from microbus_py.errors.traced import TracedError
from microbus_py.wire.codec import decode_response


class _In(BaseModel):
    x: int


class _Out(BaseModel):
    y: int


@pytest.mark.asyncio
async def test_function_handler_traced_error_400_is_surfaced() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)

    @server.function(route="/check", method="POST")
    async def check(_inp: _In) -> _Out:
        raise TracedError("invalid input", status_code=400)

    async with Application().add(server).add(client).run_in_test():
        wire = await client.request(
            method="POST",
            url="https://srv.example:443/check",
            body=_In(x=1).model_dump_json().encode("utf-8"),
            timeout=2.0,
        )

    resp = decode_response(wire)
    assert resp.status_code == 400
    envelope = json.loads(resp.body)
    assert "err" in envelope
    assert envelope["err"]["statusCode"] == 400
    assert envelope["err"]["error"] == "invalid input"


@pytest.mark.asyncio
async def test_function_handler_traced_error_403_is_surfaced() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv2.example", transport=transport)
    client = Connector(hostname="cli2.example", transport=transport)

    @server.function(route="/forbid", method="POST")
    async def forbid(_inp: _In) -> _Out:
        raise TracedError("nope", status_code=403)

    async with Application().add(server).add(client).run_in_test():
        wire = await client.request(
            method="POST",
            url="https://srv2.example:443/forbid",
            body=_In(x=1).model_dump_json().encode("utf-8"),
            timeout=2.0,
        )

    resp = decode_response(wire)
    assert resp.status_code == 403
    envelope = json.loads(resp.body)
    assert envelope["err"]["statusCode"] == 403


@pytest.mark.asyncio
async def test_unhandled_exception_still_yields_500() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="srv3.example", transport=transport)
    client = Connector(hostname="cli3.example", transport=transport)

    @server.function(route="/boom", method="POST")
    async def boom(_inp: _In) -> _Out:
        raise RuntimeError("uncaught bug")

    async with Application().add(server).add(client).run_in_test():
        wire = await client.request(
            method="POST",
            url="https://srv3.example:443/boom",
            body=_In(x=1).model_dump_json().encode("utf-8"),
            timeout=2.0,
        )

    resp = decode_response(wire)
    assert resp.status_code == 500
    envelope = json.loads(resp.body)
    assert envelope["err"]["statusCode"] == 500
