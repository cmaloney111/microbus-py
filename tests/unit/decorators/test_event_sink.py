"""Tests for the ``@svc.event_sink`` decorator (inbound events)."""

# mypy: disable-error-code="attr-defined, empty-body"

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

import microbus_py.decorators.event as _ev
import microbus_py.decorators.event_sink as _ev_sink
from microbus_py import Connector, InMemoryBroker

_INSTALLS = (_ev, _ev_sink)


class Evt(BaseModel):
    v: int


class Resp(BaseModel):
    ok: bool = True


@pytest.mark.asyncio
async def test_event_sink_records_feature() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="sink.example", transport=transport)

    @svc.event_sink(source="src.example", route=":417/on-x", name="OnX")
    async def h(inp: Evt) -> Resp:
        return Resp()

    feats = svc.__dict__.get("_event_sink_features", [])
    assert any(f.name == "OnX" and f.source == "src.example" and f.port == 417 for f in feats)


@pytest.mark.asyncio
async def test_event_sink_handler_receives_payload() -> None:
    transport = InMemoryBroker()
    src = Connector(hostname="src.example", transport=transport)
    snk = Connector(hostname="snk.example", transport=transport)

    @src.event(route=":417/on-evt", method="POST")
    async def on_evt(inp: Evt) -> Resp: ...

    received: list[int] = []

    @snk.event_sink(source="src.example", route=":417/on-evt", name="OnEvt")
    async def h(inp: Evt) -> Resp:
        received.append(inp.v)
        return Resp(ok=True)

    await src.startup()
    await snk.startup()
    try:
        async for _ in on_evt(Evt(v=99)):
            pass
        assert received == [99]
    finally:
        await snk.shutdown()
        await src.shutdown()


@pytest.mark.asyncio
async def test_event_sink_queue_load_balances_replicas() -> None:
    transport = InMemoryBroker()
    src = Connector(hostname="src.example", transport=transport)
    a = Connector(hostname="rep-a.example", transport=transport)
    b = Connector(hostname="rep-b.example", transport=transport)

    @src.event(route=":417/on-q", method="POST")
    async def on_q(inp: Evt) -> Resp: ...

    counts = {"a": 0, "b": 0}

    @a.event_sink(source="src.example", route=":417/on-q", name="OnQ", queue="workers")
    async def ha(inp: Evt) -> Resp:
        counts["a"] += 1
        return Resp()

    @b.event_sink(source="src.example", route=":417/on-q", name="OnQ", queue="workers")
    async def hb(inp: Evt) -> Resp:
        counts["b"] += 1
        return Resp()

    await src.startup()
    await a.startup()
    await b.startup()
    try:
        for i in range(6):
            async for _ in on_q(Evt(v=i)):
                pass
        await asyncio.sleep(0)
        assert counts["a"] + counts["b"] == 6
        assert counts["a"] >= 1 and counts["b"] >= 1
    finally:
        await b.shutdown()
        await a.shutdown()
        await src.shutdown()


@pytest.mark.asyncio
async def test_event_sink_required_claims_block_missing_actor() -> None:
    transport = InMemoryBroker()
    src = Connector(hostname="src.example", transport=transport)
    snk = Connector(hostname="snk.example", transport=transport)

    @src.event(route=":417/on-secure", method="POST")
    async def on_secure(inp: Evt) -> Resp: ...

    received: list[int] = []

    @snk.event_sink(
        source="src.example",
        route=":417/on-secure",
        name="OnSecure",
        required_claims="roles.admin",
    )
    async def h(inp: Evt) -> Resp:
        received.append(inp.v)
        return Resp()

    await src.startup()
    await snk.startup()
    try:
        async for _ in on_secure(Evt(v=1)):
            pass
        assert received == []
    finally:
        await snk.shutdown()
        await src.shutdown()
