"""Tests for the ``@svc.event`` decorator (outbound events)."""

# mypy: disable-error-code="attr-defined, empty-body"

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

import microbus_py.decorators.event as _ev
import microbus_py.decorators.event_sink as _ev_sink
from microbus_py import Connector, InMemoryBroker

# Side-effect imports install methods on Connector.
_INSTALLS = (_ev, _ev_sink)


class OnOrderIn(BaseModel):
    order_id: str


class OnOrderOut(BaseModel):
    accepted: bool = True


@pytest.mark.asyncio
async def test_event_decorator_records_feature() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="orders.example", transport=transport)

    @svc.event(route=":417/on-order", method="POST")
    async def on_order(inp: OnOrderIn) -> OnOrderOut: ...

    feats = svc.__dict__.get("_event_features", [])
    assert any(f.name == "on_order" and f.port == 417 and f.method == "POST" for f in feats)


@pytest.mark.asyncio
async def test_event_trigger_publishes_to_subscribers() -> None:
    transport = InMemoryBroker()
    source = Connector(hostname="orders.example", transport=transport)
    sink = Connector(hostname="audit.example", transport=transport)

    @source.event(route=":417/on-order", method="POST")
    async def on_order(inp: OnOrderIn) -> OnOrderOut: ...

    received: list[OnOrderIn] = []

    @sink.event_sink(source="orders.example", route=":417/on-order", name="OnOrder")
    async def handle(inp: OnOrderIn) -> OnOrderOut:
        received.append(inp)
        return OnOrderOut(accepted=True)

    await source.startup()
    await sink.startup()
    try:
        responses = []
        async for r in on_order(OnOrderIn(order_id="42")):
            responses.append(r)
        assert len(received) == 1
        assert received[0].order_id == "42"
        assert len(responses) == 1
        assert responses[0].accepted is True
    finally:
        await sink.shutdown()
        await source.shutdown()


@pytest.mark.asyncio
async def test_event_trigger_fire_and_forget_does_not_block() -> None:
    transport = InMemoryBroker()
    source = Connector(hostname="orders.example", transport=transport)
    sink = Connector(hostname="sink.example", transport=transport)

    @source.event(route=":417/on-x", method="POST")
    async def on_x(inp: OnOrderIn) -> OnOrderOut: ...

    seen: list[str] = []

    @sink.event_sink(source="orders.example", route=":417/on-x", name="OnX")
    async def h(inp: OnOrderIn) -> OnOrderOut:
        seen.append(inp.order_id)
        return OnOrderOut()

    await source.startup()
    await sink.startup()
    try:
        await on_x.fire(OnOrderIn(order_id="ff"))
        for _ in range(20):
            if seen:
                break
            await asyncio.sleep(0.01)
        assert seen == ["ff"]
    finally:
        await sink.shutdown()
        await source.shutdown()


@pytest.mark.asyncio
async def test_event_trigger_multicast_collects_multiple_responses() -> None:
    transport = InMemoryBroker()
    source = Connector(hostname="orders.example", transport=transport)
    sink_a = Connector(hostname="a.example", transport=transport)
    sink_b = Connector(hostname="b.example", transport=transport)

    @source.event(route=":417/on-multi", method="POST")
    async def on_multi(inp: OnOrderIn) -> OnOrderOut: ...

    @sink_a.event_sink(source="orders.example", route=":417/on-multi", name="OnMultiA")
    async def ha(inp: OnOrderIn) -> OnOrderOut:
        return OnOrderOut(accepted=True)

    @sink_b.event_sink(source="orders.example", route=":417/on-multi", name="OnMultiB")
    async def hb(inp: OnOrderIn) -> OnOrderOut:
        return OnOrderOut(accepted=False)

    await source.startup()
    await sink_a.startup()
    await sink_b.startup()
    try:
        responses = []
        async for r in on_multi(OnOrderIn(order_id="m")):
            responses.append(r)
        assert len(responses) == 2
        assert sorted([r.accepted for r in responses]) == [False, True]
    finally:
        await sink_b.shutdown()
        await sink_a.shutdown()
        await source.shutdown()


@pytest.mark.asyncio
async def test_event_trigger_no_subscribers_yields_nothing() -> None:
    transport = InMemoryBroker()
    source = Connector(hostname="orders.example", transport=transport)

    @source.event(route=":417/on-empty", method="POST")
    async def on_empty(inp: OnOrderIn) -> OnOrderOut: ...

    await source.startup()
    try:
        responses = []
        async for r in on_empty(OnOrderIn(order_id="x")):
            responses.append(r)
        assert responses == []
    finally:
        await source.shutdown()


@pytest.mark.asyncio
async def test_event_default_port_is_417() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="x.example", transport=transport)

    @svc.event(route="/on-y")
    async def on_y(inp: OnOrderIn) -> OnOrderOut: ...

    feats = svc.__dict__.get("_event_features", [])
    feat = next(f for f in feats if f.name == "on_y")
    assert feat.port == 417
