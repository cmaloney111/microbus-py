"""Tests for the ``@svc.ticker`` decorator (virtual-clock driven)."""

# mypy: disable-error-code="attr-defined, empty-body"

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

import microbus_py.decorators.ticker as _tk
from microbus_py import Connector, InMemoryBroker
from microbus_py.mock.time import VirtualClock

_INSTALL = _tk


@pytest.mark.asyncio
async def test_ticker_fires_n_times_for_n_intervals() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)
    clock = VirtualClock()
    svc.use_clock(clock)

    fired = 0

    @svc.ticker(name="beat", interval=timedelta(seconds=1))
    async def beat() -> None:
        nonlocal fired
        fired += 1

    await svc.start_tickers()
    try:
        await clock.advance(timedelta(seconds=3))
        await asyncio.sleep(0)
        assert fired == 3
    finally:
        await svc.stop_tickers()


@pytest.mark.asyncio
async def test_ticker_stop_prevents_further_fires() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)
    clock = VirtualClock()
    svc.use_clock(clock)

    fired = 0

    @svc.ticker(name="b", interval=timedelta(seconds=1))
    async def b() -> None:
        nonlocal fired
        fired += 1

    await svc.start_tickers()
    await clock.advance(timedelta(seconds=2))
    await asyncio.sleep(0)
    assert fired == 2
    await svc.stop_tickers()
    await clock.advance(timedelta(seconds=5))
    await asyncio.sleep(0)
    assert fired == 2


@pytest.mark.asyncio
async def test_ticker_records_feature() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)

    @svc.ticker(name="b", interval=timedelta(seconds=10))
    async def b() -> None: ...

    feats = svc.__dict__.get("_ticker_features", [])
    assert any(f.name == "b" and f.interval == timedelta(seconds=10) for f in feats)


@pytest.mark.asyncio
async def test_ticker_with_zero_interval_raises() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)

    with pytest.raises(ValueError, match="positive"):

        @svc.ticker(name="b", interval=timedelta(seconds=0))
        async def b() -> None: ...


@pytest.mark.asyncio
async def test_ticker_handler_exception_does_not_kill_ticker() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)
    clock = VirtualClock()
    svc.use_clock(clock)

    fires: list[int] = []

    @svc.ticker(name="b", interval=timedelta(seconds=1))
    async def b() -> None:
        fires.append(len(fires) + 1)
        if len(fires) == 1:
            raise RuntimeError("first fail")

    await svc.start_tickers()
    try:
        await clock.advance(timedelta(seconds=3))
        await asyncio.sleep(0)
        assert len(fires) == 3
    finally:
        await svc.stop_tickers()


@pytest.mark.asyncio
async def test_ticker_wall_clock_fires_without_virtual_clock() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)

    fired = 0

    @svc.ticker(name="b", interval=timedelta(milliseconds=10))
    async def b() -> None:
        nonlocal fired
        fired += 1

    await svc.start_tickers()
    try:
        await asyncio.sleep(0.05)
        assert fired >= 1
    finally:
        await svc.stop_tickers()


@pytest.mark.asyncio
async def test_ticker_wall_clock_handler_exception_keeps_loop_alive() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)

    calls = 0

    @svc.ticker(name="b", interval=timedelta(milliseconds=10))
    async def b() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("boom")

    await svc.start_tickers()
    try:
        await asyncio.sleep(0.06)
        assert calls >= 2
    finally:
        await svc.stop_tickers()


@pytest.mark.asyncio
async def test_virtual_clock_fractional_advance_no_fire_below_interval() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="t.example", transport=transport)
    clock = VirtualClock()
    svc.use_clock(clock)

    fired = 0

    @svc.ticker(name="b", interval=timedelta(seconds=10))
    async def b() -> None:
        nonlocal fired
        fired += 1

    await svc.start_tickers()
    try:
        await clock.advance(timedelta(seconds=3))
        await asyncio.sleep(0)
        assert fired == 0
        await clock.advance(timedelta(seconds=7))
        await asyncio.sleep(0)
        assert fired == 1
    finally:
        await svc.stop_tickers()
