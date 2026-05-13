"""Tests for the ``@svc.metric`` decorator."""

# mypy: disable-error-code="attr-defined, empty-body"

from __future__ import annotations

import pytest

import microbus_py.decorators.metric as _m
from microbus_py import Connector, InMemoryBroker

_INSTALL = _m


@pytest.mark.asyncio
async def test_metric_counter_decorator_records() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="m.example", transport=transport)

    @svc.metric(name="api_calls_total", kind="counter", description="API calls")
    def api_calls(value: float) -> None: ...

    api_calls(1.0)
    api_calls(2.0)
    out = svc.scrape_metrics()
    assert b"api_calls_total" in out


@pytest.mark.asyncio
async def test_metric_gauge_decorator_records() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="m.example", transport=transport)

    @svc.metric(name="depth", kind="gauge", description="queue depth")
    def depth(value: float) -> None: ...

    depth(7.5)
    out = svc.scrape_metrics().decode("utf-8")
    assert "depth" in out


@pytest.mark.asyncio
async def test_metric_histogram_records_with_buckets() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="m.example", transport=transport)

    @svc.metric(
        name="lat_s",
        kind="histogram",
        description="latency",
        buckets=[0.1, 0.5, 1.0],
    )
    def lat(value: float) -> None: ...

    lat(0.2)
    lat(0.7)
    out = svc.scrape_metrics().decode("utf-8")
    assert "lat_s_bucket" in out


@pytest.mark.asyncio
async def test_metric_records_feature() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="m.example", transport=transport)

    @svc.metric(name="x", kind="counter", description="x")
    def x(value: float) -> None: ...

    feats = svc.__dict__.get("_metric_features", [])
    assert any(f.name == "x" and f.kind == "counter" for f in feats)


@pytest.mark.asyncio
async def test_metric_with_labels() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="m.example", transport=transport)

    @svc.metric(name="hits", kind="counter", description="hits", labels=["route"])
    def hits(value: float, *, route: str) -> None: ...

    hits(1.0, route="/a")
    hits(3.0, route="/b")
    out = svc.scrape_metrics().decode("utf-8")
    assert 'route="/a"' in out
    assert 'route="/b"' in out


@pytest.mark.asyncio
async def test_metric_unknown_kind_raises() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="m.example", transport=transport)

    with pytest.raises(ValueError, match="kind"):

        @svc.metric(name="bad", kind="quantile", description="bad")
        def bad(value: float) -> None: ...
