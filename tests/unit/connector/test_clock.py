"""Tests for ``Connector.now()`` with optional injected clock callable."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from microbus_py import Connector, InMemoryBroker


def test_now_returns_aware_utc_datetime_by_default() -> None:
    svc = Connector(hostname="t.example", transport=InMemoryBroker())
    n = svc.now()
    assert isinstance(n, datetime)
    assert n.tzinfo is not None
    delta = abs((datetime.now(UTC) - n).total_seconds())
    assert delta < 5


def test_now_uses_injected_clock_callable() -> None:
    fixed = datetime(2026, 5, 7, 0, 0, 0, tzinfo=UTC)
    svc = Connector(
        hostname="t.example",
        transport=InMemoryBroker(),
        clock=lambda: fixed,
    )
    assert svc.now() == fixed


def test_now_clock_callable_can_advance_between_calls() -> None:
    cur = [datetime(2026, 5, 7, 0, 0, 0, tzinfo=UTC)]
    svc = Connector(
        hostname="t.example",
        transport=InMemoryBroker(),
        clock=lambda: cur[0],
    )
    assert svc.now() == cur[0]
    cur[0] = datetime(2026, 5, 7, 0, 1, 0, tzinfo=UTC)
    assert svc.now() == cur[0]


@pytest.mark.asyncio
async def test_now_during_handler_uses_clock() -> None:
    cur = [datetime(2026, 5, 7, 0, 0, 0, tzinfo=UTC)]
    svc = Connector(
        hostname="t.example",
        transport=InMemoryBroker(),
        clock=lambda: cur[0],
    )
    await svc.startup()
    try:
        assert svc.now() == cur[0]
    finally:
        await svc.shutdown()
