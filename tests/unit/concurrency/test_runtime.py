"""Tests for ``Connector.go`` and ``Connector.parallel``."""

from __future__ import annotations

import asyncio

import pytest

from microbus_py import Connector, InMemoryBroker


@pytest.fixture
def svc() -> Connector:
    return Connector(hostname="conc.example", transport=InMemoryBroker())


@pytest.mark.asyncio
async def test_go_schedules_coroutine_and_returns_task(svc: Connector) -> None:
    seen: list[int] = []

    async def work() -> None:
        await asyncio.sleep(0)
        seen.append(1)

    task = svc.go(work())
    await task
    assert seen == [1]


@pytest.mark.asyncio
async def test_parallel_awaits_all_and_returns_results_in_order(
    svc: Connector,
) -> None:
    async def a() -> int:
        await asyncio.sleep(0.01)
        return 1

    async def b() -> int:
        await asyncio.sleep(0)
        return 2

    results = await svc.parallel(a(), b())
    assert results == [1, 2]


@pytest.mark.asyncio
async def test_parallel_propagates_first_exception(svc: Connector) -> None:
    async def ok() -> int:
        await asyncio.sleep(0.01)
        return 1

    async def boom() -> int:
        raise RuntimeError("kaboom")

    with pytest.raises(RuntimeError, match="kaboom"):
        await svc.parallel(ok(), boom())


@pytest.mark.asyncio
async def test_go_tracks_pending_tasks_for_shutdown(svc: Connector) -> None:
    finished = asyncio.Event()

    async def work() -> None:
        await asyncio.sleep(0.02)
        finished.set()

    svc.go(work())
    assert svc.pending_task_count() == 1
    await finished.wait()
    await asyncio.sleep(0)
    assert svc.pending_task_count() == 0
