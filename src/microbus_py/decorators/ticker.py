"""``@ticker`` decorator — recurring async jobs driven by wall or virtual clock.

Tickers run at a fixed interval starting from :meth:`Connector.start_tickers`.
A :class:`microbus_py.mock.time.VirtualClock` can be installed via
``svc.use_clock(clock)`` for deterministic tests; otherwise a wall-clock
``asyncio.sleep`` loop is used.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import timedelta

    from microbus_py.connector.connector import Connector
    from microbus_py.mock.time import VirtualClock

__all__ = ["TickerFeature", "ticker_decorator"]


TickerHandler = Callable[[], Awaitable[None]]
_TICKER_FAILURES: tuple[type[Exception], ...] = (Exception,)


@dataclass(slots=True, frozen=True)
class TickerFeature:
    name: str
    interval: timedelta
    handler: TickerHandler


@dataclass(slots=True)
class _RunningTicker:
    name: str
    interval: timedelta
    handler: TickerHandler
    task: asyncio.Task[None] | None = None
    stop_clock: Callable[[], None] | None = None
    stopped: bool = False


def ticker_decorator(svc: Connector) -> Callable[..., Callable[[TickerHandler], TickerHandler]]:
    def deco(
        *,
        name: str,
        interval: timedelta,
    ) -> Callable[[TickerHandler], TickerHandler]:
        if interval.total_seconds() <= 0:
            raise ValueError(f"ticker '{name}' interval must be positive")

        def wrap(handler: TickerHandler) -> TickerHandler:
            feature = TickerFeature(name=name, interval=interval, handler=handler)
            features = svc.__dict__.setdefault("_ticker_features", [])
            features.append(feature)
            running: dict[str, _RunningTicker] = svc.__dict__.setdefault("_ticker_runners", {})
            running[name] = _RunningTicker(name=name, interval=interval, handler=handler)
            return handler

        return wrap

    return deco


async def _start_tickers(svc: Connector) -> None:
    runners: dict[str, _RunningTicker] = svc.__dict__.setdefault("_ticker_runners", {})
    clock: VirtualClock | None = svc.__dict__.get("_virtual_clock")
    for name, runner in runners.items():
        if runner.task is not None or runner.stop_clock is not None:
            continue
        if clock is not None:
            runner.stop_clock = clock.schedule_recurring(
                name=name,
                interval=runner.interval,
                callback=_make_safe_callback(runner.handler),
            )
        else:
            runner.task = asyncio.create_task(_run_loop(runner))


async def _stop_tickers(svc: Connector) -> None:
    runners: dict[str, _RunningTicker] = svc.__dict__.setdefault("_ticker_runners", {})
    for runner in runners.values():
        runner.stopped = True
        if runner.stop_clock is not None:
            runner.stop_clock()
            runner.stop_clock = None
        if runner.task is not None:
            runner.task.cancel()
            with contextlib.suppress(BaseException):
                await runner.task
            runner.task = None


def _use_clock(svc: Connector, clock: VirtualClock) -> None:
    svc.__dict__["_virtual_clock"] = clock


def _make_safe_callback(handler: TickerHandler) -> Callable[[], Awaitable[None]]:
    async def cb() -> None:
        with contextlib.suppress(*_TICKER_FAILURES):
            await handler()

    return cb


async def _run_loop(runner: _RunningTicker) -> None:
    interval = runner.interval.total_seconds()
    while not runner.stopped:
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
        if runner.stopped:
            return
        try:
            await runner.handler()
        except _TICKER_FAILURES:
            continue
