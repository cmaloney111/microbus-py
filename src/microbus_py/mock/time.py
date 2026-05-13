"""Virtual clock for deterministic ticker tests.

Drives ticker handlers via :meth:`VirtualClock.advance` instead of wall-clock
time. Each handler registered via :meth:`schedule_recurring` records its
period and last-fire mark; ``advance`` walks the clock forward and invokes
the handler exactly ``floor(elapsed / interval)`` times.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import timedelta

__all__ = ["VirtualClock"]


AsyncCallback = Callable[[], Awaitable[None]]


@dataclass(slots=True)
class _Recurring:
    name: str
    interval: float
    callback: AsyncCallback
    next_fire: float
    stopped: bool = False


@dataclass(slots=True)
class VirtualClock:
    """Deterministic clock that fires recurring callbacks on demand."""

    _now: float = 0.0
    _jobs: list[_Recurring] = field(default_factory=list)

    @property
    def now(self) -> float:
        return self._now

    def schedule_recurring(
        self,
        *,
        name: str,
        interval: timedelta,
        callback: AsyncCallback,
    ) -> Callable[[], None]:
        """Register an async callback fired every ``interval`` of virtual time.

        Returns an idempotent stop function.
        """
        if interval.total_seconds() <= 0:
            raise ValueError("interval must be positive")
        job = _Recurring(
            name=name,
            interval=interval.total_seconds(),
            callback=callback,
            next_fire=self._now + interval.total_seconds(),
        )
        self._jobs.append(job)

        def stop() -> None:
            job.stopped = True

        return stop

    async def advance(self, by: timedelta) -> None:
        """Advance virtual time by ``by`` and run any due callbacks in order."""
        target = self._now + by.total_seconds()
        while True:
            due = [j for j in self._jobs if not j.stopped and j.next_fire <= target]
            if not due:
                self._now = target
                return
            due.sort(key=lambda j: j.next_fire)
            job = due[0]
            self._now = job.next_fire
            job.next_fire += job.interval
            with contextlib.suppress(Exception):
                await job.callback()
            await asyncio.sleep(0)
