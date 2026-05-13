"""Multicast ACK collection.

When a caller publishes a multicast request, Microbus subscribers reply with
a 100/202 ACK frame before sending the actual response. The caller's
collector waits until either a known number of ACKs have been received, or a
timeout fires. The collected ACK records inform the caller how many real
responses to expect.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

__all__ = ["AckCollector", "AckRecord"]


@dataclass(slots=True, frozen=True)
class AckRecord:
    """One ACK sent by a downstream subscriber."""

    from_id: str
    from_host: str


class AckCollector:
    """Coroutine-friendly collector that waits for ACKs.

    *expected*: number of ACKs to wait for before completing. ``None`` means
    "wait until the timeout, then return whatever arrived". ``0`` completes
    immediately.

    *timeout_seconds*: hard upper bound on ``wait()``. If the expected count
    is reached sooner, ``wait()`` returns immediately with the records.
    """

    __slots__ = ("_done", "_event", "_expected", "_records", "_timeout")

    def __init__(self, *, expected: int | None, timeout_seconds: float) -> None:
        if expected is not None and expected < 0:
            raise ValueError(f"expected must be >= 0 or None, got {expected}")
        if timeout_seconds < 0:
            raise ValueError(f"timeout_seconds must be >= 0, got {timeout_seconds}")
        self._expected = expected
        self._timeout = timeout_seconds
        self._records: list[AckRecord] = []
        self._event = asyncio.Event()
        self._done = False
        if expected == 0:
            self._done = True
            self._event.set()

    def add(self, record: AckRecord) -> None:
        """Record an incoming ACK. No-op once collection has completed."""
        if self._done:
            return
        self._records.append(record)
        if self._expected is not None and len(self._records) >= self._expected:
            self._done = True
            self._event.set()

    async def wait(self) -> list[AckRecord]:
        """Wait until the expected count is reached or the timeout fires.

        Returns a snapshot of the records collected. Subsequent calls return
        the same snapshot (collection is single-shot).
        """
        if self._done:
            return list(self._records)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._event.wait(), timeout=self._timeout)
        self._done = True
        return list(self._records)
