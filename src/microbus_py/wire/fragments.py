"""Fragmentation and reassembly for oversize NATS payloads.

NATS imposes a max-payload limit (default 1 MiB). Microbus splits larger
payloads into ordered fragments and reassembles them on receipt. The framing
header ``Microbus-Fragment: N/M`` carries the index and total count.

This module provides the body-only chunking primitives. The wire codec layer
attaches the framing header to each fragment.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

__all__ = ["FRAGMENT_THRESHOLD", "Reassembler", "split"]


FRAGMENT_THRESHOLD: int = 1 << 20
"""Default fragment cutoff in bytes — matches NATS ``max_payload`` default."""


def split(payload: bytes, max_size: int = FRAGMENT_THRESHOLD) -> list[bytes]:
    """Split *payload* into chunks each at most *max_size* bytes.

    A payload at or below the limit is returned as a single chunk.
    Concatenating the returned chunks reconstructs *payload* exactly.
    """
    if max_size <= 0:
        raise ValueError(f"max_size must be positive, got {max_size}")
    if len(payload) <= max_size:
        return [payload]
    return [payload[i : i + max_size] for i in range(0, len(payload), max_size)]


@dataclass(slots=True)
class Reassembler:
    """Stateful collector that reassembles fragments into the original payload.

    Fragments arrive in any order. The reassembler holds them keyed by index
    and emits the integrated payload only when every expected fragment has
    been seen.
    """

    deadline_seconds: float = 60.0
    _total: int | None = field(default=None, init=False)
    _chunks: dict[int, bytes] = field(default_factory=dict, init=False)
    _started_at: float = field(default_factory=time.monotonic, init=False)
    _completed: bool = field(default=False, init=False)

    @property
    def complete(self) -> bool:
        return self._completed

    def add(self, *, idx: int, total: int, chunk: bytes) -> bytes | None:
        """Record one fragment. Return the assembled payload when complete, else None."""
        if total < 1:
            raise ValueError(f"total must be >= 1, got {total}")
        if idx < 1 or idx > total:
            raise ValueError(f"idx {idx} out of range for total {total}")
        if self._total is None:
            self._total = total
        elif self._total != total:
            raise ValueError(f"total mismatch: previously {self._total}, now {total}")
        if idx in self._chunks:
            # Duplicate — silently ignore for idempotency
            return self._maybe_finish()
        self._chunks[idx] = chunk
        return self._maybe_finish()

    def _maybe_finish(self) -> bytes | None:
        assert self._total is not None
        if len(self._chunks) < self._total:
            return None
        ordered = [self._chunks[i] for i in range(1, self._total + 1)]
        self._completed = True
        return b"".join(ordered)

    def expired(self) -> bool:
        return (time.monotonic() - self._started_at) > self.deadline_seconds
