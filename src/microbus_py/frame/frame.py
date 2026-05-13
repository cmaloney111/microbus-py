"""Frame — accessors over a request/response's Microbus-* headers.

A Frame wraps a mutable mapping of HTTP-style headers (the same dict the
codec layer parses) and exposes typed read/write accessors for each
``Microbus-*`` field. Setting a value clears the underlying header when the
value is the documented zero (empty string, zero duration, single-fragment
indicator, etc.) so wire output stays minimal.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from microbus_py.frame.headers import H

if TYPE_CHECKING:
    from collections.abc import MutableMapping

__all__ = ["Frame"]


def _parse_go_duration(s: str) -> timedelta:
    """Parse a Go-format duration string into a timedelta.

    Supports a small subset sufficient for Microbus headers: combinations of
    ``ns``, ``us``, ``µs``, ``ms``, ``s``, ``m``, ``h`` with integer or
    decimal coefficients, including a leading sign.
    """
    if not s:
        return timedelta(0)
    sign = 1
    idx = 0
    if s[0] in "+-":
        if s[0] == "-":
            sign = -1
        idx = 1
    total = timedelta(0)
    units = {
        "ns": timedelta(microseconds=0.001),
        "us": timedelta(microseconds=1),
        "µs": timedelta(microseconds=1),
        "ms": timedelta(milliseconds=1),
        "s": timedelta(seconds=1),
        "m": timedelta(minutes=1),
        "h": timedelta(hours=1),
    }
    while idx < len(s):
        # Read number
        num_start = idx
        while idx < len(s) and (s[idx].isdigit() or s[idx] == "."):
            idx += 1
        if num_start == idx:
            raise ValueError(f"invalid duration: {s!r}")
        value = float(s[num_start:idx])
        # Read unit (longest match: "ns", "us", "µs", "ms", "s", "m", "h")
        unit_match = None
        for u in ("ns", "us", "µs", "ms"):
            if s.startswith(u, idx):
                unit_match = u
                break
        if unit_match is None and idx < len(s) and s[idx] in {"s", "m", "h"}:
            unit_match = s[idx]
        if unit_match is None:
            raise ValueError(f"invalid duration unit at {s[idx:]!r}")
        total += value * units[unit_match]
        idx += len(unit_match)
    return sign * total


def _format_go_duration(d: timedelta) -> str:
    """Format a timedelta as a Go-style duration string (millisecond precision)."""
    if d == timedelta(0):
        return "0s"
    total_ms = int(d.total_seconds() * 1000)
    if total_ms == 0:
        return "0s"
    sign = ""
    if total_ms < 0:
        sign = "-"
        total_ms = -total_ms
    if total_ms % 1000 == 0:
        return f"{sign}{total_ms // 1000}s"
    return f"{sign}{total_ms}ms"


class Frame:
    """Typed accessor over a header mapping.

    ``Frame()`` defaults to a fresh empty dict; pass an existing mapping to
    work over already-parsed headers (typical use).
    """

    def __init__(self, headers: MutableMapping[str, str] | None = None) -> None:
        self._h: MutableMapping[str, str] = headers if headers is not None else {}

    @property
    def headers(self) -> MutableMapping[str, str]:
        return self._h

    # --- msg id ---------------------------------------------------------
    @property
    def msg_id(self) -> str:
        return self._h.get(H.MSG_ID, "")

    def set_msg_id(self, value: str) -> None:
        if value:
            self._h[H.MSG_ID] = value
        else:
            self._h.pop(H.MSG_ID, None)

    # --- from-host / from-id / from-version ----------------------------
    @property
    def from_host(self) -> str:
        return self._h.get(H.FROM_HOST, "")

    def set_from_host(self, value: str) -> None:
        if value:
            self._h[H.FROM_HOST] = value
        else:
            self._h.pop(H.FROM_HOST, None)

    @property
    def from_id(self) -> str:
        return self._h.get(H.FROM_ID, "")

    def set_from_id(self, value: str) -> None:
        if value:
            self._h[H.FROM_ID] = value
        else:
            self._h.pop(H.FROM_ID, None)

    @property
    def from_version(self) -> int:
        v = self._h.get(H.FROM_VERSION, "")
        if not v:
            return 0
        try:
            return int(v)
        except ValueError:
            return 0

    def set_from_version(self, value: int) -> None:
        if value < 0:
            raise ValueError(f"from_version must be >= 0, got {value}")
        if value == 0:
            self._h.pop(H.FROM_VERSION, None)
        else:
            self._h[H.FROM_VERSION] = str(value)

    # --- op code --------------------------------------------------------
    @property
    def op_code(self) -> str:
        return self._h.get(H.OP_CODE, "")

    def set_op_code(self, value: str) -> None:
        if value:
            self._h[H.OP_CODE] = value
        else:
            self._h.pop(H.OP_CODE, None)

    # --- call depth -----------------------------------------------------
    @property
    def call_depth(self) -> int:
        v = self._h.get(H.CALL_DEPTH, "")
        if not v:
            return 0
        try:
            return int(v)
        except ValueError:
            return 0

    def set_call_depth(self, value: int) -> None:
        if value < 0:
            raise ValueError(f"call_depth must be >= 0, got {value}")
        if value == 0:
            self._h.pop(H.CALL_DEPTH, None)
        else:
            self._h[H.CALL_DEPTH] = str(value)

    # --- time budget ----------------------------------------------------
    @property
    def time_budget(self) -> timedelta:
        v = self._h.get(H.TIME_BUDGET, "")
        if not v:
            return timedelta(0)
        try:
            return timedelta(milliseconds=int(v))
        except ValueError:
            return timedelta(0)

    def set_time_budget(self, value: timedelta) -> None:
        ms = int(value.total_seconds() * 1000)
        if ms <= 0:
            self._h.pop(H.TIME_BUDGET, None)
        else:
            self._h[H.TIME_BUDGET] = str(ms)

    # --- locality / queue ----------------------------------------------
    @property
    def locality(self) -> str:
        return self._h.get(H.LOCALITY, "")

    def set_locality(self, value: str) -> None:
        if value:
            self._h[H.LOCALITY] = value
        else:
            self._h.pop(H.LOCALITY, None)

    @property
    def queue(self) -> str:
        return self._h.get(H.QUEUE, "")

    def set_queue(self, value: str) -> None:
        if value:
            self._h[H.QUEUE] = value
        else:
            self._h.pop(H.QUEUE, None)

    # --- fragment -------------------------------------------------------
    @property
    def fragment(self) -> tuple[int, int]:
        v = self._h.get(H.FRAGMENT, "")
        if not v:
            return (1, 1)
        parts = v.split("/")
        if len(parts) != 2:
            return (1, 1)
        try:
            return (int(parts[0]), int(parts[1]))
        except ValueError:
            return (1, 1)

    def set_fragment(self, idx: int, total: int) -> None:
        if idx < 1 or total < 1 or (idx == 1 and total == 1):
            self._h.pop(H.FRAGMENT, None)
        else:
            self._h[H.FRAGMENT] = f"{idx}/{total}"

    # --- clock shift ----------------------------------------------------
    @property
    def clock_shift(self) -> timedelta:
        v = self._h.get(H.CLOCK_SHIFT, "")
        if not v:
            return timedelta(0)
        try:
            return _parse_go_duration(v)
        except ValueError:
            return timedelta(0)

    def set_clock_shift(self, value: timedelta) -> None:
        if value == timedelta(0):
            self._h.pop(H.CLOCK_SHIFT, None)
        else:
            self._h[H.CLOCK_SHIFT] = _format_go_duration(value)

    # --- baggage --------------------------------------------------------
    @property
    def baggage(self) -> dict[str, str]:
        prefix = str(H.BAGGAGE_PREFIX)
        return {
            name[len(prefix) :]: value for name, value in self._h.items() if name.startswith(prefix)
        }

    def set_baggage(self, name: str, value: str) -> None:
        full = f"{H.BAGGAGE_PREFIX}{name}"
        if value:
            self._h[full] = value
        else:
            self._h.pop(full, None)
