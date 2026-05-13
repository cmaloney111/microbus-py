"""Flow — task-side workflow carrier object.

Mirrors fabric ``workflow.Flow`` JSON wire format (camelCase keys, Go duration
strings, omit-zero, Foreman orchestration metadata). Tasks consume and produce
``Flow`` documents over POST ``:428/<task-name>``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

__all__ = ["Flow", "format_go_duration", "parse_go_duration"]


_DURATION_UNITS: dict[str, float] = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}

_DURATION_RE = re.compile(r"(-?\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")


def parse_go_duration(s: str) -> timedelta:
    if s in ("", "0", "0s"):
        return timedelta()
    negative = False
    rest = s
    if rest.startswith("-"):
        negative = True
        rest = rest[1:]
    elif rest.startswith("+"):
        rest = rest[1:]
    seconds = 0.0
    pos = 0
    while pos < len(rest):
        m = _DURATION_RE.match(rest, pos)
        if m is None:
            raise ValueError(f"invalid Go duration: {s!r}")
        seconds += float(m.group(1)) * _DURATION_UNITS[m.group(2)]
        pos = m.end()
    if pos != len(rest):
        raise ValueError(f"invalid Go duration: {s!r}")
    if negative:
        seconds = -seconds
    return timedelta(seconds=seconds)


def format_go_duration(td: timedelta) -> str:
    total_ns = round(td.total_seconds() * 1_000_000_000)
    if total_ns == 0:
        return "0s"
    sign = ""
    if total_ns < 0:
        sign = "-"
        total_ns = -total_ns
    if total_ns < 1_000:
        return f"{sign}{total_ns}ns"
    if total_ns < 1_000_000:
        return f"{sign}{_trim_float(total_ns / 1_000.0)}us"
    if total_ns < 1_000_000_000:
        return f"{sign}{_trim_float(total_ns / 1_000_000.0)}ms"
    hours, rem = divmod(total_ns, 3_600_000_000_000)
    minutes, rem = divmod(rem, 60_000_000_000)
    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    seconds = rem / 1_000_000_000
    parts.append(f"{_trim_float(seconds)}s")
    return sign + "".join(parts)


def _trim_float(x: float) -> str:
    if x == int(x):
        return str(int(x))
    s = f"{x:.9f}".rstrip("0").rstrip(".")
    return s if s else "0"


@dataclass(slots=True)
class Flow:
    flow_key: str = ""
    workflow_name: str = ""
    task_name: str = ""
    step_num: int = 0
    state: dict[str, Any] = field(default_factory=dict)
    changes: dict[str, Any] = field(default_factory=dict)
    goto: str | None = None
    retry: bool = False
    sleep_duration: timedelta | None = None
    interrupt: bool = False
    interrupt_payload: dict[str, Any] | None = None
    subgraph_workflow: str | None = None
    subgraph_input: Any = None
    attempt: int = 0
    backoff_max_attempts: int = 0
    backoff_initial_delay: timedelta = field(default_factory=timedelta)
    backoff_delay_multiplier: float = 0.0
    backoff_max_delay: timedelta = field(default_factory=timedelta)

    def to_json(self) -> dict[str, Any]:
        zero = timedelta()
        sleep = self.sleep_duration
        sleep_str = format_go_duration(sleep) if sleep and sleep > zero else None
        boi_str = (
            format_go_duration(self.backoff_initial_delay)
            if self.backoff_initial_delay > zero
            else None
        )
        bod_str = (
            format_go_duration(self.backoff_max_delay) if self.backoff_max_delay > zero else None
        )
        candidates: list[tuple[str, Any]] = [
            ("flowKey", self.flow_key or None),
            ("workflowName", self.workflow_name or None),
            ("taskName", self.task_name or None),
            ("stepNum", self.step_num or None),
            ("state", self.state or None),
            ("changes", self.changes or None),
            ("goto", self.goto),
            ("retry", True if self.retry else None),
            ("sleepDuration", sleep_str),
            ("interrupt", True if self.interrupt else None),
            ("interruptPayload", self.interrupt_payload or None),
            ("subgraphWorkflow", self.subgraph_workflow),
            ("subgraphInput", self.subgraph_input),
            ("attempt", self.attempt or None),
            ("backoffMaxAttempts", self.backoff_max_attempts or None),
            ("backoffInitialDelay", boi_str),
            ("backoffDelayMultiplier", self.backoff_delay_multiplier or None),
            ("backoffMaxDelay", bod_str),
        ]
        return {k: v for k, v in candidates if v is not None}

    @classmethod
    def from_json(cls, doc: dict[str, Any]) -> Flow:
        sleep_str = doc.get("sleepDuration")
        boi_str = doc.get("backoffInitialDelay")
        bod_str = doc.get("backoffMaxDelay")
        payload = doc.get("interruptPayload")
        return cls(
            flow_key=str(doc.get("flowKey") or ""),
            workflow_name=str(doc.get("workflowName") or ""),
            task_name=str(doc.get("taskName") or ""),
            step_num=int(doc.get("stepNum") or 0),
            state=dict(doc.get("state") or {}),
            changes=dict(doc.get("changes") or {}),
            goto=doc.get("goto") or None,
            retry=bool(doc.get("retry") or False),
            sleep_duration=parse_go_duration(sleep_str) if sleep_str else None,
            interrupt=bool(doc.get("interrupt") or False),
            interrupt_payload=dict(payload) if isinstance(payload, dict) else None,
            subgraph_workflow=doc.get("subgraphWorkflow") or None,
            subgraph_input=doc.get("subgraphInput"),
            attempt=int(doc.get("attempt") or 0),
            backoff_max_attempts=int(doc.get("backoffMaxAttempts") or 0),
            backoff_initial_delay=parse_go_duration(boi_str) if boi_str else timedelta(),
            backoff_delay_multiplier=float(doc.get("backoffDelayMultiplier") or 0.0),
            backoff_max_delay=parse_go_duration(bod_str) if bod_str else timedelta(),
        )
