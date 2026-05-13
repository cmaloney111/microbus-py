"""Structured logging — Go-slog-style ``name=value`` pairs as JSON lines.

Mirrors fabric ``LogDebug``/``LogInfo``/``LogWarn``/``LogError`` semantics.
The first ``args`` after ``msg`` are interpreted as alternating key/value
pairs; an unpaired trailing arg is dropped. Output is one JSON object per
line on stderr so downstream collectors (Fluent Bit, Promtail, Vector)
can parse without a custom format.
"""

from __future__ import annotations

import json
from typing import Any, TextIO

__all__ = ["emit_log", "log_pairs"]


def log_pairs(args: tuple[Any, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    it = iter(args)
    for k in it:
        try:
            v = next(it)
        except StopIteration:
            break
        if not isinstance(k, str):
            continue
        out[k] = _coerce(v)
    return out


def _coerce(v: Any) -> Any:
    if isinstance(v, BaseException):
        return f"{type(v).__name__}: {v}"
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return repr(v)


def _emit(
    stream: TextIO,
    level: str,
    hostname: str,
    instance_id: str,
    msg: str,
    fields: dict[str, Any],
) -> None:
    record: dict[str, Any] = {
        "level": level,
        "msg": msg,
        "service": hostname,
        "instance_id": instance_id,
    }
    record.update(fields)
    stream.write(json.dumps(record) + "\n")
    stream.flush()


def emit_log(
    stream: TextIO,
    level: str,
    hostname: str,
    instance_id: str,
    msg: str,
    args: tuple[Any, ...],
) -> None:
    _emit(stream, level, hostname, instance_id, msg, log_pairs(args))
