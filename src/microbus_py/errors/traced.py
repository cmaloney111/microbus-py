"""TracedError — wire-compatible error envelope for Microbus.

Mirrors fabric@v1.27.1 ``errors.TracedError.MarshalJSON``::

    {
      "error": "<message>",
      "statusCode": 500,
      "trace": "<32-hex>",
      "stack": [{"func": "...", "file": "...", "line": 123}]
    }

``statusCode``, ``trace`` and ``stack`` are omitted when zero/empty so the
produced bytes match Go output for the same input. The all-zero trace
sentinel is treated as empty.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Any, TypedDict, Unpack

__all__ = ["StackFrame", "TracedError", "TracedErrorKwargs"]


class TracedErrorKwargs(TypedDict, total=False):
    status_code: int
    trace: str
    cause: BaseException | str | None
    stack: list[StackFrame] | None


class _TracedErrorKwargsNoStatus(TypedDict, total=False):
    trace: str
    cause: BaseException | str | None
    stack: list[StackFrame] | None


_ZERO_TRACE = "0" * 32


@dataclass(frozen=True, slots=True)
class StackFrame:
    func: str
    file: str
    line: int

    def to_json(self) -> dict[str, Any]:
        return {"func": self.func, "file": self.file, "line": self.line}

    @classmethod
    def from_json(cls, doc: dict[str, Any]) -> StackFrame:
        return cls(
            func=str(doc.get("func", "")),
            file=str(doc.get("file", "")),
            line=int(doc.get("line", 0)),
        )


def _capture_stack() -> list[StackFrame]:
    return [
        StackFrame(func=fr.name, file=fr.filename, line=fr.lineno or 0)
        for fr in traceback.extract_stack()[:-2]
    ]


class TracedError(Exception):
    """Exception with HTTP status code, trace id, captured stack and JSON I/O."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 500,
        trace: str = "",
        cause: BaseException | str | None = None,
        stack: list[StackFrame] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.trace = trace
        self.cause = cause
        self.stack = stack if stack is not None else _capture_stack()

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"error": self.message}
        if self.status_code:
            out["statusCode"] = self.status_code
        if self.trace and self.trace != _ZERO_TRACE:
            out["trace"] = self.trace
        if self.stack:
            out["stack"] = [f.to_json() for f in self.stack]
        underlying = self.cause if self.cause is not None else self.__cause__
        if underlying is not None:
            if isinstance(underlying, str):
                out["cause"] = underlying
            else:
                out["cause"] = f"{type(underlying).__name__}: {underlying}"
        return out

    @classmethod
    def from_json(cls, doc: dict[str, Any]) -> TracedError:
        cause_raw = doc.get("cause")
        status_code = int(doc.get("statusCode", 500))
        target = _select_class(cls, status_code)
        return target(
            str(doc.get("error", "")),
            status_code=status_code,
            trace=str(doc.get("trace", "")),
            cause=str(cause_raw) if cause_raw is not None else None,
            stack=[StackFrame.from_json(f) for f in doc.get("stack", [])],
        )

    @classmethod
    def from_status(
        cls,
        status_code: int,
        msg: str,
        **kw: Unpack[_TracedErrorKwargsNoStatus],
    ) -> TracedError:
        target = _select_class(cls, status_code)
        return target(msg, status_code=status_code, **kw)

    def __str__(self) -> str:
        return self.message


_STATUS_REGISTRY: dict[int, type[TracedError]] = {}


def register_status_subclass(status_code: int, cls: type[TracedError]) -> None:
    """Register a TracedError subclass for a given HTTP status code.

    Called from ``microbus_py.errors.types`` to wire up the dispatch table
    without forcing ``traced.py`` to import its subclasses at load time.
    """
    _STATUS_REGISTRY[status_code] = cls


def _select_class(requested: type[TracedError], status_code: int) -> type[TracedError]:
    """Resolve the most specific TracedError subclass for a status code.

    Only dispatches when the caller invoked the lookup via the base
    ``TracedError`` itself — a caller asking for ``NotFound.from_json`` keeps
    their own class. Unmapped status codes fall back to ``requested``.
    """
    if requested is not TracedError:
        return requested
    return _STATUS_REGISTRY.get(status_code, TracedError)
