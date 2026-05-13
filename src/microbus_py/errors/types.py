"""Typed ``TracedError`` subclasses keyed to HTTP status families.

These subclasses carry no extra behaviour — they exist so callers can write
``except NotFound:`` instead of ``except TracedError as e: if e.status_code ==
404: ...``. The wire format is unchanged: subclasses inherit
``to_json``/``from_json`` from ``TracedError``, and decode dispatches through
``TracedError.from_status`` to recover the correct subclass.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Unpack

from microbus_py.errors.traced import TracedError, register_status_subclass

if TYPE_CHECKING:
    from microbus_py.errors.traced import TracedErrorKwargs


__all__ = [
    "BadRequest",
    "Conflict",
    "Forbidden",
    "InternalError",
    "NotFound",
    "TooManyRequests",
    "Unauthorized",
    "Unavailable",
]


class BadRequest(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 400)
        super().__init__(msg, **kw)


class Unauthorized(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 401)
        super().__init__(msg, **kw)


class Forbidden(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 403)
        super().__init__(msg, **kw)


class NotFound(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 404)
        super().__init__(msg, **kw)


class Conflict(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 409)
        super().__init__(msg, **kw)


class TooManyRequests(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 429)
        super().__init__(msg, **kw)


class InternalError(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 500)
        super().__init__(msg, **kw)


class Unavailable(TracedError):
    def __init__(self, msg: str, **kw: Unpack[TracedErrorKwargs]) -> None:
        kw.setdefault("status_code", 503)
        super().__init__(msg, **kw)


STATUS_MAP: dict[int, type[TracedError]] = {
    400: BadRequest,
    401: Unauthorized,
    403: Forbidden,
    404: NotFound,
    409: Conflict,
    429: TooManyRequests,
    500: InternalError,
    503: Unavailable,
}

for _status, _cls in STATUS_MAP.items():
    register_status_subclass(_status, _cls)
