"""HTTP status reason phrases and error-body serialization."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from microbus_py.errors.traced import TracedError

__all__ = ["HTTP_REASONS", "json_error_body"]


HTTP_REASONS: dict[int, str] = {
    100: "Continue",
    202: "Accepted",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    408: "Request Timeout",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}


def json_error_body(err: TracedError) -> bytes:
    return json.dumps({"err": err.to_json()}).encode("utf-8")
