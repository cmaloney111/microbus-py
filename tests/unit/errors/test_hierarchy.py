"""Tests for the typed exception hierarchy keyed to HTTP status families.

These subclasses let callers narrow ``except TracedError`` into status-specific
clauses (``except NotFound``, ``except Forbidden``, ...). The wire format is
unchanged — subclasses inherit ``to_json``/``from_json`` and reconstruction
routes through ``TracedError.from_status`` so the subclass is recovered on
decode.
"""

from __future__ import annotations

import json
import pickle

from microbus_py.errors import (
    BadRequest,
    Conflict,
    Forbidden,
    InternalError,
    NotFound,
    TooManyRequests,
    TracedError,
    Unauthorized,
    Unavailable,
)


class TestSubclassRelationship:
    def test_bad_request_subclasses_traced_error(self) -> None:
        assert issubclass(BadRequest, TracedError)
        assert isinstance(BadRequest("x"), TracedError)


class TestDefaultStatusCodes:
    def test_bad_request_default_status_400(self) -> None:
        assert BadRequest("x").status_code == 400

    def test_unauthorized_default_status_401(self) -> None:
        assert Unauthorized("x").status_code == 401

    def test_forbidden_default_status_403(self) -> None:
        assert Forbidden("x").status_code == 403

    def test_not_found_default_status_404(self) -> None:
        assert NotFound("x").status_code == 404

    def test_conflict_default_status_409(self) -> None:
        assert Conflict("x").status_code == 409

    def test_too_many_requests_default_status_429(self) -> None:
        assert TooManyRequests("x").status_code == 429

    def test_internal_error_default_status_500(self) -> None:
        assert InternalError("x").status_code == 500

    def test_unavailable_default_status_503(self) -> None:
        assert Unavailable("x").status_code == 503

    def test_subclass_status_can_be_overridden(self) -> None:
        err = NotFound("x", status_code=410)
        assert err.status_code == 410


class TestFromStatusDispatch:
    def test_traced_error_from_status_returns_correct_subclass_404(self) -> None:
        err = TracedError.from_status(404, "missing")
        assert isinstance(err, NotFound)

    def test_traced_error_from_status_returns_correct_subclass_403(self) -> None:
        err = TracedError.from_status(403, "denied")
        assert isinstance(err, Forbidden)

    def test_traced_error_from_status_unmapped_returns_base(self) -> None:
        err = TracedError.from_status(418, "teapot")
        assert type(err) is TracedError


class TestRoundTripPreservesSubclass:
    def test_from_json_round_trip_preserves_subclass(self) -> None:
        doc = {"error": "missing widget", "statusCode": 404}
        rebuilt = TracedError.from_json(doc)
        assert isinstance(rebuilt, NotFound)


class TestWireFormatStability:
    def test_traced_error_to_json_unchanged_golden_bytes(self) -> None:
        err = TracedError("x", status_code=404, stack=[])
        got = json.dumps(err.to_json(), separators=(",", ":")).encode("utf-8")
        assert got == b'{"error":"x","statusCode":404}'


class TestPickling:
    def test_pickling_round_trip_preserves_subclass(self) -> None:
        original = NotFound("x")
        restored = pickle.loads(pickle.dumps(original))
        assert isinstance(restored, NotFound)
        assert restored.status_code == 404
        assert restored.message == "x"
