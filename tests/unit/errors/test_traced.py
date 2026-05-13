"""Tests for errors/traced.py — TracedError on-wire JSON format.

Schema mirrors fabric@v1.27.1 ``errors.TracedError.MarshalJSON``::

    {
      "error": "<message>",
      "statusCode": 500,
      "trace": "<32-hex-trace-id>",
      "stack": [{"func": "...", "file": "...", "line": 123}, ...]
    }

``statusCode``, ``trace`` and ``stack`` are all ``omitempty`` on the Go
side, so callers must accept missing fields.
"""

from __future__ import annotations

from microbus_py.errors.traced import StackFrame, TracedError


class TestTracedErrorBasics:
    def test_default_construction(self) -> None:
        err = TracedError("something failed")
        assert err.message == "something failed"
        assert err.status_code == 500
        assert err.trace == ""

    def test_explicit_status_code(self) -> None:
        err = TracedError("not found", status_code=404)
        assert err.status_code == 404

    def test_explicit_trace_id(self) -> None:
        err = TracedError("x", status_code=500, trace="0123456789abcdef0123456789abcdef")
        assert err.trace == "0123456789abcdef0123456789abcdef"

    def test_default_code_is_500(self) -> None:
        err = TracedError("x")
        assert err.status_code == 500

    def test_str_contains_message(self) -> None:
        err = TracedError("hello")
        assert "hello" in str(err)


class TestStackCapture:
    def test_stack_is_list_of_frames(self) -> None:
        err = TracedError("oops")
        assert all(isinstance(f, StackFrame) for f in err.stack)
        assert all(isinstance(f.func, str) for f in err.stack)
        assert all(isinstance(f.file, str) for f in err.stack)
        assert all(isinstance(f.line, int) for f in err.stack)

    def test_stack_can_be_overridden(self) -> None:
        custom = [StackFrame(func="f", file="a.py", line=1)]
        err = TracedError("x", stack=custom)
        assert err.stack == custom


class TestToJsonGoSchema:
    def test_emits_error_field_not_msg(self) -> None:
        err = TracedError("oops", status_code=400)
        doc = err.to_json()
        assert doc["error"] == "oops"
        assert "msg" not in doc

    def test_emits_status_code_field(self) -> None:
        err = TracedError("oops", status_code=400)
        doc = err.to_json()
        assert doc["statusCode"] == 400

    def test_omits_status_code_when_zero(self) -> None:
        err = TracedError("oops", status_code=0)
        doc = err.to_json()
        assert "statusCode" not in doc

    def test_emits_trace_when_set(self) -> None:
        err = TracedError("oops", trace="abc123")
        doc = err.to_json()
        assert doc["trace"] == "abc123"

    def test_omits_trace_when_empty(self) -> None:
        err = TracedError("oops")
        doc = err.to_json()
        assert "trace" not in doc

    def test_omits_trace_when_zero(self) -> None:
        err = TracedError("oops", trace="00000000000000000000000000000000")
        doc = err.to_json()
        assert "trace" not in doc

    def test_emits_stack_as_list_of_objects(self) -> None:
        err = TracedError("oops", stack=[StackFrame("f", "a.py", 7)])
        doc = err.to_json()
        assert doc["stack"] == [{"func": "f", "file": "a.py", "line": 7}]

    def test_omits_stack_when_empty(self) -> None:
        err = TracedError("oops", stack=[])
        doc = err.to_json()
        assert "stack" not in doc


class TestFromJsonGoSchema:
    def test_parses_error_field(self) -> None:
        rebuilt = TracedError.from_json({"error": "boom", "statusCode": 502})
        assert rebuilt.message == "boom"
        assert rebuilt.status_code == 502

    def test_parses_stack_objects(self) -> None:
        doc = {
            "error": "x",
            "stack": [
                {"func": "main", "file": "m.py", "line": 1},
                {"func": "handler", "file": "h.py", "line": 22},
            ],
        }
        rebuilt = TracedError.from_json(doc)
        assert len(rebuilt.stack) == 2
        assert rebuilt.stack[0].func == "main"
        assert rebuilt.stack[1].line == 22

    def test_missing_status_code_defaults_to_500(self) -> None:
        rebuilt = TracedError.from_json({"error": "x"})
        assert rebuilt.status_code == 500

    def test_round_trip_preserves_fields(self) -> None:
        original = TracedError(
            "boom",
            status_code=502,
            trace="deadbeef00000000deadbeef00000000",
            stack=[StackFrame("a", "b.py", 3)],
        )
        rebuilt = TracedError.from_json(original.to_json())
        assert rebuilt.message == original.message
        assert rebuilt.status_code == original.status_code
        assert rebuilt.trace == original.trace
        assert rebuilt.stack == original.stack


class TestCauseField:
    def test_to_json_preserves_cause_when_chained(self) -> None:
        try:
            try:
                raise ValueError("inner")
            except ValueError as inner:
                raise TracedError("outer", status_code=500) from inner
        except TracedError as outer:
            doc = outer.to_json()
        assert doc["cause"] == "ValueError: inner"

    def test_to_json_omits_cause_when_no_chain(self) -> None:
        err = TracedError("outer", status_code=500)
        doc = err.to_json()
        assert "cause" not in doc

    def test_to_json_cause_format_is_type_colon_message(self) -> None:
        class MyCustomErr(RuntimeError):
            pass

        try:
            try:
                raise MyCustomErr("specific reason")
            except MyCustomErr as inner:
                raise TracedError("wrap", status_code=500) from inner
        except TracedError as wrapped:
            doc = wrapped.to_json()
        assert doc["cause"] == "MyCustomErr: specific reason"

    def test_to_json_cause_via_explicit_kwarg(self) -> None:
        inner = ValueError("kw-cause")
        err = TracedError("outer", status_code=500, cause=inner)
        doc = err.to_json()
        assert doc["cause"] == "ValueError: kw-cause"

    def test_from_json_round_trips_cause(self) -> None:
        try:
            try:
                raise ValueError("inner")
            except ValueError as inner:
                raise TracedError("outer", status_code=500) from inner
        except TracedError as outer:
            doc = outer.to_json()
        rebuilt = TracedError.from_json(doc)
        assert rebuilt.cause == "ValueError: inner"
        rebuilt_doc = rebuilt.to_json()
        assert rebuilt_doc["cause"] == "ValueError: inner"

    def test_from_json_no_cause_field_yields_none(self) -> None:
        rebuilt = TracedError.from_json({"error": "x"})
        assert rebuilt.cause is None
