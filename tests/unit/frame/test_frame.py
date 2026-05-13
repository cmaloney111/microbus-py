"""Tests for frame/frame.py — Frame accessors over Microbus-* headers."""

from __future__ import annotations

from datetime import timedelta

import pytest

from microbus_py.frame.frame import Frame


class TestFrameAccessors:
    def test_msg_id_round_trip(self) -> None:
        f = Frame()
        f.set_msg_id("abc123")
        assert f.msg_id == "abc123"

    def test_from_host_round_trip(self) -> None:
        f = Frame()
        f.set_from_host("svc.example")
        assert f.from_host == "svc.example"

    def test_from_id_round_trip(self) -> None:
        f = Frame()
        f.set_from_id("inst-7")
        assert f.from_id == "inst-7"

    def test_from_version_int_round_trip(self) -> None:
        f = Frame()
        f.set_from_version(42)
        assert f.from_version == 42

    def test_from_version_default_zero(self) -> None:
        f = Frame()
        assert f.from_version == 0

    def test_op_code_round_trip(self) -> None:
        f = Frame()
        f.set_op_code("Req")
        assert f.op_code == "Req"

    def test_call_depth_round_trip(self) -> None:
        f = Frame()
        f.set_call_depth(3)
        assert f.call_depth == 3

    def test_call_depth_default_zero(self) -> None:
        f = Frame()
        assert f.call_depth == 0

    def test_time_budget_round_trip(self) -> None:
        f = Frame()
        f.set_time_budget(timedelta(seconds=2, milliseconds=500))
        assert f.time_budget == timedelta(milliseconds=2500)

    def test_time_budget_zero_clears_header(self) -> None:
        f = Frame({"Microbus-Time-Budget": "1000"})
        f.set_time_budget(timedelta(0))
        assert f.time_budget == timedelta(0)
        assert "Microbus-Time-Budget" not in f.headers

    def test_locality_round_trip(self) -> None:
        f = Frame()
        f.set_locality("a.1.east.us")
        assert f.locality == "a.1.east.us"

    def test_queue_round_trip(self) -> None:
        f = Frame()
        f.set_queue("workers")
        assert f.queue == "workers"

    def test_queue_empty_clears(self) -> None:
        f = Frame({"Microbus-Queue": "x"})
        f.set_queue("")
        assert "Microbus-Queue" not in f.headers

    def test_fragment_round_trip(self) -> None:
        f = Frame()
        f.set_fragment(2, 5)
        idx, total = f.fragment
        assert (idx, total) == (2, 5)

    def test_fragment_default_one_of_one(self) -> None:
        f = Frame()
        assert f.fragment == (1, 1)

    def test_fragment_single_clears_header(self) -> None:
        f = Frame()
        f.set_fragment(1, 1)
        assert "Microbus-Fragment" not in f.headers

    def test_fragment_invalid_string_returns_default(self) -> None:
        f = Frame({"Microbus-Fragment": "garbage"})
        assert f.fragment == (1, 1)

    def test_clock_shift_signed_duration(self) -> None:
        f = Frame()
        f.set_clock_shift(timedelta(seconds=-5))
        assert f.clock_shift == timedelta(seconds=-5)

    def test_clock_shift_zero_clears(self) -> None:
        f = Frame({"Microbus-Clock-Shift": "5s"})
        f.set_clock_shift(timedelta(0))
        assert "Microbus-Clock-Shift" not in f.headers


class TestBaggage:
    def test_baggage_round_trip(self) -> None:
        f = Frame()
        f.set_baggage("RequestId", "abc-123")
        assert f.baggage["RequestId"] == "abc-123"

    def test_baggage_collects_all_prefixed_headers(self) -> None:
        f = Frame(
            {
                "Microbus-Baggage-A": "1",
                "Microbus-Baggage-B": "2",
                "X-Other": "ignored",
            }
        )
        bg = f.baggage
        assert bg == {"A": "1", "B": "2"}

    def test_baggage_delete(self) -> None:
        f = Frame({"Microbus-Baggage-Foo": "x"})
        f.set_baggage("Foo", "")
        assert "Foo" not in f.baggage


class TestZeroValueClears:
    def test_set_msg_id_empty_clears(self) -> None:
        f = Frame({"Microbus-Msg-Id": "x"})
        f.set_msg_id("")
        assert "Microbus-Msg-Id" not in f.headers

    def test_set_from_host_empty_clears(self) -> None:
        f = Frame({"Microbus-From-Host": "x"})
        f.set_from_host("")
        assert "Microbus-From-Host" not in f.headers

    def test_set_from_id_empty_clears(self) -> None:
        f = Frame({"Microbus-From-Id": "x"})
        f.set_from_id("")
        assert "Microbus-From-Id" not in f.headers

    def test_set_op_code_empty_clears(self) -> None:
        f = Frame({"Microbus-Op-Code": "Req"})
        f.set_op_code("")
        assert "Microbus-Op-Code" not in f.headers

    def test_set_locality_empty_clears(self) -> None:
        f = Frame({"Microbus-Locality": "us"})
        f.set_locality("")
        assert "Microbus-Locality" not in f.headers

    def test_set_call_depth_zero_clears(self) -> None:
        f = Frame({"Microbus-Call-Depth": "5"})
        f.set_call_depth(0)
        assert "Microbus-Call-Depth" not in f.headers

    def test_set_from_version_zero_clears(self) -> None:
        f = Frame({"Microbus-From-Version": "5"})
        f.set_from_version(0)
        assert "Microbus-From-Version" not in f.headers


class TestMalformedHeaderValues:
    def test_invalid_call_depth_returns_zero(self) -> None:
        f = Frame({"Microbus-Call-Depth": "not-a-number"})
        assert f.call_depth == 0

    def test_invalid_from_version_returns_zero(self) -> None:
        f = Frame({"Microbus-From-Version": "garbage"})
        assert f.from_version == 0

    def test_invalid_time_budget_returns_zero(self) -> None:
        f = Frame({"Microbus-Time-Budget": "garbage"})
        assert f.time_budget == timedelta(0)

    def test_fragment_three_part_returns_default(self) -> None:
        f = Frame({"Microbus-Fragment": "1/2/3"})
        assert f.fragment == (1, 1)

    def test_fragment_non_numeric_returns_default(self) -> None:
        f = Frame({"Microbus-Fragment": "a/b"})
        assert f.fragment == (1, 1)

    def test_clock_shift_invalid_returns_zero(self) -> None:
        f = Frame({"Microbus-Clock-Shift": "garbage"})
        assert f.clock_shift == timedelta(0)


class TestDurationParsing:
    def test_clock_shift_milliseconds(self) -> None:
        f = Frame()
        f.set_clock_shift(timedelta(milliseconds=500))
        assert f.headers["Microbus-Clock-Shift"] == "500ms"

    def test_clock_shift_seconds_round_trip(self) -> None:
        f = Frame({"Microbus-Clock-Shift": "5s"})
        assert f.clock_shift == timedelta(seconds=5)

    def test_clock_shift_minutes(self) -> None:
        f = Frame({"Microbus-Clock-Shift": "2m"})
        assert f.clock_shift == timedelta(minutes=2)

    def test_clock_shift_hours(self) -> None:
        f = Frame({"Microbus-Clock-Shift": "3h"})
        assert f.clock_shift == timedelta(hours=3)

    def test_clock_shift_negative(self) -> None:
        f = Frame({"Microbus-Clock-Shift": "-1h"})
        assert f.clock_shift == timedelta(hours=-1)

    def test_clock_shift_microseconds(self) -> None:
        f = Frame({"Microbus-Clock-Shift": "100us"})
        assert f.clock_shift == timedelta(microseconds=100)


class TestFromRequest:
    def test_from_dict_round_trip(self) -> None:
        # The Frame works over a plain header mapping; from_request is convenience
        headers = {"Microbus-Msg-Id": "X1", "Microbus-From-Host": "svc"}
        f = Frame(headers)
        assert f.msg_id == "X1"
        assert f.from_host == "svc"


class TestFrameValidation:
    def test_set_negative_call_depth_rejected(self) -> None:
        f = Frame()
        with pytest.raises(ValueError):
            f.set_call_depth(-1)

    def test_set_negative_version_rejected(self) -> None:
        f = Frame()
        with pytest.raises(ValueError):
            f.set_from_version(-1)
