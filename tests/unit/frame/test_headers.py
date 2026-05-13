"""Tests for frame/headers.py — Microbus-* header constants."""

from __future__ import annotations

from microbus_py.frame.headers import H, OpCode


class TestHeaderConstants:
    def test_msg_id_constant(self) -> None:
        assert H.MSG_ID.value == "Microbus-Msg-Id"

    def test_from_host(self) -> None:
        assert H.FROM_HOST.value == "Microbus-From-Host"

    def test_from_id(self) -> None:
        assert H.FROM_ID.value == "Microbus-From-Id"

    def test_from_version(self) -> None:
        assert H.FROM_VERSION.value == "Microbus-From-Version"

    def test_op_code(self) -> None:
        assert H.OP_CODE.value == "Microbus-Op-Code"

    def test_call_depth(self) -> None:
        assert H.CALL_DEPTH.value == "Microbus-Call-Depth"

    def test_time_budget(self) -> None:
        assert H.TIME_BUDGET.value == "Microbus-Time-Budget"

    def test_locality(self) -> None:
        assert H.LOCALITY.value == "Microbus-Locality"

    def test_queue(self) -> None:
        assert H.QUEUE.value == "Microbus-Queue"

    def test_fragment(self) -> None:
        assert H.FRAGMENT.value == "Microbus-Fragment"

    def test_clock_shift(self) -> None:
        assert H.CLOCK_SHIFT.value == "Microbus-Clock-Shift"

    def test_actor(self) -> None:
        assert H.ACTOR.value == "Microbus-Actor"

    def test_baggage_prefix(self) -> None:
        assert H.BAGGAGE_PREFIX.value == "Microbus-Baggage-"


class TestOpCode:
    def test_request(self) -> None:
        assert OpCode.REQ.value == "Req"

    def test_response(self) -> None:
        assert OpCode.RES.value == "Res"

    def test_ack(self) -> None:
        assert OpCode.ACK.value == "Ack"

    def test_error(self) -> None:
        assert OpCode.ERR.value == "Err"
