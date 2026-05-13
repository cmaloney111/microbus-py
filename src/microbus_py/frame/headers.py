"""Microbus-* HTTP header constants used as wire framing.

Constants mirror github.com/microbus-io/fabric@v1.27.1/frame/frame.go exactly.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = ["H", "OpCode"]


class H(StrEnum):
    """All Microbus-* header names."""

    PREFIX = "Microbus-"
    BAGGAGE_PREFIX = "Microbus-Baggage-"
    MSG_ID = "Microbus-Msg-Id"
    FROM_HOST = "Microbus-From-Host"
    FROM_ID = "Microbus-From-Id"
    FROM_VERSION = "Microbus-From-Version"
    OP_CODE = "Microbus-Op-Code"
    CALL_DEPTH = "Microbus-Call-Depth"
    TIME_BUDGET = "Microbus-Time-Budget"
    LOCALITY = "Microbus-Locality"
    QUEUE = "Microbus-Queue"
    FRAGMENT = "Microbus-Fragment"
    CLOCK_SHIFT = "Microbus-Clock-Shift"
    ACTOR = "Microbus-Actor"


class OpCode(StrEnum):
    """Microbus-Op-Code values."""

    REQ = "Req"
    RES = "Res"
    ACK = "Ack"
    ERR = "Err"
