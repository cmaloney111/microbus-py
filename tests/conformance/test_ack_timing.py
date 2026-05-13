"""Raw NATS ACK timing checks against the Go sidecar."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from microbus_py.frame.frame import Frame
from microbus_py.frame.headers import H, OpCode
from microbus_py.transport.nats import NATSTransport
from microbus_py.wire.codec import HTTPRequest, decode_response, encode_request
from microbus_py.wire.subjects import request_subject, response_subject

if TYPE_CHECKING:
    from microbus_py.transport.base import IncomingMessage
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

_CALLER_HOST = "ack-tester.example"
_CALLER_ID = "ack-tester-1"
_MSG_ID = "ack-timing-1"
_ACK_DEADLINE_SECONDS = 1.0
_FINAL_DEADLINE_SECONDS = 5.0
_SLOW_HANDLER_MIN_DELAY_SECONDS = 0.25


@dataclass(frozen=True, slots=True)
class TimedResponse:
    elapsed: float
    status_code: int
    op_code: str
    body: bytes


async def _next_response(
    queue: asyncio.Queue[IncomingMessage],
    *,
    started_at: float,
    timeout: float,
) -> TimedResponse:
    msg = await asyncio.wait_for(queue.get(), timeout=timeout)
    elapsed = time.monotonic() - started_at
    resp = decode_response(msg.payload)
    frame = Frame(dict(resp.headers))
    return TimedResponse(
        elapsed=elapsed,
        status_code=resp.status_code,
        op_code=frame.op_code,
        body=resp.body,
    )


async def test_go_sidecar_sends_ack_before_slow_handler_finishes(
    conformance_env: ConformanceEnv,
) -> None:
    transport = NATSTransport()
    await transport.connect(conformance_env.nats_url, name=_CALLER_HOST)
    queue: asyncio.Queue[IncomingMessage] = asyncio.Queue()
    sub = await transport.subscribe(
        subject=response_subject(
            plane=conformance_env.plane,
            hostname=_CALLER_HOST,
            instance_id=_CALLER_ID,
        ),
        queue=_CALLER_ID,
        cb=queue.put,
    )
    try:
        req = HTTPRequest(
            method="POST",
            url="https://echo.example:443/slow-echo",
            headers=[
                (H.MSG_ID, _MSG_ID),
                (H.FROM_HOST, _CALLER_HOST),
                (H.FROM_ID, _CALLER_ID),
                (H.OP_CODE, OpCode.REQ),
            ],
            body=b"ack timing",
        )
        started_at = time.monotonic()
        await transport.publish(
            subject=request_subject(
                plane=conformance_env.plane,
                port="443",
                hostname="echo.example",
                method="POST",
                path="/slow-echo",
            ),
            payload=encode_request(req),
        )

        ack = await _next_response(
            queue,
            started_at=started_at,
            timeout=_ACK_DEADLINE_SECONDS,
        )
        final = await _next_response(
            queue,
            started_at=started_at,
            timeout=_FINAL_DEADLINE_SECONDS,
        )
    finally:
        await sub.unsubscribe()
        await transport.close()

    assert ack.op_code == OpCode.ACK
    assert ack.status_code == 202
    assert ack.body == b""
    assert final.op_code == OpCode.RES
    assert final.status_code == 200
    assert final.body == b"ack timing"
    assert final.elapsed - ack.elapsed >= _SLOW_HANDLER_MIN_DELAY_SECONDS
