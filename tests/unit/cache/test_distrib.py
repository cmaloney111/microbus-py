"""Tests for connector-owned distributed cache parity."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from microbus_py.cache import distrib as distrib_module
from microbus_py.connector.connector import Connector
from microbus_py.frame.frame import Frame
from microbus_py.frame.headers import H, OpCode
from microbus_py.transport.inmemory import InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_request, encode_response
from microbus_py.wire.subjects import request_subject, response_subject

if TYPE_CHECKING:
    from microbus_py.transport.base import IncomingMessage


async def _replicas(
    hostname: str = "cache.example",
) -> tuple[Connector, Connector, Connector]:
    transport = InMemoryBroker()
    alpha = Connector(hostname=hostname, instance_id="alpha", transport=transport)
    beta = Connector(hostname=hostname, instance_id="beta", transport=transport)
    gamma = Connector(hostname=hostname, instance_id="gamma", transport=transport)
    await alpha.startup()
    await beta.startup()
    await gamma.startup()
    return alpha, beta, gamma


async def _shutdown(*connectors: Connector) -> None:
    for connector in reversed(connectors):
        await connector.shutdown()


@pytest.mark.asyncio
async def test_connector_owned_cache_loads_from_peer_replica() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("A", b"AAA")

        assert alpha.distrib_cache.local_len() == 1
        assert beta.distrib_cache.local_len() == 0
        assert gamma.distrib_cache.local_len() == 0
        assert await beta.distrib_cache.load("A") == b"AAA"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_load_returns_none_on_miss() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        assert await beta.distrib_cache.load("missing") is None
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_local_store_then_load_round_trips() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("k", b"hello")

        assert await alpha.distrib_cache.load("k") == b"hello"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_get_and_set_aliases_round_trip() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.set("k", b"hello", ttl=60.0)

        assert await alpha.distrib_cache.get("k") == b"hello"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_replicated_store_writes_to_peer_local_shards() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("k", b"v", replicate=True)

        assert alpha.distrib_cache.local_len() == 1
        assert beta.distrib_cache.local_len() == 1
        assert gamma.distrib_cache.local_len() == 1
        assert await gamma.distrib_cache.load("k") == b"v"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_local_inconsistency_deletes_local_copy() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("k", b"good", replicate=True)
        beta.distrib_cache._store["k"] = b"bad"

        assert await alpha.distrib_cache.load("k") is None
        assert alpha.distrib_cache.local_len() == 0
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_remote_inconsistency_returns_miss() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        alpha.distrib_cache._store["k"] = b"one"
        beta.distrib_cache._store["k"] = b"two"

        assert await gamma.distrib_cache.load("k") is None
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_delete_removes_key_from_all_replicas() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("k", b"v", replicate=True)
        await gamma.distrib_cache.delete("k")

        assert await alpha.distrib_cache.load("k") is None
        assert await beta.distrib_cache.load("k") is None
        assert await gamma.distrib_cache.load("k") is None
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_delete_prefix_removes_matching_keys_from_all_replicas() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("prefix.1", b"a")
        await beta.distrib_cache.store("prefix.2", b"b")
        await gamma.distrib_cache.store("other.1", b"c")

        await gamma.distrib_cache.delete_prefix("prefix.")

        assert await alpha.distrib_cache.load("prefix.1") is None
        assert await beta.distrib_cache.load("prefix.2") is None
        assert await gamma.distrib_cache.load("other.1") == b"c"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_delete_contains_removes_matching_keys_from_all_replicas() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("alpha.1.end", b"a")
        await beta.distrib_cache.store("beta.2.end", b"b")
        await gamma.distrib_cache.store("gamma.keep", b"c")

        await gamma.distrib_cache.delete_contains(".end")

        assert await alpha.distrib_cache.load("alpha.1.end") is None
        assert await beta.distrib_cache.load("beta.2.end") is None
        assert await gamma.distrib_cache.load("gamma.keep") == b"c"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_clear_empties_all_replicas() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("a", b"1", replicate=True)
        await beta.distrib_cache.store("b", b"2", replicate=True)

        await gamma.distrib_cache.clear()

        assert alpha.distrib_cache.local_len() == 0
        assert beta.distrib_cache.local_len() == 0
        assert gamma.distrib_cache.local_len() == 0
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_shutdown_offloads_cache_entries_to_peer_replicas() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("a", b"1")
        await alpha.distrib_cache.store("b", b"2")
        await alpha.distrib_cache.store("c", b"3")

        await alpha.shutdown()

        assert alpha.distrib_cache.local_len() == 0
        assert beta.distrib_cache.local_len() + gamma.distrib_cache.local_len() == 3
        assert await beta.distrib_cache.load("a") == b"1"
        assert await beta.distrib_cache.load("b") == b"2"
        assert await beta.distrib_cache.load("c") == b"3"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_peek_and_has_do_not_require_local_shard() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("k", b"v")

        assert await beta.distrib_cache.peek("k") == b"v"
        assert await gamma.distrib_cache.has("k") is True
        assert await gamma.distrib_cache.has("missing") is False
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_len_and_weight_sum_all_replicas() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        await alpha.distrib_cache.store("a", b"11", replicate=True)
        await beta.distrib_cache.store("b", b"222")

        assert await gamma.distrib_cache.len() == 4
        assert await gamma.distrib_cache.weight() == 9
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_len_and_weight_ignore_non_ok_replies(monkeypatch: pytest.MonkeyPatch) -> None:
    alpha, beta, gamma = await _replicas()
    try:

        async def fake_publish_all(
            method: str,
            query: dict[str, str],
            *,
            body: bytes = b"",
        ) -> list[HTTPResponse]:
            _ = method
            _ = query
            _ = body
            return [HTTPResponse(status_code=500, reason="Server Error")]

        monkeypatch.setattr(alpha.distrib_cache, "_publish_all", fake_publish_all)

        assert await alpha.distrib_cache.len() == 0
        assert await alpha.distrib_cache.weight() == 0
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_publish_collect_returns_empty_when_no_handler_acks() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        responses = await alpha.distrib_cache._publish_collect(
            "GET",
            "https://cache.example:888/dcache/no-such-handler",
            b"",
        )

        assert responses == []
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_publish_collect_ignores_invalid_wrong_and_duplicate_acks() -> None:
    alpha, beta, gamma = await _replicas()
    subject = request_subject(
        plane=alpha.plane,
        port="888",
        hostname=alpha.hostname,
        method="GET",
        path="/dcache/manual",
    )
    reply = response_subject(
        plane=alpha.plane,
        hostname=alpha.hostname,
        instance_id=alpha.instance_id,
    )

    async def responder(msg: IncomingMessage) -> None:
        req = decode_request(msg.payload)
        msg_id = Frame(dict(req.headers)).msg_id
        assert msg_id is not None
        ack = _resp(status=202, msg_id=msg_id, from_id="observer", op_code=OpCode.ACK)
        await alpha.transport.publish(subject=reply, payload=b"not an http response")
        await alpha.transport.publish(subject=reply, payload=encode_response(_resp(msg_id="wrong")))
        await alpha.transport.publish(subject=reply, payload=encode_response(ack))
        await alpha.transport.publish(subject=reply, payload=encode_response(ack))

    sub = await alpha.transport.subscribe(subject=subject, queue=None, cb=responder)
    try:
        responses = await alpha.distrib_cache._publish_collect(
            "GET",
            "https://cache.example:888/dcache/manual",
            b"",
        )

        assert responses == []
    finally:
        await sub.unsubscribe()
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_publish_collect_completes_after_late_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(distrib_module, "_ACK_WINDOW_SECONDS", 0.01)
    monkeypatch.setattr(distrib_module, "_COLLECT_WINDOW_SECONDS", 0.2)
    alpha, beta, gamma = await _replicas()
    subject = request_subject(
        plane=alpha.plane,
        port="888",
        hostname=alpha.hostname,
        method="GET",
        path="/dcache/late",
    )
    reply = response_subject(
        plane=alpha.plane,
        hostname=alpha.hostname,
        instance_id=alpha.instance_id,
    )

    async def responder(msg: IncomingMessage) -> None:
        req = decode_request(msg.payload)
        msg_id = Frame(dict(req.headers)).msg_id
        assert msg_id is not None
        await alpha.transport.publish(
            subject=reply,
            payload=encode_response(
                _resp(status=202, msg_id=msg_id, from_id="observer", op_code=OpCode.ACK)
            ),
        )
        await asyncio.sleep(0.03)
        await alpha.transport.publish(
            subject=reply,
            payload=encode_response(_resp(msg_id=msg_id, from_id="observer", body=b"done")),
        )

    sub = await alpha.transport.subscribe(subject=subject, queue=None, cb=responder)
    try:
        responses = await alpha.distrib_cache._publish_collect(
            "GET",
            "https://cache.example:888/dcache/late",
            b"",
        )

        assert [resp.body for resp in responses] == [b"done"]
    finally:
        await sub.unsubscribe()
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_cache_handlers_reject_invalid_actions_and_missing_keys() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        cache = alpha.distrib_cache

        invalid = await cache._handle_all(_req("/dcache/all?do=nope"))
        missing_load = await cache._handle_all(_req("/dcache/all?do=load"))
        missing_store = await cache._handle_all(_req("/dcache/all?do=store"))
        missing_checksum = await cache._handle_all(_req("/dcache/all?do=checksum&key=k"))
        missing_delete = await cache._handle_all(_req("/dcache/all?do=delete"))
        missing_rescue = await cache._handle_rescue(_req("/dcache/rescue"))

        assert invalid.status_code == 400
        assert missing_load.status_code == 400
        assert missing_store.status_code == 400
        assert missing_checksum.status_code == 400
        assert missing_delete.status_code == 400
        assert missing_rescue.status_code == 400
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_rescue_ignores_self_and_accepts_peer_payload() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        self_resp = await alpha.distrib_cache._handle_rescue(
            _req("/dcache/rescue?key=k", from_id="alpha", body=b"self"),
        )
        peer_resp = await alpha.distrib_cache._handle_rescue(
            _req("/dcache/rescue?key=k", from_id="peer", body=b"peer"),
        )

        assert self_resp.status_code == 200
        assert peer_resp.status_code == 200
        assert alpha.distrib_cache._store["k"] == b"peer"
    finally:
        await _shutdown(alpha, beta, gamma)


@pytest.mark.asyncio
async def test_empty_key_raises() -> None:
    alpha, beta, gamma = await _replicas()
    try:
        with pytest.raises(ValueError, match="key"):
            await alpha.distrib_cache.store("", b"v")
        with pytest.raises(ValueError, match="key"):
            await alpha.distrib_cache.load("")
        with pytest.raises(ValueError, match="prefix"):
            await alpha.distrib_cache.delete_prefix("")
        with pytest.raises(ValueError, match="substring"):
            await alpha.distrib_cache.delete_contains("")
    finally:
        await _shutdown(alpha, beta, gamma)


def _req(path: str, *, from_id: str = "other", body: bytes = b"") -> HTTPRequest:
    return HTTPRequest(
        method="GET",
        url=f"https://cache.example:888{path}",
        headers=[
            ("Microbus-Msg-Id", "test"),
            ("Microbus-From-Host", "cache.example"),
            ("Microbus-From-Id", from_id),
            ("Microbus-Op-Code", "Req"),
        ],
        body=body,
    )


def _resp(
    *,
    status: int = 200,
    msg_id: str,
    from_id: str = "responder",
    op_code: OpCode = OpCode.RES,
    body: bytes = b"",
) -> HTTPResponse:
    reasons = {200: "OK", 202: "Accepted", 500: "Server Error"}
    return HTTPResponse(
        status_code=status,
        reason=reasons[status],
        headers=[
            (H.MSG_ID, msg_id),
            (H.FROM_HOST, "cache.example"),
            (H.FROM_ID, from_id),
            (H.OP_CODE, op_code),
        ],
        body=body,
    )
