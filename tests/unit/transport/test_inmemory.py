"""Tests for transport/inmemory.py — in-process NATS-substitute broker."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from microbus_py.transport.inmemory import InMemoryBroker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from microbus_py.transport.base import IncomingMessage


@pytest.mark.asyncio
async def test_publish_delivers_to_one_subscriber() -> None:
    broker = InMemoryBroker()
    received: list[bytes] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.payload)

    await broker.subscribe(subject="x.y", queue=None, cb=handler)
    await broker.publish(subject="x.y", payload=b"hello")
    await asyncio.sleep(0.01)
    assert received == [b"hello"]


@pytest.mark.asyncio
async def test_no_queue_broadcasts_to_all_subscribers() -> None:
    broker = InMemoryBroker()
    a: list[bytes] = []
    b: list[bytes] = []

    async def ha(msg: IncomingMessage) -> None:
        a.append(msg.payload)

    async def hb(msg: IncomingMessage) -> None:
        b.append(msg.payload)

    await broker.subscribe(subject="x", queue=None, cb=ha)
    await broker.subscribe(subject="x", queue=None, cb=hb)
    await broker.publish(subject="x", payload=b"hi")
    await asyncio.sleep(0.01)
    assert a == [b"hi"]
    assert b == [b"hi"]


@pytest.mark.asyncio
async def test_queue_group_load_balances() -> None:
    broker = InMemoryBroker()
    counts = {"a": 0, "b": 0}

    def make_handler(name: str) -> Callable[[IncomingMessage], Awaitable[None]]:
        async def h(_msg: IncomingMessage) -> None:
            counts[name] += 1

        return h

    await broker.subscribe(subject="x", queue="workers", cb=make_handler("a"))
    await broker.subscribe(subject="x", queue="workers", cb=make_handler("b"))
    for _ in range(20):
        await broker.publish(subject="x", payload=b"")
    await asyncio.sleep(0.05)
    # Only one handler per message in a queue group
    assert counts["a"] + counts["b"] == 20


@pytest.mark.asyncio
async def test_request_reply_round_trip() -> None:
    broker = InMemoryBroker()

    async def handler(msg: IncomingMessage) -> None:
        assert msg.reply is not None
        await broker.publish(subject=msg.reply, payload=b"pong")

    await broker.subscribe(subject="ping", queue=None, cb=handler)
    response = await broker.request(subject="ping", payload=b"ping", timeout=1.0)
    assert response.payload == b"pong"


@pytest.mark.asyncio
async def test_request_timeout_raises_when_no_reply() -> None:
    broker = InMemoryBroker()
    with pytest.raises(TimeoutError):
        await broker.request(subject="nowhere", payload=b"", timeout=0.05)


@pytest.mark.asyncio
async def test_unsubscribe_stops_delivery() -> None:
    broker = InMemoryBroker()
    received: list[bytes] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.payload)

    sub = await broker.subscribe(subject="x", queue=None, cb=handler)
    await broker.publish(subject="x", payload=b"first")
    await asyncio.sleep(0.01)
    await sub.unsubscribe()
    await broker.publish(subject="x", payload=b"second")
    await asyncio.sleep(0.01)
    assert received == [b"first"]


@pytest.mark.asyncio
async def test_unsubscribe_drops_subsequent_messages() -> None:
    broker = InMemoryBroker()
    received: list[bytes] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.payload)

    sub = await broker.subscribe(subject="x", queue=None, cb=handler)
    await sub.unsubscribe()
    await broker.publish(subject="x", payload=b"orphan")
    await asyncio.sleep(0.01)
    assert received == []


@pytest.mark.asyncio
async def test_handler_failure_is_dropped_without_loop_exception() -> None:
    broker = InMemoryBroker()
    loop = asyncio.get_running_loop()
    loop_errors: list[object] = []
    handler_calls = 0
    previous_handler = loop.get_exception_handler()

    def record_loop_error(_loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        loop_errors.append(context)

    async def handler(_msg: IncomingMessage) -> None:
        nonlocal handler_calls
        handler_calls += 1
        raise ValueError("boom")

    loop.set_exception_handler(record_loop_error)
    try:
        await broker.subscribe(subject="x", queue=None, cb=handler)
        await broker.publish(subject="x", payload=b"payload")
        await asyncio.sleep(0.01)
    finally:
        loop.set_exception_handler(previous_handler)

    assert handler_calls == 1
    assert loop_errors == []
