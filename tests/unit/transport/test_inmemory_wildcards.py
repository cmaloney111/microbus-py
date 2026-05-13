"""NATS-style wildcard matching for InMemoryBroker."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from microbus_py.transport.inmemory import InMemoryBroker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from microbus_py.transport.base import IncomingMessage


@pytest.mark.asyncio
async def test_star_matches_single_segment() -> None:
    broker = InMemoryBroker()
    received: list[str] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.subject)

    await broker.subscribe(
        subject="microbus.443.example.com.|.POST.cancel.*",
        queue=None,
        cb=handler,
    )
    await broker.publish(
        subject="microbus.443.example.com.|.POST.cancel.abcd-1234",
        payload=b"",
    )
    await asyncio.sleep(0.01)
    assert received == ["microbus.443.example.com.|.POST.cancel.abcd-1234"]


@pytest.mark.asyncio
async def test_star_does_not_match_multiple_segments() -> None:
    broker = InMemoryBroker()
    received: list[str] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.subject)

    await broker.subscribe(subject="microbus.443.a.|.GET.*", queue=None, cb=handler)
    await broker.publish(subject="microbus.443.a.|.GET.foo.bar", payload=b"")
    await asyncio.sleep(0.01)
    assert received == []


@pytest.mark.asyncio
async def test_gt_matches_rest_of_subject() -> None:
    broker = InMemoryBroker()
    received: list[str] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.subject)

    await broker.subscribe(subject="microbus.443.a.|.GET.>", queue=None, cb=handler)
    await broker.publish(subject="microbus.443.a.|.GET.foo.bar/baz", payload=b"")
    await asyncio.sleep(0.01)
    assert received == ["microbus.443.a.|.GET.foo.bar/baz"]


@pytest.mark.asyncio
async def test_exact_subject_still_delivers() -> None:
    broker = InMemoryBroker()
    received: list[bytes] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.payload)

    await broker.subscribe(subject="x.y.z", queue=None, cb=handler)
    await broker.publish(subject="x.y.z", payload=b"hi")
    await asyncio.sleep(0.01)
    assert received == [b"hi"]


@pytest.mark.asyncio
async def test_star_in_middle_segment() -> None:
    broker = InMemoryBroker()
    received: list[str] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.subject)

    await broker.subscribe(subject="microbus.443.a.|.*.foo", queue=None, cb=handler)
    await broker.publish(subject="microbus.443.a.|.GET.foo", payload=b"")
    await asyncio.sleep(0.01)
    assert received == ["microbus.443.a.|.GET.foo"]


@pytest.mark.asyncio
async def test_queue_group_with_wildcard_load_balances() -> None:
    broker = InMemoryBroker()
    counts = {"a": 0, "b": 0}

    def make_handler(name: str) -> Callable[[IncomingMessage], Awaitable[None]]:
        async def h(_msg: IncomingMessage) -> None:
            counts[name] += 1

        return h

    await broker.subscribe(subject="cancel.*", queue="workers", cb=make_handler("a"))
    await broker.subscribe(subject="cancel.*", queue="workers", cb=make_handler("b"))
    for i in range(20):
        await broker.publish(subject=f"cancel.job-{i}", payload=b"")
    await asyncio.sleep(0.05)
    assert counts["a"] + counts["b"] == 20
    assert counts["a"] > 0
    assert counts["b"] > 0


@pytest.mark.asyncio
async def test_publish_no_match_is_noop() -> None:
    broker = InMemoryBroker()
    received: list[bytes] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.payload)

    await broker.subscribe(subject="a.b.c", queue=None, cb=handler)
    await broker.publish(subject="x.y.z", payload=b"orphan")
    await asyncio.sleep(0.01)
    assert received == []


@pytest.mark.asyncio
async def test_star_segment_treated_as_wildcard_not_literal() -> None:
    broker = InMemoryBroker()
    received: list[str] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg.subject)

    await broker.subscribe(subject="a.*.c", queue=None, cb=handler)
    await broker.publish(subject="a.*.c", payload=b"literal-star")
    await broker.publish(subject="a.middle.c", payload=b"normal")
    await asyncio.sleep(0.01)
    assert sorted(received) == ["a.*.c", "a.middle.c"]
