"""Tests for wire/ack.py — multicast ack collection.

Microbus's multicast pattern: caller publishes a request and listens on its
reply subject; subscribers send 100/202 ACKs first, then their actual
responses. The collector waits until either all expected ACKs have arrived,
or the deadline fires.

This module models the ACK-collection state machine in pure logic; the
transport layer feeds it.
"""

from __future__ import annotations

import asyncio

import pytest

from microbus_py.wire.ack import AckCollector, AckRecord


class TestAckCollector:
    @pytest.mark.asyncio
    async def test_completes_when_expected_count_reached(self) -> None:
        collector = AckCollector(expected=2, timeout_seconds=1.0)
        collector.add(AckRecord(from_id="a", from_host="svc.a"))
        collector.add(AckRecord(from_id="b", from_host="svc.b"))
        records = await collector.wait()
        assert len(records) == 2
        assert {r.from_id for r in records} == {"a", "b"}

    @pytest.mark.asyncio
    async def test_unbounded_returns_on_timeout(self) -> None:
        collector = AckCollector(expected=None, timeout_seconds=0.05)
        collector.add(AckRecord(from_id="a", from_host="svc.a"))
        records = await collector.wait()
        assert len(records) == 1

    @pytest.mark.asyncio
    async def test_records_after_completion_are_ignored(self) -> None:
        collector = AckCollector(expected=1, timeout_seconds=1.0)
        collector.add(AckRecord(from_id="a", from_host="svc.a"))
        records = await collector.wait()
        assert len(records) == 1
        # Adding more after wait() returned is a no-op
        collector.add(AckRecord(from_id="late", from_host="svc.late"))
        records2 = await collector.wait()
        assert records2 == records

    @pytest.mark.asyncio
    async def test_concurrent_add_during_wait(self) -> None:
        collector = AckCollector(expected=3, timeout_seconds=1.0)

        async def feeder() -> None:
            for i in range(3):
                await asyncio.sleep(0.01)
                collector.add(AckRecord(from_id=f"id-{i}", from_host=f"host-{i}"))

        task = asyncio.create_task(feeder())
        records = await collector.wait()
        await task
        assert len(records) == 3

    @pytest.mark.asyncio
    async def test_zero_expected_completes_immediately(self) -> None:
        collector = AckCollector(expected=0, timeout_seconds=10.0)
        records = await collector.wait()
        assert records == []

    def test_negative_expected_rejected(self) -> None:
        with pytest.raises(ValueError):
            AckCollector(expected=-1, timeout_seconds=1.0)

    def test_negative_timeout_rejected(self) -> None:
        with pytest.raises(ValueError):
            AckCollector(expected=1, timeout_seconds=-0.1)
