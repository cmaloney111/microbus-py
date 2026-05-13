"""Tests for wire/fragments.py — payload chunking and reassembly.

Microbus splits payloads larger than the NATS server's max_payload (default
1 MiB) into ordered fragments. Each fragment carries a `Microbus-Fragment: N/M`
header. The receiver reassembles by collecting all M fragments and
concatenating their bodies in index order.

Our implementation is body-only: callers wrap the raw payload bytes; the
codec layer attaches the framing header.
"""

from __future__ import annotations

import time

import pytest

from microbus_py.wire.fragments import FRAGMENT_THRESHOLD, Reassembler, split


class TestFragmentThreshold:
    def test_default_threshold_is_one_mib(self) -> None:
        assert FRAGMENT_THRESHOLD == 1 << 20


class TestSplit:
    def test_below_threshold_returns_single_chunk(self) -> None:
        payload = b"x" * 1024
        chunks = split(payload, max_size=4096)
        assert chunks == [payload]

    def test_at_threshold_returns_single_chunk(self) -> None:
        payload = b"x" * 4096
        chunks = split(payload, max_size=4096)
        assert chunks == [payload]

    def test_above_threshold_splits_evenly(self) -> None:
        payload = b"a" * 4 + b"b" * 4 + b"c" * 4  # 12 bytes
        chunks = split(payload, max_size=4)
        assert chunks == [b"aaaa", b"bbbb", b"cccc"]

    def test_above_threshold_with_remainder(self) -> None:
        payload = b"x" * 10
        chunks = split(payload, max_size=4)
        assert chunks == [b"xxxx", b"xxxx", b"xx"]

    def test_concatenated_chunks_recover_payload(self) -> None:
        payload = bytes(range(256)) * 17
        chunks = split(payload, max_size=512)
        assert b"".join(chunks) == payload

    def test_zero_max_size_rejected(self) -> None:
        with pytest.raises(ValueError):
            split(b"abc", max_size=0)

    def test_negative_max_size_rejected(self) -> None:
        with pytest.raises(ValueError):
            split(b"abc", max_size=-1)


class TestReassembler:
    def test_in_order_complete(self) -> None:
        r = Reassembler()
        assert r.add(idx=1, total=3, chunk=b"aaa") is None
        assert r.add(idx=2, total=3, chunk=b"bbb") is None
        result = r.add(idx=3, total=3, chunk=b"ccc")
        assert result == b"aaabbbccc"

    def test_out_of_order_complete(self) -> None:
        r = Reassembler()
        assert r.add(idx=2, total=3, chunk=b"bbb") is None
        assert r.add(idx=3, total=3, chunk=b"ccc") is None
        result = r.add(idx=1, total=3, chunk=b"aaa")
        assert result == b"aaabbbccc"

    def test_missing_fragment_returns_none(self) -> None:
        r = Reassembler()
        assert r.add(idx=1, total=3, chunk=b"aaa") is None
        assert r.add(idx=3, total=3, chunk=b"ccc") is None
        # idx=2 never arrives
        assert r.complete is False

    def test_single_fragment_completes_immediately(self) -> None:
        r = Reassembler()
        result = r.add(idx=1, total=1, chunk=b"only")
        assert result == b"only"

    def test_total_mismatch_raises(self) -> None:
        r = Reassembler()
        r.add(idx=1, total=3, chunk=b"aaa")
        with pytest.raises(ValueError):
            r.add(idx=2, total=5, chunk=b"bbb")

    def test_duplicate_index_idempotent(self) -> None:
        # If the same fragment arrives twice, behaviour is to ignore the duplicate
        r = Reassembler()
        r.add(idx=1, total=2, chunk=b"aaa")
        r.add(idx=1, total=2, chunk=b"aaa")  # duplicate, no effect
        result = r.add(idx=2, total=2, chunk=b"bbb")
        assert result == b"aaabbb"

    def test_duplicate_completing_fragment_returns_payload(self) -> None:
        # Edge case: receive idx=2 then idx=2 again — second arrival should still
        # report the assembled payload once idx=1 has also arrived.
        r = Reassembler()
        r.add(idx=1, total=2, chunk=b"aaa")
        # Send idx=2 to complete; subsequent duplicate should not crash
        result1 = r.add(idx=2, total=2, chunk=b"bbb")
        assert result1 == b"aaabbb"
        result2 = r.add(idx=2, total=2, chunk=b"bbb")  # duplicate after complete
        assert result2 == b"aaabbb"

    def test_total_zero_rejected(self) -> None:
        r = Reassembler()
        with pytest.raises(ValueError):
            r.add(idx=1, total=0, chunk=b"x")

    def test_invalid_index_zero_raises(self) -> None:
        r = Reassembler()
        with pytest.raises(ValueError):
            r.add(idx=0, total=2, chunk=b"x")

    def test_invalid_index_above_total_raises(self) -> None:
        r = Reassembler()
        with pytest.raises(ValueError):
            r.add(idx=3, total=2, chunk=b"x")

    def test_expired_after_deadline(self) -> None:
        r = Reassembler(deadline_seconds=0.01)
        r.add(idx=1, total=2, chunk=b"aaa")
        time.sleep(0.05)
        assert r.expired() is True

    def test_not_expired_before_deadline(self) -> None:
        r = Reassembler(deadline_seconds=10)
        r.add(idx=1, total=2, chunk=b"aaa")
        assert r.expired() is False
