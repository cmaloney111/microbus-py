"""In-process broker — the unit-test transport.

A single-process pub/sub bus that mirrors the API of :class:`Transport` so
the connector and decorators can be exercised without a real NATS server.

Subjects support exact-match delivery and NATS-style wildcards: ``*`` matches
exactly one segment, ``>`` (only as the last segment) matches one-or-more
trailing segments.

Queue groups: subscribers sharing a ``queue`` name form a load-balanced
group — only one of them receives any given message. Subscribers with
``queue=None`` form their own broadcast tier and each receives every
message published to the subject.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from microbus_py.transport.base import (
    IncomingMessage,
    MessageHandler,
    Subscription,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["InMemoryBroker", "InMemorySubscription"]

_SUBSCRIBER_FAILURES: tuple[type[Exception], ...] = (Exception,)


def _subject_matches(pattern_segments: list[str], subject_segments: list[str]) -> bool:
    n = len(pattern_segments)
    m = len(subject_segments)
    for i, seg in enumerate(pattern_segments):
        if seg == ">":
            return i == n - 1 and m >= n
        if i >= m:
            return False
        if seg == "*":
            continue
        if seg != subject_segments[i]:
            return False
    return n == m


@dataclass(slots=True)
class _SubRecord:
    subject: str
    queue: str | None
    cb: MessageHandler
    sid: str


class InMemorySubscription:
    """Subscription handle returned by ``InMemoryBroker.subscribe``."""

    def __init__(self, broker: InMemoryBroker, sid: str) -> None:
        self._broker = broker
        self._sid = sid

    async def unsubscribe(self) -> None:
        await self._broker._unsubscribe(self._sid)


class InMemoryBroker:
    """In-process broker satisfying the :class:`Transport` protocol."""

    def __init__(self) -> None:
        self._subs: dict[str, _SubRecord] = {}
        self._by_subject: dict[str, list[str]] = defaultdict(list)
        self._wildcard_sids: dict[str, list[str]] = {}
        self._next_queue_pick: dict[tuple[str, str], int] = {}
        self._closed = False

    async def connect(self, url: str = "", *, name: str = "") -> None:
        # In-memory broker has no remote — connect is a no-op kept for
        # protocol parity with NATSTransport.
        return None

    async def close(self) -> None:
        return None

    def max_payload(self) -> int:
        return 1 << 20

    async def publish(
        self,
        *,
        subject: str,
        payload: bytes,
        reply: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        if self._closed:
            return
        sids = self._sids_for_subject(subject)
        if not sids:
            return
        targets = self._select_targets(sids)

        msg = IncomingMessage(
            subject=subject,
            payload=payload,
            reply=reply,
            headers=dict(headers) if headers else {},
        )
        for sid in targets:
            rec = self._subs.get(sid)
            if rec is not None:
                self._spawn(rec.cb, msg)

    def _sids_for_subject(self, subject: str) -> list[str]:
        sids = list(self._by_subject.get(subject, ()))
        if self._wildcard_sids:
            subject_segments = subject.split(".")
            for pattern, wsids in self._wildcard_sids.items():
                if _subject_matches(pattern.split("."), subject_segments):
                    sids.extend(wsids)
        return sids

    def _select_targets(self, sids: list[str]) -> list[str]:
        broadcast: list[str] = []
        per_queue: dict[tuple[str, str], list[str]] = defaultdict(list)
        for sid in sids:
            rec = self._subs.get(sid)
            if rec is None:
                continue
            if rec.queue is None:
                broadcast.append(sid)
            else:
                per_queue[(rec.subject, rec.queue)].append(sid)
        targets = list(broadcast)
        for (pattern, queue), members in per_queue.items():
            picked = self._round_robin(pattern, queue, members)
            if picked is not None:
                targets.append(picked)
        return targets

    @staticmethod
    def _spawn(cb: MessageHandler, msg: IncomingMessage) -> None:
        async def runner() -> None:
            try:
                await cb(msg)
            except _SUBSCRIBER_FAILURES:
                logging.getLogger("microbus_py.transport.inmemory").exception(
                    "subscriber handler raised; dropping",
                )

        asyncio.get_event_loop().create_task(runner())

    async def subscribe(
        self,
        *,
        subject: str,
        queue: str | None,
        cb: MessageHandler,
    ) -> Subscription:
        sid = secrets.token_hex(8)
        self._subs[sid] = _SubRecord(subject=subject, queue=queue, cb=cb, sid=sid)
        if "*" in subject.split(".") or ">" in subject.split("."):
            self._wildcard_sids.setdefault(subject, []).append(sid)
        else:
            self._by_subject[subject].append(sid)
        return InMemorySubscription(broker=self, sid=sid)

    async def request(
        self,
        *,
        subject: str,
        payload: bytes,
        timeout: float,
        headers: Mapping[str, str] | None = None,
    ) -> IncomingMessage:
        reply_subject = f"_INBOX.{secrets.token_hex(8)}"
        future: asyncio.Future[IncomingMessage] = asyncio.get_event_loop().create_future()

        async def reply_cb(msg: IncomingMessage) -> None:
            if not future.done():
                future.set_result(msg)

        sub = await self.subscribe(subject=reply_subject, queue=None, cb=reply_cb)
        try:
            await self.publish(
                subject=subject,
                payload=payload,
                reply=reply_subject,
                headers=headers,
            )
            try:
                return await asyncio.wait_for(future, timeout=timeout)
            except TimeoutError as exc:
                raise TimeoutError(f"request to {subject!r} timed out after {timeout}s") from exc
        finally:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()

    async def _unsubscribe(self, sid: str) -> None:
        rec = self._subs.pop(sid, None)
        if rec is None:
            return
        bucket = self._wildcard_sids.get(rec.subject) or self._by_subject.get(rec.subject)
        if bucket is None:
            return
        with contextlib.suppress(ValueError):
            bucket.remove(sid)
        if not bucket and rec.subject in self._wildcard_sids:
            del self._wildcard_sids[rec.subject]

    def _round_robin(
        self,
        subject: str,
        queue: str,
        members: list[str],
    ) -> str | None:
        if not members:
            return None
        key = (subject, queue)
        idx = self._next_queue_pick.get(key, 0) % len(members)
        self._next_queue_pick[key] = (idx + 1) % len(members)
        return members[idx]
