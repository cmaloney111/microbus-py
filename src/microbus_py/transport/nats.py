"""NATS-backed transport implementation.

Wraps ``nats-py`` and exposes the :class:`Transport` protocol used by the
connector. NATS subjects use Microbus's dot-separated grammar
(``microbus.443.com.example.|.POST.path``); ``|`` is preserved as a
literal token by NATS.

Subscriptions opt into queue-group load balancing by passing a queue name;
``queue=None`` becomes a normal (broadcast) subscription.

Reconnect strategy
------------------
The transport adds exponential backoff + half-and-full jitter on top of
``nats-py``'s connect path. When several replicas of the same service
lose their connection at the same moment (e.g. a NATS server restart),
a fixed reconnect interval makes them race to reattach in lockstep and
hammer the recovering server. Jittered exponential backoff spreads the
retries over a window:

    raw = base * multiplier ** attempt
    capped = min(raw, max_reconnect_wait)
    delay = uniform(capped * 0.5, capped)

The attempt counter resets to zero on a successful (re)connect so a flaky
link does not slowly walk to a 30 s wait.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import nats
import nats.errors

from microbus_py.transport.base import (
    IncomingMessage,
    MessageHandler,
    Subscription,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from nats.aio.client import Client as NATSClient
    from nats.aio.msg import Msg as NATSMsg
    from nats.aio.subscription import Subscription as NATSSubscription

__all__ = ["NATSSubscriptionHandle", "NATSTransport"]


@dataclass(slots=True)
class _SubscriptionRecord:
    """One active subscription, replayable onto a fresh client after reconnect.

    The transport keeps a list of these in ``_active_subscriptions`` and rebinds
    them in ``_connect_once_for_reconnect`` after the library hands back a new
    ``NATSClient``. Records are appended by ``subscribe()`` and removed by
    ``NATSSubscriptionHandle.unsubscribe()``.

    ``current_sub`` is the live ``nats.aio.subscription.Subscription`` bound to
    whichever ``NATSClient`` is currently serving this record. Replay rewrites
    it after each fresh ``client.subscribe()`` so a handle issued before a
    reconnect still resolves to the live sub on the new client.
    """

    subject: str
    queue: str
    adapter: Callable[[NATSMsg], Awaitable[None]]
    current_sub: NATSSubscription


_log = logging.getLogger("microbus_py.transport.nats")
_SUBSCRIBER_FAILURES: tuple[type[Exception], ...] = (Exception,)
_CONNECT_FAILURES: tuple[type[BaseException], ...] = (
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
    nats.errors.Error,
)
# Captured at import so the health-check poller is isolated from tests that
# monkeypatch asyncio.sleep for unrelated assertions in _reconnect_loop.
_sleep = asyncio.sleep


class NATSSubscriptionHandle:
    def __init__(
        self,
        *,
        record: _SubscriptionRecord,
        registry: list[_SubscriptionRecord],
        lock: asyncio.Lock,
    ) -> None:
        self._record: _SubscriptionRecord | None = record
        self._registry = registry
        self._lock = lock

    async def unsubscribe(self) -> None:
        async with self._lock:
            record = self._record
            if record is None:
                return
            with contextlib.suppress(ValueError):
                self._registry.remove(record)
            self._record = None
            sub = record.current_sub
        await sub.unsubscribe()


class NATSTransport:
    def __init__(
        self,
        *,
        reconnect_time_wait: float = 2.0,
        max_reconnect_attempts: int = -1,
        reconnect_base_wait: float = 0.5,
        reconnect_multiplier: float = 2.0,
        max_reconnect_wait: float = 30.0,
        health_check_interval: float = 1.0,
        nats_options: Mapping[str, Any] | None = None,
    ) -> None:
        """Construct a NATS transport.

        Args:
            reconnect_time_wait: retained for back-compat; unused by the
                transport's own reconnect loop.
            max_reconnect_attempts: retained for back-compat; unused.
            reconnect_base_wait: initial pre-jitter backoff in seconds.
            reconnect_multiplier: per-attempt growth factor.
            max_reconnect_wait: hard cap on raw delay before jitter; final
                post-jitter delay lies in ``[cap * 0.5, cap]``.
            health_check_interval: period in seconds between
                ``client.is_connected`` polls used to detect a live
                disconnect; nats-py tears down ``_reading_task`` before
                dispatching ``disconnected_cb`` under
                ``allow_reconnect=False``, so polling is the only reliable
                signal of a server bounce.
        """
        self._client: NATSClient | None = None
        self._reconnect_time_wait = reconnect_time_wait
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_base_wait = reconnect_base_wait
        self._reconnect_multiplier = reconnect_multiplier
        self._max_reconnect_wait = max_reconnect_wait
        self._health_check_interval = health_check_interval
        self._attempt = 0
        self._rng = random.Random()
        self._closed = False
        self._url: str = ""
        self._name: str = ""
        self._reconnect_in_progress = False
        self._health_check_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._active_subscriptions: list[_SubscriptionRecord] = []
        # Serialises bookkeeping on ``_active_subscriptions`` and per-record
        # ``current_sub`` assignment so a user-side ``handle.unsubscribe()``
        # cannot race with the replay loop's snapshot/rebind. Held only
        # around the in-memory mutations; never across ``await
        # client.subscribe()`` for fresh user subscribes, since that would
        # serialise unrelated startup subscribes.
        self._subscriptions_lock: asyncio.Lock = asyncio.Lock()
        # Extra options forwarded to ``nats.connect`` (tls, user_credentials,
        # nkeys_seed, token, etc.). Documented as a passthrough so callers
        # control transport security without this layer dictating policy.
        self._nats_options: dict[str, Any] = dict(nats_options or {})

    async def connect(self, url: str, *, name: str) -> None:
        if self._client is not None and self._client.is_connected:
            return
        servers = url or "nats://127.0.0.1:4222"
        self._url = servers
        self._name = name
        # allow_reconnect=False + max_reconnect_attempts=0 disables nats-py's
        # internal fixed-interval reconnect loop so this transport's jittered
        # _reconnect_loop is the sole retry path (prevents thundering-herd
        # storms across replicas after a server restart).
        connect_kwargs: dict[str, Any] = {
            "servers": servers,
            "name": name,
            "allow_reconnect": False,
            "max_reconnect_attempts": 0,
            "error_cb": self._on_error,
            "disconnected_cb": self._on_disconnected,
            "reconnected_cb": self._on_reconnected,
        }
        for key, value in self._nats_options.items():
            if key in {
                "servers",
                "allow_reconnect",
                "max_reconnect_attempts",
                "error_cb",
                "disconnected_cb",
                "reconnected_cb",
            }:
                # Reserved by this transport; ignore overrides.
                continue
            connect_kwargs[key] = value
        self._client = await nats.connect(**connect_kwargs)
        self._attempt = 0
        if self._health_check_task is None or self._health_check_task.done():
            self._health_check_task = asyncio.create_task(self._health_check_poller())

    async def close(self) -> None:
        self._closed = True
        if self._health_check_task is not None:
            self._health_check_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_check_task
            self._health_check_task = None
        # Cancel any in-flight cb-driven reconnect so a fresh client cannot be
        # assigned into a closed transport after this returns. The post-connect
        # ``self._closed`` check in ``_connect_once_for_reconnect`` is the
        # second line of defence for the case where ``nats.connect`` returns
        # right between this cancel and the task's next checkpoint.
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconnect_task
            self._reconnect_task = None
        if self._client is None:
            return
        if self._client.is_connected:
            await self._client.drain()
        self._client = None

    def max_payload(self) -> int:
        if self._client is None:
            return 1 << 20
        return self._client.max_payload

    async def publish(
        self,
        *,
        subject: str,
        payload: bytes,
        reply: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        client = self._require_client()
        await client.publish(
            subject=subject,
            payload=payload,
            reply=reply or "",
            headers=dict(headers) if headers else None,
        )

    async def request(
        self,
        *,
        subject: str,
        payload: bytes,
        timeout: float,
        headers: Mapping[str, str] | None = None,
    ) -> IncomingMessage:
        client = self._require_client()
        msg = await client.request(
            subject=subject,
            payload=payload,
            timeout=timeout,
            headers=dict(headers) if headers else None,
        )
        return IncomingMessage(
            subject=msg.subject,
            payload=msg.data,
            reply=msg.reply or None,
            headers=dict(msg.headers) if msg.headers else {},
        )

    async def subscribe(
        self,
        *,
        subject: str,
        queue: str | None,
        cb: MessageHandler,
    ) -> Subscription:
        client = self._require_client()

        async def adapter(msg: NATSMsg) -> None:
            try:
                await cb(
                    IncomingMessage(
                        subject=msg.subject,
                        payload=msg.data,
                        reply=msg.reply or None,
                        headers=dict(msg.headers) if msg.headers else {},
                    )
                )
            except _SUBSCRIBER_FAILURES:
                _log.exception("subscriber handler raised; dropping")

        queue_str = queue or ""
        sub = await client.subscribe(
            subject=subject,
            queue=queue_str,
            cb=adapter,
        )
        await client.flush()
        record = _SubscriptionRecord(
            subject=subject,
            queue=queue_str,
            adapter=adapter,
            current_sub=sub,
        )
        async with self._subscriptions_lock:
            self._active_subscriptions.append(record)
        return NATSSubscriptionHandle(
            record=record,
            registry=self._active_subscriptions,
            lock=self._subscriptions_lock,
        )

    async def _compute_reconnect_delay(self, attempt: int) -> float:
        """Return a jittered backoff delay for ``attempt``.

        Half-and-full jitter on top of an exponential ramp capped by
        ``max_reconnect_wait``. Pure function aside from the bound RNG;
        no I/O, safe to call repeatedly.
        """
        raw = self._reconnect_base_wait * (self._reconnect_multiplier**attempt)
        capped = min(raw, self._max_reconnect_wait)
        return self._rng.uniform(capped * 0.5, capped)

    async def _reconnect_loop(
        self,
        *,
        connect_once: Callable[[], Awaitable[bool]],
        sleep: Callable[[float], Awaitable[None]] | None = None,
        is_closed: Callable[[], bool] | None = None,
        stop_when_connected: bool = True,
    ) -> None:
        """Drive (re)connect attempts with exponential backoff + jitter.

        ``connect_once`` returns ``True`` on a successful connect (loop exits if
        ``stop_when_connected``), or raises a connection-class exception on
        failure. Each failure sleeps for the jittered delay computed from the
        current ``_attempt`` counter, then increments it. On success the
        counter resets to zero. ``is_closed`` lets the caller short-circuit
        a long-running loop (e.g. during ``close()``).
        """
        closed_check = is_closed if is_closed is not None else (lambda: self._closed)
        sleep_fn = sleep if sleep is not None else asyncio.sleep
        while not closed_check():
            try:
                ok = await connect_once()
            except _CONNECT_FAILURES as exc:
                delay = await self._compute_reconnect_delay(self._attempt)
                _log.debug(
                    "nats reconnect backoff: attempt=%d delay=%.3fs err=%r",
                    self._attempt,
                    delay,
                    exc,
                )
                await sleep_fn(delay)
                self._attempt += 1
                continue
            if ok:
                self._attempt = 0
                if stop_when_connected:
                    return

    def _require_client(self) -> NATSClient:
        if self._client is None:
            raise RuntimeError("transport not connected")
        return self._client

    @staticmethod
    async def _on_error(exc: Exception) -> None:
        _log.warning("nats client error: %r", exc)

    async def _on_disconnected(self) -> None:
        _log.warning("nats client disconnected")
        if self._closed or self._reconnect_in_progress:
            return
        self._reconnect_in_progress = True
        task = asyncio.create_task(
            self._reconnect_loop(connect_once=self._connect_once_for_reconnect)
        )
        self._reconnect_task = task
        try:
            await task
        except asyncio.CancelledError:
            # close() cancels the reconnect task during shutdown; surface
            # nothing to the caller (the library's disconnected_cb).
            return
        finally:
            self._reconnect_in_progress = False
            if self._reconnect_task is task:
                self._reconnect_task = None

    async def _health_check_poller(self) -> None:
        """Poll ``client.is_connected`` and drive ``_on_disconnected`` on flip.

        nats-py's ``_process_op_err`` → ``_close()`` cancels the reading task
        before dispatching ``disconnected_cb`` under ``allow_reconnect=False``,
        making the cb-driven path unreachable on real server bounce. Polling
        the client's own ``is_connected`` flag is the reliable signal. The
        ``_reconnect_in_progress`` guard inside ``_on_disconnected`` prevents
        double-fire if both the cb and the poller observe the disconnect.

        Uses the module-level ``_sleep`` captured at import so unrelated tests
        that monkeypatch ``asyncio.sleep`` do not turn this loop into a tight
        non-yielding spin.
        """
        while not self._closed:
            await _sleep(self._health_check_interval)
            if self._closed:
                return
            client = self._client
            if client is None:
                continue
            if not client.is_connected and not self._reconnect_in_progress:
                await self._on_disconnected()

    async def _connect_once_for_reconnect(self) -> bool:
        """Single reconnect attempt invoked by ``_reconnect_loop``.

        Creates a fresh ``nats.connect`` (the old client is torn down by
        the library before ``disconnected_cb`` fires), replays all active
        subscriptions onto it, and replaces ``self._client`` on success.
        Raises a connection-class exception on failure so the loop can
        sleep + retry.

        Re-checks ``self._closed`` after ``nats.connect`` returns to catch
        the close-during-reconnect race: if the transport was closed while
        the connect was in flight, drain the new client and return ``False``
        without assigning. ``close()`` cancels ``_reconnect_task`` as the
        primary defence; this check covers the narrow window between the
        cancel call and the next checkpoint.
        """
        client = await nats.connect(
            servers=self._url,
            name=self._name,
            allow_reconnect=False,
            max_reconnect_attempts=0,
            error_cb=self._on_error,
            disconnected_cb=self._on_disconnected,
            reconnected_cb=self._on_reconnected,
        )
        if self._closed:
            await client.drain()
            return False
        async with self._subscriptions_lock:
            records = list(self._active_subscriptions)
        for record in records:
            new_sub = await client.subscribe(
                subject=record.subject,
                queue=record.queue,
                cb=record.adapter,
            )
            async with self._subscriptions_lock:
                if record in self._active_subscriptions:
                    record.current_sub = new_sub
                else:
                    # Record was pruned mid-replay by a user-side
                    # ``handle.unsubscribe()``. Drop the just-created sub so
                    # it does not leak as a zombie on the new client.
                    await new_sub.unsubscribe()
        await client.flush()
        self._client = client
        return True

    @staticmethod
    async def _on_reconnected() -> None:
        _log.info("nats client reconnected")
