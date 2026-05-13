"""Tests for exponential backoff + jitter on the NATS transport reconnect path.

The transport must avoid a thundering-herd reconnect storm by:
  * growing the delay exponentially across attempts,
  * applying half-and-full random jitter (``random.uniform(d*0.5, d)``),
  * capping the raw delay at ``max_reconnect_wait``,
  * resetting the attempt counter on a successful (re)connect.

Tests drive ``_compute_reconnect_delay`` and ``_reconnect_loop`` directly so
no real NATS server is required.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import random
import statistics
from typing import TYPE_CHECKING

import pytest

from microbus_py.transport.nats import NATSTransport

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.mark.asyncio
async def test_reconnect_delay_grows_exponentially() -> None:
    transport = NATSTransport(
        reconnect_base_wait=0.5,
        reconnect_multiplier=2.0,
        max_reconnect_wait=60.0,
    )
    # Pin jitter to its upper bound so attempt-N delay == raw delay for attempt-N.
    rng = random.Random()
    rng.uniform = lambda _lo, hi: hi  # type: ignore[assignment]
    transport._rng = rng

    delays = [await transport._compute_reconnect_delay(n) for n in range(5)]
    # Strictly monotonic up to the cap.
    for prev, curr in itertools.pairwise(delays):
        assert curr > prev, f"delays not monotonic: {delays}"
    # Exponential: each step doubles.
    assert delays == [0.5, 1.0, 2.0, 4.0, 8.0]


@pytest.mark.asyncio
async def test_reconnect_delay_capped_at_max() -> None:
    transport = NATSTransport(
        reconnect_base_wait=0.5,
        reconnect_multiplier=2.0,
        max_reconnect_wait=5.0,
    )
    rng = random.Random()
    rng.uniform = lambda _lo, hi: hi  # type: ignore[assignment]
    transport._rng = rng

    # Attempt 10 would yield 0.5 * 2**10 = 512s without cap.
    delays = [await transport._compute_reconnect_delay(n) for n in range(12)]
    assert all(d <= 5.0 for d in delays), delays
    # Once capped, stays capped.
    assert delays[-1] == 5.0


@pytest.mark.asyncio
async def test_reconnect_delay_has_jitter_variance() -> None:
    transport = NATSTransport(
        reconnect_base_wait=1.0,
        reconnect_multiplier=2.0,
        max_reconnect_wait=30.0,
    )
    # Fixed seed → reproducible variance check, but real uniform exercised.
    transport._rng = random.Random(0xDEADBEEF)

    samples = [await transport._compute_reconnect_delay(3) for _ in range(50)]
    assert statistics.pvariance(samples) > 0.0, samples
    # Within half-and-full bounds: raw = 1.0 * 2**3 = 8.0; jitter range [4.0, 8.0].
    assert all(4.0 <= s <= 8.0 for s in samples), samples


@pytest.mark.asyncio
async def test_reconnect_attempt_counter_resets_on_successful_connect() -> None:
    """The reset must happen inside ``_reconnect_loop`` between success and
    the next failure cycle - not be performed by ``connect_once`` itself.
    Drives fail, fail, fail, success, then a fresh fail and asserts the
    post-success failure uses the attempt=0 delay rather than continuing
    the pre-success ramp.
    """
    transport = NATSTransport(
        reconnect_base_wait=0.5,
        reconnect_multiplier=2.0,
        max_reconnect_wait=60.0,
    )
    rng = random.Random()
    rng.uniform = lambda _lo, hi: hi  # type: ignore[assignment]
    transport._rng = rng

    sleep_calls: list[float] = []
    attempts_seen_on_failure: list[int] = []

    async def fake_sleep(d: float) -> None:
        sleep_calls.append(d)

    outcomes = iter([False, False, False, True, False])

    async def fake_connect_once() -> bool:
        ok = next(outcomes)
        if not ok:
            attempts_seen_on_failure.append(transport._attempt)
            raise ConnectionError("simulated")
        # Crucial: do NOT reset _attempt here. The loop owns that invariant.
        return True

    await transport._reconnect_loop(
        connect_once=fake_connect_once,
        sleep=fake_sleep,
        is_closed=lambda: False,
        stop_when_connected=True,
    )

    # 3 failures @ attempts 0,1,2 → loop slept 0.5,1.0,2.0; then success.
    assert attempts_seen_on_failure == [0, 1, 2]
    assert sleep_calls == [0.5, 1.0, 2.0]
    # Loop reset the counter after success (NOT the connect_once callback).
    assert transport._attempt == 0

    # Drive one more failure pass; the loop must have reset to attempt=0,
    # so this failure observes attempt=0 (not attempt=3).
    sleep_calls.clear()
    attempts_seen_on_failure.clear()
    closed_flag = {"val": False}

    def is_closed() -> bool:
        closed = closed_flag["val"]
        closed_flag["val"] = True  # exit after one full pass
        return closed

    await transport._reconnect_loop(
        connect_once=fake_connect_once,
        sleep=fake_sleep,
        is_closed=is_closed,
        stop_when_connected=False,
    )
    assert attempts_seen_on_failure == [0], attempts_seen_on_failure
    assert sleep_calls == [0.5], sleep_calls


@pytest.mark.asyncio
async def test_jitter_does_not_underflow_below_half_of_raw() -> None:
    transport = NATSTransport(
        reconnect_base_wait=0.5,
        reconnect_multiplier=2.0,
        max_reconnect_wait=60.0,
    )
    transport._rng = random.Random(12345)

    samples = [await transport._compute_reconnect_delay(0) for _ in range(200)]
    # Half-and-full jitter on raw=0.5 → range [0.25, 0.5].
    assert min(samples) >= 0.25
    assert max(samples) <= 0.5
    assert min(samples) > 0.0


@pytest.mark.asyncio
async def test_reconnect_loop_stops_when_closed() -> None:
    transport = NATSTransport()
    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    async def always_fail() -> bool:
        raise ConnectionError("simulated")

    closed_state = {"val": False}
    call_count = {"val": 0}

    def is_closed() -> bool:
        call_count["val"] += 1
        if call_count["val"] >= 4:
            closed_state["val"] = True
        return closed_state["val"]

    await transport._reconnect_loop(
        connect_once=always_fail,
        sleep=fake_sleep,
        is_closed=is_closed,
        stop_when_connected=False,
    )
    assert len(sleeps) > 0
    assert transport._attempt > 0


def _delay_picker(values: list[float]) -> Callable[[float, float], float]:
    it = iter(values)

    def pick(_lo: float, _hi: float) -> float:
        return next(it)

    return pick


@pytest.mark.asyncio
async def test_max_reconnect_wait_constructor_default_is_thirty_seconds() -> None:
    transport = NATSTransport()
    # Drive a very high attempt count; with default cap = 30, the raw should saturate.
    rng = random.Random()
    rng.uniform = lambda _lo, hi: hi  # type: ignore[assignment]
    transport._rng = rng
    d = await transport._compute_reconnect_delay(20)
    assert d == 30.0


@pytest.mark.asyncio
async def test_reconnect_loop_invokes_real_asyncio_sleep_zero_when_attempts_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Sanity: the loop actually awaits sleep with the computed delay (not a hard-coded one).
    transport = NATSTransport(
        reconnect_base_wait=0.1, reconnect_multiplier=2.0, max_reconnect_wait=1.0
    )
    rng = random.Random()
    rng.uniform = _delay_picker([0.07, 0.13])  # type: ignore[method-assign, assignment]  # deterministic per call
    transport._rng = rng

    sleep_args: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleep_args.append(d)

    fail_then_succeed = iter([False, True])

    async def connect_once() -> bool:
        if not next(fail_then_succeed):
            raise ConnectionError("nope")
        transport._attempt = 0
        return True

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await transport._reconnect_loop(
        connect_once=connect_once,
        sleep=fake_sleep,
        is_closed=lambda: False,
        stop_when_connected=True,
    )
    # Single failure → single sleep with the first jitter draw.
    assert sleep_args == [0.07]


class _FakeSubscription:
    def __init__(self) -> None:
        self.unsubscribed = False

    async def unsubscribe(self) -> None:
        self.unsubscribed = True


class _FakeNATSClient:
    """Stand-in for ``nats.aio.client.Client`` for unit tests.

    Tracks connect, subscribe and flush calls. Lets the test toggle
    ``is_connected``.
    """

    def __init__(self) -> None:
        self.is_connected = True
        self.max_payload = 1 << 20
        self.drained = False
        self.closed = False
        self.subscribed: list[tuple[str, str, object]] = []
        self.subs: list[_FakeSubscription] = []
        self.flushed = 0

    async def drain(self) -> None:
        self.drained = True
        self.is_connected = False

    async def close(self) -> None:
        self.closed = True
        self.is_connected = False

    async def subscribe(self, *, subject: str, queue: str, cb: object) -> _FakeSubscription:
        self.subscribed.append((subject, queue, cb))
        sub = _FakeSubscription()
        self.subs.append(sub)
        return sub

    async def flush(self) -> None:
        self.flushed += 1


@pytest.mark.asyncio
async def test_connect_disables_library_reconnect_via_max_attempts_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transport must own the reconnect loop. To prevent nats-py from
    running its own fixed-interval reconnect (the thundering-herd bug),
    ``connect()`` must disable the library's internal reconnect by passing
    ``allow_reconnect=False`` (and ``max_reconnect_attempts=0`` so the
    initial connect path also stops library retries on first failure).
    """
    transport = NATSTransport()
    captured: dict[str, object] = {}

    async def fake_connect(**kwargs: object) -> _FakeNATSClient:
        captured.update(kwargs)
        return _FakeNATSClient()

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    await transport.connect("nats://example:4222", name="svc")

    assert captured.get("allow_reconnect") is False, captured
    assert captured.get("max_reconnect_attempts") == 0, captured


@pytest.mark.asyncio
async def test_disconnected_callback_triggers_jittered_reconnect_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When nats-py fires ``disconnected_cb`` for a live disconnect, the
    transport must drive its own jittered ``_reconnect_loop`` instead of
    letting the library reconnect at a fixed interval. Asserts that the
    sleep delays between reconnect attempts vary (non-zero variance),
    which is the observable signal of jitter being applied to the live
    reconnect path.
    """
    transport = NATSTransport(
        reconnect_base_wait=1.0,
        reconnect_multiplier=2.0,
        max_reconnect_wait=30.0,
    )
    transport._rng = random.Random(0xC0FFEE)

    connect_calls = {"n": 0}

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        connect_calls["n"] += 1
        # Initial connect succeeds; next 3 fail; 5th succeeds.
        if connect_calls["n"] in (2, 3, 4):
            raise ConnectionError("simulated drop")
        return _FakeNATSClient()

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    sleep_delays: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleep_delays.append(d)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    # Initial connect (succeeds, populates url/name for reconnect).
    await transport.connect("nats://example:4222", name="svc")
    assert connect_calls["n"] == 1

    # Simulate the library firing disconnected_cb on a live drop.
    await transport._on_disconnected()

    # The loop tried connect 3 more times (calls 2,3,4 fail) before call 5 succeeded.
    assert connect_calls["n"] == 5
    # 3 failures → 3 sleeps; each was a jittered exponential draw.
    assert len(sleep_delays) == 3, sleep_delays
    # Half-and-full jitter: attempt-N raw is `base * mult**N`; jittered into
    # [raw/2, raw]. With base=1, mult=2: attempt 0 in [0.5,1.0], 1 in [1.0,2.0],
    # 2 in [2.0,4.0]. Variance > 0 proves jitter ran on each step.
    assert statistics.pvariance(sleep_delays) > 0.0, sleep_delays
    # Monotonic-ish ramp: each draw inside its attempt window.
    assert 0.5 <= sleep_delays[0] <= 1.0, sleep_delays
    assert 1.0 <= sleep_delays[1] <= 2.0, sleep_delays
    assert 2.0 <= sleep_delays[2] <= 4.0, sleep_delays
    # Counter reset after successful reconnect.
    assert transport._attempt == 0


@pytest.mark.asyncio
async def test_health_check_poller_drives_reconnect_when_client_disconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """V14: in production, ``nats-py``'s ``_close()`` cancels the reading task
    before dispatching ``disconnected_cb``, so the cb-driven reconnect path
    is unreachable on live server bounce. ``connect()`` must spawn a
    background health-check poller that observes ``client.is_connected`` and
    invokes ``_on_disconnected()`` itself when the flag flips.
    """
    transport = NATSTransport(health_check_interval=0.0)
    fake_client = _FakeNATSClient()

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return fake_client

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    on_disc_calls = {"n": 0}

    async def spy_on_disc() -> None:
        on_disc_calls["n"] += 1
        fake_client.is_connected = True  # simulate successful reconnect

    transport._on_disconnected = spy_on_disc  # type: ignore[method-assign]

    await transport.connect("nats://example:4222", name="svc")
    assert transport._health_check_task is not None
    assert not transport._health_check_task.done()

    fake_client.is_connected = False
    for _ in range(20):
        await asyncio.sleep(0)
        if on_disc_calls["n"] > 0:
            break
    await transport.close()
    assert on_disc_calls["n"] == 1


@pytest.mark.asyncio
async def test_health_check_poller_cancelled_on_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = NATSTransport(health_check_interval=0.05)

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return _FakeNATSClient()

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    await transport.connect("nats://example:4222", name="svc")
    poller = transport._health_check_task
    assert poller is not None
    await transport.close()
    assert poller.cancelled() or poller.done()


@pytest.mark.asyncio
async def test_health_check_poller_does_not_double_fire_when_already_reconnecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = NATSTransport(health_check_interval=0.0)
    fake_client = _FakeNATSClient()

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return fake_client

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    await transport.connect("nats://example:4222", name="svc")
    # Simulate an in-flight reconnect already running.
    transport._reconnect_in_progress = True
    fake_client.is_connected = False

    on_disc_calls = {"n": 0}

    async def spy_on_disc() -> None:
        on_disc_calls["n"] += 1

    transport._on_disconnected = spy_on_disc  # type: ignore[method-assign]

    for _ in range(10):
        await asyncio.sleep(0)
    await transport.close()
    assert on_disc_calls["n"] == 0


@pytest.mark.asyncio
async def test_reconnect_replays_subscriptions_on_fresh_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live-disconnect reconnect must replay all active subscriptions onto the
    fresh nats client. Without replay, ``_connect_once_for_reconnect`` swaps
    ``self._client`` but the new client has no bound subjects, so the service
    silently stops receiving traffic.
    """
    transport = NATSTransport(health_check_interval=0.0)
    old_client = _FakeNATSClient()
    new_client = _FakeNATSClient()
    clients = iter([old_client, new_client])

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return next(clients)

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    async def fake_sleep(_d: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await transport.connect("nats://example:4222", name="svc")

    async def handler(_msg: object) -> None:
        return None

    await transport.subscribe(subject="test.subject", queue="q1", cb=handler)
    assert len(old_client.subscribed) == 1
    assert old_client.subscribed[0][0] == "test.subject"
    assert old_client.subscribed[0][1] == "q1"

    old_client.is_connected = False
    await transport._on_disconnected()

    assert id(transport._client) == id(new_client)
    assert len(new_client.subscribed) == 1
    assert new_client.subscribed[0][0] == "test.subject"
    assert new_client.subscribed[0][1] == "q1"
    assert new_client.flushed >= 1


@pytest.mark.asyncio
async def test_close_cancels_in_flight_reconnect_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``close()`` must track and cancel the cb-driven reconnect task so a
    fresh client cannot be assigned into a closed transport after shutdown
    returns. Asserts the reconnect task is recorded on ``_reconnect_task``
    and that ``close()`` cancels + awaits it before the client teardown.
    """
    real_sleep = asyncio.sleep
    transport = NATSTransport(health_check_interval=0.0)
    old_client = _FakeNATSClient()
    new_client = _FakeNATSClient()
    release_new = asyncio.Event()
    state = {"n": 0}

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        state["n"] += 1
        if state["n"] == 1:
            return old_client
        await release_new.wait()
        return new_client

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    await transport.connect("nats://example:4222", name="svc")

    old_client.is_connected = False
    on_disc_task = asyncio.create_task(transport._on_disconnected())
    for _ in range(20):
        await real_sleep(0)
        if state["n"] >= 2:
            break
    assert state["n"] == 2
    assert transport._reconnect_task is not None
    assert not transport._reconnect_task.done()

    await transport.close()
    # close() must have cancelled the reconnect task before returning.
    with contextlib.suppress(asyncio.CancelledError):
        await on_disc_task
    assert transport._client is None
    # Unblock the dangling fake_connect so the test runtime can exit cleanly.
    release_new.set()
    for _ in range(5):
        await real_sleep(0)


@pytest.mark.asyncio
async def test_connect_once_for_reconnect_drains_new_client_when_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-connect closed-check: if ``self._closed`` is True by the time
    ``nats.connect`` returns, ``_connect_once_for_reconnect`` must drain the
    fresh client and return ``False`` without assigning it.
    """
    transport = NATSTransport(health_check_interval=0.0)
    new_client = _FakeNATSClient()

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return new_client

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)
    transport._url = "nats://example:4222"
    transport._name = "svc"
    transport._closed = True

    ok = await transport._connect_once_for_reconnect()
    assert ok is False
    assert transport._client is None
    assert new_client.drained


@pytest.mark.asyncio
async def test_unsubscribe_removes_record_so_replay_skips_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = NATSTransport(health_check_interval=0.0)
    old_client = _FakeNATSClient()
    new_client = _FakeNATSClient()
    clients = iter([old_client, new_client])

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return next(clients)

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    async def fake_sleep(_d: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await transport.connect("nats://example:4222", name="svc")

    async def handler(_msg: object) -> None:
        return None

    handle = await transport.subscribe(subject="test.subject", queue="q1", cb=handler)
    await handle.unsubscribe()

    old_client.is_connected = False
    await transport._on_disconnected()

    assert id(transport._client) == id(new_client)
    assert new_client.subscribed == []


@pytest.mark.asyncio
async def test_unsubscribe_after_reconnect_targets_new_client_sub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-reconnect ``handle.unsubscribe()`` must target the LIVE sub on the
    new client, not the dead sub object captured at the original ``subscribe()``
    call. Without this, replay creates an untracked sub on the new client and
    the handle's unsubscribe is a no-op against the old dead sub - silent leak.
    """
    transport = NATSTransport(health_check_interval=0.0)
    old_client = _FakeNATSClient()
    new_client = _FakeNATSClient()
    clients = iter([old_client, new_client])

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return next(clients)

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    async def fake_sleep(_d: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await transport.connect("nats://example:4222", name="svc")

    async def handler(_msg: object) -> None:
        return None

    handle = await transport.subscribe(subject="t.sub", queue="q", cb=handler)
    old_sub = old_client.subs[0]

    old_client.is_connected = False
    await transport._on_disconnected()
    assert id(transport._client) == id(new_client)
    assert len(new_client.subs) == 1
    new_sub = new_client.subs[0]

    old_sub.unsubscribed = False  # reset to detect post-reconnect activity
    await handle.unsubscribe()

    assert new_sub.unsubscribed is True
    assert old_sub.unsubscribed is False


@pytest.mark.asyncio
async def test_concurrent_unsubscribe_during_replay_does_not_double_subscribe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replay snapshots ``_active_subscriptions`` and resubscribes each record
    on the new client. If user code calls ``handle.unsubscribe()`` concurrently
    with the replay, the resulting state must not leak a zombie sub on the new
    client: either the record is replayed then promptly unsubscribed, or it is
    pruned before replay and never resubscribed.
    """
    real_sleep = asyncio.sleep
    transport = NATSTransport(health_check_interval=0.0)
    old_client = _FakeNATSClient()
    new_client = _FakeNATSClient()
    clients = iter([old_client, new_client])
    release_subscribe = asyncio.Event()

    async def fake_connect(**_kwargs: object) -> _FakeNATSClient:
        return next(clients)

    monkeypatch.setattr("microbus_py.transport.nats.nats.connect", fake_connect)

    async def fake_sleep(_d: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await transport.connect("nats://example:4222", name="svc")

    async def handler(_msg: object) -> None:
        return None

    handle = await transport.subscribe(subject="t.sub", queue="q", cb=handler)

    real_new_subscribe = new_client.subscribe

    async def gated_subscribe(*, subject: str, queue: str, cb: object) -> _FakeSubscription:
        await release_subscribe.wait()
        return await real_new_subscribe(subject=subject, queue=queue, cb=cb)

    new_client.subscribe = gated_subscribe  # type: ignore[method-assign]

    old_client.is_connected = False
    reconnect_task = asyncio.create_task(transport._on_disconnected())
    for _ in range(10):
        await real_sleep(0)

    unsub_task = asyncio.create_task(handle.unsubscribe())
    for _ in range(5):
        await real_sleep(0)
    release_subscribe.set()
    await reconnect_task
    await unsub_task

    live_subs = [s for s in new_client.subs if not s.unsubscribed]
    assert live_subs == []
