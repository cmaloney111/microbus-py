"""Tests for connector lifecycle ordering."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from microbus_py.connector.connector import Connector
from microbus_py.mock.time import VirtualClock
from microbus_py.transport.inmemory import InMemoryBroker
from microbus_py.wire.codec import HTTPRequest, HTTPResponse, decode_response


@pytest.mark.asyncio
async def test_startup_then_shutdown_idempotent_signals() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    await svc.startup()
    assert svc.is_started
    await svc.shutdown()
    assert not svc.is_started


@pytest.mark.asyncio
async def test_on_startup_callback_runs() -> None:
    calls: list[str] = []

    svc = Connector(hostname="x.example", transport=InMemoryBroker())

    @svc.on_startup
    async def starter() -> None:
        calls.append("started")

    await svc.startup()
    await svc.shutdown()
    assert calls == ["started"]


@pytest.mark.asyncio
async def test_on_startup_failure_aborts_startup() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())

    @svc.on_startup
    async def fail() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await svc.startup()
    assert not svc.is_started


@pytest.mark.asyncio
async def test_on_shutdown_callback_runs() -> None:
    calls: list[str] = []
    svc = Connector(hostname="x.example", transport=InMemoryBroker())

    @svc.on_shutdown
    async def stopper() -> None:
        calls.append("stopped")

    await svc.startup()
    await svc.shutdown()
    assert calls == ["stopped"]


@pytest.mark.asyncio
async def test_double_startup_raises() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    await svc.startup()
    with pytest.raises(RuntimeError):
        await svc.startup()
    await svc.shutdown()


@pytest.mark.asyncio
async def test_shutdown_before_startup_is_noop() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    await svc.shutdown()  # idempotent
    assert not svc.is_started


@pytest.mark.asyncio
async def test_identity_defaults() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    assert svc.hostname == "x.example"
    assert isinstance(svc.instance_id, str)
    assert len(svc.instance_id) >= 8
    assert svc.version == 0


@pytest.mark.asyncio
async def test_explicit_plane_and_locality() -> None:
    svc = Connector(
        hostname="x.example",
        plane="testplane",
        locality="us.east",
        transport=InMemoryBroker(),
    )
    assert svc.plane == "testplane"
    assert svc.locality == "us.east"


@pytest.mark.asyncio
async def test_default_plane_is_microbus() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    assert svc.plane == "microbus"


@pytest.mark.asyncio
async def test_empty_hostname_rejected() -> None:
    with pytest.raises(ValueError, match="hostname must be non-empty"):
        Connector(hostname="", transport=InMemoryBroker())


@pytest.mark.asyncio
async def test_shutdown_grace_must_be_positive() -> None:
    with pytest.raises(ValueError, match="shutdown grace must be positive"):
        Connector(
            hostname="x.example",
            transport=InMemoryBroker(),
            shutdown_grace=0,
        )


@pytest.mark.asyncio
async def test_hostname_lowercased() -> None:
    svc = Connector(hostname="X.Example", transport=InMemoryBroker())
    assert svc.hostname == "x.example"


@pytest.mark.asyncio
async def test_deployment_and_transport_properties() -> None:
    transport = InMemoryBroker()
    svc = Connector(
        hostname="x.example",
        transport=transport,
        deployment="LOCAL",
    )
    assert svc.deployment == "LOCAL"
    assert svc.transport is transport


@pytest.mark.asyncio
async def test_on_shutdown_exception_logged_not_raised() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())

    @svc.on_shutdown
    async def boom() -> None:
        raise RuntimeError("not raised, only logged")

    await svc.startup()
    # Must not raise even though callback throws
    await svc.shutdown()


@pytest.mark.asyncio
async def test_on_shutdown_ordinary_exception_logged_not_raised() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())

    @svc.on_shutdown
    async def boom() -> None:
        raise ValueError("not raised, only logged")

    await svc.startup()
    await svc.shutdown()
    assert not svc.is_started


@pytest.mark.asyncio
async def test_startup_starts_tickers_and_shutdown_stops_them() -> None:
    svc = Connector(hostname="ticker.example", transport=InMemoryBroker())
    clock = VirtualClock()
    svc.use_clock(clock)
    fired = 0

    @svc.ticker(name="beat", interval=timedelta(seconds=1))
    async def beat() -> None:
        nonlocal fired
        fired += 1

    await svc.startup()
    try:
        await clock.advance(timedelta(seconds=2))
        await asyncio.sleep(0)
        assert fired == 2
        await svc.shutdown()
        await clock.advance(timedelta(seconds=2))
        await asyncio.sleep(0)
        assert fired == 2
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_shutdown_waits_for_pending_go_tasks() -> None:
    svc = Connector(hostname="drain.example", transport=InMemoryBroker())
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def work() -> None:
        started.set()
        await release.wait()
        finished.set()

    await svc.startup()
    svc.go(work())
    await started.wait()

    shutdown = asyncio.create_task(svc.shutdown())
    try:
        await asyncio.sleep(0)
        assert not shutdown.done()

        release.set()
        await shutdown
        assert finished.is_set()
    finally:
        release.set()
        if not shutdown.done():
            await shutdown


@pytest.mark.asyncio
async def test_shutdown_cancels_pending_go_tasks_after_grace() -> None:
    svc = Connector(
        hostname="drain-timeout.example",
        transport=InMemoryBroker(),
        shutdown_grace=0.001,
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()
    never_release = asyncio.Event()

    async def work() -> None:
        started.set()
        try:
            await never_release.wait()
        finally:
            cancelled.set()

    await svc.startup()
    task = svc.go(work())
    await started.wait()

    await svc.shutdown()
    assert cancelled.is_set()
    assert task.cancelled()


@pytest.mark.asyncio
async def test_shutdown_logs_completed_go_task_failures(caplog: pytest.LogCaptureFixture) -> None:
    svc = Connector(hostname="drain-error.example", transport=InMemoryBroker())
    started = asyncio.Event()
    release = asyncio.Event()

    async def work() -> None:
        started.set()
        await release.wait()
        raise ValueError("background boom")

    await svc.startup()
    svc.go(work())
    await started.wait()

    shutdown = asyncio.create_task(svc.shutdown())
    try:
        await asyncio.sleep(0)
        release.set()
        await shutdown
    finally:
        release.set()
        if not shutdown.done():
            await shutdown
    assert "pending task raised during shutdown" in caplog.text


@pytest.mark.asyncio
async def test_shutdown_keeps_response_subscription_until_pending_tasks_drain() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="server.example", transport=transport)
    client = Connector(
        hostname="client.example",
        transport=transport,
        shutdown_grace=0.05,
    )
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    responses: list[bytes] = []

    async def slow_handler(req: HTTPRequest) -> HTTPResponse:
        handler_started.set()
        await release_handler.wait()
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=req.body)

    server.subscribe(
        name="Slow",
        method="POST",
        path="/slow",
        handler=slow_handler,
    )

    await server.startup()
    await client.startup()

    async def call_slow() -> None:
        resp = decode_response(
            await client.request(
                method="POST",
                url="https://server.example/slow",
                body=b"drained",
            )
        )
        responses.append(resp.body)

    client.go(call_slow())
    await handler_started.wait()

    shutdown = asyncio.create_task(client.shutdown())
    try:
        await asyncio.sleep(0)
        release_handler.set()
        await shutdown
    finally:
        release_handler.set()
        if not shutdown.done():
            await shutdown
        await server.shutdown()
    assert responses == [b"drained"]


@pytest.mark.asyncio
async def test_shutdown_waits_for_in_flight_handlers() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="handler-drain.example", transport=transport)
    client = Connector(hostname="caller.example", transport=transport)
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    async def slow_handler(req: HTTPRequest) -> HTTPResponse:
        handler_started.set()
        await release_handler.wait()
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=req.body)

    server.subscribe(
        name="Slow",
        method="POST",
        path="/slow",
        handler=slow_handler,
    )

    await server.startup()
    await client.startup()
    request = asyncio.create_task(
        client.request(
            method="POST",
            url="https://handler-drain.example/slow",
            body=b"handler-drained",
        )
    )
    await handler_started.wait()

    shutdown = asyncio.create_task(server.shutdown())
    try:
        await asyncio.sleep(0)
        assert not shutdown.done()

        release_handler.set()
        await shutdown
        resp = decode_response(await request)
        assert resp.body == b"handler-drained"
    finally:
        release_handler.set()
        if not shutdown.done():
            await shutdown
        await client.shutdown()
