"""Connector shutdown clears fragment tables and cancels pending futures.

Memory pin: per V7 audit, Connector.shutdown does not clear
_request_fragments / _response_fragments. Per-dispatch sweep
_drop_expired_fragments addresses dispatch-time staleness, but a
mid-fragmented-request shutdown leaves partial chunks pinned until GC.
At 15+ services this compounds. These tests pin the shutdown-time
cleanup contract.
"""

from __future__ import annotations

import asyncio

import pytest

from microbus_py.connector.connector import (
    Connector,
    _Pending,
    _RequestFragments,
    _ResponseFragments,
)
from microbus_py.transport.inmemory import InMemoryBroker


@pytest.mark.asyncio
async def test_shutdown_clears_pending_request_fragments() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    await svc.startup()
    svc._request_fragments["mock-req-id"] = _RequestFragments(total=3)
    await svc.shutdown()
    assert svc._request_fragments == {}


@pytest.mark.asyncio
async def test_shutdown_clears_pending_response_fragments() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    await svc.startup()
    svc._response_fragments["mock-resp-id"] = _ResponseFragments(total=3)
    await svc.shutdown()
    assert svc._response_fragments == {}


@pytest.mark.asyncio
async def test_shutdown_cancels_pending_futures() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    await svc.startup()
    loop = asyncio.get_running_loop()
    pending = _Pending(ack=loop.create_future(), response=loop.create_future())
    svc._pending["mock-msg-id"] = pending
    await svc.shutdown()
    assert pending.ack.done()
    assert pending.ack.cancelled()
    assert pending.response.done()
    assert pending.response.cancelled()
    assert svc._pending == {}


@pytest.mark.asyncio
async def test_startup_starts_with_empty_fragment_tables() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    await svc.startup()
    assert svc._request_fragments == {}
    assert svc._response_fragments == {}
    assert svc._pending == {}
    await svc.shutdown()


@pytest.mark.asyncio
async def test_start_stop_cycle_100x_does_not_grow_fragments() -> None:
    svc = Connector(hostname="x.example", transport=InMemoryBroker())
    for _ in range(100):
        await svc.startup()
        svc._request_fragments["leak-req"] = _RequestFragments(total=2)
        svc._response_fragments["leak-resp"] = _ResponseFragments(total=2)
        loop = asyncio.get_running_loop()
        svc._pending["leak-msg"] = _Pending(
            ack=loop.create_future(),
            response=loop.create_future(),
        )
        await svc.shutdown()
    assert svc._request_fragments == {}
    assert svc._response_fragments == {}
    assert svc._pending == {}
