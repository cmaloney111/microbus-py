"""Tests for transport/nats.py."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import nats as nats_client_module
import pytest

from microbus_py.connector.connector import Connector
from microbus_py.transport.base import IncomingMessage, Transport
from microbus_py.transport.nats import NATSTransport

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping


@dataclass(slots=True)
class PublishCall:
    subject: str
    payload: bytes
    reply: str
    headers: dict[str, str] | None


@dataclass(slots=True)
class RequestCall:
    subject: str
    payload: bytes
    timeout: float
    headers: dict[str, str] | None


@dataclass(slots=True)
class SubscribeCall:
    subject: str
    queue: str
    cb: Callable[[FakeMsg], Awaitable[None]]


@dataclass(slots=True)
class FakeMsg:
    subject: str
    data: bytes
    reply: str = ""
    headers: Mapping[str, str] | None = None


class FakeSubscription:
    def __init__(self) -> None:
        self.unsubscribed = False

    async def unsubscribe(self) -> None:
        self.unsubscribed = True


class FakeClient:
    def __init__(self) -> None:
        self.is_connected = True
        self.drained = False
        self.published: list[PublishCall] = []
        self.requests: list[RequestCall] = []
        self.subscriptions: list[SubscribeCall] = []
        self.subscription_handles: list[FakeSubscription] = []
        self.next_request = FakeMsg(subject="_INBOX.reply", data=b"")
        self.flush_count = 0
        self.max_payload = 1024 * 1024

    async def drain(self) -> None:
        self.drained = True

    async def flush(self) -> None:
        self.flush_count += 1

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        reply: str = "",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.published.append(
            PublishCall(subject=subject, payload=payload, reply=reply, headers=headers)
        )

    async def request(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: float = 0.5,
        old_style: bool = False,
        headers: dict[str, str] | None = None,
    ) -> FakeMsg:
        assert old_style is False
        self.requests.append(
            RequestCall(subject=subject, payload=payload, timeout=timeout, headers=headers)
        )
        return self.next_request

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb: Callable[[FakeMsg], Awaitable[None]] | None = None,
    ) -> FakeSubscription:
        assert cb is not None
        handle = FakeSubscription()
        self.subscriptions.append(SubscribeCall(subject=subject, queue=queue, cb=cb))
        self.subscription_handles.append(handle)
        return handle


def install_fake_connect(
    monkeypatch: pytest.MonkeyPatch,
    client: FakeClient,
) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    async def fake_connect(**kwargs: object) -> FakeClient:
        calls.append(kwargs)
        return client

    monkeypatch.setattr(nats_client_module, "connect", fake_connect)
    return calls


@pytest.mark.asyncio
async def test_transport_satisfies_protocol_and_request_round_trips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    install_fake_connect(monkeypatch, client)
    transport = NATSTransport()
    assert isinstance(transport, Transport)

    await transport.connect("nats://nats.example:4222", name="caller.example")
    assert transport.max_payload() == client.max_payload
    client.next_request = FakeMsg(
        subject="_INBOX.reply",
        data=b"pong",
        reply="",
        headers={"Microbus-Msg-Id": "m1"},
    )

    msg = await transport.request(
        subject="microbus.443.com.example.|.GET.ping",
        payload=b"ping",
        timeout=1.25,
        headers={"Traceparent": "00-abc"},
    )

    assert client.requests == [
        RequestCall(
            subject="microbus.443.com.example.|.GET.ping",
            payload=b"ping",
            timeout=1.25,
            headers={"Traceparent": "00-abc"},
        )
    ]
    assert msg == IncomingMessage(
        subject="_INBOX.reply",
        payload=b"pong",
        reply=None,
        headers={"Microbus-Msg-Id": "m1"},
    )


@pytest.mark.asyncio
async def test_connect_publish_and_close_use_nats_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    connect_calls = install_fake_connect(monkeypatch, client)
    transport = NATSTransport(reconnect_time_wait=3.5, max_reconnect_attempts=9)

    await transport.connect("", name="service.example")

    assert connect_calls
    call = connect_calls[0]
    assert call["servers"] == "nats://127.0.0.1:4222"
    assert call["name"] == "service.example"
    # nats-py's internal reconnect must be disabled; this transport owns
    # the jittered reconnect loop. The legacy ctor kwargs (reconnect_time_wait,
    # max_reconnect_attempts) are retained for back-compat but no longer
    # forwarded.
    assert call["allow_reconnect"] is False
    assert call["max_reconnect_attempts"] == 0
    assert callable(call["error_cb"])
    assert callable(call["disconnected_cb"])
    assert callable(call["reconnected_cb"])

    await transport.publish(subject="svc.req", payload=b"one")
    await transport.publish(
        subject="svc.req",
        payload=b"two",
        reply="_INBOX.1",
        headers={"H": "V"},
    )

    assert client.published == [
        PublishCall(subject="svc.req", payload=b"one", reply="", headers=None),
        PublishCall(subject="svc.req", payload=b"two", reply="_INBOX.1", headers={"H": "V"}),
    ]

    await transport.close()
    assert client.drained is True
    await transport.close()


@pytest.mark.asyncio
async def test_connector_startup_preserves_preconnected_nats_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    connect_calls = install_fake_connect(monkeypatch, client)
    transport = NATSTransport()
    svc = Connector(hostname="service.example", transport=transport)

    await transport.connect("nats://nats.example:4222", name="service.example")
    await svc.startup()
    try:
        assert [call["servers"] for call in connect_calls] == ["nats://nats.example:4222"]
    finally:
        await svc.shutdown()


@pytest.mark.asyncio
async def test_close_skips_drain_when_client_is_disconnected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    client.is_connected = False
    install_fake_connect(monkeypatch, client)
    transport = NATSTransport()

    await transport.connect("nats://nats.example:4222", name="service.example")
    await transport.close()

    assert client.drained is False


@pytest.mark.asyncio
async def test_publish_and_subscribe_require_connect() -> None:
    transport = NATSTransport()

    with pytest.raises(RuntimeError, match="transport not connected"):
        await transport.publish(subject="svc.req", payload=b"")

    with pytest.raises(RuntimeError, match="transport not connected"):
        await transport.subscribe(subject="svc.req", queue=None, cb=lambda _msg: _noop())

    with pytest.raises(RuntimeError, match="transport not connected"):
        await transport.request(subject="svc.req", payload=b"", timeout=0.01)


async def _noop() -> None:
    return None


@pytest.mark.asyncio
async def test_subscribe_adapts_nats_messages_and_unsubscribes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    install_fake_connect(monkeypatch, client)
    transport = NATSTransport()
    received: list[IncomingMessage] = []

    async def handler(msg: IncomingMessage) -> None:
        received.append(msg)

    await transport.connect("nats://nats.example:4222", name="service.example")
    handle = await transport.subscribe(subject="svc.*", queue=None, cb=handler)

    assert client.subscriptions[0].subject == "svc.*"
    assert client.subscriptions[0].queue == ""
    assert client.flush_count == 1

    await client.subscriptions[0].cb(
        FakeMsg(
            subject="svc.req",
            data=b"payload",
            reply="_INBOX.2",
            headers={"A": "B"},
        )
    )
    await handle.unsubscribe()

    assert received == [
        IncomingMessage(
            subject="svc.req",
            payload=b"payload",
            reply="_INBOX.2",
            headers={"A": "B"},
        )
    ]
    assert client.subscription_handles[0].unsubscribed is True


@pytest.mark.asyncio
async def test_subscribe_uses_queue_and_drops_handler_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeClient()
    install_fake_connect(monkeypatch, client)
    transport = NATSTransport()
    handler_calls = 0

    async def handler(_msg: IncomingMessage) -> None:
        nonlocal handler_calls
        handler_calls += 1
        raise ValueError("boom")

    await transport.connect("nats://nats.example:4222", name="service.example")
    await transport.subscribe(subject="svc.req", queue="workers", cb=handler)

    assert client.subscriptions[0].queue == "workers"
    await client.subscriptions[0].cb(FakeMsg(subject="svc.req", data=b"payload"))
    assert handler_calls == 1


@pytest.mark.asyncio
async def test_logging_callbacks_do_not_raise() -> None:
    await NATSTransport._on_error(RuntimeError("boom"))
    await NATSTransport._on_reconnected()
    # _on_disconnected is now an instance method that drives the reconnect
    # loop; on a fresh (un-connected, closed-by-default-False) transport
    # with no _url set, it must short-circuit safely. Mark _closed to skip
    # the reconnect attempt.
    transport = NATSTransport()
    transport._closed = True
    await transport._on_disconnected()


def test_internal_client_accessor_rejects_unconnected_transport() -> None:
    transport = NATSTransport()

    with pytest.raises(RuntimeError, match="transport not connected"):
        transport._require_client()
