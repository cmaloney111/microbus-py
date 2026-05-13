"""Pluggable transport protocol.

The Connector talks to a Transport rather than to a specific broker
implementation, so unit tests can swap in an in-process broker
(``InMemoryBroker``) and integration tests use ``NATSTransport``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

__all__ = ["IncomingMessage", "MessageHandler", "Subscription", "Transport"]


@dataclass(slots=True)
class IncomingMessage:
    """One message delivered to a subscriber."""

    subject: str
    payload: bytes
    reply: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict)


MessageHandler = Callable[[IncomingMessage], Awaitable[None]]


@runtime_checkable
class Subscription(Protocol):
    """Handle returned by ``Transport.subscribe`` so callers can unsubscribe."""

    async def unsubscribe(self) -> None: ...


@runtime_checkable
class Transport(Protocol):
    """Minimum API the Connector needs from any underlying broker."""

    async def connect(self, url: str, *, name: str) -> None: ...

    async def close(self) -> None: ...

    def max_payload(self) -> int: ...

    async def publish(
        self,
        *,
        subject: str,
        payload: bytes,
        reply: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None: ...

    async def subscribe(
        self,
        *,
        subject: str,
        queue: str | None,
        cb: MessageHandler,
    ) -> Subscription: ...

    async def request(
        self,
        *,
        subject: str,
        payload: bytes,
        timeout: float,
        headers: Mapping[str, str] | None = None,
    ) -> IncomingMessage: ...
