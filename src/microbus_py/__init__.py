"""microbus-py — Python SDK for Microbus.

Lets Python services participate as first-class peers in a Microbus
deployment, communicating over NATS using Microbus's HTTP-over-NATS wire
protocol.
"""

from __future__ import annotations

from microbus_py._version import __version__
from microbus_py.connector.connector import Connector, Handler
from microbus_py.errors.traced import TracedError
from microbus_py.errors.types import (
    BadRequest,
    Conflict,
    Forbidden,
    InternalError,
    NotFound,
    TooManyRequests,
    Unauthorized,
    Unavailable,
)
from microbus_py.transport.base import Transport
from microbus_py.transport.inmemory import InMemoryBroker

__all__ = [
    "BadRequest",
    "Conflict",
    "Connector",
    "Forbidden",
    "Handler",
    "InMemoryBroker",
    "InternalError",
    "NotFound",
    "TooManyRequests",
    "TracedError",
    "Transport",
    "Unauthorized",
    "Unavailable",
    "__version__",
]
