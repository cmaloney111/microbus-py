"""``Application`` — multi-connector lifecycle aggregator.

Bundles connectors so a single ``startup()`` brings them up in order and
``shutdown()`` tears them down in reverse. ``run_in_test()`` is the async
context-manager idiom for tests; it guarantees teardown on exception.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from microbus_py.connector.connector import Connector

__all__ = ["Application"]

_APPLICATION_FAILURES: tuple[type[Exception], ...] = (Exception,)


class Application:
    def __init__(self) -> None:
        self._connectors: list[Connector] = []

    def add(self, c: Connector) -> Application:
        self._connectors.append(c)
        return self

    @property
    def connectors(self) -> list[Connector]:
        return list(self._connectors)

    async def startup(self) -> None:
        started: list[Connector] = []
        try:
            for c in self._connectors:
                await c.startup()
                started.append(c)
        except _APPLICATION_FAILURES:
            for c in reversed(started):
                with contextlib.suppress(*_APPLICATION_FAILURES):
                    await c.shutdown()
            raise

    async def shutdown(self) -> None:
        for c in reversed(self._connectors):
            with contextlib.suppress(*_APPLICATION_FAILURES):
                await c.shutdown()

    @contextlib.asynccontextmanager
    async def run_in_test(self) -> AsyncIterator[Application]:
        await self.startup()
        try:
            yield self
        finally:
            await self.shutdown()
