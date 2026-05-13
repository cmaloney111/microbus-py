"""``microbus-py run <module>`` — boot a Connector and block until SIGINT."""

from __future__ import annotations

import asyncio
import importlib
import signal

import click

__all__ = ["run_cmd"]


async def _wait_forever() -> None:
    loop = asyncio.get_event_loop()
    stop: asyncio.Future[None] = loop.create_future()

    def _signal_stop() -> None:
        if not stop.done():
            stop.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_stop)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, lambda _signum, _frame: _signal_stop())
    await stop


@click.command(name="run")
@click.argument("module")
def run_cmd(module: str) -> None:
    """Import ``module``, call ``svc.startup()``, wait for SIGINT, then ``shutdown()``."""
    try:
        mod = importlib.import_module(module)
    except ImportError as exc:
        raise click.ClickException(f"could not import module {module!r}: {exc}") from exc
    svc = getattr(mod, "svc", None)
    if svc is None:
        raise click.ClickException(f"module {module!r} does not expose a top-level `svc`")

    async def _main() -> None:
        await svc.startup()
        try:
            await _wait_forever()
        finally:
            await svc.shutdown()

    asyncio.run(_main())
