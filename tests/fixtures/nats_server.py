"""pytest fixture that ensures a nats-server is reachable for integration tests.

Two modes:

1. **External**: if ``$NATS_URL`` is set (CI provides it), assume the server is
   already running and just yield the URL.
2. **Docker-managed**: otherwise spin up the ``nats:2.10-alpine`` container via
   docker compose, wait for the healthcheck, yield, then tear down.

Tests opt in with ``@pytest.mark.integration`` and the ``nats_url`` fixture.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker" / "docker-compose.integration.yml"
DEFAULT_NATS_PORT = 4222


def _managed_nats_port() -> int:
    return int(os.environ.get("NATS_PORT", str(DEFAULT_NATS_PORT)))


def _managed_nats_url() -> str:
    return f"nats://127.0.0.1:{_managed_nats_port()}"


def _is_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _wait_for_port(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _is_port_open(host, port):
            return
        time.sleep(0.2)
    raise RuntimeError(f"nats-server at {host}:{port} did not come up within {timeout}s")


@pytest.fixture(scope="session")
def nats_url() -> Iterator[str]:
    external = os.environ.get("NATS_URL")
    if external:
        yield external
        return

    if not COMPOSE_FILE.exists():
        pytest.skip(f"docker-compose file missing: {COMPOSE_FILE}")
    try:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "nats"],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        pytest.skip(f"docker compose unavailable: {exc}")

    try:
        _wait_for_port("127.0.0.1", _managed_nats_port(), timeout=30.0)
        yield _managed_nats_url()
    finally:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
            check=False,
            capture_output=True,
        )


@pytest.fixture
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    return asyncio.DefaultEventLoopPolicy()
