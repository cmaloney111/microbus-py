"""pytest fixture for the conformance Go sidecar.

Two modes:
1. **External**: ``$GO_SIDECAR_URL`` set → assume stack already running.
2. **Docker-managed**: spin up ``docker/docker-compose.conformance.yml`` for the
   session, wait for the sidecar's healthcheck, yield, tear down.

Tests opt in with ``@pytest.mark.conformance`` and the ``conformance_env``
fixture. Tests are skipped cleanly when neither mode is available so that
``pytest tests/conformance`` is safe in CI without docker.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from microbus_py import Connector
from microbus_py.transport.nats import NATSTransport

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker" / "docker-compose.conformance.yml"
DEFAULT_NATS_PORT = 4222
DEFAULT_GO_SIDECAR_PORT = 8080


@dataclass(slots=True)
class ConformanceEnv:
    nats_url: str
    sidecar_url: str
    plane: str = "microbus"
    deployment: str = "TESTING"
    locality: str = "conformance"


def _env_port(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _managed_nats_port() -> int:
    return _env_port("NATS_PORT", DEFAULT_NATS_PORT)


def _managed_nats_url() -> str:
    return f"nats://127.0.0.1:{_managed_nats_port()}"


def _managed_sidecar_url() -> str:
    port = _env_port("GO_SIDECAR_PORT", DEFAULT_GO_SIDECAR_PORT)
    return f"http://127.0.0.1:{port}"


def _sidecar_health_url() -> str:
    return f"{_managed_sidecar_url()}/health.example/health"


def _managed_conformance_env() -> ConformanceEnv:
    return ConformanceEnv(
        nats_url=_managed_nats_url(),
        sidecar_url=_managed_sidecar_url(),
    )


async def connector_for(
    env: ConformanceEnv,
    hostname: str,
    *,
    version: int = 0,
    description: str = "",
    deployment: str | None = None,
    trusted_issuer_hosts: Sequence[str] | None = None,
) -> Connector:
    transport = NATSTransport()
    svc = Connector(
        hostname=hostname,
        transport=transport,
        version=version,
        description=description,
        plane=env.plane,
        deployment=deployment or env.deployment,
        locality=env.locality,
        trusted_issuer_hosts=trusted_issuer_hosts,
    )
    await transport.connect(env.nats_url, name=hostname)
    return svc


def _is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_for_url(url: str, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                if resp.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"sidecar at {url} did not come up within {timeout}s")


@pytest.fixture(scope="session")
def conformance_env() -> Iterator[ConformanceEnv]:
    external = os.environ.get("GO_SIDECAR_URL")
    if external:
        nats_url = os.environ.get("NATS_URL", _managed_nats_url())
        yield ConformanceEnv(nats_url=nats_url, sidecar_url=external)
        return

    if not COMPOSE_FILE.exists():
        pytest.skip(f"compose file missing: {COMPOSE_FILE}")
    try:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait"],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        pytest.skip(f"docker compose unavailable: {exc}")

    try:
        nats_port = _managed_nats_port()
        if not _is_port_open("127.0.0.1", nats_port):
            pytest.skip(f"nats not reachable on 127.0.0.1:{nats_port} after compose up")
        _wait_for_url(_sidecar_health_url(), timeout=60.0)
        yield _managed_conformance_env()
    finally:
        if os.environ.get("KEEP") != "1":
            subprocess.run(
                ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
                check=False,
                capture_output=True,
            )
