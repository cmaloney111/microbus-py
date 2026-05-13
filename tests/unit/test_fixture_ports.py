"""Fixture-managed Docker stacks honor configurable host ports."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tests.fixtures.go_sidecar import _managed_conformance_env, _sidecar_health_url
from tests.fixtures.nats_server import _managed_nats_port, _managed_nats_url

if TYPE_CHECKING:
    import pytest


def test_integration_fixture_uses_configured_nats_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NATS_PORT", "14222")

    assert _managed_nats_port() == 14222
    assert _managed_nats_url() == "nats://127.0.0.1:14222"


def test_conformance_fixture_uses_configured_host_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NATS_PORT", "14223")
    monkeypatch.setenv("GO_SIDECAR_PORT", "18081")

    env = _managed_conformance_env()

    assert env.nats_url == "nats://127.0.0.1:14223"
    assert env.sidecar_url == "http://127.0.0.1:18081"
    assert _sidecar_health_url() == "http://127.0.0.1:18081/health.example/health"
