"""Local gate runner contracts."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_conformance_compose_uses_configurable_host_ports() -> None:
    compose = (ROOT / "docker" / "docker-compose.conformance.yml").read_text(encoding="utf-8")

    assert '"${NATS_PORT:-4222}:4222"' in compose
    assert '"${NATS_HTTP_PORT:-8222}:8222"' in compose
    assert '"${GO_SIDECAR_PORT:-8080}:8080"' in compose


def test_conformance_runner_passes_configurable_ports_to_compose_and_pytest() -> None:
    runner = (ROOT / "scripts" / "conformance.sh").read_text(encoding="utf-8")

    assert 'NATS_PORT="${NATS_PORT:-4222}"' in runner
    assert 'NATS_HTTP_PORT="${NATS_HTTP_PORT:-8222}"' in runner
    assert 'GO_SIDECAR_PORT="${GO_SIDECAR_PORT:-8080}"' in runner
    compose_env = (
        'NATS_PORT="$NATS_PORT" NATS_HTTP_PORT="$NATS_HTTP_PORT" GO_SIDECAR_PORT="$GO_SIDECAR_PORT"'
    )
    assert compose_env in runner
    assert '"http://127.0.0.1:$GO_SIDECAR_PORT/health.example/health"' in runner
    assert 'GO_SIDECAR_URL="http://127.0.0.1:$GO_SIDECAR_PORT"' in runner


def test_local_gate_scripts_run_python_tools_through_uv() -> None:
    check = (ROOT / "scripts" / "check.sh").read_text(encoding="utf-8")
    integration = (ROOT / "scripts" / "integration.sh").read_text(encoding="utf-8")
    conformance = (ROOT / "scripts" / "conformance.sh").read_text(encoding="utf-8")

    assert "uv run ruff check ." in check
    assert "uv run ruff format --check ." in check
    assert "uv run mypy --strict src/microbus_py tests" in check
    assert 'uv run pytest tests/unit -m "not integration and not conformance"' in check
    assert 'NATS_URL="$NATS_URL" uv run pytest tests/integration -m integration "$@"' in integration
    assert "uv run pytest tests/conformance -m conformance --cov-fail-under=0" in conformance
