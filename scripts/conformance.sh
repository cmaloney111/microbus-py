#!/usr/bin/env bash
# Spin up docker-compose with nats-server + Go-Microbus sidecar, run conformance.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found on PATH" >&2
    exit 2
fi

COMPOSE_FILE="docker/docker-compose.conformance.yml"
NATS_PORT="${NATS_PORT:-4222}"
NATS_HTTP_PORT="${NATS_HTTP_PORT:-8222}"
GO_SIDECAR_PORT="${GO_SIDECAR_PORT:-8080}"

compose() {
    NATS_PORT="$NATS_PORT" NATS_HTTP_PORT="$NATS_HTTP_PORT" GO_SIDECAR_PORT="$GO_SIDECAR_PORT" \
        docker compose -f "$COMPOSE_FILE" "$@"
}

cleanup() {
    if [[ "${KEEP:-0}" != "1" ]]; then
        compose down -v --remove-orphans
    fi
}
trap cleanup EXIT

echo "==> docker compose build"
compose build

echo "==> docker compose up --wait"
compose up -d --wait

echo "==> waiting for sidecar /health"
for _ in $(seq 1 60); do
    if curl -sf "http://127.0.0.1:$GO_SIDECAR_PORT/health.example/health" >/dev/null 2>&1; then
        echo "sidecar ready"
        break
    fi
    sleep 1
done

echo "==> pytest conformance"
NATS_URL="nats://127.0.0.1:$NATS_PORT" \
    GO_SIDECAR_URL="http://127.0.0.1:$GO_SIDECAR_PORT" \
    uv run pytest tests/conformance -m conformance --cov-fail-under=0 "$@"

echo "==> conformance suite passed"
