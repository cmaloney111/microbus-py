#!/usr/bin/env bash
# Run integration tests against external, host, or Docker-managed NATS.
set -euo pipefail
cd "$(dirname "$0")/.."

NATS_PORT="${NATS_PORT:-4222}"
NATS_HTTP_PORT="${NATS_HTTP_PORT:-8222}"
COMPOSE_FILE="docker/docker-compose.integration.yml"
NATS_PID=""
COMPOSE_STARTED="0"

cleanup() {
    if [[ -n "$NATS_PID" ]] && kill -0 "$NATS_PID" 2>/dev/null; then
        kill "$NATS_PID"
        wait "$NATS_PID" 2>/dev/null || true
    fi
    if [[ "$COMPOSE_STARTED" == "1" ]] && [[ "${KEEP:-0}" != "1" ]]; then
        NATS_PORT="$NATS_PORT" NATS_HTTP_PORT="$NATS_HTTP_PORT" \
            docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
    fi
}
trap cleanup EXIT

if [[ -z "${NATS_URL:-}" ]]; then
    if command -v nats-server >/dev/null 2>&1; then
        echo "==> starting nats-server on :$NATS_PORT"
        nats-server --port "$NATS_PORT" --log /tmp/microbus-py-nats.log &
        NATS_PID=$!
        sleep 0.5
    else
        if ! command -v docker >/dev/null 2>&1; then
            echo "nats-server and docker are both unavailable" >&2
            exit 2
        fi
        if ! docker compose version >/dev/null 2>&1; then
            echo "docker compose is unavailable" >&2
            exit 2
        fi
        echo "==> starting Docker NATS on :$NATS_PORT"
        COMPOSE_STARTED="1"
        NATS_PORT="$NATS_PORT" NATS_HTTP_PORT="$NATS_HTTP_PORT" \
            docker compose -f "$COMPOSE_FILE" up -d --wait nats
    fi
    NATS_URL="nats://127.0.0.1:$NATS_PORT"
fi

echo "==> pytest integration"
NATS_URL="$NATS_URL" uv run pytest tests/integration -m integration "$@"

echo "==> integration suite passed"
