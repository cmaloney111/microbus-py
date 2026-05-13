#!/usr/bin/env bash
# Lint + types + unit tests + coverage gate. Local equivalent of CI.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> ruff check"
uv run ruff check .

echo "==> ruff format --check"
uv run ruff format --check .

echo "==> mypy --strict"
uv run mypy --strict src/microbus_py tests

echo "==> pytest unit"
uv run pytest tests/unit -m "not integration and not conformance" --cov-fail-under=94 "$@"

echo "==> all checks passed"
