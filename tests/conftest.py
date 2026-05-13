"""Shared pytest fixtures."""

from __future__ import annotations

import os

# Tests use the unsigned-actor minter and the TESTING deployment mode.
# Both require explicit opt-in via env so they cannot be reached from
# production import paths.
os.environ.setdefault("MICROBUS_TESTING", "1")
os.environ.setdefault("MICROBUS_ALLOW_TESTING", "1")

from tests.fixtures.go_sidecar import conformance_env
from tests.fixtures.nats_server import nats_url
from tests.fixtures.otlp_collector import otlp_collector

__all__ = ["conformance_env", "nats_url", "otlp_collector"]
