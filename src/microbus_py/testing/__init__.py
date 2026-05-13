"""TESTING-context helpers for microbus_py consumers.

This package produces unsigned JWTs and is intended for test contexts only.
Importing it requires ``MICROBUS_TESTING=1`` in the environment so it cannot
be reached accidentally from production code.
"""

from __future__ import annotations

import os

if os.environ.get("MICROBUS_TESTING") != "1":
    raise ImportError(
        "microbus_py.testing produces unsigned and forgeable JWTs. "
        "Set MICROBUS_TESTING=1 to import (intended for test contexts only)."
    )

from microbus_py.testing.actor import mint_signed_actor, mint_unsigned_actor

__all__ = ["mint_signed_actor", "mint_unsigned_actor"]
