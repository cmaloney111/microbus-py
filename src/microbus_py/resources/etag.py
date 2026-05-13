"""Content-hash ETags for resource files."""

from __future__ import annotations

import hashlib

__all__ = ["compute_etag"]


def compute_etag(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]
