"""Resource filesystem wrapper.

Mirrors fabric's ``Connector.ReadResFile`` / ``ResFS`` family. Reads files
from a Python package via :mod:`importlib.resources`, or from a plain
directory in tests. ETags are content hashes (SHA-256, first 16 hex chars).
"""

from __future__ import annotations

import mimetypes
from importlib.resources import as_file, files
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from microbus_py.resources.etag import compute_etag
from microbus_py.wire.codec import HTTPRequest, HTTPResponse

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = ["Resources"]


class Resources:
    """Read static resources from a package or filesystem directory."""

    def __init__(self, package: str, root: str = "resources") -> None:
        self._package = package
        self._root = root
        self._dir: Path | None = None

    @classmethod
    def from_dir(cls, directory: str | Path) -> Resources:
        self = cls.__new__(cls)
        self._package = ""
        self._root = ""
        self._dir = Path(directory)
        return self

    def _resolve(self, path: str) -> Path:
        if self._dir is not None:
            target = self._dir / path
            if not target.is_file():
                raise FileNotFoundError(path)
            return target
        anchor = files(self._package)
        if self._root:
            anchor = anchor.joinpath(self._root)
        ref = anchor.joinpath(path)
        if not ref.is_file():
            raise FileNotFoundError(path)
        with as_file(ref) as p:
            return Path(p)

    def read_bytes(self, path: str) -> bytes:
        return self._resolve(path).read_bytes()

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self._resolve(path).read_text(encoding=encoding)

    def etag(self, path: str) -> str:
        return compute_etag(self.read_bytes(path))

    def serve_handler(
        self,
        *,
        mount: str,
    ) -> Callable[[HTTPRequest], Awaitable[HTTPResponse]]:
        prefix = mount.rstrip("/")

        async def handler(req: HTTPRequest) -> HTTPResponse:
            path = urlsplit(req.url).path
            rel = path[len(prefix) :] if prefix and path.startswith(prefix) else path
            rel = rel.lstrip("/")
            if not rel:
                return HTTPResponse(status_code=404, reason="Not Found")
            try:
                data = self.read_bytes(rel)
            except FileNotFoundError:
                return HTTPResponse(status_code=404, reason="Not Found")
            tag = compute_etag(data)
            inm = next(
                (v for n, v in req.headers if n.lower() == "if-none-match"),
                None,
            )
            if inm == tag:
                return HTTPResponse(
                    status_code=304,
                    reason="Not Modified",
                    headers=[("ETag", tag)],
                )
            ctype, _ = mimetypes.guess_type(rel)
            return HTTPResponse(
                status_code=200,
                reason="OK",
                headers=[
                    ("ETag", tag),
                    ("Cache-Control", "max-age=3600, private, stale-while-revalidate=3600"),
                    ("Content-Type", ctype or "application/octet-stream"),
                ],
                body=data,
            )

        return handler
