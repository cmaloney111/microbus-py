"""Tests for the resource filesystem wrapper."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from microbus_py.resources.etag import compute_etag
from microbus_py.resources.fs import Resources
from microbus_py.wire.codec import HTTPRequest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def res_dir(tmp_path: Path) -> Path:
    root = tmp_path / "resources"
    root.mkdir()
    (root / "hello.txt").write_text("hello world", encoding="utf-8")
    (root / "page.html").write_bytes(b"<h1>x</h1>")
    sub = root / "nested"
    sub.mkdir()
    (sub / "deep.txt").write_text("deep", encoding="utf-8")
    return root


def test_read_bytes_known_fixture(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    assert res.read_bytes("hello.txt") == b"hello world"


def test_read_text_decodes_utf8(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    assert res.read_text("hello.txt") == "hello world"


def test_read_bytes_nested_path(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    assert res.read_bytes("nested/deep.txt") == b"deep"


def test_etag_stable_for_same_bytes(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    assert res.etag("hello.txt") == res.etag("hello.txt")


def test_etag_differs_for_different_bytes(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    assert res.etag("hello.txt") != res.etag("page.html")


def test_etag_format_is_16_hex_chars(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    tag = res.etag("hello.txt")
    assert len(tag) == 16
    int(tag, 16)


def test_compute_etag_directly() -> None:
    assert compute_etag(b"hello world") == compute_etag(b"hello world")
    assert compute_etag(b"a") != compute_etag(b"b")
    assert len(compute_etag(b"x")) == 16


def test_missing_file_raises_filenotfound(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    with pytest.raises(FileNotFoundError):
        res.read_bytes("missing.txt")


def test_missing_file_etag_raises(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    with pytest.raises(FileNotFoundError):
        res.etag("missing.txt")


def test_resources_from_package_uses_importlib() -> None:
    # microbus_py itself ships at least __init__.py; loading via importlib should work.
    res = Resources("microbus_py", root="")
    data = res.read_bytes("__init__.py")
    assert isinstance(data, bytes)
    assert len(data) > 0


def test_resources_from_package_with_root_subdir() -> None:
    # Use a known subpackage: microbus_py.resources/__init__.py exists.
    res = Resources("microbus_py", root="resources")
    data = res.read_bytes("__init__.py")
    assert isinstance(data, bytes)
    assert len(data) > 0


def test_resources_from_package_missing_raises() -> None:
    res = Resources("microbus_py", root="")
    with pytest.raises(FileNotFoundError):
        res.read_bytes("does_not_exist.bin")


@pytest.mark.asyncio
async def test_serve_handler_returns_file_with_etag(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    handler = res.serve_handler(mount="/static")

    resp = await handler(HTTPRequest(method="GET", url="https://x.example:443/static/hello.txt"))
    assert resp.status_code == 200
    assert resp.body == b"hello world"
    headers = dict(resp.headers)
    assert "ETag" in headers
    assert headers["Content-Type"].startswith("text/plain")


@pytest.mark.asyncio
async def test_serve_handler_returns_304_when_etag_matches(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    handler = res.serve_handler(mount="/static")

    first = await handler(HTTPRequest(method="GET", url="https://x.example:443/static/hello.txt"))
    tag = dict(first.headers)["ETag"]
    second = await handler(
        HTTPRequest(
            method="GET",
            url="https://x.example:443/static/hello.txt",
            headers=[("If-None-Match", tag)],
        )
    )
    assert second.status_code == 304
    assert second.body == b""


@pytest.mark.asyncio
async def test_serve_handler_404_for_missing(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    handler = res.serve_handler(mount="/static")
    resp = await handler(HTTPRequest(method="GET", url="https://x.example:443/static/missing.bin"))
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_serve_handler_404_for_empty_path(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    handler = res.serve_handler(mount="/static")
    resp = await handler(HTTPRequest(method="GET", url="https://x.example:443/static/"))
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_serve_handler_no_mount_serves_root(res_dir: Path) -> None:
    res = Resources.from_dir(res_dir)
    handler = res.serve_handler(mount="")
    resp = await handler(HTTPRequest(method="GET", url="https://x.example:443/hello.txt"))
    assert resp.status_code == 200
    assert resp.body == b"hello world"
