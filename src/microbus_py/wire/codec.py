"""HTTP/1.1 wire codec — request and response serialization.

Microbus carries each request as the byte output of Go's
``http.Request.WriteProxy()`` (absolute-URI form on the request line, with an
explicit ``Host`` header) and each response as ``http.Response.Write()``.

Our encoder reproduces a wire-compatible subset of those formats; round-trip
fidelity is guaranteed within Python. Byte-for-byte parity against captured
Go fixtures is enforced by the conformance suite, not here.

Defensive parsing: line endings on inbound payloads may be CRLF (strict Go
emit) or LF-only (lenient peer). The decoder accepts either.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlsplit

__all__ = [
    "HTTPRequest",
    "HTTPResponse",
    "decode_request",
    "decode_response",
    "encode_request",
    "encode_response",
]


@dataclass(slots=True)
class HTTPRequest:
    """A wire-level HTTP/1.1 request."""

    method: str
    url: str
    headers: list[tuple[str, str]] = field(default_factory=list)
    body: bytes = b""


@dataclass(slots=True)
class HTTPResponse:
    """A wire-level HTTP/1.1 response."""

    status_code: int
    reason: str
    headers: list[tuple[str, str]] = field(default_factory=list)
    body: bytes = b""


_CRLF = b"\r\n"


def _host_from_url(url: str) -> str:
    parts = urlsplit(url)
    if not parts.netloc:
        raise ValueError(f"URL has no host: {url!r}")
    return parts.netloc


def _has_header(headers: list[tuple[str, str]], name: str) -> bool:
    needle = name.lower()
    return any(n.lower() == needle for n, _ in headers)


def encode_request(req: HTTPRequest) -> bytes:
    """Serialize an HTTPRequest to its NATS-wire byte form.

    Mirrors Go's ``http.Request.WriteProxy()``:

    * Absolute-URI request line: ``METHOD https://host/path HTTP/1.1``.
    * ``Host`` header is always the first header (Go convention).
      Provided ``Host`` value wins over the URL netloc.
    * Implicit ``Content-Length`` for non-empty bodies.
    * Order of all other headers preserved as provided.
    """
    out: list[bytes] = []
    out.append(f"{req.method} {req.url} HTTP/1.1".encode("ascii"))
    out.append(_CRLF)

    host_value: str | None = None
    rest: list[tuple[str, str]] = []
    for name, value in req.headers:
        if name.lower() == "host" and host_value is None:
            host_value = value
        else:
            rest.append((name, value))
    if host_value is None:
        host_value = _host_from_url(req.url)
    out.append(f"Host: {host_value}".encode("ascii"))
    out.append(_CRLF)

    has_content_length = any(n.lower() == "content-length" for n, _ in rest)
    for name, value in rest:
        out.append(f"{name}: {value}".encode("ascii"))
        out.append(_CRLF)

    if req.body and not has_content_length:
        out.append(f"Content-Length: {len(req.body)}".encode("ascii"))
        out.append(_CRLF)

    out.append(_CRLF)
    out.append(req.body)
    return b"".join(out)


def encode_response(resp: HTTPResponse) -> bytes:
    """Serialize an HTTPResponse to its NATS-wire byte form."""
    out: list[bytes] = []
    out.append(f"HTTP/1.1 {resp.status_code} {resp.reason}".encode("ascii"))
    out.append(_CRLF)

    has_content_length = _has_header(resp.headers, "Content-Length")
    for name, value in resp.headers:
        out.append(f"{name}: {value}".encode("ascii"))
        out.append(_CRLF)

    if resp.body and not has_content_length:
        out.append(f"Content-Length: {len(resp.body)}".encode("ascii"))
        out.append(_CRLF)

    out.append(_CRLF)
    out.append(resp.body)
    return b"".join(out)


def _split_head_and_body(data: bytes) -> tuple[list[bytes], bytes]:
    """Split a raw HTTP/1.1 message into header lines and body bytes.

    Tolerant: accepts both CRLF and LF line endings.
    """
    crlf2 = data.find(b"\r\n\r\n")
    lf2 = data.find(b"\n\n")
    sep_idx = -1
    sep_len = 0
    if crlf2 != -1 and (lf2 == -1 or crlf2 < lf2):
        sep_idx = crlf2
        sep_len = 4
    elif lf2 != -1:
        sep_idx = lf2
        sep_len = 2
    if sep_idx == -1:
        raise ValueError("malformed HTTP message: no header/body separator")
    head_bytes = data[:sep_idx]
    body = data[sep_idx + sep_len :]

    lines = head_bytes.split(b"\r\n") if b"\r\n" in head_bytes else head_bytes.split(b"\n")
    return lines, body


def _parse_headers(lines: list[bytes]) -> list[tuple[str, str]]:
    headers: list[tuple[str, str]] = []
    for raw in lines:
        if not raw:
            continue
        sep = raw.find(b":")
        if sep == -1:
            raise ValueError(f"malformed header line: {raw!r}")
        name = raw[:sep].decode("ascii").strip()
        value = raw[sep + 1 :].decode("ascii").strip()
        headers.append((name, value))
    return headers


def decode_request(data: bytes) -> HTTPRequest:
    """Parse a wire-format HTTP/1.1 request emitted by ``encode_request``."""
    lines, body = _split_head_and_body(data)
    if not lines:
        raise ValueError("empty HTTP request")
    request_line = lines[0].decode("ascii", errors="strict")
    parts = request_line.split(" ", 2)
    if len(parts) != 3 or not parts[2].startswith("HTTP/"):
        raise ValueError(f"malformed request line: {request_line!r}")
    method, url, _version = parts
    headers = _parse_headers(lines[1:])
    return HTTPRequest(method=method, url=url, headers=headers, body=body)


def decode_response(data: bytes) -> HTTPResponse:
    """Parse a wire-format HTTP/1.1 response emitted by ``encode_response``."""
    lines, body = _split_head_and_body(data)
    if not lines:
        raise ValueError("empty HTTP response")
    status_line = lines[0].decode("ascii", errors="strict")
    parts = status_line.split(" ", 2)
    if len(parts) < 2 or not parts[0].startswith("HTTP/"):
        raise ValueError(f"malformed status line: {status_line!r}")
    try:
        status_code = int(parts[1])
    except ValueError as exc:
        raise ValueError(f"non-numeric status code: {status_line!r}") from exc
    reason = parts[2] if len(parts) == 3 else ""
    headers = _parse_headers(lines[1:])
    return HTTPResponse(
        status_code=status_code,
        reason=reason,
        headers=headers,
        body=body,
    )
