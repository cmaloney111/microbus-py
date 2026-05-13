"""Tests for wire/codec.py — HTTP/1.1 request/response serialization.

The Microbus wire payload is a standard HTTP/1.1 message produced by Go's
`http.Request.WriteProxy()` for requests and `http.Response.Write()` for
responses. Our codec must produce wire-compatible output and round-trip
arbitrary input exactly.

Byte-for-byte parity against captured Go fixtures is a byte-parity conformance
goal. Here we verify:

* round-trip fidelity (encode → decode → equal)
* HTTP/1.1 framing — request line, status line, headers, CRLF separators
* absolute-URI form on the request line (the WriteProxy contract)
* explicit Content-Length on bodies of known size
* exact header preservation (case, order)
* tolerant decode of LF-only line endings (defensive parser, strict emitter)
"""

from __future__ import annotations

import pytest

from microbus_py.wire.codec import (
    HTTPRequest,
    HTTPResponse,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
)


class TestEncodeRequest:
    def test_minimal_get(self) -> None:
        req = HTTPRequest(
            method="GET",
            url="https://example.com/x",
            headers=[],
            body=b"",
        )
        wire = encode_request(req)
        assert wire == (b"GET https://example.com/x HTTP/1.1\r\nHost: example.com\r\n\r\n")

    def test_post_with_json_body(self) -> None:
        req = HTTPRequest(
            method="POST",
            url="https://api.example.com/v1/items",
            headers=[("Content-Type", "application/json")],
            body=b'{"hello":"world"}',
        )
        wire = encode_request(req)
        assert wire == (
            b"POST https://api.example.com/v1/items HTTP/1.1\r\n"
            b"Host: api.example.com\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 17\r\n"
            b"\r\n"
            b'{"hello":"world"}'
        )

    def test_preserves_header_order(self) -> None:
        req = HTTPRequest(
            method="GET",
            url="https://example.com/x",
            headers=[
                ("X-Alpha", "1"),
                ("X-Beta", "2"),
                ("X-Alpha", "3"),  # duplicate header name
            ],
            body=b"",
        )
        wire = encode_request(req)
        # Body of headers must preserve order (after Host)
        text = wire.decode("ascii")
        # Host always emitted first
        assert "X-Alpha: 1\r\nX-Beta: 2\r\nX-Alpha: 3" in text

    def test_explicit_host_header_overrides_url_host(self) -> None:
        req = HTTPRequest(
            method="GET",
            url="https://example.com/x",
            headers=[("Host", "override.example")],
            body=b"",
        )
        wire = encode_request(req)
        assert b"Host: override.example\r\n" in wire
        # Only one Host header should appear
        assert wire.count(b"Host:") == 1

    def test_url_with_port(self) -> None:
        req = HTTPRequest(
            method="GET",
            url="http://example.com:8080/x",
            headers=[],
            body=b"",
        )
        wire = encode_request(req)
        assert b"GET http://example.com:8080/x HTTP/1.1\r\n" in wire
        assert b"Host: example.com:8080\r\n" in wire


class TestDecodeRequest:
    def test_round_trip_minimal_get(self) -> None:
        original = HTTPRequest(
            method="GET",
            url="https://example.com/x",
            headers=[],
            body=b"",
        )
        wire = encode_request(original)
        parsed = decode_request(wire)
        assert parsed.method == "GET"
        assert parsed.url == "https://example.com/x"
        assert parsed.body == b""

    def test_round_trip_post_with_body(self) -> None:
        original = HTTPRequest(
            method="POST",
            url="https://api.example.com/v1/items",
            headers=[("Content-Type", "application/json")],
            body=b'{"k":1}',
        )
        wire = encode_request(original)
        parsed = decode_request(wire)
        assert parsed.method == "POST"
        assert parsed.url == "https://api.example.com/v1/items"
        assert parsed.body == b'{"k":1}'

    def test_decode_tolerates_lf_only_line_endings(self) -> None:
        wire = b"GET https://example.com/x HTTP/1.1\nHost: example.com\n\n"
        parsed = decode_request(wire)
        assert parsed.method == "GET"
        assert parsed.url == "https://example.com/x"

    def test_round_trip_preserves_headers(self) -> None:
        original = HTTPRequest(
            method="POST",
            url="https://example.com/x",
            headers=[
                ("X-Alpha", "1"),
                ("X-Beta", "two"),
                ("Microbus-From-Host", "client.example"),
            ],
            body=b"hello",
        )
        wire = encode_request(original)
        parsed = decode_request(wire)
        # Body bytes preserved exactly
        assert parsed.body == b"hello"
        # Headers contain originals; Host is added by encoder
        names = [n.lower() for n, _ in parsed.headers]
        assert "x-alpha" in names
        assert "x-beta" in names
        assert "microbus-from-host" in names

    def test_decode_rejects_garbage(self) -> None:
        with pytest.raises(ValueError):
            decode_request(b"this is not http")

    def test_decode_rejects_malformed_header_line(self) -> None:
        wire = b"GET https://example.com/x HTTP/1.1\r\nbroken\r\n\r\n"
        with pytest.raises(ValueError, match="malformed header line"):
            decode_request(wire)

    def test_decode_rejects_request_without_version(self) -> None:
        wire = b"GET https://example.com/x\r\n\r\n"
        with pytest.raises(ValueError, match="malformed request line"):
            decode_request(wire)

    def test_decode_request_skips_blank_header_lines(self) -> None:
        # Tolerant parser: extra blank header lines after first are ignored
        wire = b"GET https://example.com/x HTTP/1.1\r\nX-Foo: 1\r\n\r\n"
        parsed = decode_request(wire)
        assert any(n == "X-Foo" for n, _ in parsed.headers)


class TestEncodeResponse:
    def test_minimal_200(self) -> None:
        resp = HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[],
            body=b"",
        )
        wire = encode_response(resp)
        assert wire.startswith(b"HTTP/1.1 200 OK\r\n")
        assert wire.endswith(b"\r\n\r\n") or wire.endswith(b"\r\n")

    def test_response_with_body_includes_content_length(self) -> None:
        resp = HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "text/plain")],
            body=b"hello",
        )
        wire = encode_response(resp)
        assert b"Content-Length: 5\r\n" in wire
        assert wire.endswith(b"\r\n\r\nhello")

    def test_404_with_error_body(self) -> None:
        resp = HTTPResponse(
            status_code=404,
            reason="Not Found",
            headers=[("Content-Type", "application/json")],
            body=b'{"err":"missing"}',
        )
        wire = encode_response(resp)
        assert wire.startswith(b"HTTP/1.1 404 Not Found\r\n")
        assert b"Content-Length: 17\r\n" in wire


class TestDecodeResponse:
    def test_round_trip_200(self) -> None:
        original = HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[("Content-Type", "text/plain")],
            body=b"hello",
        )
        wire = encode_response(original)
        parsed = decode_response(wire)
        assert parsed.status_code == 200
        assert parsed.reason == "OK"
        assert parsed.body == b"hello"

    def test_decode_rejects_malformed_status_line(self) -> None:
        wire = b"NOT-HTTP\r\n\r\n"
        with pytest.raises(ValueError, match="malformed status line"):
            decode_response(wire)

    def test_decode_rejects_non_numeric_status_code(self) -> None:
        wire = b"HTTP/1.1 ABC OK\r\n\r\n"
        with pytest.raises(ValueError, match="non-numeric status code"):
            decode_response(wire)

    def test_decode_response_without_reason(self) -> None:
        wire = b"HTTP/1.1 204\r\n\r\n"
        parsed = decode_response(wire)
        assert parsed.status_code == 204
        assert parsed.reason == ""

    def test_decode_request_rejects_url_without_host(self) -> None:
        # encode_request raises when no Host header and URL has no netloc
        with pytest.raises(ValueError, match="no host"):
            encode_request(HTTPRequest(method="GET", url="/relative", headers=[], body=b""))

    def test_round_trip_500(self) -> None:
        original = HTTPResponse(
            status_code=500,
            reason="Internal Server Error",
            headers=[],
            body=b"oops",
        )
        wire = encode_response(original)
        parsed = decode_response(wire)
        assert parsed.status_code == 500
        assert parsed.reason == "Internal Server Error"
        assert parsed.body == b"oops"


class TestCodecEdgeCases:
    def test_empty_body_omits_content_length(self) -> None:
        # WriteProxy parity: GET with empty body has no Content-Length header
        req = HTTPRequest(
            method="GET",
            url="https://example.com/x",
            headers=[],
            body=b"",
        )
        wire = encode_request(req)
        assert b"Content-Length:" not in wire

    def test_large_body_round_trip(self) -> None:
        body = b"x" * 16_384
        original = HTTPRequest(
            method="POST",
            url="https://example.com/x",
            headers=[],
            body=body,
        )
        wire = encode_request(original)
        parsed = decode_request(wire)
        assert parsed.body == body

    def test_binary_body_round_trip(self) -> None:
        body = bytes(range(256))
        original = HTTPResponse(
            status_code=200,
            reason="OK",
            headers=[],
            body=body,
        )
        wire = encode_response(original)
        parsed = decode_response(wire)
        assert parsed.body == body
