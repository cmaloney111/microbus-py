"""Semantic-parity tests for the wire codec against Go's ``WriteProxy`` output.

Goldens live under ``tests/fixtures/golden/`` as paired ``*.json`` (input
descriptor) + ``*.bin`` (Go-emitted bytes). Capture is one-shot via
``docker/gosidecar/cmd/golden`` — see ``tests/conformance/README.md``.

We assert *semantic* equivalence (decoded request line, headers as a set,
body bytes) rather than byte-for-byte equality because Go's ``WriteProxy``
alphabetizes header names while Python's ``encode_request`` preserves
insertion order. Both encodings are valid HTTP/1.1; the receiver doesn't
care about header order. The conformance guarantee that matters is:
Python's decoder accepts Go's bytes verbatim, and the recovered fields
match what Python's encoder would have emitted from the same input.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from microbus_py.wire.codec import HTTPRequest, decode_request, encode_request

GOLDEN_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "golden"


def _golden_pairs() -> list[tuple[Path, Path]]:
    if not GOLDEN_DIR.is_dir():
        return []
    pairs: list[tuple[Path, Path]] = []
    for js in sorted(GOLDEN_DIR.glob("*.json")):
        bin_path = js.with_suffix(".bin")
        if bin_path.exists():
            pairs.append((js, bin_path))
    return pairs


GOLDENS = _golden_pairs()
pytestmark = pytest.mark.conformance


@pytest.mark.skipif(not GOLDENS, reason="no golden fixtures committed yet")
@pytest.mark.parametrize(
    ("descriptor", "expected_bin"),
    GOLDENS,
    ids=[p[0].stem for p in GOLDENS] if GOLDENS else [],
)
def test_python_decoder_accepts_go_writeproxy_output(descriptor: Path, expected_bin: Path) -> None:
    spec = json.loads(descriptor.read_text())
    go_bytes = expected_bin.read_bytes()
    decoded = decode_request(go_bytes)

    assert decoded.method == spec["method"]
    assert decoded.url == spec["url"]
    expected_body = bytes.fromhex(spec.get("bodyHex", ""))
    assert decoded.body == expected_body

    decoded_headers = {(k, v) for k, v in decoded.headers if k.lower() != "host"}
    expected_headers = {(k, v) for k, v in spec.get("headers", [])}
    if expected_body:
        expected_headers.add(("Content-Length", str(len(expected_body))))
    assert expected_headers <= decoded_headers, (
        f"missing headers in decode: {expected_headers - decoded_headers}"
    )


@pytest.mark.skipif(not GOLDENS, reason="no golden fixtures committed yet")
@pytest.mark.parametrize(
    ("descriptor", "expected_bin"),
    GOLDENS,
    ids=[p[0].stem for p in GOLDENS] if GOLDENS else [],
)
def test_python_encoder_round_trips_through_decode(descriptor: Path, expected_bin: Path) -> None:
    spec = json.loads(descriptor.read_text())
    body = bytes.fromhex(spec.get("bodyHex", ""))
    req = HTTPRequest(
        method=spec["method"],
        url=spec["url"],
        headers=[(k, v) for k, v in spec.get("headers", [])],
        body=body,
    )
    py_bytes = encode_request(req)
    redecoded = decode_request(py_bytes)
    assert redecoded.method == req.method
    assert redecoded.url == req.url
    assert redecoded.body == req.body
