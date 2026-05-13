"""Tests for microbus_py.testing.actor — TESTING-context JWT minting helpers."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from microbus_py.frame.actor import parse_actor
from microbus_py.testing import mint_signed_actor, mint_unsigned_actor


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _decode_segment(seg: str) -> dict[str, object]:
    decoded: dict[str, object] = json.loads(_b64url_decode(seg))
    return decoded


def test_mint_unsigned_actor_returns_three_segments() -> None:
    token = mint_unsigned_actor()
    assert token.endswith(".")
    parts = token.split(".")
    assert len(parts) == 3
    assert parts[2] == ""


def test_mint_unsigned_actor_default_claims() -> None:
    token = mint_unsigned_actor()
    header_b64, payload_b64, _ = token.split(".")
    header = _decode_segment(header_b64)
    payload = _decode_segment(payload_b64)
    assert header == {"alg": "none", "typ": "JWT"}
    assert payload["sub"] == "user-test"
    assert payload["iss"] == "auth.example"
    assert "iat" in payload
    assert "exp" in payload


def test_mint_unsigned_actor_custom_iss() -> None:
    token = mint_unsigned_actor(iss="other.svc")
    _, payload_b64, _ = token.split(".")
    payload = _decode_segment(payload_b64)
    assert payload["iss"] == "other.svc"


def test_mint_unsigned_actor_ttl_sets_exp_minus_iat() -> None:
    ttl = timedelta(minutes=5)
    token = mint_unsigned_actor(ttl=ttl)
    _, payload_b64, _ = token.split(".")
    payload = _decode_segment(payload_b64)
    iat = payload["iat"]
    exp = payload["exp"]
    assert isinstance(iat, int)
    assert isinstance(exp, int)
    assert exp - iat == int(ttl.total_seconds())


def test_mint_unsigned_actor_roundtrip_through_parse_actor() -> None:
    token = mint_unsigned_actor(sub="alice")
    actor = parse_actor(token, verify=False)
    assert actor.sub == "alice"


def test_mint_unsigned_actor_extra_claims_merge() -> None:
    token = mint_unsigned_actor(extra={"roles": ["admin"], "tier": "pro"})
    _, payload_b64, _ = token.split(".")
    payload = _decode_segment(payload_b64)
    assert payload["roles"] == ["admin"]
    assert payload["tier"] == "pro"
    assert payload["iss"] == "auth.example"
    assert payload["sub"] == "user-test"
    assert "iat" in payload
    assert "exp" in payload


def test_mint_unsigned_actor_roles_kwarg_lands_in_claims() -> None:
    token = mint_unsigned_actor(roles=["admin", "elevated"])
    _, payload_b64, _ = token.split(".")
    payload = _decode_segment(payload_b64)
    assert payload["roles"] == ["admin", "elevated"]


def test_mint_unsigned_actor_explicit_now_overrides_clock() -> None:
    pinned = datetime(2026, 1, 1, tzinfo=UTC)
    token = mint_unsigned_actor(now=pinned)
    _, payload_b64, _ = token.split(".")
    payload = _decode_segment(payload_b64)
    assert payload["iat"] == int(pinned.timestamp())


def test_mint_signed_actor_ed25519_verifies() -> None:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    token = mint_signed_actor(
        private_key=private_key,
        kid="v1",
        sub="bob",
        iss="microbus://access.token.core",
    )
    actor = parse_actor(token, verify=True, public_keys={"v1": public_key})
    assert actor.sub == "bob"


def test_mint_signed_actor_kid_in_header() -> None:
    private_key = Ed25519PrivateKey.generate()
    token = mint_signed_actor(private_key=private_key, kid="rotated-key")
    header_b64, _, _ = token.split(".")
    header = _decode_segment(header_b64)
    assert header["kid"] == "rotated-key"


def test_mint_signed_actor_tampered_payload_fails_verification() -> None:
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    token = mint_signed_actor(
        private_key=private_key,
        kid="v1",
        sub="bob",
        iss="microbus://access.token.core",
    )
    header_b64, payload_b64, sig_b64 = token.split(".")
    payload = _decode_segment(payload_b64)
    payload["sub"] = "mallory"
    tampered_payload = (
        base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        .rstrip(b"=")
        .decode("ascii")
    )
    tampered_token = f"{header_b64}.{tampered_payload}.{sig_b64}"
    with pytest.raises(ValueError, match="signature"):
        parse_actor(
            tampered_token,
            verify=True,
            public_keys={"v1": Ed25519PublicKey.from_public_bytes(public_key.public_bytes_raw())},
        )
