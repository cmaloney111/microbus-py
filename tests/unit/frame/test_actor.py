"""Tests for frame/actor.py — JWT actor parsing."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from microbus_py.frame.actor import (
    Actor,
    UnknownActorKeyError,
    parse_actor,
    public_key_from_jwk,
    token_issuer,
)


def _build_unsigned_token(claims: dict[str, object]) -> str:
    return pyjwt.encode(claims, key="", algorithm="none")


def _build_signed_token(claims: dict[str, object], *, kid: str = "default") -> tuple[str, bytes]:
    private_key = Ed25519PrivateKey.generate()
    public_key_bytes = private_key.public_key().public_bytes_raw()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    merged_claims = {
        "iss": "https://access.token.core",
        "microbus": "1",
        **claims,
    }
    token = pyjwt.encode(merged_claims, key=private_pem, algorithm="EdDSA", headers={"kid": kid})
    return token, public_key_bytes


class TestParseActorUnsigned:
    def test_parses_subject(self) -> None:
        token = _build_unsigned_token({"sub": "user@example.com"})
        actor = parse_actor(token, verify=False)
        assert actor.sub == "user@example.com"

    def test_parses_iss_and_extra_claims(self) -> None:
        token = _build_unsigned_token({"sub": "u1", "iss": "access.token.core", "roles": ["admin"]})
        actor = parse_actor(token, verify=False)
        assert actor.iss == "access.token.core"
        assert actor.claims["roles"] == ["admin"]


class TestParseActorSigned:
    def test_verifies_with_correct_public_key(self) -> None:
        token, pub_bytes = _build_signed_token({"sub": "u1"})
        pub_key = Ed25519PublicKey.from_public_bytes(pub_bytes)
        actor = parse_actor(token, verify=True, public_keys={"default": pub_key})
        assert actor.sub == "u1"

    def test_rejects_with_wrong_public_key(self) -> None:
        token, _ = _build_signed_token({"sub": "u1"})
        wrong_pub = Ed25519PrivateKey.generate().public_key().public_bytes_raw()
        wrong_key = Ed25519PublicKey.from_public_bytes(wrong_pub)
        with pytest.raises(ValueError, match="signature"):
            parse_actor(token, verify=True, public_keys={"default": wrong_key})

    def test_rejects_expired_token(self) -> None:
        past = datetime.now(UTC) - timedelta(hours=1)
        token, pub_bytes = _build_signed_token({"sub": "u1", "exp": int(past.timestamp())})
        pub_key = Ed25519PublicKey.from_public_bytes(pub_bytes)
        with pytest.raises(ValueError, match="expired"):
            parse_actor(token, verify=True, public_keys={"default": pub_key})

    def test_rejects_unknown_kid(self) -> None:
        token, pub_bytes = _build_signed_token({"sub": "u1"}, kid="rotated")
        pub_key = Ed25519PublicKey.from_public_bytes(pub_bytes)
        with pytest.raises(ValueError, match="unknown token kid"):
            parse_actor(token, verify=True, public_keys={"default": pub_key})

    def test_rejects_signed_non_microbus_token(self) -> None:
        private_key = Ed25519PrivateKey.generate()
        public_key_bytes = private_key.public_key().public_bytes_raw()
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        token = pyjwt.encode(
            {"sub": "u1", "iss": "https://other.example"},
            key=private_pem,
            algorithm="EdDSA",
            headers={"kid": "default"},
        )
        pub_key = Ed25519PublicKey.from_public_bytes(public_key_bytes)

        with pytest.raises(ValueError, match="microbus issuer"):
            parse_actor(token, verify=True, public_keys={"default": pub_key})


class TestActorClaimsAccess:
    def test_actor_exposes_claims_dict(self) -> None:
        token = _build_unsigned_token({"sub": "u", "tid": 7, "roles": ["a", "b"]})
        actor = parse_actor(token, verify=False)
        assert actor.claims == {"sub": "u", "tid": 7, "roles": ["a", "b"]}

    def test_actor_is_frozen(self) -> None:
        token = _build_unsigned_token({"sub": "u"})
        actor = parse_actor(token, verify=False)
        with pytest.raises((AttributeError, TypeError)):
            actor.__setattr__("sub", "other")


class TestActorTokenIssuer:
    def test_reads_unverified_issuer(self) -> None:
        token = _build_unsigned_token({"sub": "u", "iss": "https://access.token.core"})
        assert token_issuer(token) == "https://access.token.core"

    def test_missing_issuer_is_empty_string(self) -> None:
        token = _build_unsigned_token({"sub": "u"})
        assert token_issuer(token) == ""

    def test_require_microbus_rejects_non_microbus_issuer(self) -> None:
        token = _build_unsigned_token({"sub": "u", "iss": "https://other.example"})
        with pytest.raises(ValueError, match="microbus issuer"):
            token_issuer(token, require_microbus=True)

    def test_require_microbus_allows_backcompat_scheme(self) -> None:
        token = _build_unsigned_token({"sub": "u", "iss": "microbus://access.token.core"})
        assert token_issuer(token, require_microbus=True) == "microbus://access.token.core"

    def test_malformed_token_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid token"):
            token_issuer("not.a.jwt")


class TestActorJWK:
    def test_public_key_from_jwk_verifies_signed_token(self) -> None:
        token, pub_bytes = _build_signed_token({"sub": "u1"}, kid="k1")
        encoded_key = base64.urlsafe_b64encode(pub_bytes).decode("ascii").rstrip("=")

        kid, key = public_key_from_jwk(
            {"kid": "k1", "x": encoded_key, "kty": "OKP", "crv": "Ed25519"}
        )
        actor = parse_actor(token, verify=True, public_keys={kid: key})

        assert kid == "k1"
        assert actor.sub == "u1"

    def test_public_key_from_jwk_allows_minimal_go_shape(self) -> None:
        _, pub_bytes = _build_signed_token({"sub": "u1"})
        encoded_key = base64.urlsafe_b64encode(pub_bytes).decode("ascii").rstrip("=")

        kid, key = public_key_from_jwk({"kid": "k1", "x": encoded_key})

        assert kid == "k1"
        assert isinstance(key, Ed25519PublicKey)

    def test_public_key_from_jwk_requires_kid_and_x(self) -> None:
        with pytest.raises(ValueError, match="jwk kid and x are required"):
            public_key_from_jwk({"kid": "k1"})

        with pytest.raises(ValueError, match="jwk kid and x are required"):
            public_key_from_jwk({"x": "abc"})

    def test_public_key_from_jwk_rejects_unsupported_type(self) -> None:
        with pytest.raises(ValueError, match="unsupported jwk key type"):
            public_key_from_jwk({"kid": "k1", "x": "abc", "kty": "RSA"})

    def test_public_key_from_jwk_rejects_unsupported_curve(self) -> None:
        with pytest.raises(ValueError, match="unsupported jwk curve"):
            public_key_from_jwk({"kid": "k1", "x": "abc", "crv": "P-256"})

    def test_public_key_from_jwk_rejects_invalid_public_key(self) -> None:
        with pytest.raises(ValueError, match="invalid jwk public key"):
            public_key_from_jwk({"kid": "k1", "x": "abc"})


class TestParseActorErrors:
    def test_verify_without_keys_raises(self) -> None:
        token, _ = _build_signed_token({"sub": "u"})
        with pytest.raises(UnknownActorKeyError, match="unknown token kid"):
            parse_actor(token, verify=True, public_keys=None)

    def test_verify_with_empty_keys_raises(self) -> None:
        token, _ = _build_signed_token({"sub": "u"})
        with pytest.raises(UnknownActorKeyError, match="unknown token kid"):
            parse_actor(token, verify=True, public_keys={})

    def test_unverify_malformed_token_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid token"):
            parse_actor("not.a.jwt", verify=False)

    def test_verify_malformed_token_raises(self) -> None:
        priv = Ed25519PrivateKey.generate()
        pub = Ed25519PublicKey.from_public_bytes(priv.public_key().public_bytes_raw())
        with pytest.raises(ValueError, match="invalid token"):
            parse_actor("garbage.token.here", verify=True, public_keys={"k": pub})


class TestActorDataclass:
    def test_actor_dataclass_construction(self) -> None:
        a = Actor(sub="u", iss="i", claims={"sub": "u", "iss": "i"})
        assert a.sub == "u"
        assert a.iss == "i"
