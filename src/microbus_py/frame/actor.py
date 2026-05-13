"""Actor — JWT claims attached to a request via the Microbus-Actor header.

The Microbus access-token service mints Ed25519-signed JWTs whose payload
identifies the caller. Production deployments verify signatures; the
TESTING deployment accepts unsigned tokens (algorithm ``none``).
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import jwt as pyjwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ["Actor", "UnknownActorKeyError", "parse_actor", "public_key_from_jwk", "token_issuer"]


class UnknownActorKeyError(ValueError):
    """Actor token names a valid key ID that is not in the local cache."""

    def __init__(self, kid: str) -> None:
        super().__init__(f"unknown token kid: {kid}")
        self.kid = kid


def _is_microbus_actor(claims: Mapping[str, Any]) -> bool:
    issuer = str(claims.get("iss", ""))
    return isinstance(claims.get("microbus"), str) or issuer.startswith("microbus://")


def _unverified_token_parts(token: str) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        header = pyjwt.get_unverified_header(token)
        claims = pyjwt.decode(
            token,
            options={"verify_signature": False, "verify_exp": False},
        )
    except pyjwt.InvalidTokenError as exc:
        raise ValueError(f"invalid token: {exc}") from exc
    return dict(header), dict(claims)


@dataclass(frozen=True, slots=True)
class Actor:
    """Authenticated caller derived from a Microbus-Actor JWT."""

    sub: str = ""
    iss: str = ""
    claims: Mapping[str, Any] = field(default_factory=dict)


def parse_actor(
    token: str,
    *,
    verify: bool,
    public_keys: Mapping[str, Ed25519PublicKey] | None = None,
) -> Actor:
    """Parse a JWT token into an Actor.

    *verify*: when ``True``, the signature is verified against one of the
    keys in ``public_keys``. ``False`` is for the TESTING deployment only.

    Validation errors (bad signature, expired token, malformed payload)
    raise ``ValueError``.
    """
    header, claims = _unverified_token_parts(token)
    if not verify:
        pass
    else:
        if not _is_microbus_actor(claims):
            raise ValueError("signed actor token must carry a microbus issuer")
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            raise ValueError("token kid required")
        key = public_keys.get(kid) if public_keys else None
        if key is None:
            raise UnknownActorKeyError(kid)
        try:
            decoded: dict[str, Any] = pyjwt.decode(token, key=key, algorithms=["EdDSA"])
        except pyjwt.ExpiredSignatureError as exc:
            raise ValueError("token expired") from exc
        except pyjwt.InvalidSignatureError as exc:
            raise ValueError("signature verification failed") from exc
        except pyjwt.InvalidTokenError as exc:
            raise ValueError(f"invalid token: {exc}") from exc
        claims = decoded
    return Actor(
        sub=str(claims.get("sub", "")),
        iss=str(claims.get("iss", "")),
        claims=dict(claims),
    )


def token_issuer(token: str, *, require_microbus: bool = False) -> str:
    _, claims = _unverified_token_parts(token)
    if require_microbus and not _is_microbus_actor(claims):
        raise ValueError("signed actor token must carry a microbus issuer")
    return str(claims.get("iss", ""))


def public_key_from_jwk(jwk: Mapping[str, Any]) -> tuple[str, Ed25519PublicKey]:
    if jwk.get("kty") not in ("", "OKP") and jwk.get("kty") is not None:
        raise ValueError("unsupported jwk key type")
    if jwk.get("crv") not in ("", "Ed25519") and jwk.get("crv") is not None:
        raise ValueError("unsupported jwk curve")
    kid = str(jwk.get("kid", ""))
    x = jwk.get("x")
    if not kid or not isinstance(x, str):
        raise ValueError("jwk kid and x are required")
    try:
        key_bytes = base64.urlsafe_b64decode(x + "=" * (-len(x) % 4))
        key = Ed25519PublicKey.from_public_bytes(key_bytes)
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid jwk public key") from exc
    return kid, key
