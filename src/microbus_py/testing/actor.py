"""JWT minting helpers for TESTING contexts.

``mint_unsigned_actor`` produces ``alg=none`` JWTs that round-trip through
:func:`microbus_py.frame.actor.parse_actor` with ``verify=False``. Use it
from tests where the bus is an :class:`microbus_py.InMemoryBroker` and
signature verification is disabled.

``mint_signed_actor`` produces Ed25519-signed JWTs (algorithm ``EdDSA``)
that round-trip through ``parse_actor`` with ``verify=True``. Use it from
tests that need to exercise the signature-verification code path against
a known keypair.

Both helpers default to ``iss="auth.example"`` and ``sub="user-test"``
to match a generic JWT shape. Override any field via keyword arguments to
match your own auth service.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import jwt as pyjwt
from cryptography.hazmat.primitives import serialization

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

__all__ = ["mint_signed_actor", "mint_unsigned_actor"]


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _build_claims(
    *,
    sub: str,
    iss: str | None,
    roles: Sequence[str] | None,
    ttl: timedelta,
    now: datetime | None,
    extra: Mapping[str, object] | None,
) -> dict[str, object]:
    issued_at = now if now is not None else datetime.now(UTC)
    expires_at = issued_at + ttl
    claims: dict[str, object] = {
        "sub": sub,
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    if iss is not None:
        claims["iss"] = iss
    if roles is not None:
        claims["roles"] = list(roles)
    if extra:
        claims.update(dict(extra))
    return claims


def mint_unsigned_actor(
    *,
    sub: str = "user-test",
    iss: str | None = "auth.example",
    roles: Sequence[str] | None = None,
    ttl: timedelta = timedelta(minutes=15),
    now: datetime | None = None,
    extra: Mapping[str, object] | None = None,
) -> str:
    """Mint an unsigned JWT (``alg=none``) for TESTING contexts.

    Produces a token of the form ``<header>.<payload>.`` with an empty
    signature segment. Compatible with
    :func:`microbus_py.frame.actor.parse_actor` when ``verify=False``.

    Pass ``iss=None`` to omit the issuer claim entirely (for negative-path
    tests that verify ``required_claims`` rejection of issuer-less tokens).

    The signed-JWT path is *not* this helper's responsibility; use
    :func:`mint_signed_actor` for that.
    """
    header = {"alg": "none", "typ": "JWT"}
    claims = _build_claims(sub=sub, iss=iss, roles=roles, ttl=ttl, now=now, extra=extra)
    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(claims, separators=(",", ":")).encode("utf-8"))
    return f"{header_b64}.{payload_b64}."


def mint_signed_actor(
    *,
    private_key: Ed25519PrivateKey,
    kid: str = "v1",
    sub: str = "user-test",
    iss: str | None = "auth.example",
    roles: Sequence[str] | None = None,
    ttl: timedelta = timedelta(minutes=15),
    now: datetime | None = None,
    extra: Mapping[str, object] | None = None,
) -> str:
    """Mint an Ed25519-signed JWT with ``kid`` in the JOSE header.

    Compatible with :func:`microbus_py.frame.actor.parse_actor` when
    ``verify=True`` and ``public_keys={kid: private_key.public_key()}``.

    Note: ``parse_actor`` enforces a Microbus-style issuer for signed
    tokens — either ``iss`` starts with ``microbus://`` or the payload
    carries a ``microbus`` claim. Pass ``iss="microbus://..."`` or set
    ``extra={"microbus": "1"}`` to satisfy that check.
    """
    claims = _build_claims(sub=sub, iss=iss, roles=roles, ttl=ttl, now=now, extra=extra)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pyjwt.encode(claims, key=private_pem, algorithm="EdDSA", headers={"kid": kid})
