"""NATS subject construction.

Mirrors the algorithm in github.com/microbus-io/fabric@v1.27.1/connector/subjects.go.
Two distinct subject forms exist:

* **request subjects** — used by callers when publishing a request. Path arguments
  are escaped as literal characters; no wildcards.
* **subscription subjects** — used by services when binding a handler. Path
  arguments expressed as ``{name}`` become ``*`` (single-segment NATS wildcard)
  and trailing greedy arguments ``{name...}`` become ``>`` (multi-segment).

Subject grammar (both forms)::

    {plane}.{port}.{reversed_lowercase_hostname}.|.{METHOD}.{escaped_path}

Path encoding rules (per-segment, joined with ``.``):

* Empty segment → ``_``.
* ASCII letter, digit, or hyphen → as-is.
* ``.`` → ``_``.
* Anything else → ``%XXXX`` where ``XXXX`` is the lowercase 4-digit hex of the
  Unicode code point.

Response subjects use a different shape::

    {plane}.r.{reversed_lowercase_hostname}.{lowercase_instance_id}
"""

from __future__ import annotations

__all__ = [
    "escape_path_part",
    "request_subject",
    "response_subject",
    "reverse_hostname",
    "subscription_subject",
]


def reverse_hostname(hostname: str) -> str:
    """Reverse the dot-separated segments of a hostname.

    ``www.example.com`` becomes ``com.example.www``. Hostnames without dots
    are returned unchanged. Casing is preserved (the caller lowercases as
    needed).
    """
    if "." not in hostname:
        return hostname
    return ".".join(reversed(hostname.split(".")))


def escape_path_part(part: str) -> str:
    """Escape a single path segment for inclusion in a NATS subject.

    See module docstring for the full rule set.
    """
    out: list[str] = []
    for ch in part:
        cp = ord(ch)
        if ch == ".":
            out.append("_")
        elif (
            (0x41 <= cp <= 0x5A)  # A-Z
            or (0x61 <= cp <= 0x7A)  # a-z
            or (0x30 <= cp <= 0x39)  # 0-9
            or ch == "-"
        ):
            out.append(ch)
        else:
            out.append(f"%{cp:04x}")
    return "".join(out)


def _subject_of(
    *,
    wildcards: bool,
    plane: str,
    port: str,
    hostname: str,
    method: str,
    path: str,
) -> str:
    parts: list[str] = [plane, "."]

    if wildcards and port == "0":
        parts.append("*")
    else:
        parts.append(port)
    parts.append(".")

    parts.append(reverse_hostname(hostname).lower())
    parts.append(".|.")

    method_upper = method.upper()
    if wildcards and method_upper == "ANY":
        parts.append("*")
    else:
        parts.append(method_upper)
    parts.append(".")

    if path.startswith("/"):
        path = path[1:]
    if path == "":
        parts.append("_")
        return "".join(parts)

    segments = path.split("/")
    last = len(segments) - 1
    for i, seg in enumerate(segments):
        if i > 0:
            parts.append(".")
        if wildcards and seg.startswith("{") and seg.endswith("}"):
            if i == last and seg.endswith("...}"):
                parts.append(">")
            else:
                parts.append("*")
            continue
        if wildcards and seg == "*":
            parts.append("*")
            continue
        if seg == "":
            parts.append("_")
        else:
            parts.append(escape_path_part(seg))
    return "".join(parts)


def request_subject(
    *,
    plane: str,
    port: str,
    hostname: str,
    method: str,
    path: str,
) -> str:
    """Compose the NATS subject a caller publishes to."""
    return _subject_of(
        wildcards=False,
        plane=plane,
        port=port,
        hostname=hostname,
        method=method,
        path=path,
    )


def subscription_subject(
    *,
    plane: str,
    port: str,
    hostname: str,
    method: str,
    path: str,
) -> str:
    """Compose the NATS subject a service binds a handler to."""
    return _subject_of(
        wildcards=True,
        plane=plane,
        port=port,
        hostname=hostname,
        method=method,
        path=path,
    )


def response_subject(*, plane: str, hostname: str, instance_id: str) -> str:
    """Compose the NATS subject a service receives unicast responses on."""
    return f"{plane}.r.{reverse_hostname(hostname).lower()}.{instance_id.lower()}"
