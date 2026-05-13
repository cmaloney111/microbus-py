"""Route string parsing for ``Connector.subscribe`` and decorators."""

from __future__ import annotations

__all__ = ["parse_route"]


def parse_route(route: str) -> tuple[int, str, str | None]:
    """Return ``(port, path, host_override)`` for a route string.

    Forms accepted (mirrors fabric ``sub.Route``):

    * ``""`` / ``"/"`` → root path on the bus hostname, port 443.
    * ``"x"`` (no slash) → ``"/x"`` on the bus hostname, port 443.
    * ``":N/x"`` / ``":N"`` → port override, bus hostname.
    * ``"//host[:N]/x"`` → absolute address; routes are bound under
      ``host`` instead of the bus hostname.
    * ``"https://host[:N]/x"`` / ``"http://host[:N]/x"`` → absolute
      address with explicit scheme; default port 443 (https) or 80 (http).
    """
    if route.startswith(("https://", "http://")):
        scheme, _, rest = route.partition("://")
        host_part, _, path = rest.partition("/")
        host, port = _split_host_port(host_part, 443 if scheme == "https" else 80)
        return port, "/" + path if path else "/", host

    if route.startswith("//"):
        rest = route[2:]
        host_part, _, path = rest.partition("/")
        host, port = _split_host_port(host_part, 443)
        return port, "/" + path if path else "/", host

    if route.startswith(":"):
        idx = route.find("/")
        if idx == -1:
            return int(route[1:]), "/", None
        return int(route[1:idx]), route[idx:], None

    if route in ("", "/"):
        return 443, "/", None
    if route.startswith("/"):
        return 443, route, None
    return 443, "/" + route, None


def _split_host_port(host_part: str, default_port: int) -> tuple[str, int]:
    if ":" in host_part:
        host, _, port_str = host_part.partition(":")
        return host, int(port_str)
    return host_part, default_port
