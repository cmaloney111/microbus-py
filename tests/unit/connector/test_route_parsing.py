"""Tests for `_parse_route` — route forms accepted by Connector.subscribe."""

from __future__ import annotations

from microbus_py.connector.connector import _parse_route


class TestRelativeRoutes:
    def test_leading_slash_path(self) -> None:
        port, path, host_override = _parse_route("/echo")
        assert (port, path, host_override) == (443, "/echo", None)

    def test_no_leading_slash_path(self) -> None:
        port, path, host_override = _parse_route("echo")
        assert (port, path, host_override) == (443, "/echo", None)

    def test_empty_route_is_root(self) -> None:
        port, path, host_override = _parse_route("")
        assert (port, path, host_override) == (443, "/", None)

    def test_root_route(self) -> None:
        port, path, host_override = _parse_route("/")
        assert (port, path, host_override) == (443, "/", None)


class TestPortPrefixedRoutes:
    def test_port_with_path(self) -> None:
        port, path, host_override = _parse_route(":444/admin")
        assert (port, path, host_override) == (444, "/admin", None)

    def test_port_with_no_path(self) -> None:
        port, path, host_override = _parse_route(":888")
        assert (port, path, host_override) == (888, "/", None)

    def test_special_internal_port_417(self) -> None:
        port, _, _ = _parse_route(":417/on-event")
        assert port == 417

    def test_special_task_port_428(self) -> None:
        port, _, _ = _parse_route(":428/my-task")
        assert port == 428


class TestAbsoluteRoutes:
    def test_double_slash_overrides_host(self) -> None:
        port, path, host_override = _parse_route("//other.example/path")
        assert (port, path, host_override) == (443, "/path", "other.example")

    def test_double_slash_with_port(self) -> None:
        port, path, host_override = _parse_route("//other.example:8080/path")
        assert (port, path, host_override) == (8080, "/path", "other.example")

    def test_https_full_url(self) -> None:
        port, path, host_override = _parse_route("https://api.example/v1/items")
        assert (port, path, host_override) == (443, "/v1/items", "api.example")

    def test_http_full_url(self) -> None:
        port, path, host_override = _parse_route("http://api.example/v1/items")
        assert (port, path, host_override) == (80, "/v1/items", "api.example")

    def test_full_url_with_explicit_port(self) -> None:
        port, path, host_override = _parse_route("https://api.example:8443/x")
        assert (port, path, host_override) == (8443, "/x", "api.example")

    def test_double_slash_root(self) -> None:
        port, path, host_override = _parse_route("//root")
        assert (port, path, host_override) == (443, "/", "root")
