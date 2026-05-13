"""Tests for ``Connector.log_*`` — structured logging in Go-slog name=value form."""

from __future__ import annotations

import json

import pytest

from microbus_py import Connector, InMemoryBroker


@pytest.fixture
def svc() -> Connector:
    transport = InMemoryBroker()
    return Connector(hostname="my.service", transport=transport)


def _read_lines(capsys: pytest.CaptureFixture[str]) -> list[dict[str, object]]:
    captured = capsys.readouterr()
    out = []
    for raw_line in (captured.err or captured.out).splitlines():
        line = raw_line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def test_log_info_emits_json_with_message_and_pairs(
    svc: Connector, capsys: pytest.CaptureFixture[str]
) -> None:
    svc.log_info("File uploaded", "size_gb", 4.2, "user", "abc")
    [record] = _read_lines(capsys)
    assert record["msg"] == "File uploaded"
    assert record["level"] == "info"
    assert record["service"] == "my.service"
    assert record["size_gb"] == 4.2
    assert record["user"] == "abc"


def test_log_warn_emits_warn_level(svc: Connector, capsys: pytest.CaptureFixture[str]) -> None:
    svc.log_warn("queue saturated", "queue_size", 1000)
    [record] = _read_lines(capsys)
    assert record["level"] == "warning"
    assert record["queue_size"] == 1000


def test_log_error_records_error_label(svc: Connector, capsys: pytest.CaptureFixture[str]) -> None:
    err = ValueError("bad input")
    svc.log_error("handler failed", "error", err, "user", "abc")
    [record] = _read_lines(capsys)
    assert record["level"] == "error"
    rendered_error = record["error"]
    assert isinstance(rendered_error, str)
    assert "bad input" in rendered_error
    assert record["user"] == "abc"


def test_log_debug_suppressed_by_default(
    svc: Connector, capsys: pytest.CaptureFixture[str]
) -> None:
    svc.log_debug("trace step", "i", 7)
    assert _read_lines(capsys) == []


def test_log_debug_enabled_via_env(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("MICROBUS_LOG_DEBUG", "1")
    transport = InMemoryBroker()
    svc = Connector(hostname="dbg.service", transport=transport)
    svc.log_debug("trace step", "i", 7)
    [record] = _read_lines(capsys)
    assert record["level"] == "debug"
    assert record["i"] == 7


def test_log_unpaired_argument_is_ignored_safely(
    svc: Connector, capsys: pytest.CaptureFixture[str]
) -> None:
    svc.log_info("orphan", "lonely")
    [record] = _read_lines(capsys)
    assert record["msg"] == "orphan"
    assert "lonely" not in record


def test_log_includes_hostname_and_instance_id(
    svc: Connector, capsys: pytest.CaptureFixture[str]
) -> None:
    svc.log_info("hi")
    [record] = _read_lines(capsys)
    assert record["service"] == "my.service"
    assert record["instance_id"] == svc.instance_id
