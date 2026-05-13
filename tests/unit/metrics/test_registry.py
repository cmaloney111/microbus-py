"""Tests for ``microbus_py.metrics.registry``."""

from __future__ import annotations

import pytest

from microbus_py.metrics.registry import MetricRegistry


def test_define_counter_and_increment() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_counter("requests_total", description="reqs")
    reg.increment("requests_total", 1.0)
    reg.increment("requests_total", 2.5)
    out = reg.scrape()
    assert b"requests_total" in out


def test_counter_rejects_negative_increment() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_counter("x", description="x")
    with pytest.raises(ValueError, match="negative"):
        reg.increment("x", -1.0)


def test_define_gauge_and_record() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_gauge("queue_depth", description="depth")
    reg.record("queue_depth", 7.0)
    reg.record("queue_depth", 3.0)
    out = reg.scrape()
    assert b"queue_depth" in out
    assert b"3" in out


def test_define_histogram_and_observe_buckets() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_histogram("latency_seconds", description="lat", buckets=[0.1, 0.5, 1.0, 5.0])
    reg.record("latency_seconds", 0.05)
    reg.record("latency_seconds", 0.3)
    reg.record("latency_seconds", 2.0)
    out = reg.scrape().decode("utf-8")
    assert "latency_seconds_bucket" in out
    assert "latency_seconds_sum" in out
    assert "latency_seconds_count" in out


def test_redefine_metric_raises() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_counter("x", description="x")
    with pytest.raises(ValueError, match="already defined"):
        reg.define_counter("x", description="x")


def test_increment_unknown_raises() -> None:
    reg = MetricRegistry(service="svc.example")
    with pytest.raises(ValueError, match="unknown"):
        reg.increment("missing", 1.0)


def test_record_unknown_raises() -> None:
    reg = MetricRegistry(service="svc.example")
    with pytest.raises(ValueError, match="unknown"):
        reg.record("missing", 1.0)


def test_increment_on_gauge_kind_mismatch() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_gauge("g", description="g")
    with pytest.raises(ValueError, match="not a counter"):
        reg.increment("g", 1.0)


def test_record_on_counter_kind_mismatch() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_counter("c", description="c")
    with pytest.raises(ValueError, match="not a gauge"):
        reg.record("c", 1.0)


def test_labels_filter_distinct_series() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_counter("hits", description="hits", labels=["route"])
    reg.increment("hits", 1.0, route="/a")
    reg.increment("hits", 2.0, route="/b")
    reg.increment("hits", 5.0, route="/a")
    out = reg.scrape().decode("utf-8")
    assert 'route="/a"' in out
    assert 'route="/b"' in out


def test_scrape_emits_prometheus_text_format() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_counter("hello_total", description="hello counter")
    reg.increment("hello_total", 3.0)
    out = reg.scrape().decode("utf-8")
    assert out.startswith("# HELP") or "# HELP" in out
    assert "# TYPE hello_total" in out


def test_histogram_requires_buckets() -> None:
    reg = MetricRegistry(service="svc.example")
    with pytest.raises(ValueError, match="bucket"):
        reg.define_histogram("h", description="h", buckets=[])


def test_kind_returns_kind_for_metric() -> None:
    reg = MetricRegistry(service="svc.example")
    reg.define_counter("c", description="c")
    reg.define_gauge("g", description="g")
    reg.define_histogram("h", description="h", buckets=[1.0])
    assert reg.kind("c") == "counter"
    assert reg.kind("g") == "gauge"
    assert reg.kind("h") == "histogram"
