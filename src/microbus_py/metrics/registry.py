"""Per-connector metrics registry bridging to ``prometheus_client``.

Mirrors fabric@v1.27.1/connector/metrics.go: counters, gauges and
histograms are described once with a name and (for histograms) explicit
bucket bounds, then incremented or recorded with optional label values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

__all__ = ["MetricEntry", "MetricRegistry"]


_KIND_COUNTER = "counter"
_KIND_GAUGE = "gauge"
_KIND_HISTOGRAM = "histogram"


@dataclass(slots=True)
class MetricEntry:
    name: str
    kind: str
    description: str
    labels: tuple[str, ...]
    buckets: tuple[float, ...] | None
    instrument: Any


class MetricRegistry:
    """Registry of named metrics with prometheus_client backing."""

    def __init__(self, *, service: str) -> None:
        self._service = service
        self._registry = CollectorRegistry()
        self._entries: dict[str, MetricEntry] = {}

    @property
    def service(self) -> str:
        return self._service

    def kind(self, name: str) -> str:
        entry = self._entries.get(name)
        if entry is None:
            raise ValueError(f"unknown metric '{name}'")
        return entry.kind

    def define_counter(
        self,
        name: str,
        *,
        description: str,
        labels: list[str] | tuple[str, ...] = (),
    ) -> None:
        self._reject_duplicate(name)
        label_names = tuple(labels)
        instrument = Counter(name, description, label_names, registry=self._registry)
        self._entries[name] = MetricEntry(
            name=name,
            kind=_KIND_COUNTER,
            description=description,
            labels=label_names,
            buckets=None,
            instrument=instrument,
        )

    def define_gauge(
        self,
        name: str,
        *,
        description: str,
        labels: list[str] | tuple[str, ...] = (),
    ) -> None:
        self._reject_duplicate(name)
        label_names = tuple(labels)
        instrument = Gauge(name, description, label_names, registry=self._registry)
        self._entries[name] = MetricEntry(
            name=name,
            kind=_KIND_GAUGE,
            description=description,
            labels=label_names,
            buckets=None,
            instrument=instrument,
        )

    def define_histogram(
        self,
        name: str,
        *,
        description: str,
        buckets: list[float] | tuple[float, ...],
        labels: list[str] | tuple[str, ...] = (),
    ) -> None:
        if not buckets:
            raise ValueError(f"histogram '{name}' requires non-empty bucket list")
        self._reject_duplicate(name)
        label_names = tuple(labels)
        instrument = Histogram(
            name,
            description,
            label_names,
            buckets=tuple(buckets),
            registry=self._registry,
        )
        self._entries[name] = MetricEntry(
            name=name,
            kind=_KIND_HISTOGRAM,
            description=description,
            labels=label_names,
            buckets=tuple(buckets),
            instrument=instrument,
        )

    def increment(self, name: str, value: float, **labels: str) -> None:
        entry = self._require(name)
        if entry.kind != _KIND_COUNTER:
            raise ValueError(f"metric '{name}' is not a counter")
        if value < 0:
            raise ValueError(f"counter '{name}' cannot be incremented by negative value")
        instrument = entry.instrument
        if entry.labels:
            instrument.labels(**labels).inc(value)
        else:
            instrument.inc(value)

    def record(self, name: str, value: float, **labels: str) -> None:
        entry = self._require(name)
        instrument = entry.instrument
        if entry.kind == _KIND_GAUGE:
            if entry.labels:
                instrument.labels(**labels).set(value)
            else:
                instrument.set(value)
            return
        if entry.kind == _KIND_HISTOGRAM:
            if entry.labels:
                instrument.labels(**labels).observe(value)
            else:
                instrument.observe(value)
            return
        raise ValueError(f"metric '{name}' is not a gauge or histogram")

    def scrape(self) -> bytes:
        return generate_latest(self._registry)

    def _reject_duplicate(self, name: str) -> None:
        if name in self._entries:
            raise ValueError(f"metric '{name}' already defined")

    def _require(self, name: str) -> MetricEntry:
        entry = self._entries.get(name)
        if entry is None:
            raise ValueError(f"unknown metric '{name}'")
        return entry
