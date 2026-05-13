"""``@metric`` decorator — counter / gauge / histogram instruments.

Each decorated function declares a metric ``name``, ``kind`` and
description; the decorator returns a callable that records values into
the connector's :class:`microbus_py.metrics.registry.MetricRegistry`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from microbus_py.metrics.registry import MetricRegistry

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector

__all__ = ["MetricFeature", "metric_decorator"]


_VALID_KINDS = ("counter", "gauge", "histogram")


@dataclass(slots=True, frozen=True)
class MetricFeature:
    name: str
    kind: str
    description: str
    labels: tuple[str, ...]
    buckets: tuple[float, ...] | None


MetricRecorder = Callable[..., None]


def _registry(svc: Connector) -> MetricRegistry:
    reg = svc.__dict__.get("_metric_registry")
    if reg is None:
        reg = MetricRegistry(service=svc.hostname)
        svc.__dict__["_metric_registry"] = reg
    return reg


def metric_decorator(svc: Connector) -> Callable[..., Callable[[MetricRecorder], MetricRecorder]]:
    def deco(
        *,
        name: str,
        kind: str,
        description: str = "",
        labels: list[str] | tuple[str, ...] = (),
        buckets: list[float] | tuple[float, ...] | None = None,
    ) -> Callable[[MetricRecorder], MetricRecorder]:
        if kind not in _VALID_KINDS:
            raise ValueError(f"unknown metric kind '{kind}', expected one of {_VALID_KINDS}")
        registry = _registry(svc)
        label_names = tuple(labels)
        bucket_tuple = tuple(buckets) if buckets is not None else None
        if kind == "counter":
            registry.define_counter(name, description=description, labels=label_names)
        elif kind == "gauge":
            registry.define_gauge(name, description=description, labels=label_names)
        else:
            if bucket_tuple is None:
                raise ValueError(f"histogram '{name}' requires buckets")
            registry.define_histogram(
                name,
                description=description,
                buckets=bucket_tuple,
                labels=label_names,
            )

        feature = MetricFeature(
            name=name,
            kind=kind,
            description=description,
            labels=label_names,
            buckets=bucket_tuple,
        )
        features = svc.__dict__.setdefault("_metric_features", [])
        features.append(feature)

        def wrap(handler: MetricRecorder) -> MetricRecorder:
            def recorder(value: float, **lbls: str) -> None:
                if kind == "counter":
                    registry.increment(name, value, **lbls)
                else:
                    registry.record(name, value, **lbls)

            recorder.__name__ = handler.__name__
            recorder.__doc__ = handler.__doc__
            return recorder

        return wrap

    return deco
