"""Collect a manifest dict from a Connector.

Mirrors the Go-side ``manifest.yaml`` schema produced by the Microbus
fabric add-feature skills. Walks the connector's typed feature lists for
``functions`` and ``webs``, plus any optional per-instance attribute
lists set by other modules (tasks, workflows, events, tickers, configs,
metrics) — read defensively so this module does not depend on milestones
that are still in flight.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from microbus_py._version import __version__

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector
    from microbus_py.decorators.function import FunctionFeature
    from microbus_py.decorators.web import WebFeature

__all__ = ["collect_manifest"]


def collect_manifest(svc: Connector) -> dict[str, Any]:
    doc: dict[str, Any] = {"general": _general(svc)}

    downstream = _downstream(svc)
    if downstream:
        doc["downstream"] = downstream

    functions = _functions(svc.list_function_features())
    if functions:
        doc["functions"] = functions

    webs = _webs(svc.list_web_features())
    if webs:
        doc["webs"] = webs

    outbound = _from_dynamic(svc, "_event_features", _event_entry)
    if outbound:
        doc["outboundEvents"] = outbound

    inbound = _from_dynamic(svc, "_event_sink_features", _event_sink_entry)
    if inbound:
        doc["inboundEvents"] = inbound

    configs = _from_dynamic(svc, "_config_entries", _config_entry)
    if configs:
        doc["configs"] = configs

    tickers = _from_dynamic(svc, "_ticker_features", _ticker_entry)
    if tickers:
        doc["tickers"] = tickers

    metrics = _from_dynamic(svc, "_metric_specs", _metric_entry)
    if metrics:
        doc["metrics"] = metrics

    tasks = _from_dynamic(svc, "_task_features", _task_entry)
    if tasks:
        doc["tasks"] = tasks

    workflows = _from_dynamic(svc, "_workflow_features", _workflow_entry)
    if workflows:
        doc["workflows"] = workflows

    return doc


def _general(svc: Connector) -> dict[str, Any]:
    out: dict[str, Any] = {"hostname": svc.hostname}
    name = getattr(svc, "_name", "") or ""
    if name:
        out["name"] = name
    out["description"] = getattr(svc, "_description", "") or ""
    pkg = getattr(svc, "_package", "") or ""
    if pkg:
        out["package"] = pkg
    out["frameworkVersion"] = __version__
    db = getattr(svc, "_db", "") or ""
    if db:
        out["db"] = db
    cloud = getattr(svc, "_cloud", "") or ""
    if cloud:
        out["cloud"] = cloud
    out["modifiedAt"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return out


def _downstream(svc: Connector) -> list[dict[str, str]]:
    lister = getattr(svc, "list_downstream", None)
    if lister is None:
        return []
    return list(lister())


def _functions(features: Iterable[FunctionFeature]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for feat in features:
        entry: dict[str, Any] = {
            "signature": _function_signature(feat),
            "description": feat.description,
            "method": feat.method,
            "route": _port_prefixed(feat.port, feat.route),
            "loadBalancing": getattr(feat, "load_balancing", "default"),
        }
        if feat.required_claims:
            entry["requiredClaims"] = feat.required_claims
        out[feat.name] = entry
    return out


def _webs(features: Iterable[WebFeature]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for feat in features:
        entry: dict[str, Any] = {
            "description": feat.description,
            "method": feat.method,
            "route": _port_prefixed(feat.port, feat.route),
            "loadBalancing": getattr(feat, "load_balancing", "default"),
        }
        if feat.required_claims:
            entry["requiredClaims"] = feat.required_claims
        out[feat.name] = entry
    return out


def _from_dynamic(
    svc: Connector,
    attr: str,
    builder: Any,
) -> dict[str, dict[str, Any]]:
    raw = getattr(svc, attr, None)
    if not raw:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for item in raw:
        name, entry = builder(item)
        if name is None:
            continue
        out[name] = entry
    return out


def _function_signature(feat: FunctionFeature) -> str:
    in_fields = ", ".join(_pydantic_field_sig(feat.input_model))
    out_fields = ", ".join(_pydantic_field_sig(feat.output_model))
    return f"{feat.name}({in_fields}) ({out_fields})"


def _pydantic_field_sig(model: Any) -> list[str]:
    if not inspect.isclass(model) or not hasattr(model, "model_fields"):
        return []
    return [f"{name} {_py_type(field.annotation)}" for name, field in model.model_fields.items()]


def _py_type(annotation: Any) -> str:
    if annotation is None:
        return "any"
    if hasattr(annotation, "__name__"):
        return str(annotation.__name__)
    return str(annotation)


def _port_prefixed(port: int, path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f":{port}{path}"


def _entry_get(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


def _event_entry(item: Any) -> tuple[str | None, dict[str, Any]]:
    name = _entry_get(item, "name")
    if not name:
        return None, {}
    entry: dict[str, Any] = {}
    for k in ("signature", "description", "method", "route"):
        v = _entry_get(item, k)
        if v is not None:
            entry[k] = v
    return name, entry


def _event_sink_entry(item: Any) -> tuple[str | None, dict[str, Any]]:
    name = _entry_get(item, "name")
    if not name:
        return None, {}
    entry: dict[str, Any] = {}
    for k in ("signature", "description", "loadBalancing", "requiredClaims", "source"):
        v = _entry_get(item, k)
        if v is not None:
            entry[k] = v
    return name, entry


def _config_entry(item: Any) -> tuple[str | None, dict[str, Any]]:
    name = _entry_get(item, "name")
    if not name:
        return None, {}
    entry: dict[str, Any] = {}
    for k in ("signature", "description", "validation", "default", "secret", "callback"):
        v = _entry_get(item, k)
        if v is not None:
            entry[k] = v
    return name, entry


def _ticker_entry(item: Any) -> tuple[str | None, dict[str, Any]]:
    name = _entry_get(item, "name")
    if not name:
        return None, {}
    entry: dict[str, Any] = {}
    for k in ("signature", "description", "interval"):
        v = _entry_get(item, k)
        if v is not None:
            entry[k] = v
    return name, entry


def _metric_entry(item: Any) -> tuple[str | None, dict[str, Any]]:
    name = _entry_get(item, "name")
    if not name:
        return None, {}
    entry: dict[str, Any] = {}
    for k in ("signature", "description", "kind", "buckets", "otelName", "observable"):
        v = _entry_get(item, k)
        if v is not None:
            entry[k] = v
    return name, entry


def _task_entry(item: Any) -> tuple[str | None, dict[str, Any]]:
    name = _entry_get(item, "name")
    if not name:
        return None, {}
    entry: dict[str, Any] = {}
    for k in ("signature", "description", "route", "requiredClaims"):
        v = _entry_get(item, k)
        if v is not None:
            entry[k] = v
    return name, entry


def _workflow_entry(item: Any) -> tuple[str | None, dict[str, Any]]:
    name = _entry_get(item, "name")
    if not name:
        return None, {}
    entry: dict[str, Any] = {}
    for k in ("signature", "description", "route"):
        v = _entry_get(item, k)
        if v is not None:
            entry[k] = v
    return name, entry
