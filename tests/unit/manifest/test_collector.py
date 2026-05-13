"""Tests for ``manifest.collector.collect_manifest``."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

from microbus_py import Connector, InMemoryBroker
from microbus_py.manifest.collector import (
    _py_type,
    _pydantic_field_sig,
    collect_manifest,
)
from microbus_py.wire.codec import HTTPRequest, HTTPResponse


class _In(BaseModel):
    x: int


class _Out(BaseModel):
    y: int


def _svc() -> Connector:
    return Connector(
        hostname="my.service",
        description="MyService does X.",
        transport=InMemoryBroker(),
    )


def test_collect_manifest_general_section() -> None:
    svc = _svc()
    doc = collect_manifest(svc)
    assert doc["general"]["hostname"] == "my.service"
    assert doc["general"]["description"] == "MyService does X."
    assert "frameworkVersion" in doc["general"]
    assert "modifiedAt" in doc["general"]
    parsed = datetime.fromisoformat(doc["general"]["modifiedAt"])
    assert parsed.tzinfo is not None
    delta = abs((datetime.now(UTC) - parsed).total_seconds())
    assert delta < 5


def test_collect_manifest_two_functions() -> None:
    svc = _svc()

    @svc.function(route="/alpha", method="POST", description="Alpha desc")
    async def alpha(inp: _In) -> _Out:
        return _Out(y=inp.x)

    @svc.function(
        route="/beta",
        method="GET",
        description="Beta desc",
        required_claims="roles.admin",
    )
    async def beta(inp: _In) -> _Out:
        return _Out(y=inp.x + 1)

    doc = collect_manifest(svc)
    fns = doc["functions"]
    assert "alpha" in fns
    assert "beta" in fns
    assert fns["alpha"]["route"] == ":443/alpha"
    assert fns["alpha"]["method"] == "POST"
    assert fns["alpha"]["description"] == "Alpha desc"
    assert fns["beta"]["requiredClaims"] == "roles.admin"
    assert "signature" in fns["alpha"]
    assert "alpha(" in fns["alpha"]["signature"]


def test_collect_manifest_web_entry() -> None:
    svc = _svc()

    @svc.web(
        route=":444/raw",
        method="GET",
        description="Raw handler",
        required_claims="roles.manager",
    )
    async def raw(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    doc = collect_manifest(svc)
    webs = doc["webs"]
    assert "raw" in webs
    assert webs["raw"]["route"] == ":444/raw"
    assert webs["raw"]["method"] == "GET"
    assert webs["raw"]["description"] == "Raw handler"
    assert webs["raw"]["requiredClaims"] == "roles.manager"


def test_collect_manifest_defensive_when_extra_features_absent() -> None:
    svc = _svc()
    doc = collect_manifest(svc)
    assert "tasks" not in doc or doc["tasks"] == {}
    assert "workflows" not in doc or doc["workflows"] == {}
    assert "outboundEvents" not in doc or doc["outboundEvents"] == {}
    assert "inboundEvents" not in doc or doc["inboundEvents"] == {}
    assert "tickers" not in doc or doc["tickers"] == {}
    assert "configs" not in doc or doc["configs"] == {}
    assert "metrics" not in doc or doc["metrics"] == {}


def test_collect_manifest_picks_up_dynamic_attrs() -> None:
    svc = _svc()
    svc.__dict__.setdefault("_ticker_features", []).append(
        {"name": "MyTicker", "interval": "5m", "description": "tickit"}
    )
    svc.__dict__.setdefault("_config_entries", []).append(
        {
            "name": "MyConfig",
            "signature": "MyConfig() (myConfig int)",
            "description": "Cfg",
            "validation": "int [1,100]",
            "default": "10",
        }
    )
    svc.__dict__.setdefault("_metric_specs", []).append(
        {
            "name": "MyMetric",
            "signature": "MyMetric(value int)",
            "description": "metric",
            "kind": "counter",
        }
    )
    svc.__dict__.setdefault("_event_features", []).append(
        {
            "name": "OnFoo",
            "signature": "OnFoo()",
            "description": "evt",
            "method": "POST",
            "route": ":417/on-foo",
        }
    )
    svc.__dict__.setdefault("_event_sink_features", []).append(
        {
            "name": "OnBar",
            "signature": "OnBar()",
            "description": "sink",
            "source": "pkg/path",
        }
    )
    svc.__dict__.setdefault("_task_features", []).append(
        {
            "name": "MyTask",
            "signature": "MyTask(x string)",
            "description": "task",
            "route": ":428/my-task",
        }
    )
    svc.__dict__.setdefault("_workflow_features", []).append(
        {
            "name": "MyWf",
            "signature": "MyWf()",
            "description": "wf",
            "route": ":428/my-wf",
        }
    )
    doc = collect_manifest(svc)
    assert doc["tickers"]["MyTicker"]["interval"] == "5m"
    assert doc["configs"]["MyConfig"]["validation"] == "int [1,100]"
    assert doc["metrics"]["MyMetric"]["kind"] == "counter"
    assert doc["outboundEvents"]["OnFoo"]["route"] == ":417/on-foo"
    assert doc["inboundEvents"]["OnBar"]["source"] == "pkg/path"
    assert doc["tasks"]["MyTask"]["route"] == ":428/my-task"
    assert doc["workflows"]["MyWf"]["route"] == ":428/my-wf"


def test_collect_manifest_function_route_default_port() -> None:
    svc = _svc()

    @svc.function(route="/no-port", method="POST")
    async def no_port(inp: _In) -> _Out:
        return _Out(y=inp.x)

    doc = collect_manifest(svc)
    assert doc["functions"]["no_port"]["route"] == ":443/no-port"


def test_collect_manifest_omits_empty_required_claims() -> None:
    svc = _svc()

    @svc.function(route="/plain", method="POST")
    async def plain(inp: _In) -> _Out:
        return _Out(y=inp.x)

    doc = collect_manifest(svc)
    assert "requiredClaims" not in doc["functions"]["plain"]


def test_collect_manifest_skips_dynamic_item_without_name() -> None:
    svc = _svc()
    svc.__dict__.setdefault("_ticker_features", []).extend(
        [
            {"interval": "1m"},
            {"name": "Good", "interval": "2m"},
        ]
    )
    doc = collect_manifest(svc)
    assert list(doc["tickers"].keys()) == ["Good"]


@dataclass
class _FakeTicker:
    name: str
    interval: str
    description: str = "x"


def test_collect_manifest_handles_dataclass_like_dynamic_item() -> None:
    svc = _svc()
    svc.__dict__.setdefault("_ticker_features", []).append(_FakeTicker(name="DC", interval="3m"))
    doc = collect_manifest(svc)
    assert doc["tickers"]["DC"]["interval"] == "3m"


def test_function_signature_handles_non_class_model_and_none_annotation() -> None:
    assert _pydantic_field_sig(None) == []
    assert _py_type(None) == "any"
    assert _py_type("ManualTypeName") == "ManualTypeName"


@pytest.mark.parametrize("route,expected", [(":444/x", ":444/x"), ("/y", ":443/y")])
def test_collect_manifest_route_port_prefix(route: str, expected: str) -> None:
    svc = _svc()

    @svc.function(route=route, method="POST", name=f"f_{route}")
    async def f(inp: _In) -> _Out:
        return _Out(y=inp.x)

    doc = collect_manifest(svc)
    found = next(iter(doc["functions"].values()))
    assert found["route"] == expected


def test_collect_manifest_general_emits_optional_metadata() -> None:
    svc = Connector(
        hostname="my.service",
        description="MyService does X.",
        name="My service",
        package="github.com/me/my/myservice",
        db="SQL",
        cloud="api.example.com",
        transport=InMemoryBroker(),
    )
    doc = collect_manifest(svc)
    g = doc["general"]
    assert g["name"] == "My service"
    assert g["package"] == "github.com/me/my/myservice"
    assert g["db"] == "SQL"
    assert g["cloud"] == "api.example.com"


def test_collect_manifest_general_omits_unset_optional_fields() -> None:
    svc = _svc()
    doc = collect_manifest(svc)
    g = doc["general"]
    assert "name" not in g
    assert "package" not in g
    assert "db" not in g
    assert "cloud" not in g


def test_collect_manifest_emits_downstream_section() -> None:
    svc = _svc()
    svc.register_downstream(
        hostname="another.service",
        package="github.com/me/my/anotherservice",
    )
    svc.register_downstream(
        hostname="third.service",
        package="github.com/me/my/thirdservice",
    )
    doc = collect_manifest(svc)
    assert doc["downstream"] == [
        {"hostname": "another.service", "package": "github.com/me/my/anotherservice"},
        {"hostname": "third.service", "package": "github.com/me/my/thirdservice"},
    ]


def test_collect_manifest_no_downstream_section_when_unregistered() -> None:
    svc = _svc()
    doc = collect_manifest(svc)
    assert "downstream" not in doc


def test_collect_manifest_function_load_balancing_field() -> None:
    svc = _svc()

    @svc.function(route="/lb", method="POST", load_balancing="worker.pool")
    async def lb(inp: _In) -> _Out:
        return _Out(y=inp.x)

    doc = collect_manifest(svc)
    assert doc["functions"]["lb"]["loadBalancing"] == "worker.pool"


def test_collect_manifest_function_load_balancing_default() -> None:
    svc = _svc()

    @svc.function(route="/d", method="POST")
    async def d(inp: _In) -> _Out:
        return _Out(y=inp.x)

    doc = collect_manifest(svc)
    assert doc["functions"]["d"]["loadBalancing"] == "default"


def test_collect_manifest_web_load_balancing_none_is_multicast() -> None:
    svc = _svc()

    @svc.web(route=":444/w", method="GET", load_balancing="none")
    async def w(_req: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(status_code=200, reason="OK", headers=[], body=b"")

    doc = collect_manifest(svc)
    assert doc["webs"]["w"]["loadBalancing"] == "none"
