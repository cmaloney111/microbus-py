"""Tests for the ``@svc.workflow`` decorator (graph definition endpoints)."""

from __future__ import annotations

import json

import jwt as pyjwt
import pytest

from microbus_py import Connector, InMemoryBroker
from microbus_py.decorators.workflow import workflow_decorator
from microbus_py.wire.codec import decode_response
from microbus_py.workflow import Graph, Reducer
from microbus_py.workflow.registry import list_workflow_features


def _build_graph() -> Graph:
    g = Graph("demo")
    g.task("a")
    g.task("b")
    g.transition("a", "b")
    g.transition("b", "END")
    g.set_reducer("results", Reducer.Append)
    g.declare_inputs("seed")
    g.declare_outputs("result")
    return g


@pytest.mark.asyncio
async def test_workflow_decorator_returns_foreman_graph_wrapper_json() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="wf.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)
    workflow = workflow_decorator(server)

    @workflow(route=":428/demo")
    def demo() -> Graph:
        return _build_graph()

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://wf.example:428/demo",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"graph": _build_graph().to_json()}
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_workflow_required_claims_returns_401_without_actor() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="wf2.example", transport=transport)
    client = Connector(hostname="cli2.example", transport=transport)
    workflow = workflow_decorator(server)

    @workflow(route=":428/secret", required_claims="roles.admin")
    def secret() -> Graph:
        return _build_graph()

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="GET",
            url="https://wf2.example:428/secret",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 401
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_workflow_required_claims_returns_403_for_insufficient_roles() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="wf3.example", transport=transport)
    client = Connector(hostname="cli3.example", transport=transport)
    workflow = workflow_decorator(server)

    @workflow(route=":428/secret", required_claims="roles.admin")
    def secret() -> Graph:
        return _build_graph()

    await server.startup()
    await client.startup()
    try:
        token = pyjwt.encode({"sub": "u", "roles": ["user"]}, key="", algorithm="none")
        resp_bytes = await client.request(
            method="GET",
            url="https://wf3.example:428/secret",
            actor_token=token,
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 403
    finally:
        await client.shutdown()
        await server.shutdown()


def test_workflow_feature_registered() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="reg.example", transport=transport)
    workflow = workflow_decorator(svc)

    @workflow(route=":428/my-wf", description="x")
    def my_wf() -> Graph:
        return _build_graph()

    features = list_workflow_features(svc)
    assert any(f.name == "my_wf" and f.route == "/my-wf" and f.port == 428 for f in features)


def test_connector_workflow_method_registers_feature() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="native.example", transport=transport)

    @svc.workflow(route="/native-wf")
    def native_wf() -> Graph:
        return _build_graph()

    features = list_workflow_features(svc)
    assert features[-1].name == "native_wf"
    assert features[-1].route == "/native-wf"
    assert features[-1].port == 428


def test_workflow_default_port_when_route_lacks_one() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="def.example", transport=transport)
    workflow = workflow_decorator(svc)

    @workflow(route="/no-port")
    def w() -> Graph:
        return _build_graph()

    features = list_workflow_features(svc)
    assert features[-1].port == 428
