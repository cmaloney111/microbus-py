"""Tests for the ``@svc.task`` decorator (workflow task endpoints)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import jwt as pyjwt
import pytest

from microbus_py import Connector, InMemoryBroker
from microbus_py.decorators.task import task_decorator
from microbus_py.wire.codec import decode_response
from microbus_py.workflow.registry import list_task_features

if TYPE_CHECKING:
    from microbus_py.workflow import Flow


@pytest.mark.asyncio
async def test_task_decorator_round_trips_flow_state() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="t.example", transport=transport)
    client = Connector(hostname="cli.example", transport=transport)
    task = task_decorator(server)

    @task(route=":428/echo")
    async def echo(flow: Flow) -> Flow:
        flow.changes["seen"] = flow.state.get("name", "")
        flow.state["seen"] = flow.state.get("name", "")
        return flow

    await server.startup()
    await client.startup()
    try:
        body = json.dumps({"state": {"name": "World"}}).encode("utf-8")
        resp_bytes = await client.request(
            method="POST",
            url="https://t.example:428/echo",
            body=body,
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        doc = json.loads(resp.body)
        assert doc["state"]["seen"] == "World"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_task_handler_changes_propagate_to_response() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="t2.example", transport=transport)
    client = Connector(hostname="cli2.example", transport=transport)
    task = task_decorator(server)

    @task(route=":428/mark")
    async def mark(flow: Flow) -> Flow:
        flow.changes["marked"] = True
        flow.state["marked"] = True
        return flow

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://t2.example:428/mark",
            body=b"{}",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        doc = json.loads(resp.body)
        assert doc["changes"] == {"marked": True}
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_task_goto_propagates_in_response() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="t3.example", transport=transport)
    client = Connector(hostname="cli3.example", transport=transport)
    task = task_decorator(server)

    @task(route=":428/branch")
    async def branch(flow: Flow) -> Flow:
        flow.goto = "https://t3.example:428/next"
        return flow

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://t3.example:428/branch",
            body=b"{}",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        doc = json.loads(resp.body)
        assert doc["goto"] == "https://t3.example:428/next"
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_task_required_claims_denies_without_actor() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="t4.example", transport=transport)
    client = Connector(hostname="cli4.example", transport=transport)
    task = task_decorator(server)

    @task(route=":428/secret", required_claims="roles.admin")
    async def secret(flow: Flow) -> Flow:
        return flow

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://t4.example:428/secret",
            body=b"{}",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 401
    finally:
        await client.shutdown()
        await server.shutdown()


@pytest.mark.asyncio
async def test_task_required_claims_admits_with_proper_actor() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="t5.example", transport=transport)
    client = Connector(hostname="cli5.example", transport=transport)
    task = task_decorator(server)

    @task(route=":428/secret", required_claims="roles.admin")
    async def secret(flow: Flow) -> Flow:
        return flow

    await server.startup()
    await client.startup()
    try:
        token = pyjwt.encode({"sub": "u", "roles": ["admin"]}, key="", algorithm="none")
        resp_bytes = await client.request(
            method="POST",
            url="https://t5.example:428/secret",
            body=b"{}",
            actor_token=token,
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
    finally:
        await client.shutdown()
        await server.shutdown()


def test_task_feature_registered_via_registry() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="reg.example", transport=transport)
    task = task_decorator(svc)

    @task(route=":428/my-task", description="hello")
    async def my_task(flow: Flow) -> Flow:
        return flow

    features = list_task_features(svc)
    assert any(f.name == "my_task" and f.route == "/my-task" and f.port == 428 for f in features)


def test_connector_task_method_registers_feature() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="native.example", transport=transport)

    @svc.task(route="/native-task")
    async def native_task(flow: Flow) -> Flow:
        return flow

    features = list_task_features(svc)
    assert features[-1].name == "native_task"
    assert features[-1].route == "/native-task"
    assert features[-1].port == 428


@pytest.mark.asyncio
async def test_task_malformed_json_returns_400() -> None:
    transport = InMemoryBroker()
    server = Connector(hostname="t6.example", transport=transport)
    client = Connector(hostname="cli6.example", transport=transport)
    task = task_decorator(server)

    @task(route=":428/parse")
    async def parse(flow: Flow) -> Flow:
        return flow

    await server.startup()
    await client.startup()
    try:
        resp_bytes = await client.request(
            method="POST",
            url="https://t6.example:428/parse",
            body=b"{not json",
            timeout=2.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 400
    finally:
        await client.shutdown()
        await server.shutdown()


def test_task_default_port_when_route_lacks_one() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="def.example", transport=transport)
    task = task_decorator(svc)

    @task(route="/no-port")
    async def t(flow: Flow) -> Flow:
        return flow

    features = list_task_features(svc)
    assert features[-1].port == 428


def test_task_absolute_url_with_explicit_port_is_respected() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="def.example", transport=transport)
    task = task_decorator(svc)

    @task(route="https://other.example:9000/x")
    async def t(flow: Flow) -> Flow:
        return flow

    features = list_task_features(svc)
    assert features[-1].port == 9000


def test_task_double_slash_route_with_port() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="def.example", transport=transport)
    task = task_decorator(svc)

    @task(route="//host.example:7777/y")
    async def t(flow: Flow) -> Flow:
        return flow

    features = list_task_features(svc)
    assert features[-1].port == 7777
