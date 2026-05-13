"""Workflow Flow round-trip + Graph fetch against Go sidecar."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from microbus_py.wire.codec import decode_response
from microbus_py.workflow import Flow
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from microbus_py import Connector
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]


async def _client(env: ConformanceEnv) -> Connector:
    svc = await connector_for(env, "py-wf.example")
    await svc.startup()
    return svc


async def test_python_flow_round_trips_through_go_double_task(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await _client(conformance_env)
    try:
        f = Flow(
            flow_key="conf-key",
            workflow_name="https://workflow.example:428/wf",
            task_name="https://workflow.example:428/tasks/double",
            step_num=1,
            state={"x": 7},
        )
        body = json.dumps(f.to_json()).encode()
        resp_bytes = await svc.request(
            method="POST",
            url="https://workflow.example:428/tasks/double",
            body=body,
            headers=[("Content-Type", "application/json")],
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        out = Flow.from_json(json.loads(resp.body))
        assert out.state["x"] == 14
    finally:
        await svc.shutdown()


async def test_python_fetches_go_emitted_graph(conformance_env: ConformanceEnv) -> None:
    svc = await _client(conformance_env)
    try:
        resp_bytes = await svc.request(
            method="GET",
            url="https://workflow.example:428/graphs/double-twice",
            timeout=5.0,
        )
        resp = decode_response(resp_bytes)
        assert resp.status_code == 200
        graph = json.loads(resp.body)["graph"]
        assert graph["name"] == "workflow.example/double-twice"
        assert graph["entryPoint"]
        assert isinstance(graph["tasks"], list)
        assert {"from", "to"} <= set(graph["transitions"][0].keys())
    finally:
        await svc.shutdown()
