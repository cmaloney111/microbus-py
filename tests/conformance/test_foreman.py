"""Foreman executes Python-emitted workflow graphs and Python tasks."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest

from microbus_py.wire.codec import decode_response
from microbus_py.workflow import END, Flow, Graph
from tests.fixtures.go_sidecar import connector_for

if TYPE_CHECKING:
    from tests.fixtures.go_sidecar import ConformanceEnv

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

_WORKFLOW_URL = "https://py-flow.example:428/graphs/increment"
_TASK_URL = "https://py-flow.example:428/tasks/increment"


async def test_go_foreman_runs_python_workflow_and_task(
    conformance_env: ConformanceEnv,
) -> None:
    svc = await connector_for(conformance_env, "py-flow.example")

    @svc.task(route="/tasks/increment")
    async def increment(flow: Flow) -> Flow:
        value = flow.state.get("x", 0)
        assert isinstance(value, int)
        flow.changes["x"] = value + 1
        return flow

    @svc.workflow(route="/graphs/increment")
    def increment_graph() -> Graph:
        graph = Graph(_WORKFLOW_URL)
        graph.task(_TASK_URL)
        graph.transition(_TASK_URL, END)
        graph.declare_inputs("x")
        graph.declare_outputs("x")
        return graph

    await svc.startup()
    try:
        body = json.dumps(
            {
                "workflowName": _WORKFLOW_URL,
                "initialState": {"x": 4},
            }
        ).encode("utf-8")
        wire = await svc.request(
            method="POST",
            url="https://foreman.core:444/run",
            body=body,
            headers=[("Content-Type", "application/json")],
            timeout=15.0,
        )
        resp = decode_response(wire)
        doc = cast("dict[str, object]", json.loads(resp.body))
        assert resp.status_code == 200
        assert doc["status"] == "completed"
        state = doc["state"]
        assert isinstance(state, dict)
        assert state["x"] == 5
    finally:
        await svc.shutdown()
