"""Tests for the ``Graph`` builder JSON output.

Wire format mirrors fabric ``jsonGraph``: ``entryPoint``, ``tasks`` array of
``{name, timeBudget?, subgraph?}`` objects, ``inputs``/``outputs`` keys, and
transition fields ``onError``/``withGoto`` (matching fabric exactly).
"""

from __future__ import annotations

from datetime import timedelta

from microbus_py.workflow import Graph, Reducer


def test_linear_pipeline_a_to_b_to_end() -> None:
    g = Graph("greet")
    g.task("a")
    g.task("b")
    g.transition("a", "b")
    g.transition("b", "END")
    doc = g.to_json()
    assert doc["name"] == "greet"
    assert doc["entryPoint"] == "a"
    assert doc["tasks"] == [{"name": "a"}, {"name": "b"}]
    assert doc["transitions"] == [
        {"from": "a", "to": "b"},
        {"from": "b", "to": "END"},
    ]
    assert "reducers" not in doc or doc["reducers"] == {}
    assert doc["inputs"] == []
    assert doc["outputs"] == []


def test_explicit_entry_point_overrides_first_task() -> None:
    g = Graph("e")
    g.task("a")
    g.task("b")
    g.transition("a", "END")
    g.transition("b", "END")
    g.set_entry_point("b")
    doc = g.to_json()
    assert doc["entryPoint"] == "b"


def test_fanout_for_each_emits_for_each_and_as() -> None:
    g = Graph("fan")
    g.task("split")
    g.task("worker")
    g.transition("split", "worker", for_each="items", as_field="item")
    g.transition("worker", "END")
    doc = g.to_json()
    assert {
        "from": "split",
        "to": "worker",
        "forEach": "items",
        "as": "item",
    } in doc["transitions"]


def test_fanin_with_reducer_and_set_reducer_method() -> None:
    g = Graph("agg")
    g.task("split")
    g.task("worker")
    g.task("collect")
    g.transition("split", "worker", for_each="items")
    g.transition("worker", "collect")
    g.transition("collect", "END")
    g.set_reducer("results", Reducer.Append)
    g.set_reducer("count", Reducer.Add)
    doc = g.to_json()
    assert doc["reducers"] == {"results": "append", "count": "add"}


def test_error_transitions_emit_on_error_flag() -> None:
    g = Graph("safe")
    g.task("risky")
    g.task("recover")
    g.transition("risky", "recover", on_error=True)
    g.transition("risky", "END")
    g.transition("recover", "END")
    doc = g.to_json()
    risky_to_recover = next(
        t for t in doc["transitions"] if t["from"] == "risky" and t["to"] == "recover"
    )
    assert risky_to_recover.get("onError") is True


def test_when_condition_emits_when_field() -> None:
    g = Graph("cond")
    g.task("a")
    g.task("b")
    g.transition("a", "b", when="x > 5")
    g.transition("b", "END")
    doc = g.to_json()
    a_to_b = next(t for t in doc["transitions"] if t["from"] == "a")
    assert a_to_b["when"] == "x > 5"


def test_time_budgets_embedded_in_task_records() -> None:
    g = Graph("timed")
    g.task("slow", time_budget=timedelta(seconds=30))
    g.task("fast", time_budget=timedelta(milliseconds=500))
    g.transition("slow", "fast")
    g.transition("fast", "END")
    doc = g.to_json()
    assert doc["tasks"] == [
        {"name": "slow", "timeBudget": "30s"},
        {"name": "fast", "timeBudget": "500ms"},
    ]


def test_existing_task_time_budget_can_be_updated() -> None:
    g = Graph("timed")
    g.task("step")
    g.task("step", time_budget=timedelta(seconds=5))
    g.transition("step", "END")
    doc = g.to_json()
    assert doc["tasks"] == [{"name": "step", "timeBudget": "5s"}]


def test_subgraph_node_emits_subgraph_flag() -> None:
    g = Graph("parent")
    g.subgraph("https://child.example:428/wf")
    g.transition("https://child.example:428/wf", "END")
    doc = g.to_json()
    assert {"name": "https://child.example:428/wf", "subgraph": True} in doc["tasks"]


def test_subgraph_ignores_end_and_marks_existing_task() -> None:
    g = Graph("parent")
    g.task("first")
    g.subgraph("child")
    g.subgraph("child")
    g.subgraph("END")
    doc = g.to_json()
    assert doc["entryPoint"] == "first"
    assert {"name": "child", "subgraph": True} in doc["tasks"]
    assert {"name": "END"} not in doc["tasks"]


def test_declared_inputs_outputs() -> None:
    g = Graph("io")
    g.task("a")
    g.transition("a", "END")
    g.declare_inputs("seed", "user")
    g.declare_outputs("result")
    doc = g.to_json()
    assert doc["inputs"] == ["seed", "user"]
    assert doc["outputs"] == ["result"]


def test_with_goto_transition_emits_with_goto_flag() -> None:
    g = Graph("gt")
    g.task("a")
    g.task("b")
    g.transition("a", "b", with_goto=True)
    g.transition("a", "END")
    doc = g.to_json()
    a_to_b = next(t for t in doc["transitions"] if t["from"] == "a" and t["to"] == "b")
    assert a_to_b.get("withGoto") is True


def test_unknown_task_in_transition_auto_registers() -> None:
    g = Graph("auto")
    g.transition("x", "y")
    g.transition("y", "END")
    doc = g.to_json()
    names = [t["name"] for t in doc["tasks"]]
    assert "x" in names
    assert "y" in names
