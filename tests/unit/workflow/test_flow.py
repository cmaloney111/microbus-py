"""Round-trip tests for ``microbus_py.workflow.Flow``.

Wire format mirrors fabric ``flowJSON``: camelCase field names, omit-zero
semantics, Go-style duration strings, and Foreman orchestration metadata
(``flowKey``, ``workflowName``, ``taskName``, ``stepNum``).
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from microbus_py.workflow import Flow
from microbus_py.workflow.flow import format_go_duration, parse_go_duration


def test_empty_flow_to_json_omits_zero_fields() -> None:
    f = Flow()
    assert f.to_json() == {}


def test_flow_round_trip_preserves_state_and_changes() -> None:
    f = Flow(state={"a": 1, "b": "two"}, changes={"b": "two"})
    doc = f.to_json()
    assert doc == {"state": {"a": 1, "b": "two"}, "changes": {"b": "two"}}
    g = Flow.from_json(doc)
    assert g.state == {"a": 1, "b": "two"}
    assert g.changes == {"b": "two"}


def test_flow_orchestration_metadata_round_trip() -> None:
    f = Flow(
        flow_key="abc-123",
        workflow_name="https://wf.example:428/wf",
        task_name="https://wf.example:428/double",
        step_num=4,
    )
    doc = f.to_json()
    assert doc == {
        "flowKey": "abc-123",
        "workflowName": "https://wf.example:428/wf",
        "taskName": "https://wf.example:428/double",
        "stepNum": 4,
    }
    g = Flow.from_json(doc)
    assert g.flow_key == "abc-123"
    assert g.workflow_name == "https://wf.example:428/wf"
    assert g.task_name == "https://wf.example:428/double"
    assert g.step_num == 4


def test_flow_goto_field() -> None:
    f = Flow(goto="https://x.example:428/next")
    doc = f.to_json()
    assert doc == {"goto": "https://x.example:428/next"}
    assert Flow.from_json(doc).goto == "https://x.example:428/next"


def test_flow_retry_is_bool_with_attempt_and_backoff() -> None:
    f = Flow(retry=True, attempt=2, backoff_max_attempts=5)
    doc = f.to_json()
    assert doc == {"retry": True, "attempt": 2, "backoffMaxAttempts": 5}
    g = Flow.from_json(doc)
    assert g.retry is True
    assert g.attempt == 2
    assert g.backoff_max_attempts == 5


def test_flow_sleep_duration_serializes_as_go_duration_string() -> None:
    f = Flow(sleep_duration=timedelta(seconds=1))
    assert f.to_json() == {"sleepDuration": "1s"}

    f2 = Flow(sleep_duration=timedelta(milliseconds=500))
    assert f2.to_json() == {"sleepDuration": "500ms"}

    g = Flow.from_json({"sleepDuration": "2s500ms"})
    assert g.sleep_duration == timedelta(seconds=2, milliseconds=500)


def test_flow_interrupt_is_bool_with_payload_map() -> None:
    f = Flow(interrupt=True, interrupt_payload={"prompt": "name?"})
    doc = f.to_json()
    assert doc == {"interrupt": True, "interruptPayload": {"prompt": "name?"}}
    g = Flow.from_json(doc)
    assert g.interrupt is True
    assert g.interrupt_payload == {"prompt": "name?"}


def test_flow_subgraph_reference_round_trip() -> None:
    f = Flow(
        subgraph_workflow="https://child.example:428/wf",
        subgraph_input={"seed": 42},
    )
    doc = f.to_json()
    assert doc == {
        "subgraphWorkflow": "https://child.example:428/wf",
        "subgraphInput": {"seed": 42},
    }
    g = Flow.from_json(doc)
    assert g.subgraph_workflow == "https://child.example:428/wf"
    assert g.subgraph_input == {"seed": 42}


def test_flow_backoff_fields_camelcase_on_wire() -> None:
    f = Flow(
        attempt=3,
        backoff_max_attempts=10,
        backoff_initial_delay=timedelta(seconds=1),
        backoff_delay_multiplier=2.0,
        backoff_max_delay=timedelta(seconds=30),
    )
    doc = f.to_json()
    assert doc == {
        "attempt": 3,
        "backoffMaxAttempts": 10,
        "backoffInitialDelay": "1s",
        "backoffDelayMultiplier": 2.0,
        "backoffMaxDelay": "30s",
    }
    g = Flow.from_json(doc)
    assert g.attempt == 3
    assert g.backoff_max_attempts == 10
    assert g.backoff_initial_delay == timedelta(seconds=1)
    assert g.backoff_delay_multiplier == 2.0
    assert g.backoff_max_delay == timedelta(seconds=30)


def test_flow_from_json_accepts_unknown_keys() -> None:
    g = Flow.from_json({"unknownField": "abc", "state": {"k": 1}})
    assert g.state == {"k": 1}


def test_flow_state_and_changes_default_to_empty_dicts() -> None:
    f = Flow()
    assert f.state == {}
    assert f.changes == {}
    f.state["x"] = 1
    assert Flow().state == {}


def test_format_go_duration_zero_and_subsecond_units() -> None:
    assert format_go_duration(timedelta()) == "0s"
    assert format_go_duration(timedelta(microseconds=1)) == "1us"
    assert format_go_duration(timedelta(microseconds=500)) == "500us"
    assert format_go_duration(timedelta(microseconds=1500)) == "1.5ms"


def test_format_go_duration_minutes_and_hours() -> None:
    assert format_go_duration(timedelta(minutes=5)) == "5m0s"
    assert format_go_duration(timedelta(hours=1, minutes=30, seconds=15)) == "1h30m15s"


def test_format_go_duration_negative() -> None:
    assert format_go_duration(timedelta(seconds=-2)) == "-2s"


def test_parse_go_duration_zero_and_signs() -> None:
    assert parse_go_duration("") == timedelta()
    assert parse_go_duration("0") == timedelta()
    assert parse_go_duration("0s") == timedelta()
    assert parse_go_duration("+1s") == timedelta(seconds=1)
    assert parse_go_duration("-1s") == timedelta(seconds=-1)
    assert parse_go_duration("1h30m") == timedelta(hours=1, minutes=30)


def test_parse_go_duration_invalid_raises() -> None:
    with pytest.raises(ValueError, match="invalid Go duration"):
        parse_go_duration("notaduration")
    with pytest.raises(ValueError, match="invalid Go duration"):
        parse_go_duration("1s_extra")
