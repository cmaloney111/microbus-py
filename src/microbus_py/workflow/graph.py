"""Graph builder for workflow definitions.

JSON wire format mirrors fabric ``jsonGraph``: ``name``, ``entryPoint``,
``tasks`` array of node records, ``transitions``, ``reducers``, ``inputs``,
``outputs``. Time budgets live inside the per-task record, not as a
separate map.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from microbus_py.workflow.flow import format_go_duration
from microbus_py.workflow.transitions import Transition

if TYPE_CHECKING:
    from datetime import timedelta

    from microbus_py.workflow.reducer import Reducer

__all__ = ["END", "Graph"]


END = "END"


@dataclass(slots=True)
class _Node:
    name: str
    time_budget: timedelta | None = None
    subgraph: bool = False


class Graph:
    def __init__(self, name: str) -> None:
        self._name = name
        self._entry_point: str = ""
        self._nodes: list[_Node] = []
        self._transitions: list[Transition] = []
        self._reducers: dict[str, Reducer] = {}
        self._inputs: list[str] = []
        self._outputs: list[str] = []

    def task(
        self,
        name: str,
        *,
        time_budget: timedelta | None = None,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
    ) -> None:
        del inputs, outputs
        if name == END:
            return
        for node in self._nodes:
            if node.name == name:
                if time_budget is not None:
                    node.time_budget = time_budget
                return
        self._nodes.append(_Node(name=name, time_budget=time_budget))
        if not self._entry_point:
            self._entry_point = name

    def subgraph(self, name: str) -> None:
        if name == END:
            return
        for node in self._nodes:
            if node.name == name:
                node.subgraph = True
                return
        self._nodes.append(_Node(name=name, subgraph=True))
        if not self._entry_point:
            self._entry_point = name

    def transition(
        self,
        frm: str,
        to: str,
        *,
        when: str | None = None,
        for_each: str | None = None,
        as_field: str | None = None,
        on_error: bool = False,
        with_goto: bool = False,
    ) -> None:
        self.task(frm)
        self.task(to)
        self._transitions.append(
            Transition(
                frm=frm,
                to=to,
                when=when,
                for_each=for_each,
                as_field=as_field,
                on_error=on_error,
                with_goto=with_goto,
            )
        )

    def set_entry_point(self, name: str) -> None:
        self._entry_point = name

    def set_reducer(self, field: str, strategy: Reducer) -> None:
        self._reducers[field] = strategy

    def declare_inputs(self, *fields: str) -> None:
        self._inputs = list(fields)

    def declare_outputs(self, *fields: str) -> None:
        self._outputs = list(fields)

    def to_json(self) -> dict[str, Any]:
        tasks: list[dict[str, Any]] = []
        for node in self._nodes:
            record: dict[str, Any] = {"name": node.name}
            if node.time_budget is not None:
                record["timeBudget"] = format_go_duration(node.time_budget)
            if node.subgraph:
                record["subgraph"] = True
            tasks.append(record)
        out: dict[str, Any] = {
            "name": self._name,
            "entryPoint": self._entry_point,
            "tasks": tasks,
            "transitions": [t.to_json() for t in self._transitions],
            "inputs": list(self._inputs),
            "outputs": list(self._outputs),
        }
        if self._reducers:
            out["reducers"] = {k: v.value for k, v in self._reducers.items()}
        return out
