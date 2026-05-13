"""Per-Connector task/workflow feature registry."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector

__all__ = [
    "TaskFeature",
    "WorkflowFeature",
    "list_task_features",
    "list_workflow_features",
    "register_task",
    "register_workflow",
]


@dataclass(slots=True, frozen=True)
class TaskFeature:
    name: str
    route: str
    description: str
    required_claims: str | None
    port: int
    host: str | None = None


@dataclass(slots=True, frozen=True)
class WorkflowFeature:
    name: str
    route: str
    description: str
    required_claims: str | None
    port: int
    host: str | None = None


def register_task(svc: Connector, feature: TaskFeature) -> None:
    svc.register_task_feature(feature)


def register_workflow(svc: Connector, feature: WorkflowFeature) -> None:
    svc.register_workflow_feature(feature)


def list_task_features(svc: Connector) -> list[TaskFeature]:
    return svc.list_task_features()


def list_workflow_features(svc: Connector) -> list[WorkflowFeature]:
    return svc.list_workflow_features()
