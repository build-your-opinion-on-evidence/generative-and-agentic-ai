"""
services/task-manager/state_machine.py
Validates and enforces the fraud task lifecycle state transitions.
"""
from __future__ import annotations

from enum import Enum

_TRANSITIONS: dict[str, set[str]] = {
    "RECEIVED":          {"READY", "CANCELLED"},
    "READY":             {"ROUTING", "CANCELLED", "PAUSED_PREEMPTED"},
    "ROUTING":           {"DAG_RUNNING", "REACT_RUNNING", "FAILED", "CANCELLED"},
    "DAG_RUNNING":       {"WAITING_APPROVAL", "COMPLETED", "FAILED", "CANCELLED"},
    "REACT_RUNNING":     {"WAITING_APPROVAL", "COMPLETED", "FAILED", "CANCELLED"},
    "WAITING_APPROVAL":  {"RESUMING", "CANCELLED", "FAILED"},
    "RESUMING":          {"DAG_RUNNING", "REACT_RUNNING", "COMPLETED", "FAILED"},
    "COMPLETED":         set(),
    "FAILED":            set(),
    "CANCELLED":         set(),
    "PAUSED_PREEMPTED":  {"READY", "CANCELLED"},
}


class TaskStatus(str, Enum):
    RECEIVED = "RECEIVED"
    READY = "READY"
    ROUTING = "ROUTING"
    DAG_RUNNING = "DAG_RUNNING"
    REACT_RUNNING = "REACT_RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    RESUMING = "RESUMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PAUSED_PREEMPTED = "PAUSED_PREEMPTED"


def assert_transition(from_status: str, to_status: str) -> None:
    """Raise ValueError if the transition is not allowed."""
    allowed = _TRANSITIONS.get(from_status, set())
    if to_status not in allowed:
        raise ValueError(
            f"Invalid task status transition: {from_status} → {to_status}. "
            f"Allowed from {from_status}: {allowed or '(terminal)'}"
        )
