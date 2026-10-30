"""Immutable task state. Reference answers remain outside this envelope."""

from dataclasses import dataclass, field
from typing import Literal
from uuid import uuid4


@dataclass(frozen=True, slots=True)
class TaskEnvelope:
    run_id: str
    task_type: str
    phase: Literal["warmup", "test"]
    original_prompt: str
    remaining_work: str

    task_id: str = field(default_factory=lambda: str(uuid4()))
    completed_results: tuple[str, ...] = ()
    hop_count: int = 0
    split_count: int = 0
    previous_agent: str | None = None
    executors: tuple[str, ...] = ()
    continuation_version: int = 0

    def __post_init__(self) -> None:
        for name in (
            "task_id", "run_id", "task_type",
            "original_prompt", "remaining_work",
        ):
            self._check_text(name, getattr(self, name))

        if self.phase not in ("warmup", "test"):
            raise ValueError("phase must be warmup or test")

        for name in ("hop_count", "split_count", "continuation_version"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")

        if self.previous_agent is not None:
            self._check_text("previous_agent", self.previous_agent)

        for name in ("completed_results", "executors"):
            values = getattr(self, name)
            if not isinstance(values, tuple):
                raise ValueError(f"{name} must be a tuple")
            for value in values:
                self._check_text(name, value)

        if len(set(self.executors)) != len(self.executors):
            raise ValueError("executors must contain distinct agent IDs")

    @staticmethod
    def _check_text(name: str, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a non-empty string")
