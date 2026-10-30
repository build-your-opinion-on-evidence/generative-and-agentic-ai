"""Mutually exclusive executor outcomes."""

from dataclasses import dataclass, field
from typing import Literal


def require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class ExecuteOutput:
    final_answer: str
    action: Literal["execute"] = field(default="execute", init=False)

    def __post_init__(self) -> None:
        require_text("final_answer", self.final_answer)


@dataclass(frozen=True, slots=True)
class SplitOutput:
    completed_results: tuple[str, ...]
    remaining_work: str
    action: Literal["split"] = field(default="split", init=False)

    def __post_init__(self) -> None:
        require_text("remaining_work", self.remaining_work)

        if not isinstance(self.completed_results, tuple):
            raise ValueError("completed_results must be a tuple")

        for result in self.completed_results:
            require_text("completed result", result)


ExecutorOutput = ExecuteOutput | SplitOutput
