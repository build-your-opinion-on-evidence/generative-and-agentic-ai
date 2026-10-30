"""Token accounting for one model call."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class TokenUsage:
    call_id: str
    task_id: str
    run_id: str
    system: str
    phase: Literal["warmup", "test"]
    stage: str
    model: str
    source: Literal["provider", "estimated", "unavailable"]

    agent_id: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cached_input_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "call_id", "task_id", "run_id", "system", "stage", "model",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")

        if self.agent_id is not None:
            if not isinstance(self.agent_id, str) or not self.agent_id.strip():
                raise ValueError("agent_id must be a non-empty string")

        if self.phase not in ("warmup", "test"):
            raise ValueError("phase must be warmup or test")

        if self.source not in ("provider", "estimated", "unavailable"):
            raise ValueError("invalid usage source")

        counts = (
            self.input_tokens,
            self.output_tokens,
            self.cached_input_tokens,
        )
        for value in counts:
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("token counts must be non-negative integers")

        if self.source == "unavailable":
            if any(value is not None for value in counts):
                raise ValueError("unavailable usage must have no counts")
        elif self.input_tokens is None and self.output_tokens is None:
            raise ValueError("measured usage requires at least one count")

        if self.cached_input_tokens is not None:
            if self.input_tokens is None:
                raise ValueError("cached tokens require input_tokens")
            if self.cached_input_tokens > self.input_tokens:
                raise ValueError("cached tokens cannot exceed input_tokens")

    @property
    def total_tokens(self) -> int | None:
        """Cached tokens are already included in input_tokens."""
        if self.input_tokens is None or self.output_tokens is None:
            return None
        return self.input_tokens + self.output_tokens
