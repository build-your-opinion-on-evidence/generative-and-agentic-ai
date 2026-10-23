"""
shared/contracts/agent_schemas.py
Versioned request/response schemas for all AI agents.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class AgentStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class AgentConstraints(BaseModel):
    max_steps: int = 5
    max_tokens: int = 2000
    timeout_seconds: int = 30


class ModelUsage(BaseModel):
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float = 0.0
    latency_ms: int = 0


class AgentRequest(BaseModel):
    request_id: str
    task_id: str
    correlation_id: str
    agent_id: str
    prompt_version: str = "v1"
    input: dict[str, Any]
    context: dict[str, Any] = Field(default_factory=dict)
    constraints: AgentConstraints = Field(default_factory=AgentConstraints)


class AgentResponse(BaseModel):
    request_id: str
    task_id: str
    agent_id: str
    agent_version: str = "1.0.0"
    prompt_version: str = "v1"
    status: AgentStatus
    confidence: float = 0.0
    findings: list[dict[str, Any]] = Field(default_factory=list)
    recommended_actions: list[Any] = Field(default_factory=list)
    evidence_references: list[str] = Field(default_factory=list)
    model_usage: ModelUsage | None = None
    errors: list[str] = Field(default_factory=list)
