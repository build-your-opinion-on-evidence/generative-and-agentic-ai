"""
Shared base agent module.
All specialized agent services use this pattern.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx

from shared.contracts.agent_schemas import AgentRequest, AgentResponse, AgentStatus, ModelUsage
from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.producer import flush, publish_event
from shared.openai_provider.provider import get_provider

import logging
logger = logging.getLogger(__name__)

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "agent")
_BANKING_API = os.environ.get("FAKE_BANKING_API_URL", "http://fake-banking-api:8001")


def _load_prompt(agent_name: str, version: str = "v1") -> str:
    """Load the system prompt from the prompts directory."""
    prompt_path = Path(f"/app/prompts/{agent_name}/{version}/system.md")
    if prompt_path.exists():
        return prompt_path.read_text()
    return f"You are the {agent_name} agent. Analyze the input and return JSON findings."


def _load_output_schema(agent_name: str, version: str = "v1") -> dict | None:
    schema_path = Path(f"/app/prompts/{agent_name}/{version}/output.schema.json")
    if schema_path.exists():
        return json.loads(schema_path.read_text())
    return None


async def call_banking_api(path: str) -> dict | list:
    """Helper to call the fake banking API."""
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{_BANKING_API}{path}")
        resp.raise_for_status()
        return resp.json()


async def post_banking_api(path: str, data: dict) -> dict:
    """Helper to POST to the fake banking API."""
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(f"{_BANKING_API}{path}", json=data)
        resp.raise_for_status()
        return resp.json()


def emit_agent_event(event_type: str, request: AgentRequest, extra: dict | None = None) -> None:
    """Emit agent lifecycle events to Kafka."""
    evt = EventEnvelope(
        type=event_type,
        source=_SERVICE_NAME,
        correlation_id=request.correlation_id,
        data={
            "request_id": request.request_id,
            "task_id": request.task_id,
            "agent_id": request.agent_id,
            **(extra or {}),
        },
    )
    publish_event(KafkaTopic.AGENT_EVENTS, evt)
    flush()


def make_error_response(request: AgentRequest, error: str) -> AgentResponse:
    return AgentResponse(
        request_id=request.request_id,
        task_id=request.task_id,
        agent_id=request.agent_id,
        status=AgentStatus.FAILED,
        errors=[error],
    )
