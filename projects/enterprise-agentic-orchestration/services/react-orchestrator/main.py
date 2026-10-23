"""
services/react-orchestrator/main.py
ReAct Orchestration Service.
Implements the Reason-Act-Observe loop for ambiguous fraud investigations.
Uses Agent Discovery to select agents dynamically, calls them iteratively,
aggregates findings, and produces structured recommendations.
"""
from __future__ import annotations

import os
import sys
import uuid
from typing import Any

sys.path.insert(0, "/app")

import httpx
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing
from shared.openai_provider.provider import get_provider

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "react-orchestrator")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

_AGENT_DISCOVERY_URL = os.environ.get("AGENT_DISCOVERY_URL", "http://agent-discovery:8006")
_POLICY_URL = os.environ.get("POLICY_ENGINE_URL", "http://policy-engine:8020")

MAX_STEPS = 8
MAX_TOKENS = 4000


class InvestigationRequest(BaseModel):
    task_id: str
    correlation_id: str
    context: dict


class InvestigationResponse(BaseModel):
    task_id: str
    status: str
    steps_taken: int
    findings: list[dict]
    recommended_actions: list[dict]
    confidence: float
    total_tokens: int


_SYSTEM_PROMPT = """You are the ReAct Fraud Investigation Orchestrator.

You perform iterative fraud investigations using available agents.
In each step, you either:
1. CALL an agent to gather more information
2. ANALYZE findings and draw conclusions
3. COMPLETE when you have sufficient evidence

Available agent capabilities:
- customer_profile: get customer risk profile
- transaction_history: analyze transaction patterns
- device_risk: assess device fingerprints
- fraud_scoring: compute overall fraud score
- card_control: freeze/unfreeze cards (requires policy approval)
- customer_notification: notify customer

Rules:
- If you find strong evidence of fraud (confidence >= 0.85), you MUST call the card_control agent (with action="FREEZE_CARD" and the card_id) BEFORE you COMPLETE the investigation!
- Do not complete a high-confidence fraud case without executing a containment action via the agent.
- Stop if total steps >= 8
- Always return structured JSON

Output format for each step:
{
  "action": "CALL_AGENT" | "COMPLETE",
  "agent_id": "...",  // only when action=CALL_AGENT
  "input": {...},     // agent input
  "reasoning": "...", // your reasoning
  "confidence": 0.0-1.0
}
"""


async def get_available_agents() -> list[dict]:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{_AGENT_DISCOVERY_URL}/agents")
        resp.raise_for_status()
        return resp.json()


async def call_agent(agent: dict, request: InvestigationRequest, agent_input: dict) -> dict:
    """Call a specialized agent and return its response."""
    req_id = f"react-{uuid.uuid4().hex[:8]}"
    async with httpx.AsyncClient(timeout=agent.get("timeout_seconds", 30)) as client:
        resp = await client.post(
            f"{agent['endpoint']}/invoke",
            json={
                "request_id": req_id,
                "task_id": request.task_id,
                "correlation_id": request.correlation_id,
                "agent_id": agent["agent_id"],
                "prompt_version": agent.get("prompt_version", "v1"),
                "input": agent_input,
                "context": request.context,
                "constraints": {"max_steps": 5, "max_tokens": 2000, "timeout_seconds": 30},
            },
        )
        resp.raise_for_status()
        return resp.json()



async def react_loop(request: InvestigationRequest) -> InvestigationResponse:
    import json
    
    provider = get_provider()
    
    seq_num = 1
    def _publish_trace(event_type: str, data: dict):
        nonlocal seq_num
        evt = EventEnvelope(
            type=event_type,
            source=_SERVICE_NAME,
            correlation_id=request.correlation_id,
            data={"task_id": request.task_id, "sequence_number": seq_num, **data},
        )
        publish_event(KafkaTopic.ORCHESTRATION_EVENTS, evt)
        seq_num += 1

    _publish_trace("AgentDiscoveryRequested", {})
    agents = await get_available_agents()
    agent_map = {a["agent_id"]: a for a in agents}
    
    _publish_trace("InvestigationGoalCreated", {"goal": "Gather evidence step by step based on available context."})

    all_findings: list[dict] = []
    recommended_actions: list[dict] = []
    step_count = 0
    total_tokens = 0
    conversation: list[dict] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": json.dumps({
                "task_id": request.task_id,
                "context": request.context,
                "available_agents": [{"agent_id": a["agent_id"], "capabilities": a["capabilities"]} for a in agents],
                "instruction": "Begin investigation. Gather evidence step by step.",
            }, default=str),
        },
    ]

    final_confidence = 0.0

    while step_count < MAX_STEPS:
        step_count += 1
        logger.info("react_step", extra={"task_id": request.task_id, "step": step_count})

        try:
            llm_response, model_usage = provider.chat(
                messages=conversation,
                max_tokens=600,
                span_name=f"react.step.{step_count}",
            )
            total_tokens += model_usage.total_tokens
        except Exception as exc:
            logger.error("react_llm_error", extra={"step": step_count, "error": str(exc)})
            _publish_trace("InvestigationFailed", {"error": str(exc)})
            break

        try:
            decision = json.loads(llm_response)
        except json.JSONDecodeError:
            import re
            match = re.search(r'\{.*\}', llm_response, re.DOTALL)
            if match:
                try:
                    decision = json.loads(match.group())
                except Exception:
                    decision = {"action": "COMPLETE", "reasoning": "Could not parse LLM output", "confidence": 0.5}
            else:
                decision = {"action": "COMPLETE", "reasoning": "LLM returned non-JSON", "confidence": 0.5}

        action = decision.get("action", "COMPLETE")
        final_confidence = float(decision.get("confidence", 0.5))
        reasoning = decision.get("reasoning", "")
        
        logger.info("react_decision", extra={
            "task_id": request.task_id,
            "step": step_count,
            "action": action,
            "confidence": final_confidence,
        })
        
        _publish_trace("NextStepSelected", {
            "iteration": step_count,
            "decision_summary": reasoning,
            "action": action
        })

        if action == "COMPLETE":
            recommended_actions = decision.get("recommended_actions", decision.get("actions", []))
            break

        if action == "CALL_AGENT":
            agent_id = decision.get("agent_id", "fraud-scoring-agent")
            agent = agent_map.get(agent_id)
            if not agent:
                agent = agent_map.get("fraud-scoring-agent")
                
            _publish_trace("AgentSelected", {
                "iteration": step_count,
                "selected_agent": agent_id,
                "selection_reason": reasoning
            })

            if agent:
                agent_input = decision.get("input", request.context)
                _publish_trace("AgentInvocationStarted", {
                    "iteration": step_count,
                    "agent_id": agent_id,
                    "input": agent_input
                })
                
                try:
                    agent_response = await call_agent(agent, request, agent_input)
                    findings = agent_response.get("findings", [])
                    all_findings.extend(findings)
                    
                    _publish_trace("AgentInvocationCompleted", {
                        "iteration": step_count,
                        "agent_id": agent_id,
                        "observation": findings
                    })
                    
                    _publish_trace("ObservationReceived", {
                        "iteration": step_count,
                        "agent_id": agent_id,
                        "observation_summary": agent_response.get("status", "Completed")
                    })

                    observation = {
                        "agent_id": agent_id,
                        "findings": findings,
                        "recommended_actions": agent_response.get("recommended_actions", []),
                        "confidence": agent_response.get("confidence", 0.0),
                        "status": agent_response.get("status"),
                    }
                    conversation.append({"role": "assistant", "content": llm_response})
                    conversation.append({
                        "role": "user",
                        "content": f"Agent {agent_id} responded: {json.dumps(observation, default=str)}. Continue investigation.",
                    })
                except Exception as exc:
                    logger.warning("react_agent_call_failed", extra={"agent_id": agent_id, "error": str(exc)})
                    _publish_trace("AgentInvocationFailed", {
                        "iteration": step_count,
                        "agent_id": agent_id,
                        "error": str(exc)
                    })
                    conversation.append({"role": "assistant", "content": llm_response})
                    conversation.append({
                        "role": "user",
                        "content": f"Agent {agent_id} failed: {exc}. Try a different approach or complete investigation.",
                    })

    evt = EventEnvelope(
        type=EventType.REACT_COMPLETED,
        source=_SERVICE_NAME,
        correlation_id=request.correlation_id,
        data={
            "task_id": request.task_id,
            "steps_taken": step_count,
            "confidence": final_confidence,
            "findings_count": len(all_findings),
        },
    )
    publish_event(KafkaTopic.ORCHESTRATION_EVENTS, evt)
    flush()

    return InvestigationResponse(
        task_id=request.task_id,
        status="COMPLETED",
        steps_taken=step_count,
        findings=all_findings,
        recommended_actions=recommended_actions,
        confidence=final_confidence,
        total_tokens=total_tokens,
    )


# ── FastAPI app ───
app = FastAPI(title="ReAct Orchestrator", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.post("/investigate", response_model=InvestigationResponse)
async def investigate(request: InvestigationRequest) -> InvestigationResponse:
    logger.info("react_investigation_started", extra={"task_id": request.task_id})
    publish_event(KafkaTopic.ORCHESTRATION_EVENTS, EventEnvelope(
        type=EventType.REACT_STARTED,
        source=_SERVICE_NAME,
        correlation_id=request.correlation_id,
        data={"task_id": request.task_id},
    ))
    flush()

    result = await react_loop(request)
    logger.info("react_investigation_completed", extra={
        "task_id": request.task_id,
        "steps": result.steps_taken,
        "confidence": result.confidence,
        "total_tokens": result.total_tokens,
    })
    return result


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8005, log_config=None)
