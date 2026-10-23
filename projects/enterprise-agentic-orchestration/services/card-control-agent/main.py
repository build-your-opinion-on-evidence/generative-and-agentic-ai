"""
services/card-control-agent/main.py
Card Control Agent — executes approved card freeze/unfreeze actions.
Must always check policy before executing any write action.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "/app")

import httpx
import uvicorn
from fastapi import FastAPI

from shared.agent_base import call_banking_api, post_banking_api, emit_agent_event, make_error_response
from shared.contracts.agent_schemas import AgentRequest, AgentResponse, AgentStatus
from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "card-control-agent")
_AGENT_ID = "card-control-agent"
_POLICY_URL = os.environ.get("POLICY_ENGINE_URL", "http://policy-engine:8020")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

app = FastAPI(title="Card Control Agent", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.post("/invoke", response_model=AgentResponse)
async def invoke(request: AgentRequest) -> AgentResponse:
    emit_agent_event(EventType.AGENT_INVOCATION_STARTED, request)
    try:
        card_id = request.input.get("card_id")
        action = request.input.get("action", "FREEZE_CARD")  # FREEZE_CARD or UNFREEZE_CARD

        if not card_id:
            return make_error_response(request, "card_id required")

        # Check policy first
        async with httpx.AsyncClient(timeout=10) as client:
            policy_resp = await client.post(
                f"{_POLICY_URL}/evaluate",
                json={
                    "task_id": request.task_id,
                    "correlation_id": request.correlation_id,
                    "action_type": action,
                    "action_payload": {"card_id": card_id, "context": request.context},
                    "requested_by": _AGENT_ID,
                },
            )
            policy_resp.raise_for_status()
            policy_decision = policy_resp.json()

        decision = policy_decision.get("decision")
        logger.info("policy_decision_received", extra={
            "task_id": request.task_id,
            "action": action,
            "decision": decision,
        })

        if decision == "AUTO_DENY":
            return AgentResponse(
                request_id=request.request_id,
                task_id=request.task_id,
                agent_id=_AGENT_ID,
                status=AgentStatus.FAILED,
                errors=["Action denied by policy engine"],
            )

        if decision == "REQUIRE_APPROVAL":
            # Publish approval request event
            evt = EventEnvelope(
                type=EventType.APPROVAL_REQUESTED,
                source=_AGENT_ID,
                correlation_id=request.correlation_id,
                data={
                    "task_id": request.task_id,
                    "action_type": action,
                    "action_payload": {"card_id": card_id},
                    "requested_by": _AGENT_ID,
                    "policy_decision_id": policy_decision.get("decision_id"),
                },
            )
            publish_event(KafkaTopic.APPROVAL_EVENTS, evt)
            flush()
            return AgentResponse(
                request_id=request.request_id,
                task_id=request.task_id,
                agent_id=_AGENT_ID,
                status=AgentStatus.PARTIAL,
                findings=[{"type": "approval_pending", "description": f"Action {action} requires human approval"}],
            )

        # AUTO_ALLOW — execute action
        if action == "FREEZE_CARD":
            result = await post_banking_api(f"/cards/{card_id}/freeze", {"reason": f"fraud_task:{request.task_id}"})
            publish_event(KafkaTopic.ACTION_EVENTS, EventEnvelope(
                type=EventType.CARD_FROZEN,
                source=_AGENT_ID,
                correlation_id=request.correlation_id,
                data={"card_id": card_id, "task_id": request.task_id},
            ))
        else:
            result = await post_banking_api(f"/cards/{card_id}/unfreeze", {})
            publish_event(KafkaTopic.ACTION_EVENTS, EventEnvelope(
                type=EventType.CARD_UNBLOCKED,
                source=_AGENT_ID,
                correlation_id=request.correlation_id,
                data={"card_id": card_id, "task_id": request.task_id},
            ))
        flush()
        logger.info("card_action_executed", extra={"action": action, "card_id": card_id, "result": result})

        response = AgentResponse(
            request_id=request.request_id,
            task_id=request.task_id,
            agent_id=_AGENT_ID,
            status=AgentStatus.SUCCESS,
            confidence=1.0,
            findings=[{"type": "action_completed", "description": f"{action} executed successfully", "result": result}],
            recommended_actions=[],
            evidence_references=[card_id],
        )
        emit_agent_event(EventType.AGENT_INVOCATION_COMPLETED, request, {"action": action})
        return response

    except Exception as exc:
        logger.error("agent_invocation_failed", extra={"error": str(exc)})
        emit_agent_event(EventType.AGENT_INVOCATION_FAILED, request, {"error": str(exc)})
        return make_error_response(request, str(exc))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8014, log_config=None)
