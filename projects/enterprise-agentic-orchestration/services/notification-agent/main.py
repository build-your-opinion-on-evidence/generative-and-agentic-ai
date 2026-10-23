"""
services/notification-agent/main.py
Notification Agent — simulates customer notifications (SMS, push, email).
No real external channels — records notifications in structured logs.
"""
from __future__ import annotations

import os
import sys
import uuid

sys.path.insert(0, "/app")

import uvicorn
from fastapi import FastAPI

from shared.agent_base import call_banking_api, emit_agent_event, make_error_response
from shared.contracts.agent_schemas import AgentRequest, AgentResponse, AgentStatus
from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "notification-agent")
_AGENT_ID = "notification-agent"
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

app = FastAPI(title="Notification Agent", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


_NOTIFICATION_TEMPLATES = {
    "FRAUD_ALERT": "Security alert: suspicious activity detected on your card {card_id}. Please verify this activity.",
    "CARD_FROZEN": "Your card {card_id} has been temporarily suspended for security reasons. Contact us to restore access.",
    "INVESTIGATION_COMPLETE": "Our fraud investigation on your account has been completed. Reference: {task_id}.",
}


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.post("/invoke", response_model=AgentResponse)
async def invoke(request: AgentRequest) -> AgentResponse:
    emit_agent_event(EventType.AGENT_INVOCATION_STARTED, request)
    try:
        customer_id = request.input.get("customer_id")
        notification_type = request.input.get("notification_type", "FRAUD_ALERT")
        card_id = request.input.get("card_id", "****")
        channel = request.input.get("channel", "push")  # push / sms / email

        if not customer_id:
            return make_error_response(request, "customer_id required")

        # Get customer contact info
        customer = await call_banking_api(f"/customers/{customer_id}")

        template = _NOTIFICATION_TEMPLATES.get(notification_type, "Security notification for your account.")
        message = template.format(card_id=card_id, task_id=request.task_id)

        notification_id = f"notif-{uuid.uuid4().hex[:8]}"

        # Simulate sending (structured log + Kafka event)
        logger.info("notification_sent", extra={
            "notification_id": notification_id,
            "customer_id": customer_id,
            "notification_type": notification_type,
            "channel": channel,
            "message_length": len(message),
            "task_id": request.task_id,
        })

        evt = EventEnvelope(
            type=EventType.CUSTOMER_NOTIFIED,
            source=_AGENT_ID,
            correlation_id=request.correlation_id,
            data={
                "notification_id": notification_id,
                "customer_id": customer_id,
                "notification_type": notification_type,
                "channel": channel,
                "task_id": request.task_id,
            },
        )
        publish_event(KafkaTopic.ACTION_EVENTS, evt)
        flush()

        response = AgentResponse(
            request_id=request.request_id,
            task_id=request.task_id,
            agent_id=_AGENT_ID,
            status=AgentStatus.SUCCESS,
            confidence=1.0,
            findings=[{
                "type": "notification_sent",
                "notification_id": notification_id,
                "channel": channel,
                "description": f"Notification '{notification_type}' sent via {channel}",
            }],
            evidence_references=[customer_id, notification_id],
        )
        emit_agent_event(EventType.AGENT_INVOCATION_COMPLETED, request, {"notification_id": notification_id})
        return response

    except Exception as exc:
        logger.error("agent_invocation_failed", extra={"error": str(exc)})
        emit_agent_event(EventType.AGENT_INVOCATION_FAILED, request, {"error": str(exc)})
        return make_error_response(request, str(exc))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8015, log_config=None)
