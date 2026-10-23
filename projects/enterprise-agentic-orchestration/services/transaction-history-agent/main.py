"""
services/transaction-history-agent/main.py
Transaction History Agent — analyzes transaction patterns using OpenAI.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, "/app")

import uvicorn
from fastapi import FastAPI

from shared.agent_base import call_banking_api, emit_agent_event, make_error_response, _load_prompt, _load_output_schema
from shared.contracts.agent_schemas import AgentRequest, AgentResponse, AgentStatus
from shared.contracts.events import EventType
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing
from shared.openai_provider.provider import get_provider

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "transaction-history-agent")
_AGENT_ID = "transaction-history-agent"
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

app = FastAPI(title="Transaction History Agent", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)

_SYSTEM_PROMPT = _load_prompt("transaction-history")
_OUTPUT_SCHEMA = _load_output_schema("transaction-history")


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.post("/invoke", response_model=AgentResponse)
async def invoke(request: AgentRequest) -> AgentResponse:
    emit_agent_event(EventType.AGENT_INVOCATION_STARTED, request)
    try:
        customer_id = request.input.get("customer_id")
        card_id = request.input.get("card_id")

        if card_id:
            transactions = await call_banking_api(f"/transactions/by-card/{card_id}?limit=50")
        elif customer_id:
            transactions = await call_banking_api(f"/transactions/by-customer/{customer_id}?limit=100")
        else:
            return make_error_response(request, "customer_id or card_id required")

        user_content = json.dumps({
            "task_id": request.task_id,
            "customer_id": customer_id,
            "card_id": card_id,
            "current_transaction": {
                "transaction_id": request.input.get("transaction_id"),
                "amount": request.input.get("amount"),
                "currency": request.input.get("currency"),
                "country": request.input.get("country"),
                "risk_score": request.input.get("risk_score"),
            },
            "recent_transactions": transactions[:50],
        }, default=str)

        provider = get_provider()
        content, model_usage = provider.chat(
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            output_schema=_OUTPUT_SCHEMA,
            max_tokens=1200,
            span_name=f"{_AGENT_ID}.chat",
        )

        findings_data = json.loads(content)
        response = AgentResponse(
            request_id=request.request_id,
            task_id=request.task_id,
            agent_id=_AGENT_ID,
            status=AgentStatus.SUCCESS,
            confidence=findings_data.get("confidence", 0.0),
            findings=findings_data.get("findings", []),
            recommended_actions=findings_data.get("recommended_actions", []),
            evidence_references=[customer_id or "", card_id or ""],
            model_usage=model_usage,
        )
        emit_agent_event(EventType.AGENT_INVOCATION_COMPLETED, request, {"confidence": response.confidence})
        return response

    except Exception as exc:
        logger.error("agent_invocation_failed", extra={"error": str(exc)})
        emit_agent_event(EventType.AGENT_INVOCATION_FAILED, request, {"error": str(exc)})
        return make_error_response(request, str(exc))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8011, log_config=None)
