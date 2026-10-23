"""
services/policy-engine/main.py
Policy Engine microservice.
Evaluates proposed actions against business rules and returns:
  AUTO_ALLOW, AUTO_DENY, or REQUIRE_APPROVAL
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, "/app")

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "policy-engine")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

# ── DB ────────────────────────────────────────────────────────────────────────
_HOST = os.environ.get("POLICY_DB_HOST", "localhost")
_PORT = os.environ.get("POLICY_DB_PORT", "5432")
_NAME = os.environ.get("POLICY_DB_NAME", "policy_db")
_USER = os.environ.get("POSTGRES_USER", "fraud_user")
_PASS = os.environ.get("POSTGRES_PASSWORD", "")

DATABASE_URL = f"postgresql+asyncpg://{_USER}:{_PASS}@{_HOST}:{_PORT}/{_NAME}"
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# ── Hardcoded policy rules (matches policy_init.sql seed) ────────────────────
_POLICY_RULES = [
    {"rule_id": "rule-freeze-card-stolen",    "action_type": "FREEZE_CARD",       "decision": "AUTO_ALLOW",       "condition": {"context": "stolen"}},
    {"rule_id": "rule-freeze-card-high-risk", "action_type": "FREEZE_CARD",       "decision": "REQUIRE_APPROVAL", "condition": {}},
    {"rule_id": "rule-block-tx-suspicious",   "action_type": "BLOCK_TRANSACTION", "decision": "REQUIRE_APPROVAL", "condition": {}},
    {"rule_id": "rule-notify-customer-always","action_type": "NOTIFY_CUSTOMER",   "decision": "AUTO_ALLOW",       "condition": {}},
    {"rule_id": "rule-unblock-card-low-risk", "action_type": "UNBLOCK_CARD",      "decision": "AUTO_ALLOW",       "condition": {}},
]

_ACTION_TO_RULES: dict[str, list[dict]] = {}
for rule in _POLICY_RULES:
    _ACTION_TO_RULES.setdefault(rule["action_type"], []).append(rule)


# ── Request/Response ──────────────────────────────────────────────────────────
class PolicyRequest(BaseModel):
    task_id: str
    correlation_id: str
    action_type: str
    action_payload: dict
    requested_by: str


class PolicyResponse(BaseModel):
    decision_id: str
    task_id: str
    action_type: str
    decision: str       # AUTO_ALLOW / AUTO_DENY / REQUIRE_APPROVAL
    rule_id: str | None
    rationale: str


def _evaluate(req: PolicyRequest) -> PolicyResponse:
    decision_id = f"pd-{uuid.uuid4().hex[:8]}"
    rules = _ACTION_TO_RULES.get(req.action_type, [])

    if not rules:
        # No rules → AUTO_DENY (unknown action type)
        return PolicyResponse(
            decision_id=decision_id,
            task_id=req.task_id,
            action_type=req.action_type,
            decision="AUTO_DENY",
            rule_id=None,
            rationale=f"No policy rules found for action type '{req.action_type}'",
        )

    # Evaluate rules in order of priority
    for rule in rules:
        cond = rule.get("condition", {})
        
        # Check condition if present
        if "context" in cond:
            req_context = req.action_payload.get("context")
            if req_context != cond["context"]:
                continue
                
        return PolicyResponse(
            decision_id=decision_id,
            task_id=req.task_id,
            action_type=req.action_type,
            decision=rule["decision"],
            rule_id=rule["rule_id"],
            rationale=f"Matched rule '{rule['rule_id']}': {rule['decision']}",
        )
        
    return PolicyResponse(
        decision_id=decision_id,
        task_id=req.task_id,
        action_type=req.action_type,
        decision="AUTO_DENY",
        rule_id=None,
        rationale=f"No matching policy rule found for payload in action type '{req.action_type}'",
    )


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Policy Engine", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.post("/evaluate", response_model=PolicyResponse)
async def evaluate(req: PolicyRequest) -> PolicyResponse:
    decision = _evaluate(req)

    # Emit policy decision event
    evt = EventEnvelope(
        type=EventType.POLICY_DECISION_MADE,
        source=_SERVICE_NAME,
        correlation_id=req.correlation_id,
        data={
            "decision_id": decision.decision_id,
            "task_id": req.task_id,
            "action_type": req.action_type,
            "decision": decision.decision,
            "rule_id": decision.rule_id,
        },
    )
    publish_event(KafkaTopic.ORCHESTRATION_EVENTS, evt)
    flush()

    logger.info("policy_decision_made", extra={
        "task_id": req.task_id,
        "action_type": req.action_type,
        "decision": decision.decision,
        "rule_id": decision.rule_id,
    })

    return decision


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8020, log_config=None)
