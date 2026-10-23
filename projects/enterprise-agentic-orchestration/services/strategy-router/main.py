"""
services/strategy-router/main.py
Strategy Router microservice.
Receives a task context and returns a structured routing decision:
  - USE_AIRFLOW_DAG for known, deterministic workflows
  - USE_REACT for ambiguous or multi-signal investigations
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "/app")

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "strategy-router")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

app = FastAPI(title="Strategy Router", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


# ── Request/Response schemas ──────────────────────────────────────────────────

class StrategyRequest(BaseModel):
    task_id: str
    correlation_id: str
    task_type: str
    priority: int
    context: dict


class StrategyResponse(BaseModel):
    task_id: str
    strategy: str           # USE_AIRFLOW_DAG | USE_REACT
    dag_id: str | None      # Populated when strategy == USE_AIRFLOW_DAG
    rationale: str
    rule_ids: list[str]
    confidence: float


# ── DAG routing table ─────────────────────────────────────────────────────────

_TASK_TYPE_TO_DAG: dict[str, str] = {
    "STOLEN_CARD":   "stolen_card_response_dag",
    "LARGE_TX":      "known_fraud_containment_dag",
    "TRAVEL_TX":     "travel_clearance_dag",
}


def _route(req: StrategyRequest) -> StrategyResponse:
    context = req.context
    rules_matched: list[str] = []

    # Rule 1: Known task type with pre-defined DAG
    dag_id = _TASK_TYPE_TO_DAG.get(req.task_type)
    if dag_id:
        rules_matched.append("R01-known-task-type-dag")
        return StrategyResponse(
            task_id=req.task_id,
            strategy="USE_AIRFLOW_DAG",
            dag_id=dag_id,
            rationale=f"Task type '{req.task_type}' maps to deterministic DAG '{dag_id}'.",
            rule_ids=rules_matched,
            confidence=0.95,
        )

    # Rule 2: Multiple mixed signals → ReAct
    has_phone_change = "phone" in str(context).lower()
    has_new_device = "device" in str(context).lower()
    has_suspicious_tx = "risk_score" in context

    ambiguity_count = sum([has_phone_change, has_new_device, has_suspicious_tx])
    if ambiguity_count >= 2:
        rules_matched.append("R02-multi-signal-ambiguity")
        return StrategyResponse(
            task_id=req.task_id,
            strategy="USE_REACT",
            dag_id=None,
            rationale=f"Multiple ambiguous signals detected ({ambiguity_count}). ReAct investigation required.",
            rule_ids=rules_matched,
            confidence=0.85,
        )

    # Rule 3: High risk score → ReAct for deeper investigation
    risk_score = int(context.get("risk_score", 0))
    if risk_score >= 70:
        rules_matched.append("R03-high-risk-score-react")
        return StrategyResponse(
            task_id=req.task_id,
            strategy="USE_REACT",
            dag_id=None,
            rationale=f"Risk score {risk_score} exceeds threshold. ReAct investigation required.",
            rule_ids=rules_matched,
            confidence=0.80,
        )

    # Rule 4: Default → general fraud containment DAG
    rules_matched.append("R04-default-dag")
    return StrategyResponse(
        task_id=req.task_id,
        strategy="USE_AIRFLOW_DAG",
        dag_id="known_fraud_containment_dag",
        rationale="No specific ambiguity detected. Using default fraud containment DAG.",
        rule_ids=rules_matched,
        confidence=0.70,
    )


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.post("/route", response_model=StrategyResponse)
async def route(req: StrategyRequest) -> StrategyResponse:
    decision = _route(req)
    logger.info(
        "strategy_decision",
        extra={
            "task_id": req.task_id,
            "strategy": decision.strategy,
            "dag_id": decision.dag_id,
            "rule_ids": decision.rule_ids,
            "confidence": decision.confidence,
        },
    )
    return decision


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8004, log_config=None)
