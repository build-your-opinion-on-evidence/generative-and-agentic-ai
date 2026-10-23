"""
services/integration-orchestrator/main.py
Integration Orchestrator microservice.

Consumes FraudTaskReady → calls Strategy Router → triggers Airflow DAG or
ReAct service → tracks saga state → handles approvals → publishes completion.
"""
from __future__ import annotations

import os
import sys
import threading
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

import httpx
import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, Base, engine, get_db
from models import SagaCheckpoint, SagaInstance

from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.consumer import BaseKafkaConsumer
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "integration-orchestrator")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

_STRATEGY_ROUTER_URL = os.environ.get("STRATEGY_ROUTER_URL", "http://strategy-router:8004")
_AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
_AIRFLOW_USER = os.environ.get("AIRFLOW_USERNAME", "admin")
_AIRFLOW_PASS = os.environ.get("AIRFLOW_PASSWORD", "admin123")
_REACT_URL = os.environ.get("REACT_ORCHESTRATOR_URL", "http://react-orchestrator:8005")


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Airflow client ────────────────────────────────────────────────────────────

async def trigger_airflow_dag(dag_id: str, task_id: str, context: dict) -> str:
    """Trigger an Airflow DAG run. Returns the dag_run_id."""
    dag_run_id = f"fraud-{task_id}-{uuid.uuid4().hex[:6]}"
    payload = {
        "dag_run_id": dag_run_id,
        "conf": {"task_id": task_id, **context},
    }
    async with httpx.AsyncClient(auth=(_AIRFLOW_USER, _AIRFLOW_PASS), timeout=30) as client:
        resp = await client.post(
            f"{_AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns",
            json=payload,
        )
        resp.raise_for_status()
    logger.info("airflow_dag_triggered", extra={"dag_id": dag_id, "dag_run_id": dag_run_id, "task_id": task_id})
    return dag_run_id


async def trigger_react(task_id: str, correlation_id: str, context: dict) -> None:
    """Send task to the ReAct orchestration service."""
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{_REACT_URL}/investigate",
            json={
                "task_id": task_id,
                "correlation_id": correlation_id,
                "context": context,
            },
        )
        resp.raise_for_status()
    logger.info("react_investigation_triggered", extra={"task_id": task_id})


# ── Saga orchestration ────────────────────────────────────────────────────────

async def orchestrate_task(task_id: str, correlation_id: str, task_type: str, priority: int, context: dict) -> None:
    async with AsyncSessionLocal() as session:
        # Check for existing saga (idempotency)
        existing = await session.execute(
            select(SagaInstance).where(SagaInstance.task_id == task_id)
        )
        if existing.scalars().first():
            logger.info("saga_already_exists", extra={"task_id": task_id})
            return

        saga_id = f"saga-{uuid.uuid4().hex[:12]}"
        saga = SagaInstance(
            saga_id=saga_id,
            task_id=task_id,
            correlation_id=correlation_id,
            status="ROUTING",
            context=context,
            timeout_at=_now() + timedelta(minutes=30),
        )
        session.add(saga)
        await session.flush()

        # Checkpoint: STARTED
        session.add(SagaCheckpoint(
            saga_id=saga_id,
            step_name="ORCHESTRATION_STARTED",
            step_status="COMPLETED",
            payload={"task_id": task_id},
        ))
        await session.commit()

    # Call Strategy Router
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{_STRATEGY_ROUTER_URL}/route",
                json={
                    "task_id": task_id,
                    "correlation_id": correlation_id,
                    "task_type": task_type,
                    "priority": priority,
                    "context": context,
                },
            )
            resp.raise_for_status()
            decision = resp.json()
    except Exception as exc:
        logger.error("strategy_router_failed", extra={"task_id": task_id, "error": str(exc)})
        await _fail_saga(saga_id, str(exc))
        return

    strategy = decision["strategy"]
    dag_id = decision.get("dag_id")

    # Publish StrategySelected event
    _publish_event(
        EventType.STRATEGY_SELECTED,
        correlation_id,
        {
            "task_id": task_id,
            "strategy": strategy,
            "dag_id": dag_id,
            "rule_ids": decision.get("rule_ids", []),
            "rationale": decision.get("rationale", ""),
        },
    )

    # Update saga with strategy
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(SagaInstance).where(SagaInstance.saga_id == saga_id))
        saga = result.scalars().first()
        if saga:
            ctx = dict(saga.context or {})
            ctx["rationale"] = decision.get("rationale", "")
            saga.context = ctx
            saga.strategy = strategy
            saga.status = "EXECUTING"
            saga.updated_at = _now()
            await session.commit()
        session.add(SagaCheckpoint(
            saga_id=saga_id,
            step_name="STRATEGY_SELECTED",
            step_status="COMPLETED",
            payload=decision,
        ))
        await session.commit()

    # Execute strategy
    try:
        if strategy == "USE_AIRFLOW_DAG":
            dag_run_id = await trigger_airflow_dag(dag_id, task_id, context)
            _publish_event(EventType.DAG_TRIGGERED, correlation_id, {
                "task_id": task_id,
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
            })
            async with AsyncSessionLocal() as session:
                await session.execute(
                    update(SagaInstance)
                    .where(SagaInstance.saga_id == saga_id)
                    .values(dag_run_id=dag_run_id, current_step="DAG_RUNNING", updated_at=_now())
                )
                session.add(SagaCheckpoint(
                    saga_id=saga_id,
                    step_name="DAG_TRIGGERED",
                    step_status="COMPLETED",
                    payload={"dag_run_id": dag_run_id},
                ))
                await session.commit()

        elif strategy == "USE_REACT":
            _publish_event(EventType.REACT_STARTED, correlation_id, {"task_id": task_id})
            await trigger_react(task_id, correlation_id, context)

    except Exception as exc:
        logger.error("strategy_execution_failed", extra={"task_id": task_id, "error": str(exc)})
        await _fail_saga(saga_id, str(exc))


async def _fail_saga(saga_id: str, reason: str) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(SagaInstance).where(SagaInstance.saga_id == saga_id))
        saga = result.scalars().first()
        if saga:
            await session.execute(
                update(SagaInstance)
                .where(SagaInstance.saga_id == saga_id)
                .values(status="FAILED", updated_at=_now())
            )
            session.add(SagaCheckpoint(
                saga_id=saga_id,
                step_name="SAGA_FAILED",
                step_status="FAILED",
                payload={"reason": reason},
            ))
            _publish_event(EventType.FRAUD_TASK_FAILED, saga.correlation_id, {
                "task_id": saga.task_id,
                "reason": reason,
            })
            await session.commit()


def _publish_event(event_type: str, correlation_id: str, data: dict) -> None:
    evt = EventEnvelope(
        type=event_type,
        source=_SERVICE_NAME,
        correlation_id=correlation_id,
        data=data,
    )
    topic = (
        KafkaTopic.ORCHESTRATION_EVENTS
        if event_type in (EventType.STRATEGY_SELECTED, EventType.DAG_TRIGGERED, EventType.REACT_STARTED)
        else KafkaTopic.TASK_EVENTS
    )
    publish_event(topic, evt)
    flush()


# ── Kafka Consumer ────────────────────────────────────────────────────────────

class OrchestratorConsumer(BaseKafkaConsumer):
    def __init__(self):
        super().__init__(
            topics=[KafkaTopic.TASK_EVENTS, KafkaTopic.APPROVAL_EVENTS, KafkaTopic.ORCHESTRATION_EVENTS],
            group_id="integration-orchestrator-group",
            service_name=_SERVICE_NAME,
        )

    def handle(self, envelope: EventEnvelope) -> None:
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._async_handle(envelope))
        finally:
            loop.close()

    
    async def _async_handle(self, envelope: EventEnvelope) -> None:
        # Save trace event if it's one of the ReAct trace events
        trace_events = [
            "AgentDiscoveryRequested", "InvestigationGoalCreated", "NextStepSelected",
            "AgentSelected", "AgentInvocationStarted", "AgentInvocationCompleted",
            "ObservationReceived", "AgentInvocationFailed", "InvestigationFailed"
        ]
        if envelope.type in trace_events:
            async with AsyncSessionLocal() as session:
                from models import OrchestrationTraceEvent
                trace_event = OrchestrationTraceEvent(
                    correlation_id=envelope.correlation_id,
                    sequence_number=envelope.data.get("sequence_number", 0),
                    event_type=envelope.type,
                    source=envelope.source,
                    status="COMPLETED",
                    data=envelope.data
                )
                session.add(trace_event)
                await session.commit()
                
        if envelope.type == EventType.FRAUD_TASK_READY:

            data = envelope.data
            await orchestrate_task(
                task_id=data["task_id"],
                correlation_id=data["correlation_id"],
                task_type=data.get("task_type", "FRAUD_INVESTIGATION"),
                priority=data.get("priority", 50),
                context=data.get("context", {}),
            )
        elif envelope.type == EventType.APPROVAL_REQUESTED:
            await self._handle_approval_requested(envelope)
        elif envelope.type == EventType.APPROVAL_GRANTED:
            await self._handle_approval_granted(envelope)
        elif envelope.type == EventType.APPROVAL_REJECTED:
            await self._handle_approval_rejected(envelope)
        elif envelope.type == EventType.DAG_COMPLETED:
            await self._handle_dag_completed(envelope)
        elif envelope.type == EventType.REACT_COMPLETED:
            await self._handle_react_completed(envelope)

    async def _handle_approval_requested(self, envelope: EventEnvelope) -> None:
        task_id = envelope.data.get("task_id")
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(SagaInstance).where(SagaInstance.task_id == task_id))
            saga = result.scalars().first()
            if saga:
                await session.execute(update(SagaInstance).where(SagaInstance.saga_id == saga.saga_id).values(status="AWAITING_APPROVAL", updated_at=_now()))
                await session.commit()

    async def _handle_approval_granted(self, envelope: EventEnvelope) -> None:
        task_id = envelope.data.get("task_id")
        logger.info("approval_granted_received", extra={"task_id": task_id})
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(SagaInstance).where(SagaInstance.task_id == task_id))
            saga = result.scalars().first()
            if saga:
                await session.execute(update(SagaInstance).where(SagaInstance.saga_id == saga.saga_id).values(status="APPROVED", updated_at=_now()))
                await session.commit()

    async def _handle_approval_rejected(self, envelope: EventEnvelope) -> None:
        task_id = envelope.data.get("task_id")
        logger.info("approval_rejected_received", extra={"task_id": task_id})
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(SagaInstance).where(SagaInstance.task_id == task_id))
            saga = result.scalars().first()
            if saga:
                await session.execute(update(SagaInstance).where(SagaInstance.saga_id == saga.saga_id).values(status="REJECTED", updated_at=_now()))
                await session.commit()

    async def _handle_dag_completed(self, envelope: EventEnvelope) -> None:
        task_id = envelope.data.get("task_id")
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SagaInstance).where(SagaInstance.task_id == task_id)
            )
            saga = result.scalars().first()
            if saga:
                if saga.status not in ["AWAITING_APPROVAL", "APPROVED", "REJECTED"]:
                    await session.execute(
                        update(SagaInstance)
                        .where(SagaInstance.saga_id == saga.saga_id)
                        .values(status="COMPLETED", updated_at=_now())
                    )
                await session.commit()
        _publish_event(EventType.FRAUD_INVESTIGATION_COMPLETED, envelope.correlation_id, {
            "task_id": task_id,
            "strategy": "DAG",
        })

    async def _handle_react_completed(self, envelope: EventEnvelope) -> None:
        task_id = envelope.data.get("task_id")
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SagaInstance).where(SagaInstance.task_id == task_id)
            )
            saga = result.scalars().first()
            if saga:
                if saga.status not in ["AWAITING_APPROVAL", "APPROVED", "REJECTED"]:
                    await session.execute(
                        update(SagaInstance)
                        .where(SagaInstance.saga_id == saga.saga_id)
                        .values(status="COMPLETED", updated_at=_now())
                    )
                await session.commit()
        _publish_event(EventType.FRAUD_INVESTIGATION_COMPLETED, envelope.correlation_id, {
            "task_id": task_id,
            "strategy": "REACT",
        })



# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Integration Orchestrator", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Start Kafka consumer in background
    from shared.kafka.consumer import BaseKafkaConsumer
    # Consumer is started below
    consumer = OrchestratorConsumer()
    t = threading.Thread(target=consumer.run, daemon=True)
    t.start()

@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}

@app.get("/platform-health")
async def platform_health():
    services = {
        "kafka": "healthy",
        "event_simulator": "healthy",
        "transaction_monitor": "healthy",
        "task_manager": "healthy",
        "strategy_router": "healthy",
        "airflow": "healthy",
        "react_orchestrator": "healthy",
        "agent_discovery": "healthy",
        "policy_engine": "healthy",
        "approval_service": "healthy",
        "fake_banking_api": "healthy"
    }
    # In a real app we would ping each, for demo we return 200
    return {
        "status": "healthy",
        "services": services,
        "checked_at": _now().isoformat()
    }

@app.get("/runs/{correlation_id:path}/tasks")
async def get_run_tasks(correlation_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(SagaInstance).where(SagaInstance.correlation_id == correlation_id).order_by(SagaInstance.created_at.desc()))
        saga = result.scalars().first()
        if not saga or not saga.dag_run_id:
            return {"tasks": []}
            
        try:
            # Get actual dag_id from the STRATEGY_SELECTED checkpoint
            cp_result = await session.execute(
                select(SagaCheckpoint)
                .where(SagaCheckpoint.saga_id == saga.saga_id)
                .where(SagaCheckpoint.step_name == "STRATEGY_SELECTED")
            )
            checkpoint = cp_result.scalars().first()
            dag_id = checkpoint.payload.get("dag_id") if checkpoint else None
            if not dag_id:
                # Fallback: try context
                dag_id = saga.context.get("dag_id") if isinstance(saga.context, dict) else None
            if not dag_id:
                return {"tasks": []}
            
            async with httpx.AsyncClient(auth=(_AIRFLOW_USER, _AIRFLOW_PASS), timeout=10) as client:
                resp = await client.get(
                    f"{_AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns/{saga.dag_run_id}/taskInstances"
                )
                if resp.status_code == 200:
                    tasks = resp.json().get("task_instances", [])
                    return {"tasks": [{"task_id": t["task_id"], "state": t["state"]} for t in tasks]}
                return {"tasks": []}
        except Exception as exc:
            logger.error("airflow_tasks_error", extra={"error": str(exc)})
            return {"tasks": []}

@app.get("/runs/{correlation_id:path}/trace")
async def get_trace(correlation_id: str, after_sequence: int = 0):
    async with AsyncSessionLocal() as session:
        from models import OrchestrationTraceEvent
        result = await session.execute(
            select(OrchestrationTraceEvent)
            .where(OrchestrationTraceEvent.correlation_id == correlation_id)
            .where(OrchestrationTraceEvent.sequence_number > after_sequence)
            .order_by(OrchestrationTraceEvent.sequence_number.asc())
        )
        events = result.scalars().all()
        return {
            "run_id": "run-dynamic",
            "correlation_id": correlation_id,
            "events": [
                {
                    "sequence_number": e.sequence_number,
                    "event_type": e.event_type,
                    "source": e.source,
                    "timestamp": e.timestamp.isoformat(),
                    "status": e.status,
                    "data": e.data
                }
                for e in events
            ]
        }

@app.get("/runs/{correlation_id:path}")
async def get_run(correlation_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(SagaInstance).where(SagaInstance.correlation_id == correlation_id).order_by(SagaInstance.created_at.desc()))
        saga = result.scalars().first()
        if not saga:
            raise HTTPException(status_code=404, detail="Saga not found")
        
        return {
            "run_id": saga.saga_id,
            "correlation_id": saga.correlation_id,
            "overall_status": saga.status,
            "task": {
                "task_id": saga.task_id,
                "status": "IN_PROGRESS" if saga.status not in ["COMPLETED", "FAILED", "REJECTED"] else saga.status
            },
            "routing": {
                "strategy": saga.strategy,
                "rationale": saga.context.get("rationale", "") if isinstance(saga.context, dict) else "",
                "selected_at": saga.updated_at.isoformat()
            },
            "react": {
                "status": saga.status,
            },
            "created_at": saga.created_at.isoformat(),
            "updated_at": saga.updated_at.isoformat()
        }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8003, log_config=None)

