"""
services/task-manager/main.py
Task Manager microservice.
Consumes business events, creates/merges tasks, manages lifecycle,
publishes task events to Kafka.
"""
from __future__ import annotations

import os
import sys
import threading
import uuid
from datetime import datetime, timezone

sys.path.insert(0, "/app")

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, Base, engine, get_db
from models import Task, TaskDedupKey, TaskEvent
from priority import compute_priority
from state_machine import TaskStatus, assert_transition

from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.consumer import BaseKafkaConsumer
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "task-manager")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

# ── Event types that trigger fraud task creation/merge ────────────────────────
_TRIGGER_EVENTS = {
    EventType.SUSPICIOUS_TRANSACTION_DETECTED,
    EventType.CARD_REPORTED_STOLEN,
    EventType.LARGE_TRANSACTION_ATTEMPTED,
    EventType.KNOWN_TRAVEL_PATTERN,
    EventType.CUSTOMER_CHANGED_PHONE_NUMBER,
    EventType.NEW_DEVICE_LOGIN_DETECTED,
}


def _task_type(event_type: str) -> str:
    if event_type == EventType.CARD_REPORTED_STOLEN:
        return "STOLEN_CARD"
    if event_type == EventType.LARGE_TRANSACTION_ATTEMPTED:
        return "LARGE_TX"
    if event_type == EventType.KNOWN_TRAVEL_PATTERN:
        return "TRAVEL_TX"
    if event_type == EventType.SUSPICIOUS_TRANSACTION_DETECTED:
        return "FRAUD_INVESTIGATION"
    return "UNKNOWN"


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Sync handler (runs in background thread with its own event loop) ──────────

class TaskManagerConsumer(BaseKafkaConsumer):
    """Kafka consumer for the Task Manager (runs in a daemon thread)."""

    def __init__(self):
        super().__init__(
            topics=[KafkaTopic.BUSINESS_EVENTS],
            group_id="task-manager-group",
            service_name=_SERVICE_NAME,
        )

    def should_process(self, envelope: EventEnvelope) -> bool:
        """Quick dedup check using a file-scoped in-memory set (thread-safe)."""
        return envelope.type in _TRIGGER_EVENTS

    def handle(self, envelope: EventEnvelope) -> None:
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._async_handle(envelope))
        finally:
            loop.close()

    async def _async_handle(self, envelope: EventEnvelope) -> None:
        async with AsyncSessionLocal() as session:
            # Idempotency: skip if event already processed
            existing_dedup = await session.execute(
                select(TaskDedupKey).where(TaskDedupKey.event_id == envelope.id)
            )
            if existing_dedup.scalars().first():
                logger.info("event_deduplicated", extra={"event_id": envelope.id})
                return

            # Try to find existing open task for this correlation_id
            existing_task = await session.execute(
                select(Task).where(
                    Task.correlation_id == envelope.correlation_id,
                    Task.status.notin_(["COMPLETED", "FAILED", "CANCELLED"]),
                )
            )
            task = existing_task.scalars().first()

            priority = compute_priority(envelope.type, envelope.data)

            if task:
                # Merge: update context with new signals, upgrade priority if higher
                new_context = {**task.context, **envelope.data}
                # Accumulate signal types for multi-signal detection
                existing_signals = task.context.get("signals", [])
                if task.context.get("_source_event_type"):
                    if task.context["_source_event_type"] not in existing_signals:
                        existing_signals.append(task.context["_source_event_type"])
                if envelope.type not in existing_signals:
                    existing_signals.append(envelope.type)
                new_context["signals"] = existing_signals
                new_context["signal_count"] = len(existing_signals)
                new_context["_source_event_type"] = envelope.type
                # Preserve key fields from earlier events
                if "phone" not in str(new_context) and "phone" in str(task.context):
                    new_context["has_phone_change"] = True
                if "device" not in str(new_context) and "device" in str(task.context):
                    new_context["has_new_device"] = True
                new_priority = max(task.priority, priority)
                new_type = task.task_type
                if new_type == "UNKNOWN":
                    new_type = _task_type(envelope.type)
                await session.execute(
                    update(Task)
                    .where(Task.task_id == task.task_id)
                    .values(
                        context=new_context,
                        priority=new_priority,
                        task_type=new_type,
                        updated_at=_now(),
                        version=Task.version + 1,
                    )
                )
                event_type = EventType.FRAUD_TASK_UPDATED
                logger.info("task_merged", extra={"task_id": task.task_id, "priority": new_priority})
            else:
                # Create new task
                task_id = f"task-{uuid.uuid4().hex[:12]}"
                initial_context = {**envelope.data, "_source_event_type": envelope.type, "signals": [envelope.type]}
                task = Task(
                    task_id=task_id,
                    correlation_id=envelope.correlation_id,
                    status=TaskStatus.RECEIVED,
                    priority=priority,
                    task_type=_task_type(envelope.type),
                    source_event_id=envelope.id,
                    subject=envelope.subject,
                    context=initial_context,
                )
                session.add(task)
                event_type = EventType.FRAUD_TASK_CREATED
                logger.info("task_created", extra={"task_id": task_id, "priority": priority})

            # Record dedup key
            session.add(TaskDedupKey(event_id=envelope.id, task_id=task.task_id))

            # Record history
            session.add(TaskEvent(
                task_id=task.task_id,
                from_status=None if event_type == EventType.FRAUD_TASK_CREATED else task.status,
                to_status=task.status,
                actor=_SERVICE_NAME,
                reason=f"triggered_by:{envelope.type}",
            ))

            await session.commit()

            # Advance to READY and publish if primary event has arrived
            # The current task object hasn't been refreshed if merged, but we can query it.
            # But we can just use _advance_to_ready, which queries it and checks task_type.
            # Let's just modify _advance_to_ready to enforce this.
            await self._advance_to_ready(task.task_id, event_type)

    async def _advance_to_ready(self, task_id: str, created_or_updated: str) -> None:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Task).where(Task.task_id == task_id))
            task = result.scalars().first()
            if not task:
                return

            if task.task_type == "UNKNOWN":
                logger.info("task_pending_primary_event", extra={"task_id": task_id})
                return

            try:
                assert_transition(task.status, TaskStatus.READY)
            except ValueError:
                return  # Already READY or in progress

            await session.execute(
                update(Task)
                .where(Task.task_id == task_id)
                .values(status=TaskStatus.READY, updated_at=_now(), version=Task.version + 1)
            )
            session.add(TaskEvent(
                task_id=task_id,
                from_status=task.status,
                to_status=TaskStatus.READY,
                actor=_SERVICE_NAME,
                reason="auto_advanced_to_ready",
            ))
            await session.commit()

            # Publish FraudTaskReady
            evt = EventEnvelope(
                type=EventType.FRAUD_TASK_READY,
                source=_SERVICE_NAME,
                correlation_id=task.correlation_id,
                data={
                    "task_id": task_id,
                    "correlation_id": task.correlation_id,
                    "priority": task.priority,
                    "task_type": task.task_type,
                    "context": task.context,
                },
            )
            publish_event(KafkaTopic.TASK_EVENTS, evt)
            flush()
            logger.info("fraud_task_ready_published", extra={"task_id": task_id})


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Task Manager", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    consumer = TaskManagerConsumer()
    thread = threading.Thread(target=consumer.run, daemon=True, name="kafka-consumer")
    thread.start()
    logger.info("task_manager_started")


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.get("/tasks/{task_id}")
async def get_task(task_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Task).where(Task.task_id == task_id))
    task = result.scalars().first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {
        "task_id": task.task_id,
        "correlation_id": task.correlation_id,
        "status": task.status,
        "priority": task.priority,
        "task_type": task.task_type,
        "context": task.context,
        "version": task.version,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


@app.get("/tasks")
async def list_tasks(status: str | None = None, limit: int = 50, db: AsyncSession = Depends(get_db)):
    query = select(Task).order_by(Task.priority.desc(), Task.created_at.asc()).limit(limit)
    if status:
        query = query.where(Task.status == status)
    result = await db.execute(query)
    tasks = result.scalars().all()
    return [
        {
            "task_id": t.task_id,
            "correlation_id": t.correlation_id,
            "status": t.status,
            "priority": t.priority,
            "task_type": t.task_type,
            "created_at": t.created_at,
        }
        for t in tasks
    ]


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8002, log_config=None)
