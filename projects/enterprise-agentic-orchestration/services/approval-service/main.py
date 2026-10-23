"""
services/approval-service/main.py
Human Approval Service.
Creates approval requests, supports approve/reject/expire.
Publishes resume events when approved.
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app")

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import Column, DateTime, String, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.consumer import BaseKafkaConsumer
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "approval-service")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
import threading
logger = logging.getLogger(_SERVICE_NAME)

# ── DB ────────────────────────────────────────────────────────────────────────
_HOST = os.environ.get("APPROVAL_DB_HOST", "localhost")
_PORT = os.environ.get("APPROVAL_DB_PORT", "5432")
_NAME = os.environ.get("APPROVAL_DB_NAME", "approval_db")
_USER = os.environ.get("POSTGRES_USER", "fraud_user")
_PASS = os.environ.get("POSTGRES_PASSWORD", "")

DATABASE_URL = f"postgresql+asyncpg://{_USER}:{_PASS}@{_HOST}:{_PORT}/{_NAME}"
from sqlalchemy.pool import NullPool
engine = create_async_engine(DATABASE_URL, echo=False, poolclass=NullPool)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _now():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class ApprovalRequest(Base):
    __tablename__ = "approval_requests"

    approval_id = Column(String, primary_key=True)
    task_id = Column(String, nullable=False)
    saga_id = Column(String)
    correlation_id = Column(String, nullable=False)
    action_type = Column(String, nullable=False)
    action_payload = Column(JSONB, nullable=False, default=dict)
    requested_by = Column(String, nullable=False)
    status = Column(String, nullable=False, default="PENDING")
    reviewer_id = Column(String)
    reviewer_note = Column(String)
    expires_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    decided_at = Column(DateTime(timezone=True))


async def get_db():
    """Yield an async database session for FastAPI dependency injection."""
    async with AsyncSessionLocal() as session:
        yield session


# ── Kafka consumer (listens for ApprovalRequested events) ────────────────────

class ApprovalConsumer(BaseKafkaConsumer):
    def __init__(self):
        super().__init__(
            topics=[KafkaTopic.APPROVAL_EVENTS],
            group_id="approval-service-group",
            service_name=_SERVICE_NAME,
        )

    def handle(self, envelope: EventEnvelope) -> None:
        if envelope.type != EventType.APPROVAL_REQUESTED:
            return
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._create_approval(envelope))
        finally:
            loop.close()

    async def _create_approval(self, envelope: EventEnvelope) -> None:
        data = envelope.data
        async with AsyncSessionLocal() as session:
            req = ApprovalRequest(
                approval_id=f"apr-{uuid.uuid4().hex[:8]}",
                task_id=data.get("task_id", ""),
                correlation_id=envelope.correlation_id,
                action_type=data.get("action_type", ""),
                action_payload=data.get("action_payload", {}),
                requested_by=data.get("requested_by", ""),
                expires_at=_now() + timedelta(hours=24),
            )
            session.add(req)
            await session.commit()
            logger.info("approval_request_created", extra={
                "approval_id": req.approval_id,
                "task_id": req.task_id,
                "action_type": req.action_type,
            })


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Approval Service", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    consumer = ApprovalConsumer()
    thread = threading.Thread(target=consumer.run, daemon=True, name="kafka-consumer")
    thread.start()
    logger.info("approval_service_started")


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}



@app.get("/approvals")
async def get_approvals(correlation_id: str = None, db: AsyncSession = Depends(get_db)):
    query = select(ApprovalRequest)
    if correlation_id:
        query = query.where(ApprovalRequest.correlation_id == correlation_id)
    result = await db.execute(query)
    requests = result.scalars().all()
    return [
        {
            "approval_id": r.approval_id,
            "task_id": r.task_id,
            "action_type": r.action_type,
            "action_payload": r.action_payload,
            "requested_by": r.requested_by,
            "status": r.status,
            "created_at": r.created_at,
            "expires_at": r.expires_at,
        }
        for r in requests
    ]




class ApprovalDecisionRequest(BaseModel):
    decision: str  # "APPROVE" or "REJECT"
    reviewer_id: str
    reviewer_note: str = ""

@app.post("/approvals/{approval_id}/decision")
async def make_decision(approval_id: str, body: ApprovalDecisionRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ApprovalRequest).where(ApprovalRequest.approval_id == approval_id))
    req = result.scalars().first()
    if not req:
        raise HTTPException(status_code=404, detail="Approval request not found")
    if req.status != "PENDING":
        raise HTTPException(status_code=409, detail=f"Request is already {req.status}")

    if body.decision == "APPROVE":
        req.status = "APPROVED"
        evt_type = EventType.APPROVAL_GRANTED
    elif body.decision == "REJECT":
        req.status = "REJECTED"
        evt_type = EventType.APPROVAL_REJECTED
    else:
        raise HTTPException(status_code=400, detail="Decision must be APPROVE or REJECT")

    req.reviewer_id = body.reviewer_id
    req.reviewer_note = body.reviewer_note
    req.decided_at = _now()
    await db.commit()

    evt = EventEnvelope(
        type=evt_type,
        source=_SERVICE_NAME,
        correlation_id=req.correlation_id,
        data={
            "approval_id": approval_id,
            "task_id": req.task_id,
            "action_type": req.action_type,
            "action_payload": req.action_payload,
            "reviewer_id": body.reviewer_id,
            "reason": body.reviewer_note,
        },
    )
    publish_event(KafkaTopic.APPROVAL_EVENTS, evt)
    flush()
    logger.info(f"approval_{body.decision.lower()}", extra={"approval_id": approval_id})
    return {"status": req.status, "approval_id": approval_id}



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8021, log_config=None)
