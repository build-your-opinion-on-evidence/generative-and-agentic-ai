"""
services/integration-orchestrator/models.py
SQLAlchemy ORM models for saga state tracking.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from database import Base


def _now():
    return datetime.now(timezone.utc)


class SagaInstance(Base):
    __tablename__ = "saga_instances"

    saga_id = Column(String, primary_key=True)
    task_id = Column(String, nullable=False, unique=True)
    correlation_id = Column(String, nullable=False)
    status = Column(String, nullable=False, default="PENDING")
    strategy = Column(String)
    dag_run_id = Column(String)
    current_step = Column(String)
    context = Column(JSONB, nullable=False, default=dict)
    timeout_at = Column(DateTime(timezone=True))
    version = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    checkpoints = relationship("SagaCheckpoint", back_populates="saga")


class SagaCheckpoint(Base):
    __tablename__ = "saga_checkpoints"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    saga_id = Column(String, ForeignKey("saga_instances.saga_id"), nullable=False)
    step_name = Column(String, nullable=False)
    step_status = Column(String, nullable=False)
    payload = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    saga = relationship("SagaInstance", back_populates="checkpoints")


class OrchestrationTraceEvent(Base):
    __tablename__ = "orchestration_trace_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    correlation_id = Column(String, nullable=False, index=True)
    sequence_number = Column(Integer, nullable=False)
    event_type = Column(String, nullable=False)
    source = Column(String, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False, default=_now)
    status = Column(String, nullable=False, default="COMPLETED")
    data = Column(JSONB, nullable=False, default=dict)
