"""
services/task-manager/models.py
SQLAlchemy ORM models for the task manager schema.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from database import Base


def _now():
    return datetime.now(timezone.utc)


class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(String, primary_key=True)
    correlation_id = Column(String, nullable=False)
    status = Column(String, nullable=False, default="RECEIVED")
    priority = Column(Integer, nullable=False, default=50)
    task_type = Column(String, nullable=False)
    source_event_id = Column(String)
    subject = Column(String)
    context = Column(JSONB, nullable=False, default=dict)
    version = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    history = relationship("TaskEvent", back_populates="task")


class TaskEvent(Base):
    __tablename__ = "task_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    task_id = Column(String, ForeignKey("tasks.task_id"), nullable=False)
    from_status = Column(String)
    to_status = Column(String, nullable=False)
    actor = Column(String)
    reason = Column(Text)
    event_metadata = Column("metadata", JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), nullable=False, default=_now)

    task = relationship("Task", back_populates="history")


class TaskDedupKey(Base):
    __tablename__ = "task_dedup_keys"

    event_id = Column(String, primary_key=True)
    task_id = Column(String, nullable=False)
    received_at = Column(DateTime(timezone=True), nullable=False, default=_now)
