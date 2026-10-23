"""
services/agent-discovery/models.py
Agent registry ORM model.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from database import Base


def _now():
    return datetime.now(timezone.utc)


class AgentRegistryEntry(Base):
    __tablename__ = "agent_registry"

    agent_id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    version = Column(String, nullable=False, default="1.0.0")
    capabilities = Column(ARRAY(String), nullable=False, default=list)
    endpoint = Column(String, nullable=False)
    required_permissions = Column(ARRAY(String), nullable=False, default=list)
    timeout_seconds = Column(Integer, nullable=False, default=30)
    enabled = Column(Boolean, nullable=False, default=True)
    prompt_version = Column(String, nullable=False, default="v1")
    input_schema = Column(JSONB, nullable=False, default=dict)
    output_schema = Column(JSONB, nullable=False, default=dict)
    last_health_check = Column(DateTime(timezone=True))
    registered_at = Column(DateTime(timezone=True), nullable=False, default=_now)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=_now)
