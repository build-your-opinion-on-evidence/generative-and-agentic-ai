"""
services/agent-discovery/main.py
Agent Discovery service.
Provides a registry of all available specialized agents.
Auto-seeds the registry with known agent endpoints on startup.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, "/app")

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import AsyncSessionLocal, Base, engine, get_db
from models import AgentRegistryEntry

from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "agent-discovery")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

# ── Agent registry seed ───────────────────────────────────────────────────────

def _build_seed_entries() -> list[dict]:
    return [
        {
            "agent_id": "customer-profile-agent",
            "name": "Customer Profile Agent",
            "version": "1.0.0",
            "capabilities": ["customer_profile", "risk_assessment"],
            "endpoint": os.environ.get("CUSTOMER_PROFILE_AGENT_URL", "http://customer-profile-agent:8010"),
            "required_permissions": ["read_customer"],
            "timeout_seconds": 30,
            "prompt_version": "v1",
        },
        {
            "agent_id": "transaction-history-agent",
            "name": "Transaction History Agent",
            "version": "1.0.0",
            "capabilities": ["transaction_history", "pattern_analysis"],
            "endpoint": os.environ.get("TRANSACTION_HISTORY_AGENT_URL", "http://transaction-history-agent:8011"),
            "required_permissions": ["read_transactions"],
            "timeout_seconds": 30,
            "prompt_version": "v1",
        },
        {
            "agent_id": "device-risk-agent",
            "name": "Device Risk Agent",
            "version": "1.0.0",
            "capabilities": ["device_fingerprint", "risk_assessment"],
            "endpoint": os.environ.get("DEVICE_RISK_AGENT_URL", "http://device-risk-agent:8012"),
            "required_permissions": ["read_devices"],
            "timeout_seconds": 30,
            "prompt_version": "v1",
        },
        {
            "agent_id": "fraud-scoring-agent",
            "name": "Fraud Scoring Agent",
            "version": "1.0.0",
            "capabilities": ["fraud_scoring", "risk_assessment"],
            "endpoint": os.environ.get("FRAUD_SCORING_AGENT_URL", "http://fraud-scoring-agent:8013"),
            "required_permissions": ["read_customer", "read_transactions"],
            "timeout_seconds": 45,
            "prompt_version": "v1",
        },
        {
            "agent_id": "card-control-agent",
            "name": "Card Control Agent",
            "version": "1.0.0",
            "capabilities": ["card_freeze", "card_unfreeze"],
            "endpoint": os.environ.get("CARD_CONTROL_AGENT_URL", "http://card-control-agent:8014"),
            "required_permissions": ["write_card"],
            "timeout_seconds": 30,
            "prompt_version": "v1",
        },
        {
            "agent_id": "notification-agent",
            "name": "Notification Agent",
            "version": "1.0.0",
            "capabilities": ["customer_notification", "sms", "push"],
            "endpoint": os.environ.get("NOTIFICATION_AGENT_URL", "http://notification-agent:8015"),
            "required_permissions": ["read_customer", "send_notification"],
            "timeout_seconds": 30,
            "prompt_version": "v1",
        },
    ]


async def seed_registry(session: AsyncSession) -> None:
    """Insert known agents if not present."""
    for entry in _build_seed_entries():
        existing = await session.execute(
            select(AgentRegistryEntry).where(AgentRegistryEntry.agent_id == entry["agent_id"])
        )
        if not existing.scalars().first():
            session.add(AgentRegistryEntry(**entry))
    await session.commit()
    logger.info("agent_registry_seeded")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Agent Discovery", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with AsyncSessionLocal() as session:
        await seed_registry(session)
    logger.info("agent_discovery_started")


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.get("/agents")
async def list_agents(capability: str | None = None, db: AsyncSession = Depends(get_db)):
    query = select(AgentRegistryEntry).where(AgentRegistryEntry.enabled == True)
    result = await db.execute(query)
    agents = result.scalars().all()

    if capability:
        agents = [a for a in agents if capability in (a.capabilities or [])]

    return [
        {
            "agent_id": a.agent_id,
            "name": a.name,
            "version": a.version,
            "capabilities": a.capabilities,
            "endpoint": a.endpoint,
            "required_permissions": a.required_permissions,
            "timeout_seconds": a.timeout_seconds,
            "prompt_version": a.prompt_version,
            "enabled": a.enabled,
        }
        for a in agents
    ]


@app.get("/agents/{agent_id}")
async def get_agent(agent_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(AgentRegistryEntry).where(AgentRegistryEntry.agent_id == agent_id)
    )
    agent = result.scalars().first()
    if not agent:
        raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' not found")
    return {
        "agent_id": agent.agent_id,
        "name": agent.name,
        "version": agent.version,
        "capabilities": agent.capabilities,
        "endpoint": agent.endpoint,
        "required_permissions": agent.required_permissions,
        "timeout_seconds": agent.timeout_seconds,
        "prompt_version": agent.prompt_version,
        "enabled": agent.enabled,
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8006, log_config=None)
