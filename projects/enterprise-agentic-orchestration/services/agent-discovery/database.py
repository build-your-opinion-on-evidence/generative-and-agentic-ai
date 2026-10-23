"""
services/agent-discovery/database.py
Async SQLAlchemy connection for agent_registry_db.
"""
from __future__ import annotations

import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

_HOST = os.environ.get("AGENT_REGISTRY_DB_HOST", "localhost")
_PORT = os.environ.get("AGENT_REGISTRY_DB_PORT", "5432")
_NAME = os.environ.get("AGENT_REGISTRY_DB_NAME", "agent_registry_db")
_USER = os.environ.get("POSTGRES_USER", "fraud_user")
_PASS = os.environ.get("POSTGRES_PASSWORD", "")

DATABASE_URL = f"postgresql+asyncpg://{_USER}:{_PASS}@{_HOST}:{_PORT}/{_NAME}"

from sqlalchemy.pool import NullPool

engine = create_async_engine(DATABASE_URL, echo=False, poolclass=NullPool)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    """Yield an async database session for FastAPI dependency injection."""
    async with AsyncSessionLocal() as session:
        yield session
