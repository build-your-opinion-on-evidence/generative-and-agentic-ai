"""
services/fake-banking-api/database.py
Async SQLAlchemy database connection for the banking core.
"""
from __future__ import annotations

import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

_DB_HOST = os.environ.get("BANKING_DB_HOST", "localhost")
_DB_PORT = os.environ.get("BANKING_DB_PORT", "5432")
_DB_NAME = os.environ.get("BANKING_DB_NAME", "banking_core_db")
_DB_USER = os.environ.get("POSTGRES_USER", "fraud_user")
_DB_PASS = os.environ.get("POSTGRES_PASSWORD", "")

DATABASE_URL = (
    f"postgresql+asyncpg://{_DB_USER}:{_DB_PASS}"
    f"@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}"
)

from sqlalchemy.pool import NullPool

engine = create_async_engine(DATABASE_URL, echo=False, poolclass=NullPool)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db():
    """Yield an async database session for FastAPI dependency injection."""
    async with AsyncSessionLocal() as session:
        yield session
