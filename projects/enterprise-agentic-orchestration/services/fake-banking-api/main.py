"""
services/fake-banking-api/main.py
Fake Banking API — FastAPI service exposing read/write endpoints for the
synthetic banking core database.  Publishes Kafka events on card/account
state changes.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "/app")

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import Base, engine, get_db
from models import Account, Card, Customer, Device, LoginEvent, Transaction
from seed import seed_if_empty

# ---------------------------------------------------------------------------
# Observability bootstrap
# ---------------------------------------------------------------------------
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "fake-banking-api")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging

logger = logging.getLogger(_SERVICE_NAME)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Fake Banking API", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    if os.environ.get("SEED_AUTO", "true").lower() == "true":
        async with AsyncSession(engine) as session:
            await seed_if_empty(session)
    logger.info("fake_banking_api_started")


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


# ── Customers ────────────────────────────────────────────────────────────────

@app.get("/customers/{customer_id}")
async def get_customer(customer_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Customer).where(Customer.customer_id == customer_id))
    customer = result.scalars().first()
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {
        "customer_id": customer.customer_id,
        "full_name": customer.full_name,
        "risk_segment": customer.risk_segment,
        "home_country": customer.home_country,
        "phone_number": customer.phone_number[-4:].rjust(len(customer.phone_number), "*"),
        "phone_changed_at": customer.phone_changed_at,
        "created_at": customer.created_at,
    }


# ── Accounts ─────────────────────────────────────────────────────────────────

@app.get("/accounts/{customer_id}")
async def get_accounts(customer_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Account).where(Account.customer_id == customer_id)
    )
    accounts = result.scalars().all()
    return [
        {
            "account_id": a.account_id,
            "account_type": a.account_type,
            "currency": a.currency,
            "balance": float(a.balance),
            "status": a.status,
        }
        for a in accounts
    ]


# ── Cards ─────────────────────────────────────────────────────────────────────

@app.get("/cards/{card_id}")
async def get_card(card_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Card).where(Card.card_id == card_id))
    card = result.scalars().first()
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return {
        "card_id": card.card_id,
        "customer_id": card.customer_id,
        "masked_pan": card.masked_pan,
        "card_type": card.card_type,
        "status": card.status,
        "issued_at": card.issued_at,
    }


@app.get("/cards/by-customer/{customer_id}")
async def get_cards_by_customer(customer_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Card).where(Card.customer_id == customer_id))
    cards = result.scalars().all()
    return [
        {
            "card_id": c.card_id,
            "masked_pan": c.masked_pan,
            "card_type": c.card_type,
            "status": c.status,
        }
        for c in cards
    ]


class FreezeCardRequest(BaseModel):
    reason: str = "fraud_containment"


@app.post("/cards/{card_id}/freeze")
async def freeze_card(
    card_id: str,
    body: FreezeCardRequest,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(Card).where(Card.card_id == card_id))
    card = result.scalars().first()
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    if card.status == "FROZEN":
        return {"card_id": card_id, "status": "FROZEN", "message": "Already frozen"}
    await db.execute(
        update(Card).where(Card.card_id == card_id).values(status="FROZEN")
    )
    await db.commit()
    logger.info("card_frozen", extra={"card_id": card_id, "reason": body.reason})
    return {"card_id": card_id, "status": "FROZEN"}


@app.post("/cards/{card_id}/unfreeze")
async def unfreeze_card(card_id: str, db: AsyncSession = Depends(get_db)):
    await db.execute(
        update(Card).where(Card.card_id == card_id).values(status="ACTIVE")
    )
    await db.commit()
    logger.info("card_unfrozen", extra={"card_id": card_id})
    return {"card_id": card_id, "status": "ACTIVE"}


# ── Transactions ─────────────────────────────────────────────────────────────

@app.get("/transactions/{transaction_id}")
async def get_transaction(transaction_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Transaction).where(Transaction.transaction_id == transaction_id)
    )
    tx = result.scalars().first()
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return {
        "transaction_id": tx.transaction_id,
        "card_id": tx.card_id,
        "customer_id": tx.customer_id,
        "amount": float(tx.amount),
        "currency": tx.currency,
        "country": tx.country,
        "channel": tx.channel,
        "transaction_time": tx.transaction_time,
        "status": tx.status,
        "fraud_label": tx.fraud_label,
    }


@app.get("/transactions/by-card/{card_id}")
async def get_transactions_by_card(
    card_id: str,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Transaction)
        .where(Transaction.card_id == card_id)
        .order_by(Transaction.transaction_time.desc())
        .limit(limit)
    )
    txns = result.scalars().all()
    return [
        {
            "transaction_id": t.transaction_id,
            "amount": float(t.amount),
            "currency": t.currency,
            "country": t.country,
            "channel": t.channel,
            "transaction_time": t.transaction_time,
            "status": t.status,
            "fraud_label": t.fraud_label,
        }
        for t in txns
    ]


@app.get("/transactions/by-customer/{customer_id}")
async def get_transactions_by_customer(
    customer_id: str,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Transaction)
        .where(Transaction.customer_id == customer_id)
        .order_by(Transaction.transaction_time.desc())
        .limit(limit)
    )
    txns = result.scalars().all()
    return [
        {
            "transaction_id": t.transaction_id,
            "card_id": t.card_id,
            "amount": float(t.amount),
            "currency": t.currency,
            "country": t.country,
            "channel": t.channel,
            "transaction_time": t.transaction_time,
            "status": t.status,
            "fraud_label": t.fraud_label,
        }
        for t in txns
    ]


# ── Devices ──────────────────────────────────────────────────────────────────

@app.get("/devices/by-customer/{customer_id}")
async def get_devices_by_customer(customer_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Device).where(Device.customer_id == customer_id))
    devices = result.scalars().all()
    return [
        {
            "device_id": d.device_id,
            "trusted": d.trusted,
            "first_seen_at": d.first_seen_at,
            "last_seen_at": d.last_seen_at,
            "country": d.country,
        }
        for d in devices
    ]


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8001, log_config=None)
