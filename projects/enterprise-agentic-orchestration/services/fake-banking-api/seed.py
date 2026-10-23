"""
services/fake-banking-api/seed.py
Seeds the banking_core_db with synthetic (fake) data.
100 customers, 150 accounts, 200 cards, 10 000 transactions, 250 devices.
Includes known fraud scenarios for the 5 demo scenarios.
"""
from __future__ import annotations

import hashlib
import random
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from models import Account, Card, Customer, Device, LoginEvent, Transaction

_RISK_SEGMENTS = ["LOW", "STANDARD", "STANDARD", "HIGH", "CRITICAL"]
_COUNTRIES = ["EG", "EG", "EG", "EG", "SA", "AE", "US", "GB", "DE"]
_CHANNELS = ["POS", "POS", "ONLINE", "ONLINE", "ATM", "MOBILE"]
_CARD_TYPES = ["DEBIT", "DEBIT", "CREDIT", "PREPAID"]
_MERCHANTS = [f"MERCHANT-{i:04d}" for i in range(1, 500)]

_NOW = datetime.now(timezone.utc)


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def _hash(val: str) -> str:
    return hashlib.sha256(val.encode()).hexdigest()[:16]


def _past(days: int = 365, max_days: int = 0) -> datetime:
    delta = random.randint(max_days, days)
    return _NOW - timedelta(days=delta)


async def seed_if_empty(session: AsyncSession) -> None:
    """Seed data only if the customers table is empty."""
    count = await session.execute(text("SELECT COUNT(*) FROM customers"))
    if (count.scalar() or 0) > 0:
        return
    await _seed(session)
    await session.commit()


async def _seed(session: AsyncSession) -> None:
    customers = _make_customers(100)
    session.add_all(customers)
    await session.flush()

    accounts = _make_accounts(customers, target=150)
    session.add_all(accounts)
    await session.flush()

    cards = _make_cards(customers, accounts, target=200)
    session.add_all(cards)
    await session.flush()

    devices = _make_devices(customers, target=250)
    session.add_all(devices)
    await session.flush()

    login_events = _make_login_events(customers, devices, count=500)
    session.add_all(login_events)
    await session.flush()

    txns = _make_transactions(cards, count=9000)
    session.add_all(txns)
    await session.flush()

    # Known fraud scenarios
    fraud_txns = _make_fraud_scenarios(customers, cards, devices, session)
    session.add_all(fraud_txns)


def _make_customers(n: int) -> list[Customer]:
    result = []
    for i in range(n):
        cid = f"C{i+1:04d}"
        result.append(Customer(
            customer_id=cid,
            full_name=f"Customer Synthetic {i+1}",
            national_id_hash=_hash(f"NID-{i+1}"),
            risk_segment=random.choice(_RISK_SEGMENTS),
            home_country="EG",
            phone_number=f"+201{random.randint(0,2)}{random.randint(10000000,99999999)}",
            phone_changed_at=_past(30) if random.random() < 0.1 else None,
            created_at=_past(730),
        ))
    return result


def _make_accounts(customers: list[Customer], target: int) -> list[Account]:
    result = []
    for i, customer in enumerate(customers):
        n_accounts = 1 if i < 50 else 2
        for j in range(n_accounts):
            if len(result) >= target:
                break
            result.append(Account(
                account_id=f"ACC-{customer.customer_id}-{j+1}",
                customer_id=customer.customer_id,
                account_type=random.choice(["CURRENT", "SAVINGS"]),
                currency="EGP",
                balance=round(random.uniform(100, 500000), 2),
                status="ACTIVE",
            ))
    return result[:target]


def _make_cards(customers, accounts, target: int) -> list[Card]:
    result = []
    acc_by_customer: dict[str, list[Account]] = {}
    for acc in accounts:
        acc_by_customer.setdefault(acc.customer_id, []).append(acc)

    for customer in customers:
        if len(result) >= target:
            break
        for acc in acc_by_customer.get(customer.customer_id, []):
            if len(result) >= target:
                break
            pan_suffix = random.randint(1000, 9999)
            result.append(Card(
                card_id=f"CARD-{customer.customer_id}-{acc.account_id[-1]}",
                customer_id=customer.customer_id,
                account_id=acc.account_id,
                masked_pan=f"****-****-****-{pan_suffix}",
                card_type=random.choice(_CARD_TYPES),
                status="ACTIVE",
                issued_at=_past(730, 30),
            ))
    return result[:target]


def _make_devices(customers, target: int) -> list[Device]:
    result = []
    for customer in customers:
        n = random.randint(1, 3)
        for _ in range(n):
            if len(result) >= target:
                break
            did = _uid("DEV")
            result.append(Device(
                device_id=did,
                customer_id=customer.customer_id,
                fingerprint_hash=_hash(did),
                trusted=random.random() < 0.8,
                first_seen_at=_past(365),
                last_seen_at=_past(7),
                country=random.choice(["EG", "EG", "EG", "SA"]),
            ))
    return result[:target]


def _make_login_events(customers, devices, count: int) -> list[LoginEvent]:
    result = []
    dev_by_customer: dict[str, list[Device]] = {}
    for dev in devices:
        dev_by_customer.setdefault(dev.customer_id, []).append(dev)

    for _ in range(count):
        customer = random.choice(customers)
        devs = dev_by_customer.get(customer.customer_id, [None])
        dev = random.choice(devs)
        result.append(LoginEvent(
            login_id=_uid("LOGIN"),
            customer_id=customer.customer_id,
            device_id=dev.device_id if dev else None,
            login_time=_past(90),
            country=dev.country if dev else "EG",
            success=random.random() < 0.95,
        ))
    return result


def _make_transactions(cards: list[Card], count: int) -> list[Transaction]:
    result = []
    for _ in range(count):
        card = random.choice(cards)
        amount = round(random.uniform(10, 15000), 2)
        result.append(Transaction(
            transaction_id=_uid("TX"),
            card_id=card.card_id,
            customer_id=card.customer_id,
            merchant_id=random.choice(_MERCHANTS),
            amount=amount,
            currency="EGP",
            country=random.choice(_COUNTRIES),
            channel=random.choice(_CHANNELS),
            transaction_time=_past(180),
            status="APPROVED",
            fraud_label=False,
        ))
    return result


def _make_fraud_scenarios(
    customers: list[Customer],
    cards: list[Card],
    devices: list[Device],
    session: AsyncSession,
) -> list[Transaction]:
    """Create 1 000 labeled fraud transactions for known demo scenarios."""
    result = []
    fraud_customers = customers[:20]  # First 20 customers have fraud scenarios

    for customer in fraud_customers:
        customer_cards = [c for c in cards if c.customer_id == customer.customer_id]
        if not customer_cards:
            continue
        card = customer_cards[0]
        # High-amount foreign transaction — typical fraud pattern
        for _ in range(50):
            result.append(Transaction(
                transaction_id=_uid("TX-FRAUD"),
                card_id=card.card_id,
                customer_id=customer.customer_id,
                merchant_id=_uid("MERCH-FOREIGN"),
                amount=round(random.uniform(5000, 50000), 2),
                currency=random.choice(["USD", "EUR", "GBP"]),
                country=random.choice(["US", "GB", "DE", "RU", "CN"]),
                channel="ONLINE",
                transaction_time=_past(30),
                status=random.choice(["APPROVED", "DECLINED"]),
                fraud_label=True,
            ))
    return result
