"""
services/event-simulator/main.py
Publishes scenario events to Kafka on demand and on a timer.
Exposes HTTP endpoints to trigger demo scenarios A-E.
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
import random
from datetime import datetime, timezone

sys.path.insert(0, "/app")

import uvicorn
from fastapi import FastAPI

from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "event-simulator")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

app = FastAPI(title="Event Simulator", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)

_BANKING_API = os.environ.get("FAKE_BANKING_API_URL", "http://fake-banking-api:8001")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cid(customer_id: str, card_id: str) -> str:
    return f"customer:{customer_id}/card:{card_id}"


def _publish(event_type: str, correlation_id: str, data: dict, source: str = "event-simulator", topic: str = KafkaTopic.BUSINESS_EVENTS) -> str:
    evt = EventEnvelope(
        type=event_type,
        source=source,
        correlation_id=correlation_id,
        data=data,
    )
    publish_event(topic, evt)
    flush()
    logger.info("event_published", extra={"event_type": event_type, "correlation_id": correlation_id, "event_id": evt.id})
    return evt.id


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.post("/scenarios/A", status_code=status.HTTP_202_ACCEPTED)
async def scenario_a(mode: str = "random"):
    """Scenario A: Legitimate Activity (Non-Fraudulent) process → Airflow DAG path."""
    customer_id = f"C{random.randint(1, 100):04d}"
    card_id = f"CARD-{customer_id}-{random.randint(1, 2)}"
    correlation_id = _cid(customer_id, card_id)

    is_fraud = random.choice([True, False])
    if mode == "fraud":
        is_fraud = True
    elif mode == "legit":
        is_fraud = False

    if is_fraud:
        _publish(EventType.CARD_REPORTED_STOLEN, correlation_id, {
            "customer_id": customer_id,
            "card_id": card_id,
            "reported_by": "customer",
            "channel": "mobile_app",
        })
        await asyncio.sleep(0.5)
        risk_score = random.randint(85, 100)
    else:
        risk_score = random.randint(5, 20)

    event_type = EventType.LARGE_TRANSACTION_ATTEMPTED if is_fraud else EventType.KNOWN_TRAVEL_PATTERN
    event_id = _publish(event_type, correlation_id, {
        "customer_id": customer_id,
        "card_id": card_id,
        "transaction_id": f"TX-SCENARIO-A-{uuid.uuid4().hex[:6].upper()}",
        "amount": random.randint(15000, 50000),
        "currency": random.choice(["EGP", "USD", "EUR"]),
        "country": random.choice(["US", "UK", "AE"]) if is_fraud else "EG",
        "risk_score": risk_score,
        "is_fraud": is_fraud
    })

    logger.info("scenario_A_published", extra={"customer_id": customer_id, "is_fraud": is_fraud})
    return {
        "run_id": f"run-{uuid.uuid4().hex[:8]}",
        "scenario_id": "A",
        "event_id": event_id,
        "correlation_id": correlation_id,
        "trace_id": f"trace-{uuid.uuid4().hex[:8]}",
        "status": "ACCEPTED",
        "submitted_at": _now(),
        "is_fraud": is_fraud
    }


@app.post("/scenarios/B", status_code=status.HTTP_202_ACCEPTED)
async def scenario_b(mode: str = "random"):
    """Scenario B: Ambiguous fraud investigation → ReAct path."""
    customer_id = f"C{random.randint(1, 100):04d}"
    card_id = f"CARD-{customer_id}-{random.randint(1, 2)}"
    device_id = f"DEV-{uuid.uuid4().hex[:8].upper()}"
    correlation_id = _cid(customer_id, card_id)
    
    is_fraud = random.choice([True, False])
    if mode == "fraud":
        is_fraud = True
    elif mode == "legit":
        is_fraud = False
    risk_score = random.randint(65, 85) if is_fraud else random.randint(5, 20)
    country = random.choice(["DE", "NL", "FR"]) if is_fraud else "EG"
    amount = random.randint(5000, 20000) if is_fraud else random.randint(100, 500)

    _publish(EventType.CUSTOMER_CHANGED_PHONE_NUMBER, correlation_id, {
        "customer_id": customer_id,
        "changed_at": _now(),
    })

    await asyncio.sleep(0.3)

    _publish(EventType.NEW_DEVICE_LOGIN_DETECTED, correlation_id, {
        "customer_id": customer_id,
        "device_id": device_id,
        "country": country,
        "login_time": _now(),
    })

    await asyncio.sleep(0.3)

    event_id = _publish(EventType.SUSPICIOUS_TRANSACTION_DETECTED, correlation_id, {
        "customer_id": customer_id,
        "card_id": card_id,
        "transaction_id": f"TX-SCENARIO-B-{uuid.uuid4().hex[:6].upper()}",
        "amount": amount,
        "currency": random.choice(["EGP", "SAR", "QAR"]),
        "country": country,
        "risk_score": risk_score,
        "is_fraud": is_fraud
    })

    logger.info("scenario_B_published", extra={"customer_id": customer_id, "is_fraud": is_fraud})
    return {
        "run_id": f"run-{uuid.uuid4().hex[:8]}",
        "scenario_id": "B",
        "event_id": event_id,
        "correlation_id": correlation_id,
        "trace_id": f"trace-{uuid.uuid4().hex[:8]}",
        "status": "ACCEPTED",
        "submitted_at": _now(),
        "is_fraud": is_fraud
    }


@app.post("/scenarios/C")
async def scenario_c():
    """Scenario C: Priority preemption."""
    # Low priority task first
    cid_low = _cid("C0010", "CARD-C0010-1")
    _publish(EventType.SUSPICIOUS_TRANSACTION_DETECTED, cid_low, {
        "customer_id": "C0010",
        "card_id": "CARD-C0010-1",
        "transaction_id": f"TX-LOW-{uuid.uuid4().hex[:6].upper()}",
        "amount": 500,
        "currency": "EGP",
        "country": "EG",
        "risk_score": 35,
    })

    await asyncio.sleep(1.0)

    # Critical task preempts it
    cid_critical = _cid("C0011", "CARD-C0011-1")
    _publish(EventType.CARD_REPORTED_STOLEN, cid_critical, {
        "customer_id": "C0011",
        "card_id": "CARD-C0011-1",
        "reported_by": "bank_monitoring",
    })

    return {"scenario": "C", "status": "published", "low_priority_correlation_id": cid_low, "critical_correlation_id": cid_critical}


@app.post("/scenarios/D")
async def scenario_d():
    """Scenario D: Idempotency — same event published twice."""
    customer_id = "C0020"
    card_id = "CARD-C0020-1"
    correlation_id = _cid(customer_id, card_id)
    tx_id = f"TX-IDEM-{uuid.uuid4().hex[:6].upper()}"

    for i in range(2):
        evt = EventEnvelope(
            id="evt-idempotency-test-fixed",  # Fixed ID — must be deduplicated
            type=EventType.SUSPICIOUS_TRANSACTION_DETECTED,
            source="event-simulator",
            correlation_id=correlation_id,
            data={
                "customer_id": customer_id,
                "card_id": card_id,
                "transaction_id": tx_id,
                "amount": 3000,
                "currency": "EGP",
                "country": "EG",
                "risk_score": 55,
            },
        )
        publish_event(KafkaTopic.BUSINESS_EVENTS, evt)
        flush()
        logger.info("idempotency_event_published", extra={"attempt": i + 1, "event_id": evt.id})

    return {"scenario": "D", "correlation_id": correlation_id, "status": "published_twice", "event_id": "evt-idempotency-test-fixed"}


@app.post("/scenarios/E")
async def scenario_e():
    """Scenario E: Failure and recovery."""
    customer_id = "C0030"
    card_id = "CARD-C0030-1"
    correlation_id = _cid(customer_id, card_id)

    _publish(EventType.SUSPICIOUS_TRANSACTION_DETECTED, correlation_id, {
        "customer_id": customer_id,
        "card_id": card_id,
        "transaction_id": f"TX-FAIL-{uuid.uuid4().hex[:6].upper()}",
        "amount": 8000,
        "currency": "EGP",
        "country": "SA",
        "risk_score": 78,
        "simulate_agent_timeout": True,   # Hint for agents to simulate failure
    })

    return {"scenario": "E", "correlation_id": correlation_id, "status": "published"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8030, log_config=None)
