"""
services/transaction-monitor/main.py
Consumes banking.business-events.v1, detects suspicious patterns,
and publishes SuspiciousTransactionDetected events.
"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, "/app")

import uvicorn
from fastapi import FastAPI

from shared.contracts.events import EventEnvelope, EventType, KafkaTopic
from shared.kafka.consumer import BaseKafkaConsumer
from shared.kafka.producer import flush, publish_event
from shared.observability.logging import configure_logging
from shared.observability.tracing import configure_tracing

_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "transaction-monitor")
configure_logging(_SERVICE_NAME)
configure_tracing(_SERVICE_NAME)

import logging
logger = logging.getLogger(_SERVICE_NAME)

# ── Fraud detection heuristics ────────────────────────────────────────────────
_LARGE_AMOUNT_EGP = 10_000
_HIGH_RISK_COUNTRIES = {"US", "GB", "DE", "CN", "RU", "NG"}


def _risk_score(data: dict) -> int:
    score = 0
    amount = float(data.get("amount", 0))
    country = data.get("country", "EG")
    channel = data.get("channel", "POS")

    if amount >= _LARGE_AMOUNT_EGP:
        score += 40
    elif amount >= 5000:
        score += 20

    if country in _HIGH_RISK_COUNTRIES:
        score += 30

    if channel == "ONLINE":
        score += 10

    return min(score, 100)


class TransactionMonitorConsumer(BaseKafkaConsumer):
    def handle(self, envelope: EventEnvelope) -> None:
        if envelope.type not in (
            EventType.LARGE_TRANSACTION_ATTEMPTED,
            "TransactionCompleted",
        ):
            return

        data = envelope.data
        score = _risk_score(data)

        if score >= 50:
            suspicious_evt = EventEnvelope(
                type=EventType.SUSPICIOUS_TRANSACTION_DETECTED,
                source=_SERVICE_NAME,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.id,
                trace_id=envelope.trace_id,
                data={
                    **data,
                    "risk_score": score,
                    "detection_reason": "transaction_monitor_heuristic",
                },
            )
            publish_event(KafkaTopic.BUSINESS_EVENTS, suspicious_evt)
            flush()
            logger.info(
                "suspicious_transaction_detected",
                extra={
                    "transaction_id": data.get("transaction_id"),
                    "risk_score": score,
                    "correlation_id": envelope.correlation_id,
                },
            )


app = FastAPI(title="Transaction Monitor", version="1.0.0")
from prometheus_fastapi_instrumentator import Instrumentator
Instrumentator().instrument(app).expose(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": _SERVICE_NAME}


@app.on_event("startup")
async def startup():
    consumer = TransactionMonitorConsumer(
        topics=[KafkaTopic.BUSINESS_EVENTS],
        group_id="transaction-monitor-group",
        service_name=_SERVICE_NAME,
    )
    thread = threading.Thread(target=consumer.run, daemon=True, name="kafka-consumer")
    thread.start()
    logger.info("transaction_monitor_started")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8031, log_config=None)
