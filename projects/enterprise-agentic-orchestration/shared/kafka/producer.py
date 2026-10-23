"""
shared/kafka/producer.py
Shared Kafka producer base — idempotent, with OTel trace injection.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

from confluent_kafka import Producer, KafkaException
from opentelemetry import trace
from opentelemetry.propagate import inject

from shared.contracts.events import EventEnvelope

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


def _build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 200,
            "client.id": os.environ.get("OTEL_SERVICE_NAME", "unknown-service"),
        }
    )


_producer: Producer | None = None


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = _build_producer()
    return _producer


def publish_event(
    topic: str,
    event: EventEnvelope,
    key: str | None = None,
) -> None:
    """Publish a CloudEvents envelope to Kafka with OTel trace headers."""
    producer = get_producer()

    # Inject W3C trace context into headers
    headers: dict[str, str] = {
        "correlation_id": event.correlation_id,
        "event_type": event.type,
        "source": event.source,
    }
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span(f"kafka.produce.{topic}") as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination", topic)
        span.set_attribute("messaging.destination_kind", "topic")
        span.set_attribute("event.type", event.type)
        inject(headers)

        msg_key = (key or event.correlation_id).encode()
        value = event.to_kafka_value()

        try:
            producer.produce(
                topic=topic,
                key=msg_key,
                value=value,
                headers=list(headers.items()),
                on_delivery=_delivery_report,
            )
            producer.poll(0)
        except KafkaException as exc:
            logger.error(
                "kafka_produce_error",
                extra={
                    "topic": topic,
                    "event_type": event.type,
                    "error": str(exc),
                },
            )
            raise


def flush() -> None:
    get_producer().flush()


def _delivery_report(err: Any, msg: Any) -> None:
    if err:
        logger.error("kafka_delivery_failed", extra={"error": str(err)})
    else:
        logger.debug(
            "kafka_delivery_ok",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            },
        )
