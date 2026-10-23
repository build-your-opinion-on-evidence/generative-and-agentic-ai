"""
shared/kafka/consumer.py
Shared Kafka consumer base — at-least-once, dead-letter, OTel.
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod

from confluent_kafka import Consumer, KafkaException, Message
from opentelemetry import trace
from opentelemetry.propagate import extract

from shared.contracts.events import EventEnvelope, KafkaTopic
from shared.kafka.producer import publish_event
from prometheus_client import Counter

kafka_consumer_messages_total = Counter(
    "kafka_consumer_messages_total",
    "Total number of Kafka messages consumed",
    ["topic"]
)

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


class BaseKafkaConsumer(ABC):
    """
    Base consumer with:
    - At-least-once delivery
    - Dead-letter topic publishing on repeated failure
    - OTel span extraction from headers
    - Idempotency hook (override should_process)
    """

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        service_name: str,
        max_retries: int = 3,
    ) -> None:
        self.topics = topics
        self.group_id = group_id
        self.service_name = service_name
        self.max_retries = max_retries
        self._consumer = self._build_consumer()
        self._tracer = trace.get_tracer(service_name)

    def _build_consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": BOOTSTRAP_SERVERS,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 300_000,
            }
        )

    def run(self) -> None:
        self._consumer.subscribe(self.topics)
        logger.info(
            "consumer_started",
            extra={"topics": self.topics, "group_id": self.group_id},
        )
        try:
            while True:
                msg: Message | None = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("consumer_error", extra={"error": msg.error()})
                    continue
                kafka_consumer_messages_total.labels(topic=msg.topic()).inc()
                self._process_with_retry(msg)
        except KeyboardInterrupt:
            pass
        finally:
            self._consumer.close()

    def _process_with_retry(self, msg: Message) -> None:
        headers = {k: v.decode() for k, v in (msg.headers() or [])}
        ctx = extract(headers)

        with self._tracer.start_as_current_span(
            f"kafka.consume.{msg.topic()}", context=ctx
        ) as span:
            span.set_attribute("messaging.system", "kafka")
            span.set_attribute("messaging.source", msg.topic())
            span.set_attribute("messaging.partition", msg.partition())

            for attempt in range(1, self.max_retries + 1):
                try:
                    envelope = EventEnvelope.from_kafka_value(msg.value())
                    span.set_attribute("event.type", envelope.type)
                    span.set_attribute("event.correlation_id", envelope.correlation_id)

                    if not self.should_process(envelope):
                        logger.info(
                            "event_skipped_idempotent",
                            extra={"event_id": envelope.id},
                        )
                        self._consumer.commit(msg)
                        return

                    self.handle(envelope)
                    self._consumer.commit(msg)
                    return

                except Exception as exc:
                    logger.warning(
                        "processing_attempt_failed",
                        extra={
                            "attempt": attempt,
                            "max_retries": self.max_retries,
                            "error": str(exc),
                        },
                    )
                    if attempt == self.max_retries:
                        self._send_to_dead_letter(msg, str(exc))
                        self._consumer.commit(msg)

    def _send_to_dead_letter(self, msg: Message, error: str) -> None:
        try:
            original = EventEnvelope.from_kafka_value(msg.value())
            dlq_event = EventEnvelope(
                type="DeadLetterEvent",
                source=self.service_name,
                correlation_id=original.correlation_id,
                data={
                    "original_topic": msg.topic(),
                    "original_event_type": original.type,
                    "original_event_id": original.id,
                    "error": error,
                    "consumer_group": self.group_id,
                },
            )
            publish_event(KafkaTopic.DEAD_LETTER, dlq_event)
            logger.error(
                "event_sent_to_dead_letter",
                extra={"event_id": original.id, "error": error},
            )
        except Exception as exc:
            logger.error("dead_letter_publish_failed", extra={"error": str(exc)})

    def should_process(self, envelope: EventEnvelope) -> bool:
        """Override to implement idempotency checks."""
        return True

    @abstractmethod
    def handle(self, envelope: EventEnvelope) -> None:
        """Implement event handling logic."""
        ...
