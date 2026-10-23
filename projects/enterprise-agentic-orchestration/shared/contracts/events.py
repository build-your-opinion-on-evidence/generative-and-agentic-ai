"""
shared/contracts/events.py
CloudEvents-inspired envelope for all Kafka messages.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return f"evt-{uuid.uuid4().hex[:8]}"


class EventEnvelope(BaseModel):
    """
    Mandatory CloudEvents-inspired envelope.
    All services must produce and consume this structure.
    """

    specversion: str = "1.0"
    id: str = Field(default_factory=_new_id)
    type: str
    source: str
    subject: str | None = None
    time: datetime = Field(default_factory=_now_utc)
    correlation_id: str
    causation_id: str | None = None
    trace_id: str = Field(default_factory=lambda: f"trace-{uuid.uuid4().hex[:12]}")
    schema_version: str = "1.0"
    data: dict[str, Any]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

    def to_kafka_value(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_kafka_value(cls, raw: bytes) -> "EventEnvelope":
        return cls.model_validate_json(raw)


# ── Known event types ────────────────────────────────────────────────────────

class EventType:
    # Banking domain
    SUSPICIOUS_TRANSACTION_DETECTED = "SuspiciousTransactionDetected"
    CARD_REPORTED_STOLEN = "CardReportedStolen"
    LARGE_TRANSACTION_ATTEMPTED = "LargeTransactionAttempted"
    KNOWN_TRAVEL_PATTERN = "KnownTravelPattern"
    CUSTOMER_CHANGED_PHONE_NUMBER = "CustomerChangedPhoneNumber"
    NEW_DEVICE_LOGIN_DETECTED = "NewDeviceLoginDetected"

    # Task lifecycle
    FRAUD_TASK_CREATED = "FraudTaskCreated"
    FRAUD_TASK_UPDATED = "FraudTaskUpdated"
    FRAUD_TASK_READY = "FraudTaskReady"
    FRAUD_TASK_COMPLETED = "FraudTaskCompleted"
    FRAUD_TASK_FAILED = "FraudTaskFailed"
    FRAUD_TASK_PAUSED = "FraudTaskPaused"
    FRAUD_TASK_RESUMED = "FraudTaskResumed"

    # Orchestration
    STRATEGY_SELECTED = "StrategySelected"
    DAG_TRIGGERED = "DagTriggered"
    DAG_COMPLETED = "DagCompleted"
    DAG_FAILED = "DagFailed"
    REACT_STARTED = "ReactStarted"
    REACT_COMPLETED = "ReactCompleted"
    REACT_FAILED = "ReactFailed"

    # Agent
    AGENT_INVOCATION_STARTED = "AgentInvocationStarted"
    AGENT_INVOCATION_COMPLETED = "AgentInvocationCompleted"
    AGENT_INVOCATION_FAILED = "AgentInvocationFailed"

    # Policy / Approval
    POLICY_DECISION_MADE = "PolicyDecisionMade"
    APPROVAL_REQUESTED = "ApprovalRequested"
    APPROVAL_GRANTED = "ApprovalGranted"
    APPROVAL_REJECTED = "ApprovalRejected"

    # Banking actions
    CARD_FROZEN = "CardFrozen"
    CARD_UNBLOCKED = "CardUnblocked"
    TRANSACTION_BLOCKED = "TransactionBlocked"
    CUSTOMER_NOTIFIED = "CustomerNotified"
    FRAUD_INVESTIGATION_COMPLETED = "FraudInvestigationCompleted"


# ── Topic constants ───────────────────────────────────────────────────────────

class KafkaTopic:
    BUSINESS_EVENTS = "banking.business-events.v1"
    TASK_EVENTS = "fraud.task-events.v1"
    ORCHESTRATION_EVENTS = "fraud.orchestration-events.v1"
    AGENT_EVENTS = "fraud.agent-events.v1"
    APPROVAL_EVENTS = "fraud.approval-events.v1"
    ACTION_EVENTS = "fraud.action-events.v1"
    DEAD_LETTER = "fraud.dead-letter.v1"
