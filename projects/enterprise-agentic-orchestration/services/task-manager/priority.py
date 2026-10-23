"""
services/task-manager/priority.py
Priority scoring for fraud tasks.
Higher score = higher urgency.
"""
from __future__ import annotations

from shared.contracts.events import EventType


# Base scores by event type
_BASE_SCORES: dict[str, int] = {
    EventType.CARD_REPORTED_STOLEN:          100,
    EventType.LARGE_TRANSACTION_ATTEMPTED:    85,
    EventType.SUSPICIOUS_TRANSACTION_DETECTED: 70,
    EventType.NEW_DEVICE_LOGIN_DETECTED:       55,
    EventType.CUSTOMER_CHANGED_PHONE_NUMBER:   40,
}

_DEFAULT_SCORE = 50


def compute_priority(event_type: str, data: dict) -> int:
    """Compute task priority 0–100 based on event type and data signals."""
    score = _BASE_SCORES.get(event_type, _DEFAULT_SCORE)

    # Boost for foreign country
    country = data.get("country", "EG")
    if country not in ("EG", ""):
        score = min(score + 5, 100)

    # Boost for high explicit risk score
    risk_score = int(data.get("risk_score", 0))
    if risk_score >= 80:
        score = min(score + 10, 100)

    # Boost when multiple signals in data
    if data.get("simulate_agent_timeout"):
        score = max(score, 60)

    return score
