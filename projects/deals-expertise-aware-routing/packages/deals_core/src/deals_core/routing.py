"""DEALS equations 1–3 and Algorithm 1 routing decision."""

from dataclasses import asdict, dataclass
from fractions import Fraction
from math import isfinite


def _count(name, value):
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _number(name, value, positive=False):
    if type(value) not in (int, float) or not isfinite(value):
        raise ValueError(f"{name} must be finite")
    if value < 0 or (positive and value == 0):
        raise ValueError(f"{name} has an invalid range")
    return Fraction(str(value))


def backlog(queued, in_flight):
    """Equation 1: waiting plus routing/executing tasks."""
    _count("queued", queued)
    _count("in_flight", in_flight)
    return queued + in_flight


def _quality(successes, failures, alpha=1, beta=1):
    _count("successes", successes)
    _count("failures", failures)
    a = _number("alpha", alpha, positive=True)
    b = _number("beta", beta, positive=True)
    return (successes + a) / (successes + failures + a + b)


def quality(successes, failures, alpha=1, beta=1):
    """Equation 2: Beta-smoothed verified success rate."""
    return float(_quality(successes, failures, alpha, beta))


@dataclass(frozen=True)
class TypeState:
    """One agent's snapshot for one task type."""

    agent_id: str
    agent_index: int
    task_type: str
    queued: int
    in_flight: int
    successes: int
    failures: int

    def __post_init__(self):
        for name in ("agent_id", "task_type"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        for name in (
            "agent_index", "queued", "in_flight", "successes", "failures",
        ):
            _count(name, getattr(self, name))


def route(local, direct_neighbors, *, weight, hop_count,
          previous_agent=None, hop_limit=3, margin=1, alpha=1, beta=1):
    """Choose local processing or forwarding using direct-neighbor snapshots.

    The caller must reserve the in-flight slot before taking local state.
    This function makes a decision; it does not mutate queues or transfer tasks.
    """
    _count("hop_count", hop_count)
    _count("hop_limit", hop_limit)
    v = _number("weight", weight)
    m = _number("margin", margin)

    if previous_agent is not None:
        if not isinstance(previous_agent, str) or not previous_agent.strip():
            raise ValueError("previous_agent must be non-empty or None")

    neighbors = list(direct_neighbors)
    ids, indices = {local.agent_id}, {local.agent_index}
    for neighbor in neighbors:
        if neighbor.task_type != local.task_type:
            raise ValueError("Snapshots must have the same task_type")
        if neighbor.agent_id in ids or neighbor.agent_index in indices:
            raise ValueError("Agent IDs and indices must be unique")
        ids.add(neighbor.agent_id)
        indices.add(neighbor.agent_index)

    own_b = backlog(local.queued, local.in_flight)
    own_q = _quality(local.successes, local.failures, alpha, beta)

    trace = dict(
        action="local",
        destination=None,
        reason="no_eligible_neighbor",
        task_type=local.task_type,
        weight=weight,
        margin=margin,
        alpha=alpha,
        beta=beta,
        hop_count=hop_count,
        hop_limit=hop_limit,
        previous_agent=previous_agent,
        local=asdict(local) | {
            "backlog": own_b, "quality": float(own_q),
        },
        neighbors=[],
    )

    if hop_count >= hop_limit:
        trace["reason"] = "hop_limit"
        return trace

    best, best_score = None, None
    for neighbor in sorted(neighbors, key=lambda item: item.agent_index):
        if neighbor.agent_id == previous_agent:
            continue

        other_b = backlog(neighbor.queued, neighbor.in_flight)
        other_q = _quality(neighbor.successes, neighbor.failures, alpha, beta)
        score = own_b - other_b + v * (other_q - own_q)

        trace["neighbors"].append(asdict(neighbor) | {
            "backlog": other_b,
            "quality": float(other_q),
            "score": float(score),
            "score_exact": str(score),
        })

        # Sorted iteration preserves the smallest index on equal scores.
        if best_score is None or score > best_score:
            best, best_score = neighbor, score

    if best is not None:
        trace["reason"] = "margin_not_exceeded"
        if best_score > m:
            trace.update(
                action="forward",
                destination=best.agent_id,
                reason="score_exceeds_margin",
            )

    return trace
