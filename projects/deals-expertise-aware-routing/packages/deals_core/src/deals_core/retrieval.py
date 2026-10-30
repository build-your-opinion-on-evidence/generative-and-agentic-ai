"""DEALS Appendix A, equations 4 and 5."""

from dataclasses import dataclass
from math import fsum, hypot, isfinite


def _vector(values):
    if not isinstance(values, (list, tuple)) or not values:
        raise ValueError("Embedding must be a non-empty list or tuple")
    if any(
        type(x) not in (int, float) or not isfinite(x)
        for x in values
    ):
        raise ValueError("Embedding contains invalid values")

    norm = hypot(*values)
    if not isfinite(norm) or norm == 0:
        raise ValueError("Embedding norm must be positive and finite")

    return tuple(x / norm for x in values)


def cosine(left, right):
    a, b = _vector(left), _vector(right)
    if len(a) != len(b):
        raise ValueError("Embedding dimensions must match")
    similarity = fsum(x * y for x, y in zip(a, b))
    return max(-1.0, min(1.0, similarity))


def _seconds(value):
    if (
        type(value) not in (int, float)
        or not isfinite(value)
        or value < 0
    ):
        raise ValueError("Execution seconds must be non-negative and finite")


def trajectory_score(query, embedding, local_execution_seconds):
    """Equation 4. Time means local execution, not embedding latency."""
    _seconds(local_execution_seconds)
    minutes = local_execution_seconds / 60.0
    return cosine(query, embedding) * (1.0 + 0.2 / (1.0 + minutes))


@dataclass(frozen=True)
class Trajectory:
    trajectory_id: str
    owner_id: str
    context: str
    embedding: tuple[float, ...]
    local_execution_seconds: float
    verified_correct: bool

    def __post_init__(self):
        for name in ("trajectory_id", "owner_id", "context"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")

        if not isinstance(self.embedding, tuple):
            raise ValueError("Stored embedding must be a tuple")
        _vector(self.embedding)
        _seconds(self.local_execution_seconds)

        if type(self.verified_correct) is not bool:
            raise ValueError("verified_correct must be boolean")


def retrieve(owner_id, query, trajectories, *,
             top_k=3, threshold=0.7, pool_limit=40):
    """Equation 5. Select only this agent's verified successful trajectories.

    Equal scores use trajectory ID for deterministic ordering.
    The memory store must apply its retention policy before retrieval.
    """
    if not isinstance(owner_id, str) or not owner_id.strip():
        raise ValueError("owner_id must be non-empty")

    for name, value in (("top_k", top_k), ("pool_limit", pool_limit)):
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be a positive integer")

    if (
        type(threshold) not in (int, float)
        or not isfinite(threshold)
        or threshold < 0
    ):
        raise ValueError("threshold must be non-negative and finite")

    _vector(query)
    pool = [
        t for t in trajectories
        if t.owner_id == owner_id and t.verified_correct
    ]

    if len(pool) > pool_limit:
        raise ValueError("Private memory exceeds the configured pool limit")
    if len({t.trajectory_id for t in pool}) != len(pool):
        raise ValueError("Duplicate trajectory IDs in private memory")

    hits = []
    for trajectory in pool:
        score = trajectory_score(
            query,
            trajectory.embedding,
            trajectory.local_execution_seconds,
        )
        if score >= threshold:
            hits.append({
                "trajectory_id": trajectory.trajectory_id,
                "owner_id": trajectory.owner_id,
                "context": trajectory.context,
                "local_execution_seconds": trajectory.local_execution_seconds,
                "cosine": cosine(query, trajectory.embedding),
                "score": score,
            })

    return sorted(
        hits, key=lambda hit: (-hit["score"], hit["trajectory_id"])
    )[:top_k]
