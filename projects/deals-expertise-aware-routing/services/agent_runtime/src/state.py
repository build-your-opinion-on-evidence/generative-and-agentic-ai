"""Agent-local state and admission logic for DEALS Algorithm 1."""

from collections import deque
from dataclasses import replace
from threading import RLock

from deals_contracts import TaskEnvelope
from deals_core import TypeState, route


class AgentState:
    def __init__(
        self, agent_id, agent_index, task_types, *,
        capacity=2, phase="test", run_id="local-run",
    ):
        for name, value in (("agent_id", agent_id), ("run_id", run_id)):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        if type(agent_index) is not int or agent_index < 0:
            raise ValueError("agent_index must be a non-negative integer")
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        if phase not in ("warmup", "test"):
            raise ValueError("Invalid phase")

        order = tuple(task_types)
        if (
            not order
            or any(not isinstance(d, str) or not d.strip() for d in order)
            or len(set(order)) != len(order)
        ):
            raise ValueError("task_types must be ordered, non-empty and unique")

        self.agent_id = agent_id
        self.agent_index = agent_index
        self.run_id = run_id
        self.capacity = capacity
        self.task_types = order
        self._phase = phase
        self._lock = RLock()
        self._queues = {d: deque() for d in order}
        self._in_flight = {d: 0 for d in order}
        self._counts = {d: [0, 0] for d in order}
        self._active = {}
        self._seen = set()
        self._executed = {}
        self._feedback = set()

    @property
    def phase(self):
        with self._lock:
            return self._phase

    @staticmethod
    def _key(task):
        return (
            task.run_id, task.task_id,
            task.continuation_version, task.hop_count,
        )

    def enqueue(self, task: TaskEnvelope):
        with self._lock:
            if task.run_id != self.run_id or task.phase != self._phase:
                raise ValueError("Task run/phase does not match this agent")
            if task.task_type not in self._queues:
                raise ValueError("Task type is not configured")
            key = self._key(task)
            if key in self._seen:
                return False
            self._queues[task.task_type].append(task)
            self._seen.add(key)
            return True

    def _dispatch_locked(self):
        admitted = []
        while sum(self._in_flight.values()) < self.capacity:
            # max() preserves configured type order on equal lengths.
            d = max(self.task_types, key=lambda x: len(self._queues[x]))
            if not self._queues[d]:
                break
            task = self._queues[d].popleft()
            self._in_flight[d] += 1
            self._active[self._key(task)] = task
            admitted.append(task)
        return admitted

    def dispatch(self):
        """Reserve slots atomically; caller starts each returned task async."""
        with self._lock:
            return self._dispatch_locked()

    def snapshot(self, task_type):
        with self._lock:
            successes, failures = self._counts[task_type]
            return TypeState(
                agent_id=self.agent_id,
                agent_index=self.agent_index,
                task_type=task_type,
                queued=len(self._queues[task_type]),
                in_flight=self._in_flight[task_type],
                successes=successes,
                failures=failures,
            )

    def routing_decision(self, task, direct_neighbors, **policy):
        with self._lock:
            if self._key(task) not in self._active:
                raise RuntimeError("Routing requires a reserved slot")
            return route(
                self.snapshot(task.task_type),
                direct_neighbors,
                hop_count=task.hop_count,
                previous_agent=task.previous_agent,
                **policy,
            )

    def mark_local_execution(self, task):
        with self._lock:
            key = self._key(task)
            if key not in self._active:
                raise RuntimeError("Execution requires a reserved slot")
            current = self._active[key]
            participants = current.executors
            if self.agent_id not in participants:
                participants += (self.agent_id,)
            updated = replace(current, executors=participants)
            self._active[key] = updated
            self._executed[(task.run_id, task.task_id)] = task.task_type
            return updated

    def release_and_dispatch(self, task):
        """Release once after forward/split/finish, then reserve new work."""
        with self._lock:
            active = self._active.pop(self._key(task), None)
            if active is None:
                raise RuntimeError("Slot is not active or was already released")
            self._in_flight[active.task_type] -= 1
            return self._dispatch_locked()

    def apply_verification(self, run_id, task_id, correct):
        """Consume an internal verifier result, never an executor self-rating."""
        with self._lock:
            if self._phase != "warmup":
                raise RuntimeError("Quality is frozen during test")
            if type(correct) not in (bool, int) or correct not in (0, 1):
                raise ValueError("Verifier result must be 0 or 1")
            key = (run_id, task_id)
            if key not in self._executed:
                raise RuntimeError("Forwarding-only agents receive no credit")
            if key in self._feedback:
                return False
            d = self._executed[key]
            self._counts[d][0] += int(correct)
            self._counts[d][1] += 1 - int(correct)
            self._feedback.add(key)
            return True

    def freeze_quality(self):
        """Local quality barrier; memory writes need their own barrier."""
        with self._lock:
            if self._phase != "warmup":
                raise RuntimeError("Agent is not in warmup")
            if self._active or any(self._queues.values()):
                raise RuntimeError("Drain queues and active slots first")
            if set(self._executed) - self._feedback:
                raise RuntimeError("Verification feedback is still pending")
            self._phase = "test"
