"""DEALS admission, routing, execution and cleanup lifecycle."""

import asyncio
import json
from dataclasses import asdict, replace
from pathlib import Path
from time import monotonic

from deals_contracts import ExecuteOutput, SplitOutput, TaskEnvelope

from .model_executor import ModelExecutor
from .state import AgentState

ROOT = Path(__file__).resolve().parents[3]


class AgentRuntime:
    mode = "single-agent-cold-test"

    def __init__(self, agent_id):
        self.config = json.loads(
            (ROOT / "configs/experiments/local.json").read_text()
        )
        agents = json.loads(
            (ROOT / "configs/agents/local.json").read_text()
        )["agents"]
        index = next(
            i for i, agent in enumerate(agents, start=1)
            if agent["agent_id"] == agent_id
        )
        self.state = AgentState(
            agent_id, index, self.config["task_types"],
            capacity=self.config["capacity"],
            phase="test",
            run_id=self.config["run_id"],
        )
        self.executor = ModelExecutor(agent_id)
        self.records = {}
        self.jobs = set()
        self.submitted_at = {}
        self.queued_at = {}
        self.output_dir = ROOT / ".runtime/agents" / agent_id / "tasks"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _key(task):
        return (
            task.task_id, task.continuation_version, task.hop_count
        )

    async def neighbor_states(self, task):
        return []

    async def forward(self, task, decision, record):
        raise RuntimeError("Peer transport is not configured")

    async def publish(self, record):
        pass

    def _save(self, record):
        path = self.output_dir / f"{record['task_id']}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, indent=2) + "\n")
        temporary.replace(path)

    def _launch(self, tasks):
        for task in tasks:
            admitted_at = monotonic()
            job = asyncio.create_task(self._process(task, admitted_at))
            self.jobs.add(job)
            job.add_done_callback(self.jobs.discard)

    def submit(self, prompt, task_type):
        if task_type not in self.state.task_types:
            raise ValueError("Task type is not configured")
        if len(self.records) >= 1000:
            raise RuntimeError("Local diagnostic task limit reached")

        task = TaskEnvelope(
            run_id=self.state.run_id,
            task_type=task_type,
            phase=self.state.phase,
            original_prompt=prompt,
            remaining_work=prompt,
        )
        now = monotonic()
        self.submitted_at[task.task_id] = now
        self.queued_at[self._key(task)] = now
        record = {
            "task_id": task.task_id,
            "run_id": task.run_id,
            "agent_id": self.state.agent_id,
            "mode": self.mode,
            "original_prompt": prompt,
            "task_type": task_type,
            "status": "queued",
            "result": None,
            "steps": [],
            "calls": [],
            "usage_complete": True,
        }
        self.records[task.task_id] = record
        self.state.enqueue(task)
        self._save(record)
        self._launch(self.state.dispatch())
        return task.task_id

    async def _process(self, task, admitted_at):
        record = self.records[task.task_id]
        step = {
            "agent_id": self.state.agent_id,
            "continuation_version": task.continuation_version,
            "split_count": task.split_count,
            "hop_count": task.hop_count,
            "queue_wait_seconds": (
                admitted_at - self.queued_at[self._key(task)]
            ),
        }
        record["steps"].append(step)
        record["status"] = "running"

        try:
            neighbors = await self.neighbor_states(task)
            decision = self.state.routing_decision(
                task, neighbors, **self.config["routing"]
            )
            step["routing"] = decision
            if decision["action"] == "forward":
                await self.forward(task, decision, record)
                return
            task = self.state.mark_local_execution(task)

            # Cold test starts with no learned trajectories.
            step["retrieved_trajectories"] = []
            result = await asyncio.to_thread(
                self.executor.step, task, ()
            )
            record["calls"].extend(result["calls"])
            record["usage_complete"] = all(
                call["usage"]["source"] == "provider"
                and call["usage"].get("input_tokens") is not None
                and call["usage"].get("output_tokens") is not None
                for call in record["calls"]
            )
            step["output"] = asdict(result["output"])
            step["local_execution_seconds"] = result["local_execution_seconds"]
            step["timing_scope"] = result["timing_scope"]

            if isinstance(result["output"], SplitOutput):
                output = result["output"]
                if task.split_count >= self.executor.split_limit:
                    raise RuntimeError("Split limit exceeded")
                continuation = replace(
                    task,
                    completed_results=(
                        task.completed_results + output.completed_results
                    ),
                    remaining_work=output.remaining_work,
                    split_count=task.split_count + 1,
                    continuation_version=task.continuation_version + 1,
                )
                self.queued_at[self._key(continuation)] = monotonic()
                if not self.state.enqueue(continuation):
                    raise RuntimeError("Continuation was already enqueued")
                record["status"] = "queued"

            elif isinstance(result["output"], ExecuteOutput):
                # FINISH in test returns the answer without learning updates.
                record["result"] = asdict(result["output"])
                record["executors"] = list(task.executors)
                record["status"] = "completed"
                record["latency_seconds"] = (
                    monotonic() - self.submitted_at[task.task_id]
                )
            else:
                raise RuntimeError("Unsupported executor output")

        except Exception as error:
            record["status"] = "failed"
            record["error"] = type(error).__name__
            # A failed task still spent provider tokens; keep attempted
            # calls so usage stays measurable instead of becoming unknown.
            for call in getattr(error, "calls", None) or []:
                if call not in record["calls"]:
                    record["calls"].append(call)
            record["usage_complete"] = all(
                call["usage"]["source"] == "provider"
                and call["usage"].get("input_tokens") is not None
                and call["usage"].get("output_tokens") is not None
                for call in record["calls"]
            )
            record["latency_seconds"] = (
                monotonic() - self.submitted_at[task.task_id]
            )
            # Provider-call records, including failures, remain in calls/.
        finally:
            try:
                if record["status"] in {"completed", "failed"}:
                    await self.publish(record)
            finally:
                next_tasks = self.state.release_and_dispatch(task)
                self._launch(next_tasks)
                self._save(record)

    async def close(self):
        while self.jobs:
            await asyncio.gather(
                *tuple(self.jobs), return_exceptions=True
            )
