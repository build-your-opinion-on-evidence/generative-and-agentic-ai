
"""Benchmark evaluations with cooperative stop and explicit history clearing."""

import asyncio
import json
import threading
from dataclasses import asdict, replace
from time import monotonic

from fastapi import APIRouter, HTTPException, Request
from deals_contracts import ExecuteOutput, SplitOutput

from . import research_lab as base

router = APIRouter()
DONE = base.TERMINAL | {"cancelled"}


class ControlledRuntime(base.LabRuntime):
    def __init__(self, agent_id):
        base.os.environ["DEALS_LAB_AUTO_BATCH"] = "0"
        super().__init__(agent_id)

        self.context = {"experiment": "idle", "phase": "test"}
        self.stopped_jobs = set()
        self.evaluations = {}
        self.control_lock = asyncio.Lock()
        self.clearing = False
        self.shutting_down = False
        self.provider_active = set()
        self.provider_lock = threading.Lock()

        original_step = self.executor.step

        def step(task, demos):
            self.check_task(task)
            with self.provider_lock:
                self.provider_active.add(task.task_id)
            try:
                self.check_task(task)
                return original_step(task, demos)
            finally:
                with self.provider_lock:
                    self.provider_active.discard(task.task_id)

        self.executor.step = step

    async def auto_demo(self):
        return

    def task_stopped(self, task):
        metadata = self.records.get(task.task_id, {}).get("lab", {})
        return metadata.get("job") in self.stopped_jobs

    def check_task(self, task):
        if self.task_stopped(task):
            raise EvaluationStopped("Evaluation stop requested")

    def check_job(self, job):
        if job.get("cancel_requested"):
            raise asyncio.CancelledError("Evaluation stop requested")

    def submit(self, prompt, task_type):
        if self.clearing:
            raise RuntimeError("History is being cleared")
        return super().submit(prompt, task_type)

    def receive(self, payload):
        if self.clearing:
            raise RuntimeError("History is being cleared")
        return super().receive(payload)

    def new_job(self, payload):
        if payload.get("mode") != "benchmark":
            raise ValueError("Only benchmark evaluations are supported")

        if self.clearing or any(
            job["status"] in {"queued", "running", "stopping"}
            for job in self.lab_jobs.values()
        ):
            raise ValueError("Wait for the current operation to finish")

        job = {
            "id": "lab-" + base.uuid4().hex[:10],
            "status": "queued",
            "progress": "Preparing evaluation",
            "results": [],
            "created_at": base.time(),
        }
        self.lab_jobs[job["id"]] = job
        self.evaluations[job["id"]] = self.spawn(
            self.run_job(job, payload)
        )
        return job

    async def submit_cases(
        self, cases, system, job, phase, entry=None, prompts=None
    ):
        self.check_job(job)
        job["admission_started"] = True
        result = await super().submit_cases(
            cases, system, job, phase, entry, prompts
        )
        self.check_job(job)
        return result

    def report(
        self, dataset, system, phase, cases,
        records, elapsed, job, provenance
    ):
        self.check_job(job)
        return super().report(
            dataset, system, phase, cases,
            records, elapsed, job, provenance
        )

    async def run_job(self, job, payload):
        job["supervisor_started"] = True

        try:
            self.check_job(job)
            await super().run_job(job, payload)
        except asyncio.CancelledError:
            job["cancel_requested"] = True
        finally:
            if (
                job["status"] == "failed"
                and job.get("admission_started")
            ):
                job["cancel_requested"] = True
                job["stop_due_to_error"] = True
                self.stopped_jobs.add(job["id"])
                job["status"] = "stopping"

                await asyncio.gather(
                    *(
                        self.rpc(
                            agent,
                            "/control/stop-node",
                            {"job_id": job["id"]},
                        )
                        for agent in self.peers.urls
                    ),
                    return_exceptions=True,
                )

            if job.get("cancel_requested"):
                job["status"] = "stopping"

                if self.shutting_down:
                    job.update(
                        status="interrupted",
                        progress=(
                            "Process stopped; unfinished work was not "
                            "reported as completed"
                        ),
                    )
                else:
                    while True:
                        nodes = await self.all_nodes()
                        waiting = [
                            node["agent_id"]
                            for node in nodes
                            if (
                                node["status"] != "ok"
                                or node.get("provider_calls_active", 0)
                                or node.get("runtime_jobs", 0)
                                or any(
                                    snapshot["queued"]
                                    + snapshot["in_flight"]
                                    for snapshot in node.get(
                                        "types", {}
                                    ).values()
                                )
                            )
                        ]

                        if not waiting:
                            break

                        job["progress"] = (
                            "Stopping; waiting for in-flight work: "
                            + ", ".join(waiting)
                        )
                        await asyncio.sleep(0.3)

                    if job.get("stop_due_to_error"):
                        job.update(
                            status="failed",
                            progress="Outstanding work stopped after failure",
                        )
                    else:
                        job.update(
                            status="stopped",
                            progress=(
                                "Stopped; queued work cancelled and "
                                "in-flight requests settled"
                            ),
                        )

            base.write_json(
                base.ROOT / "outputs" / job["id"] / "lab-job.json",
                job,
            )

    async def _process(self, task, admitted_at):
        record = self.records[task.task_id]

        if not record.get("lab"):
            return await super()._process(task, admitted_at)

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
            self.check_task(task)
            neighbors = await self.neighbor_states(task)
            self.check_task(task)

            decision = self.state.routing_decision(
                task, neighbors, **self.config["routing"]
            )
            step["routing"] = decision
            self.check_task(task)

            if decision["action"] == "forward":
                await self.forward(task, decision, record)
                return

            task = self.state.mark_local_execution(task)
            step["retrieved_trajectories"] = []

            # Await an already-started provider request so its usage is retained.
            result = await asyncio.to_thread(
                self.executor.step, task, ()
            )

            record["calls"].extend(result["calls"])
            record["usage_complete"] = all(
                call.get("usage", {}).get("source") == "provider"
                and all(
                    type(call["usage"].get(key)) is int
                    for key in ("input_tokens", "output_tokens")
                )
                for call in record["calls"]
            )

            step["output"] = asdict(result["output"])
            step["local_execution_seconds"] = result[
                "local_execution_seconds"
            ]
            step["timing_scope"] = result["timing_scope"]
            record["executors"] = list(task.executors)

            self.check_task(task)
            output = result["output"]

            if isinstance(output, SplitOutput):
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
                    raise RuntimeError("Continuation already enqueued")

                record["status"] = "queued"

            elif isinstance(output, ExecuteOutput):
                record.update(
                    result=asdict(output),
                    status="completed",
                )
            else:
                raise RuntimeError("Unsupported executor output")

        except EvaluationStopped:
            record.update(
                status="cancelled",
                error="Evaluation stopped by user",
                cancel_requested=True,
            )
        except Exception as error:
            # A failed task still spent provider tokens; keep attempted
            # calls so the step's token total stays measurable.
            for call in getattr(error, "calls", None) or []:
                if call not in record["calls"]:
                    record["calls"].append(call)
            record.update(
                status="failed",
                error=type(error).__name__,
                usage_complete=all(
                    call.get("usage", {}).get("source") == "provider"
                    and all(
                        type(call["usage"].get(key)) is int
                        for key in ("input_tokens", "output_tokens")
                    )
                    for call in record["calls"]
                ),
            )
        finally:
            try:
                if record["status"] in {
                    "completed", "failed", "cancelled"
                }:
                    record["latency_seconds"] = (
                        monotonic()
                        - self.submitted_at[task.task_id]
                    )
                    await self.publish(record)
            finally:
                self._launch(
                    self.state.release_and_dispatch(task)
                )
                self._save(record)

    def accept_result(self, payload):
        incoming = payload["record"]
        sender = payload["sender_id"]
        task_id = incoming["task_id"]

        if (
            sender not in self.peers.urls
            or self.origins.get(task_id) != self.state.agent_id
            or incoming["status"] not in {
                "completed", "failed", "cancelled"
            }
        ):
            raise ValueError("Invalid terminal result")

        record = self.records[task_id]

        for key in (
            "run_id",
            "task_type",
            "original_prompt",
            "ingress_agent_id",
        ):
            if incoming.get(key) != record.get(key):
                raise ValueError("Result metadata mismatch")

        if record["status"] in {
            "completed", "failed", "cancelled"
        }:
            if (
                record["status"] != incoming["status"]
                or record.get("result") != incoming.get("result")
            ):
                raise ValueError("Conflicting terminal result")
        else:
            record.clear()
            record.update(json.loads(json.dumps(incoming)))
            record.update(
                current_agent_id=sender,
                latency_seconds=(
                    monotonic() - self.submitted_at[task_id]
                ),
                latency_scope="ingress_to_result_received",
            )
            self._save(record)

        return {
            "accepted": True,
            "latency_seconds": record["latency_seconds"],
        }

    def idle(self):
        with self.provider_lock:
            providers = len(self.provider_active)

        return (
            not providers
            and not self.jobs
            and not any(
                snapshot["queued"] + snapshot["in_flight"]
                for snapshot in self.types().values()
            )
        )

    def clear_local(self):
        if not self.clearing or not self.idle():
            raise RuntimeError(
                "Stop active work before clearing history"
            )

        deleted = 0
        for folder in (
            self.output_dir,
            self.output_dir.parent / "calls",
        ):
            if folder.is_dir():
                for path in folder.rglob("*"):
                    if path.is_file() and not path.is_symlink():
                        path.unlink()
                        deleted += 1

        for name in (
            "records",
            "origins",
            "submitted_at",
            "queued_at",
            "learned",
            "lab_jobs",
            "evaluations",
        ):
            getattr(self, name).clear()

        self.feedback_seen.clear()
        self.shared_memory.clear()
        self.updates.clear()
        self.history.clear()
        self.peaks = dict.fromkeys(self.peaks, 0)
        self.context = {
            "experiment": "idle",
            "phase": "test",
        }
        return deleted

    async def close(self):
        self.shutting_down = True
        await super().close()


class EvaluationStopped(Exception):
    pass


def coordinator(runtime):
    if runtime.state.agent_id != next(iter(runtime.peers.urls)):
        raise HTTPException(
            409, "Use the lab on the first agent's port"
        )


@router.get("/lab-api/node")
async def controlled_node(request: Request):
    runtime = request.app.state.runtime
    result = await base.node(request)

    with runtime.provider_lock:
        result["provider_calls_active"] = len(
            runtime.provider_active
        )

    result["runtime_jobs"] = len(runtime.jobs)
    return result


@router.get("/lab-api/state")
async def controlled_state(request: Request):
    runtime = request.app.state.runtime
    result = await base.state(request)

    result["tasks"] = [
        task
        for task in result["tasks"]
        if task.get("lab")
        and not task["lab"].get(
            "case_id", ""
        ).startswith("synthetic-")
    ]
    result["reports"] = [
        report
        for report in result["reports"]
        if report.get("dataset") in base.CATALOG
        and report.get("kind") == "benchmark"
    ]

    active = next(
        (
            job
            for job in reversed(
                list(runtime.lab_jobs.values())
            )
            if job["id"] in runtime.evaluations
            and job["status"] in {
                "queued", "running", "stopping"
            }
        ),
        None,
    )

    busy = any(
        node["status"] != "ok"
        or node.get("runtime_jobs", 0)
        or node.get("provider_calls_active", 0)
        or any(
            snapshot["queued"] + snapshot["in_flight"]
            for snapshot in node.get("types", {}).values()
        )
        for node in result["nodes"]
    )

    result["controls"] = {
        "evaluation": active,
        "can_clear": (
            not busy
            and not runtime.clearing
            and not any(
                job["status"] in {
                    "queued", "running", "stopping"
                }
                for job in runtime.lab_jobs.values()
            )
        ),
    }
    return result


@router.post("/lab-api/jobs")
async def controlled_jobs(request: Request, data: dict):
    if data.get("mode") != "benchmark":
        raise HTTPException(
            422, "Only benchmark evaluations are supported"
        )
    return await base.jobs(request, data)


@router.post("/lab-api/admit")
async def controlled_admit(request: Request, data: dict):
    runtime = request.app.state.runtime
    tasks = data.get("tasks")

    if not isinstance(tasks, list) or not tasks:
        raise HTTPException(422, "Tasks are required")

    for task in tasks:
        metadata = task.get("lab", {})

        if (
            metadata.get("system") not in {
                "deals", "agentic_rag"
            }
            or metadata.get(
                "case_id", ""
            ).startswith("synthetic-")
        ):
            raise HTTPException(422, "Use benchmark cases")

        if (
            metadata.get("job") in runtime.stopped_jobs
            or runtime.clearing
        ):
            raise HTTPException(
                409, "Evaluation stopped or history clearing"
            )

    return await base.admit(request, data)


@router.post("/lab-api/control/stop-node")
async def stop_node(request: Request, data: dict):
    runtime = request.app.state.runtime
    job_id = data.get("job_id")

    if (
        not isinstance(job_id, str)
        or not job_id.startswith("lab-")
    ):
        raise HTTPException(422, "Invalid evaluation ID")

    runtime.stopped_jobs.add(job_id)

    for record in runtime.records.values():
        if (
            record.get("lab", {}).get("job") == job_id
            and record["status"] not in DONE
        ):
            record["cancel_requested"] = True

    return {"accepted": True}


@router.post("/lab-api/stop")
async def stop_evaluation(request: Request, data: dict):
    runtime = request.app.state.runtime
    coordinator(runtime)

    async with runtime.control_lock:
        job_id = data.get("job_id")
        job = runtime.lab_jobs.get(job_id)

        if job is None or job_id not in runtime.evaluations:
            raise HTTPException(
                404, "Evaluation not found"
            )

        if job["status"] not in {
            "queued", "running", "stopping"
        }:
            return {"status": job["status"]}

        job.update(
            cancel_requested=True,
            status="stopping",
            progress=(
                "Stop requested; settling in-flight requests"
            ),
        )
        runtime.stopped_jobs.add(job_id)

        replies = await asyncio.gather(
            *(
                runtime.rpc(
                    agent,
                    "/control/stop-node",
                    {"job_id": job_id},
                )
                for agent in runtime.peers.urls
            ),
            return_exceptions=True,
        )

        errors = [
            agent
            for agent, reply in zip(
                runtime.peers.urls, replies
            )
            if isinstance(reply, BaseException)
        ]
        job["stop_pending_nodes"] = errors
        task = runtime.evaluations[job_id]

        if (
            job.get("supervisor_started")
            and not job.get("stop_signal_sent")
            and not task.done()
        ):
            job["stop_signal_sent"] = True
            task.cancel()

        return {
            "status": "stopping",
            "pending_nodes": errors,
        }


@router.post("/lab-api/control/history-node")
async def history_node(request: Request, data: dict):
    runtime = request.app.state.runtime
    action = data.get("action")

    if action == "release":
        runtime.clearing = False
        return {"accepted": True}

    if action == "prepare":
        runtime.clearing = True

        if not runtime.idle() or any(
            job["status"] in {
                "queued", "running", "stopping"
            }
            for job in runtime.lab_jobs.values()
        ):
            runtime.clearing = False
            raise HTTPException(
                409, "Stop active work before clearing history"
            )

        return {"accepted": True}

    if action == "clear":
        try:
            return {
                "accepted": True,
                "deleted": runtime.clear_local(),
            }
        except RuntimeError as error:
            raise HTTPException(
                409, str(error)
            ) from None

    raise HTTPException(422, "Invalid action")


@router.post("/lab-api/history/clear")
async def clear_history(request: Request, data: dict):
    r = request.app.state.runtime
    coordinator(r)

    async with r.control_lock:
        try:
            prepared = await asyncio.gather(*(
                r.rpc(a, "/control/history-node", {"action": "prepare"})
                for a in r.peers.urls
            ), return_exceptions=True)

            if any(isinstance(reply, BaseException) for reply in prepared):
                raise HTTPException(
                    409, "Wait until all agents are idle before clearing history"
                )

            replies = await asyncio.gather(*(
                r.rpc(a, "/control/history-node", {"action": "clear"})
                for a in r.peers.urls
            ), return_exceptions=True)

            if any(isinstance(reply, BaseException) for reply in replies):
                raise HTTPException(
                    503,
                    "Some history could not be cleared; retry when all agents are online"
                )

            deleted = sum(reply["deleted"] for reply in replies)
            kinds = {"benchmark", "batch-demo", "network-smoke-test"}

            for folder in (base.ROOT / "outputs").glob("*"):
                if not folder.is_dir() or folder.is_symlink():
                    continue

                result = base.read_json(folder / "results.json", None)
                summary = base.read_json(folder / "summary.json", None)

                recognized = any(
                    isinstance(item, dict) and item.get("kind") in kinds
                    for item in (result, summary)
                )

                legacy = isinstance(result, list) and all(
                    isinstance(item, dict) and any(
                        key in item for key in ("task_id", "case_id", "trace")
                    )
                    for item in result
                )

                if folder.name.startswith("lab-") or recognized or legacy:
                    for name in ("results.json", "summary.json", "lab-job.json"):
                        path = folder / name
                        if path.is_file() and not path.is_symlink():
                            path.unlink()
                            deleted += 1

                    if not any(folder.iterdir()):
                        folder.rmdir()

            return {"cleared": True, "deleted_files": deleted}
        finally:
            await asyncio.gather(*(
                r.rpc(a, "/control/history-node", {"action": "release"})
                for a in r.peers.urls
            ), return_exceptions=True)


router.include_router(base.router, prefix="")
