"""Local research console: live agents, saved evidence and benchmark reports."""

import asyncio
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Request

ROOT = Path(__file__).resolve().parents[3]
router = APIRouter(prefix="/dashboard-api")

CATALOG = [
    {"id": "math", "name": "MATH", "warmup": 700, "test": 140, "types": 7},
    {"id": "bbh", "name": "BBH", "warmup": 627, "test": 100, "types": 20},
    {"id": "mmlu_pro", "name": "MMLU-Pro", "warmup": 200, "test": 100, "types": 5},
]


def token_usage(calls):
    known = all(
        call.get("usage", {}).get("source") == "provider"
        and all(
            type(call["usage"].get(key)) is int and call["usage"][key] >= 0
            for key in ("input_tokens", "output_tokens")
        )
        for call in calls
    )
    if not known:
        return None
    return sum(
        call["usage"]["input_tokens"] + call["usage"]["output_tokens"]
        for call in calls
    )


def benchmark_report(data):
    if data.get("kind") != "benchmark" or data.get("schema_version") != 1:
        raise ValueError("Expected kind=benchmark and schema_version=1")
    if data.get("dataset") not in {item["id"] for item in CATALOG}:
        raise ValueError("dataset must be math, bbh or mmlu_pro")
    if data.get("system") not in {"deals", "agentic_rag"}:
        raise ValueError("system must be deals or agentic_rag")
    if data.get("phase") not in {"warmup", "test"}:
        raise ValueError("phase must be warmup or test")

    for key in ("comparison_group", "token_scope"):
        if not isinstance(data.get(key), str) or not data[key].strip():
            raise ValueError(f"{key} is required")

    elapsed = data.get("elapsed_seconds")
    if (
        type(elapsed) not in (int, float)
        or not math.isfinite(elapsed)
        or elapsed <= 0
    ):
        raise ValueError("elapsed_seconds must be positive run wall time")

    tasks = data.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise ValueError("tasks must contain evaluated cases")

    case_ids, latencies, calls = [], [], []
    for task in tasks:
        if not isinstance(task.get("case_id"), str) or not task["case_id"]:
            raise ValueError("Every case needs a case_id")
        if type(task.get("correct")) is not bool:
            raise ValueError("Every case needs verifier correct=true/false")
        if type(task.get("usage_complete")) is not bool:
            raise ValueError("Every case needs usage_complete=true/false")

        latency = task.get("latency_seconds")
        if (
            type(latency) not in (int, float)
            or not math.isfinite(latency)
            or latency < 0
        ):
            raise ValueError("Every case needs measured latency_seconds")

        if not isinstance(task.get("calls"), list):
            raise ValueError("Every case needs its provider call records")

        case_ids.append(task["case_id"])
        latencies.append(latency)
        calls.extend(task["calls"])

    call_ids = [call["call_id"] for call in calls]
    if len(case_ids) != len(set(case_ids)) or len(call_ids) != len(set(call_ids)):
        raise ValueError("Duplicate case or call IDs")

    tokens = (
        token_usage(calls)
        if all(task["usage_complete"] for task in tasks)
        else None
    )
    return {
        "kind": "benchmark",
        "dataset": data["dataset"],
        "system": data["system"],
        "phase": data["phase"],
        "group": data["comparison_group"],
        "token_scope": data["token_scope"],
        "cases": len(tasks),
        "case_set": hashlib.sha256(
            json.dumps(sorted(case_ids)).encode()
        ).hexdigest(),
        "accuracy": sum(task["correct"] for task in tasks) / len(tasks),
        "tokens": tokens,
        "tokens_per_task": tokens / len(tasks) if tokens is not None else None,
        "median_latency": median(latencies),
        "throughput": len(tasks) / elapsed,
    }


def stored_evidence():
    candidates, reports = [], []

    for folder in (ROOT / ".runtime/agents").glob("*/tasks"):
        paths = sorted(
            folder.glob("*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[:250]
        for path in paths:
            try:
                record = json.loads(path.read_text())
                candidates.append((
                    folder.parent.name, record, path.stat().st_mtime
                ))
            except (OSError, ValueError):
                continue

    paths = sorted(
        (ROOT / "outputs").glob("*/results.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )[:100]

    for path in paths:
        try:
            data = json.loads(path.read_text())
            if data.get("kind") == "benchmark":
                item = benchmark_report(data)
            elif data.get("kind") == "network-smoke-test":
                item = {
                    "kind": "network-smoke-test",
                    "result": data.get("result"),
                    "cases": len(data.get("tasks", [])),
                    "tokens": data.get("usage_totals", {}).get("total_tokens"),
                }
                for record in data.get("tasks", []):
                    candidates.append((
                        record.get("ingress_agent_id", record.get("agent_id")),
                        record,
                        path.stat().st_mtime,
                    ))
            else:
                continue

            reports.append(item | {
                "id": path.parent.name,
                "updated_at": path.stat().st_mtime,
            })
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            continue

    return candidates, reports


def summarize(candidates):
    selected = {}
    for storage_agent, record, updated in candidates:
        if not record.get("task_id"):
            continue

        key = (record.get("run_id"), record["task_id"])
        entry = record.get(
            "ingress_agent_id", record.get("agent_id", storage_agent)
        )
        rank = (
            record.get("status") == "completed",
            len(record.get("steps", [])),
            storage_agent == entry,
            updated or 0,
        )
        if key not in selected or rank > selected[key][0]:
            selected[key] = (rank, record, entry, updated)

    tasks = []
    for _, record, entry, updated in selected.values():
        calls = record.get("calls", [])
        tokens = token_usage(calls)
        tasks.append({
            "task_id": record["task_id"],
            "run_id": record.get("run_id"),
            "entry": entry,
            "prompt": record.get("original_prompt", ""),
            "task_type": record.get("task_type"),
            "status": record.get("status"),
            "answer": (record.get("result") or {}).get("final_answer"),
            "tokens": (
                tokens if record.get("usage_complete") and calls else None
            ),
            "latency": record.get("latency_seconds"),
            "calls": calls,
            "path": [
                step.get("agent_id", record.get("agent_id"))
                for step in record.get("steps", [])
            ],
            "forwards": sum(
                step.get("routing", {}).get("action") == "forward"
                for step in record.get("steps", [])
            ),
            "updated_at": updated,
        })

    return sorted(
        tasks, key=lambda task: task["updated_at"] or 0, reverse=True
    )[:250]


@router.get("/local-tasks", include_in_schema=False)
async def local_tasks(request: Request):
    return list(request.app.state.runtime.records.values())[-250:]


@router.get("/state")
async def state(request: Request):
    runtime = request.app.state.runtime
    ids = list(runtime.peers.urls)

    async def fetch(agent, path):
        try:
            return await asyncio.to_thread(
                runtime.peers._request, agent, path
            )
        except Exception:
            return None

    health = await asyncio.gather(*(
        fetch(agent, "/health") for agent in ids
    ))
    live = await asyncio.gather(*(
        fetch(agent, "/dashboard-api/local-tasks") for agent in ids
    ))
    candidates, reports = await asyncio.to_thread(stored_evidence)

    for agent, records in zip(ids, live):
        for record in records or []:
            candidates.append((agent, record, 0))

    return {
        "agents": [
            item or {"agent_id": agent, "status": "offline"}
            for agent, item in zip(ids, health)
        ],
        "graph": runtime.peers.graph,
        "tasks": summarize(candidates),
        "reports": reports,
        "benchmarks": CATALOG,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/tasks", status_code=202)
async def submit(request: Request, payload: dict):
    runtime = request.app.state.runtime
    agent = payload.get("agent_id")
    prompt = payload.get("prompt")
    task_type = payload.get("task_type")

    if (
        agent not in runtime.peers.urls
        or not isinstance(prompt, str)
        or not prompt.strip()
    ):
        raise HTTPException(
            422, "Select a configured agent and enter a prompt"
        )
    if len(prompt) > 20000 or task_type not in runtime.state.task_types:
        raise HTTPException(422, "Invalid prompt length or task type")

    try:
        reply = await asyncio.to_thread(
            runtime.peers._request,
            agent,
            "/tasks",
            {"prompt": prompt, "task_type": task_type},
        )
        return reply | {"entry": agent}
    except Exception:
        raise HTTPException(
            503,
            "Agent did not acknowledge submission; "
            "check its task logs before retrying",
        ) from None


@router.get("/tasks/{agent}/{task_id}")
async def detail(request: Request, agent: str, task_id: UUID):
    runtime = request.app.state.runtime
    if agent not in runtime.peers.urls:
        raise HTTPException(404, "Unknown agent")

    try:
        return await asyncio.to_thread(
            runtime.peers._request, agent, f"/tasks/{task_id}"
        )
    except Exception:
        candidates, _ = await asyncio.to_thread(stored_evidence)
        for _, record, _ in candidates:
            if record.get("task_id") == str(task_id):
                return record
        raise HTTPException(404, "Task record unavailable") from None


@router.post("/reports", status_code=201)
async def import_report(payload: dict):
    try:
        if len(json.dumps(payload)) > 5_000_000:
            raise ValueError("Report exceeds 5 MB")
        metrics = benchmark_report(payload)
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        raise HTTPException(422, str(error)) from None

    path = (
        ROOT / "outputs"
        / ("benchmark-import-" + uuid4().hex[:12])
        / "results.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")
    return {"id": path.parent.name, "metrics": metrics}
