import os
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from deals_core import backlog, quality
from .lab_download_fix import ControlledRuntime as AgentRuntime, router as lab_router


@asynccontextmanager
async def lifespan(app):
    app.state.runtime = AgentRuntime(os.getenv("AGENT_ID", "agent-1"))
    try:
        yield
    finally:
        await app.state.runtime.close()


app = FastAPI(title="DEALS Agent Runtime", lifespan=lifespan)

app.include_router(lab_router)


class Submission(BaseModel):
    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True
    )
    prompt: str = Field(min_length=1, max_length=20000)
    task_type: str = Field(default="general", min_length=1)


@app.get("/health")
async def health():
    runtime = app.state.runtime
    types = {}
    for task_type in runtime.state.task_types:
        snapshot = runtime.state.snapshot(task_type)
        types[task_type] = asdict(snapshot) | {
            "backlog": backlog(snapshot.queued, snapshot.in_flight),
            "quality": quality(snapshot.successes, snapshot.failures),
        }
    return {
        "status": "ok",
        "agent_id": runtime.state.agent_id,
        "model": runtime.executor.model,
        "mode": runtime.mode,
        "phase": runtime.state.phase,
        "capacity": runtime.state.capacity,
        "private_memory_size": 0,
        "neighbors": runtime.peers.neighbors,
        "types": types,
    }


@app.post("/tasks", status_code=202)
async def submit(request: Submission):
    try:
        task_id = app.state.runtime.submit(
            request.prompt, request.task_type
        )
    except ValueError as error:
        raise HTTPException(422, str(error)) from None
    except RuntimeError as error:
        raise HTTPException(503, str(error)) from None
    return {"task_id": task_id, "status_url": f"/tasks/{task_id}"}


@app.get("/tasks/{task_id}")
async def result(task_id: str):
    record = app.state.runtime.records.get(task_id)
    if record is None:
        raise HTTPException(404, "Task not found")
    return record


@app.get("/internal/state/{task_type}")
async def peer_state(task_type: str):
    runtime = app.state.runtime
    if task_type not in runtime.state.task_types:
        raise HTTPException(422, "Unknown task type")
    return {
        "run_id": runtime.state.run_id,
        "phase": runtime.state.phase,
        "state": asdict(runtime.state.snapshot(task_type)),
    }


def peer_request(handler, payload):
    try:
        return handler(payload)
    except (KeyError, TypeError, ValueError) as error:
        raise HTTPException(
            422, "Invalid peer payload: " + type(error).__name__
        ) from None
    except RuntimeError as error:
        raise HTTPException(409, str(error)) from None


@app.post("/internal/tasks", status_code=202)
async def receive_task(payload: dict):
    return peer_request(app.state.runtime.receive, payload)


@app.post("/internal/results")
async def receive_result(payload: dict):
    return peer_request(app.state.runtime.accept_result, payload)


from fastapi.staticfiles import StaticFiles
from .dashboard import ROOT as CONSOLE_ROOT, router as dashboard_router

app.include_router(dashboard_router)
app.mount(
    "/", StaticFiles(directory=CONSOLE_ROOT / "apps/frontend", html=True),
    name="research-console",
)
