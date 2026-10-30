import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier
from time import monotonic, sleep
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def rpc(base, path, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(
        base + path,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def main():
    agents = json.loads(
        (ROOT / "configs/agents/local.json").read_text()
    )["agents"]
    urls = {
        a["agent_id"]: f"http://127.0.0.1:{a['port']}"
        for a in agents
    }
    graph = json.loads(
        (ROOT / "configs/routing/graph.json").read_text()
    )
    before = {
        aid: rpc(url, "/health")
        for aid, url in urls.items()
    }

    for aid, health in before.items():
        if (
            health["agent_id"] != aid
            or health["mode"] != "network-cold-test"
            or health["phase"] != "test"
            or set(health["neighbors"]) != set(graph[aid])
        ):
            raise SystemExit(
                f"STOP: {aid} is not running the configured cold network."
            )
        if any(s["backlog"] for s in health["types"].values()):
            raise SystemExit(
                "STOP: Wait for the current tasks to finish first."
            )

    entry = agents[0]["agent_id"]
    if "math.algebra" not in before[entry]["types"]:
        raise SystemExit("STOP: math.algebra is not configured.")

    count = 2 * before[entry]["capacity"] + 1
    barrier = Barrier(count)

    def submit(index):
        barrier.wait()
        try:
            return rpc(urls[entry], "/tasks", {
                "prompt": "What is 2 + 2? Return only the integer answer.",
                "task_type": "math.algebra",
            })
        except Exception as error:
            return {
                "submission_error": type(error).__name__,
                "index": index,
            }

    print(
        f"Ingress: {entry} | Concurrent tasks: {count}",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=count) as pool:
        submissions = list(pool.map(submit, range(count)))

    pending = {
        s["task_id"] for s in submissions if "task_id" in s
    }
    records = {}
    poll_errors = {}
    deadline = monotonic() + 180

    while pending and monotonic() < deadline:
        for task_id in tuple(pending):
            try:
                record = rpc(urls[entry], f"/tasks/{task_id}")
                records[task_id] = record
                poll_errors.pop(task_id, None)
                if record["status"] in {
                    "completed",
                    "failed",
                    "transfer_uncertain",
                    "result_delivery_pending",
                }:
                    pending.remove(task_id)
            except Exception as error:
                poll_errors[task_id] = type(error).__name__

        if pending:
            sleep(0.2)

    calls = [
        call
        for record in records.values()
        for call in record.get("calls", [])
    ]
    ids = [call["call_id"] for call in calls]
    unique_calls = {
        call["call_id"]: call for call in calls
    }

    completed = len(records) == count and all(
        r["status"] == "completed" and r.get("calls")
        for r in records.values()
    )
    correct = completed and all(
        r["result"]["final_answer"].strip() == "4"
        for r in records.values()
    )
    usage_complete = (
        completed
        and len(ids) == len(set(ids))
        and all(
            r.get("usage_complete") is True
            for r in records.values()
        )
        and all(
            call["usage"]["source"] == "provider"
            and all(
                type(call["usage"].get(key)) is int
                and call["usage"][key] >= 0
                for key in ("input_tokens", "output_tokens")
            )
            for call in calls
        )
    )

    totals = {}
    for key in (
        "input_tokens",
        "output_tokens",
        "cached_input_tokens",
    ):
        values = [
            call["usage"].get(key)
            for call in unique_calls.values()
        ]
        totals[key] = (
            sum(values)
            if usage_complete and all(
                type(value) is int and value >= 0
                for value in values
            )
            else None
        )

    totals["total_tokens"] = (
        totals["input_tokens"] + totals["output_tokens"]
        if usage_complete
        else None
    )

    forwards = sum(
        step.get("routing", {}).get("action") == "forward"
        for record in records.values()
        for step in record.get("steps", [])
    )

    if correct and usage_complete and forwards:
        status = "PASS"
    elif correct and usage_complete:
        status = "INCONCLUSIVE_NO_FORWARD"
    else:
        status = "FAIL"

    report = {
        "kind": "network-smoke-test",
        "result": status,
        "ingress_agent": entry,
        "health_before": before,
        "submissions": submissions,
        "pending_task_ids": sorted(pending),
        "poll_errors": poll_errors,
        "forwarded_steps": forwards,
        "recorded_calls": len(unique_calls),
        "usage_complete": usage_complete,
        "usage_totals": totals,
        "tasks": list(records.values()),
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (
        ROOT / "outputs"
        / f"network-check-{stamp}-{uuid4().hex[:6]}"
        / "results.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")

    for task_id, record in records.items():
        path = " / ".join(
            step["agent_id"] for step in record.get("steps", [])
        )
        answer = (record.get("result") or {}).get("final_answer")
        print(
            f"{task_id[:8]} | {record['status']} "
            f"| {path} | answer={answer}"
        )

    print(f"Forwarded steps: {forwards}")
    print(f"Recorded model calls: {len(unique_calls)}")
    print("Usage:", json.dumps(totals))
    print("Result:", status)
    print("Report:", output.relative_to(ROOT))

    raise SystemExit(0 if status == "PASS" else 1)


if __name__ == "__main__":
    main()
