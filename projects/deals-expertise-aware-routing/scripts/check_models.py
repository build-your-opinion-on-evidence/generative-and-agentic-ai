import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
config = json.loads((ROOT / "configs/agents/local.json").read_text())

key = os.getenv(config["api_key_env"], "").strip()
if not key:
    for line in (ROOT / ".env").read_text().splitlines():
        name, separator, value = line.partition("=")
        if separator and name.strip() == config["api_key_env"]:
            key = value.strip().strip("\"'")
            break

if not key:
    raise SystemExit("STOP: API key is missing.")

efforts = {
    "gpt-5-nano": "minimal",
    "gpt-6-luna": "none",
}
results = []

for agent in config["agents"]:
    payload = {
        "model": agent["model"],
        "input": "What is 2 + 2? Return only the digit.",
        "max_output_tokens": 128,
        "store": False,
    }
    effort = efforts.get(agent["model"])
    if effort is not None:
        payload["reasoning"] = {"effort": effort}

    record = {
        "agent_id": agent["agent_id"],
        "requested_model": agent["model"],
        "parameters": payload,
        "passed": False,
        "usage": None,
    }
    request = urllib.request.Request(
        config["base_url"].rstrip("/") + "/responses",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    started = monotonic()
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            body = json.load(response)
            record["request_id"] = response.headers.get("x-request-id")

        answer = "\n".join(
            content["text"]
            for item in body.get("output", [])
            if item.get("type") == "message"
            for content in item.get("content", [])
            if content.get("type") == "output_text"
        ).strip()

        record.update({
            "response_id": body.get("id"),
            "resolved_model": body.get("model"),
            "status": body.get("status"),
            "answer": answer,
            "usage": body.get("usage"),
            "incomplete_details": body.get("incomplete_details"),
            "passed": body.get("status") == "completed" and answer == "4",
        })
    except urllib.error.HTTPError as error:
        record["error"] = f"Provider HTTP {error.code}"
        record["request_id"] = error.headers.get("x-request-id")
    except (urllib.error.URLError, TimeoutError):
        record["error"] = "Connection failed or timed out"
    except (ValueError, KeyError, TypeError):
        record["error"] = "Unexpected provider response"

    record["latency_seconds"] = round(monotonic() - started, 3)
    results.append(record)

    print(f"\n{agent['agent_id']} | {agent['model']}")
    print("Result:", "PASS" if record["passed"] else "FAIL")
    print("Answer:", record.get("answer", "(none)"))
    print("Usage:", json.dumps(record["usage"]))
    if record.get("error"):
        print("Error:", record["error"])

run_name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
output_dir = ROOT / "outputs" / f"model-check-{run_name}-{uuid4().hex[:6]}"
output_dir.mkdir(parents=True)
(output_dir / "results.json").write_text(
    json.dumps(results, indent=2) + "\n"
)

print("\nReport:", (output_dir / "results.json").relative_to(ROOT))
passed = sum(item["passed"] for item in results)
print(f"Passed: {passed}/{len(results)}")
raise SystemExit(0 if passed == len(results) else 1)
