import json
import math
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
connection = json.loads(
    (ROOT / "configs/agents/local.json").read_text()
)
config = json.loads(
    (ROOT / "configs/models/embedding.json").read_text()
)

key_name = connection["api_key_env"]
key = os.getenv(key_name, "").strip()
if not key:
    for line in (ROOT / ".env").read_text().splitlines():
        name, separator, value = line.partition("=")
        if separator and name.strip() == key_name:
            key = value.strip().strip("\"'")
            break

if not key:
    raise SystemExit("STOP: API key is missing.")

payload = {
    "model": config["model"],
    "dimensions": config["dimensions"],
    "encoding_format": config["encoding_format"],
    "input": "An agent retrieves its own successful task trajectories.",
}
request = urllib.request.Request(
    connection["base_url"].rstrip("/") + "/embeddings",
    data=json.dumps(payload).encode(),
    headers={
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    },
    method="POST",
)

report = {
    "config": config,
    "input": payload["input"],
    "passed": False,
    "usage": None,
}
started = monotonic()

try:
    with urllib.request.urlopen(request, timeout=60) as response:
        body = json.load(response)
        report["request_id"] = response.headers.get("x-request-id")

    report["usage"] = body.get("usage")
    report["resolved_model"] = body.get("model")

    items = body.get("data", [])
    if len(items) != 1 or items[0].get("index") != 0:
        raise ValueError("Expected one embedding with index zero")

    vector = items[0]["embedding"]
    if not isinstance(vector, list):
        raise ValueError("Expected an embedding list")
    if len(vector) != config["dimensions"]:
        raise ValueError("Unexpected embedding dimensions")
    if not all(
        type(value) in (int, float) and math.isfinite(value)
        for value in vector
    ):
        raise ValueError("Embedding contains invalid values")

    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        raise ValueError("Embedding vector is zero")

    usage = report["usage"]
    if not isinstance(usage, dict):
        raise ValueError("Provider usage is missing")
    if type(usage.get("prompt_tokens")) is not int:
        raise ValueError("Provider input-token count is missing")
    if usage["prompt_tokens"] <= 0:
        raise ValueError("Expected positive input-token usage")

    report.update({
        "embedding": vector,
        "dimensions": len(vector),
        "vector_norm": norm,
        "passed": True,
    })

except urllib.error.HTTPError as error:
    report["error"] = f"Provider HTTP {error.code}"
except (urllib.error.URLError, TimeoutError):
    report["error"] = "Connection failed or timed out"
except (ValueError, KeyError, TypeError) as error:
    report["error"] = str(error)

report["latency_seconds"] = round(monotonic() - started, 3)

timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
output_dir = ROOT / "outputs" / (
    f"embedding-check-{timestamp}-{uuid4().hex[:6]}"
)
output_dir.mkdir(parents=True)
(output_dir / "results.json").write_text(
    json.dumps(report, indent=2) + "\n"
)

print("Model:", config["model"])
print("Result:", "PASS" if report["passed"] else "FAIL")
print("Dimensions:", report.get("dimensions"))
print("Usage:", json.dumps(report["usage"]))
if report.get("error"):
    print("Error:", report["error"])
print("Report:", (output_dir / "results.json").relative_to(ROOT))

raise SystemExit(0 if report["passed"] else 1)
