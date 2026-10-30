#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../services/agent_runtime"

export AGENT_ID="${1:-agent-1}"

port="$(.venv/bin/python -c '
import json, os
from pathlib import Path
agents = json.loads(Path("../../configs/agents/local.json").read_text())["agents"]
print(next(a["port"] for a in agents if a["agent_id"] == os.environ["AGENT_ID"]))
')"

exec .venv/bin/python -m uvicorn src.app:app \
    --host 127.0.0.1 --port "$port" --workers 1
