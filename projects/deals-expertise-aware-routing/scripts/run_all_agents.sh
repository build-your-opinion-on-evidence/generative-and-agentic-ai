#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

agents="$(services/agent_runtime/.venv/bin/python <<'PY'
import json
import re
import socket
from pathlib import Path

agents = json.loads(
    Path("configs/agents/local.json").read_text()
)["agents"]

for agent in agents:
    agent_id = agent["agent_id"]
    port = agent["port"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", agent_id):
        raise SystemExit("Invalid agent ID")
    with socket.socket() as connection:
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            connection.bind(("127.0.0.1", port))
        except OSError:
            raise SystemExit(
                f"STOP: Port {port} is occupied. Stop its current server first."
            )

for agent in agents:
    print(agent["agent_id"])
PY
)"

pids=()
names=()

cleanup() {
    trap - INT TERM HUP
    echo
    echo "Stopping agents..."
    for pid in "${pids[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

while IFS= read -r agent; do
    echo "Starting $agent..."
    bash scripts/run_agent.sh "$agent" &
    pids+=("$!")
    names+=("$agent")
done <<< "$agents"

echo "All agents launched. Press Ctrl+C to stop all."

while true; do
    for index in "${!pids[@]}"; do
        if ! kill -0 "${pids[$index]}" 2>/dev/null; then
            echo "STOP: ${names[$index]} exited. See its error above."
            exit 1
        fi
    done
    sleep 1
done
