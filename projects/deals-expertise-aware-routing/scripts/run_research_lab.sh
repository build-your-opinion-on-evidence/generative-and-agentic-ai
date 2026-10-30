#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export DEALS_LAB_AUTO_BATCH=0
exec bash scripts/run_all_agents.sh
