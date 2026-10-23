#!/usr/bin/env bash
set -e
echo "Running smoke tests..."
curl -sf http://localhost:8000/healthz > /dev/null
echo "API Gateway: OK"
curl -sf http://localhost:8000/api/orchestrator/platform-health > /dev/null
echo "Platform Health: OK"
echo "Smoke tests passed."
