#!/usr/bin/env bash
set -e

echo "Starting Enterprise Agentic Orchestration Platform..."

if ! command -v docker &> /dev/null; then
    echo "ERROR: docker is required but not installed."
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "ERROR: .env file is missing. Please create it based on .env.example"
    exit 1
fi

echo "Spinning up infrastructure..."
docker compose up --build -d

echo "Waiting for services to become healthy..."
./scripts/status.sh --wait

echo "Running smoke tests..."
./scripts/smoke-test.sh

echo "Startup complete! Dashboard available at http://localhost:8000/"
