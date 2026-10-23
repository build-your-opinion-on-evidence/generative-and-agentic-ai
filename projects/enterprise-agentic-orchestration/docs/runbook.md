# System Runbook & Operator Guide

## Prerequisites
- Docker & Docker Compose
- Python 3.12+ (for running demo scripts locally)
- `OPENAI_API_KEY` set in `.env`

## Quick Start

### 1. Launch Platform
```bash
cp .env.example .env
# Edit .env and add your OPENAI_API_KEY
docker compose up -d --build
```

### 2. Verify Health
Check container health:
```bash
docker compose ps
```

### 3. Seed Initial Data
```bash
python scripts/seed_fake_bank.py
```

### 4. Run Scenarios
Run interactive demo runner:
```bash
python scripts/run_demo.py
```
Or trigger specific scenarios:
- **Scenario A (Stolen Card - Airflow DAG)**: `python scripts/publish_scenario.py -s A`
- **Scenario B (Ambiguous Fraud - ReAct Path)**: `python scripts/publish_scenario.py -s B`
- **Scenario C (Priority Preemption)**: `python scripts/publish_scenario.py -s C`
- **Scenario D (Idempotency Test)**: `python scripts/publish_scenario.py -s D`
- **Scenario E (Failure & Recovery)**: `python scripts/publish_scenario.py -s E`

## Port Directory
- **Fake Banking API**: `http://localhost:8001`
- **Task Manager**: `http://localhost:8002`
- **Integration Orchestrator**: `http://localhost:8003`
- **Strategy Router**: `http://localhost:8004`
- **ReAct Orchestrator**: `http://localhost:8005`
- **Agent Discovery**: `http://localhost:8006`
- **Customer Profile Agent**: `http://localhost:8010`
- **Transaction History Agent**: `http://localhost:8011`
- **Device Risk Agent**: `http://localhost:8012`
- **Fraud Scoring Agent**: `http://localhost:8013`
- **Card Control Agent**: `http://localhost:8014`
- **Notification Agent**: `http://localhost:8015`
- **Policy Engine**: `http://localhost:8020`
- **Approval Service**: `http://localhost:8021`
- **Event Simulator**: `http://localhost:8030`
- **Airflow Web UI**: `http://localhost:8080` (admin/admin123)
- **Grafana Dashboards**: `http://localhost:3000` (admin/admin)
- **Prometheus Metrics**: `http://localhost:9090`
