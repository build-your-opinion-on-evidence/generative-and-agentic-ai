"""
airflow/dags/stolen_card_response_dag.py
Scenario A: Known stolen-card process — deterministic Airflow DAG.

Flow:
  load_task_context
    → retrieve_customer_profile
    → retrieve_card_and_transactions
    → run_fraud_score
    → evaluate_policy
    → freeze_card
    → notify_customer
    → publish_completion_event
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import httpx
from airflow import DAG
from airflow.operators.python import PythonOperator

_BANKING_API = os.environ.get("FAKE_BANKING_API_URL", "http://fake-banking-api:8001")
_AGENT_DISCOVERY = os.environ.get("AGENT_DISCOVERY_URL", "http://agent-discovery:8006")
_FRAUD_SCORING_AGENT = os.environ.get("FRAUD_SCORING_AGENT_URL", "http://fraud-scoring-agent:8013")
_CARD_CONTROL_AGENT = os.environ.get("CARD_CONTROL_AGENT_URL", "http://card-control-agent:8014")
_NOTIFICATION_AGENT = os.environ.get("NOTIFICATION_AGENT_URL", "http://notification-agent:8015")
_POLICY_ENGINE = os.environ.get("POLICY_ENGINE_URL", "http://policy-engine:8020")
_KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

default_args = {
    "owner": "fraud-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
}


def _get(url: str) -> dict | list:
    resp = httpx.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _post(url: str, data: dict) -> dict:
    resp = httpx.post(url, json=data, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _publish_kafka_event(event_type: str, correlation_id: str, data: dict) -> None:
    """Publish event to Kafka from within a DAG task."""
    from confluent_kafka import Producer
    import uuid

    producer = Producer({"bootstrap.servers": _KAFKA_BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    envelope = {
        "specversion": "1.0",
        "id": f"evt-{uuid.uuid4().hex[:8]}",
        "type": event_type,
        "source": "airflow-dag",
        "time": datetime.utcnow().isoformat() + "Z",
        "correlation_id": correlation_id,
        "trace_id": f"trace-dag-{uuid.uuid4().hex[:8]}",
        "schema_version": "1.0",
        "data": data,
    }
    producer.produce(
        topic="fraud.orchestration-events.v1" if event_type == "DagCompleted" else "fraud.action-events.v1",
        key=correlation_id.encode(),
        value=json.dumps(envelope).encode(),
    )
    producer.flush()


# ── Task functions ────────────────────────────────────────────────────────────

def load_task_context(**context) -> dict:
    conf = context["dag_run"].conf or {}
    task_id = conf.get("task_id", "unknown")
    card_id = conf.get("card_id") or conf.get("context", {}).get("card_id")
    customer_id = conf.get("customer_id") or conf.get("context", {}).get("customer_id")
    correlation_id = conf.get("correlation_id", f"customer:{customer_id}/card:{card_id}")

    print(f"[load_task_context] task_id={task_id} card_id={card_id} customer_id={customer_id}")
    context["ti"].xcom_push(key="task_ctx", value={
        "task_id": task_id,
        "card_id": card_id,
        "customer_id": customer_id,
        "correlation_id": correlation_id,
    })
    return {"task_id": task_id, "card_id": card_id, "customer_id": customer_id}


def retrieve_customer_profile(**context) -> None:
    ctx = context["ti"].xcom_pull(key="task_ctx")
    customer_id = ctx.get("customer_id")
    if not customer_id:
        raise ValueError("customer_id not in task context")
    customer = _get(f"{_BANKING_API}/customers/{customer_id}")
    print(f"[retrieve_customer_profile] customer_id={customer_id} risk_segment={customer.get('risk_segment')}")
    context["ti"].xcom_push(key="customer", value=customer)


def retrieve_card_and_transactions(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    card_id = ctx.get("card_id")
    customer_id = ctx.get("customer_id")

    card = _get(f"{_BANKING_API}/cards/{card_id}")
    transactions = _get(f"{_BANKING_API}/transactions/by-card/{card_id}?limit=20")
    print(f"[retrieve_card_and_transactions] card_id={card_id} status={card.get('status')} tx_count={len(transactions)}")
    ti.xcom_push(key="card", value=card)
    ti.xcom_push(key="transactions", value=transactions)


def run_fraud_score(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    customer = ti.xcom_pull(key="customer") or {}
    transactions = ti.xcom_pull(key="transactions") or []

    result = _post(f"{_FRAUD_SCORING_AGENT}/invoke", {
        "request_id": f"req-dag-{ctx['task_id']}",
        "task_id": ctx["task_id"],
        "correlation_id": ctx["correlation_id"],
        "agent_id": "fraud-scoring-agent",
        "prompt_version": "v1",
        "input": {
            "customer_id": ctx.get("customer_id"),
            "card_id": ctx.get("card_id"),
            "triggering_event": {"type": "CardReportedStolen"},
        },
        "constraints": {"max_steps": 5, "max_tokens": 1200, "timeout_seconds": 45},
    })
    fraud_score = result.get("findings", [{}])[0].get("weight", 0) if result.get("findings") else 75
    print(f"[run_fraud_score] agent_status={result.get('status')} confidence={result.get('confidence')}")
    ti.xcom_push(key="fraud_score_result", value=result)
    ti.xcom_push(key="fraud_score", value=fraud_score)


def evaluate_policy(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")

    policy_result = _post(f"{_POLICY_ENGINE}/evaluate", {
        "task_id": ctx["task_id"],
        "correlation_id": ctx["correlation_id"],
        "action_type": "FREEZE_CARD",
        "action_payload": {"card_id": ctx.get("card_id")},
        "requested_by": "stolen-card-dag",
    })
    print(f"[evaluate_policy] decision={policy_result.get('decision')} rule={policy_result.get('rule_id')}")
    ti.xcom_push(key="policy_decision", value=policy_result)


def freeze_card(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    policy_decision = ti.xcom_pull(key="policy_decision") or {}
    decision = policy_decision.get("decision", "REQUIRE_APPROVAL")

    card_id = ctx.get("card_id")
    if decision in ("AUTO_ALLOW",):
        result = _post(f"{_BANKING_API}/cards/{card_id}/freeze", {"reason": f"stolen_card:{ctx['task_id']}"})
        print(f"[freeze_card] card_id={card_id} result={result}")
        _publish_kafka_event("CardFrozen", ctx["correlation_id"], {
            "task_id": ctx["task_id"],
            "card_id": card_id,
            "reason": "stolen_card_dag",
        })
    else:
        print(f"[freeze_card] Approval required — skipping auto-freeze. decision={decision}")


def notify_customer(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    customer_id = ctx.get("customer_id")
    card_id = ctx.get("card_id")

    result = _post(f"{_NOTIFICATION_AGENT}/invoke", {
        "request_id": f"req-notif-{ctx['task_id']}",
        "task_id": ctx["task_id"],
        "correlation_id": ctx["correlation_id"],
        "agent_id": "notification-agent",
        "prompt_version": "v1",
        "input": {
            "customer_id": customer_id,
            "card_id": card_id,
            "notification_type": "CARD_FROZEN",
            "channel": "push",
        },
        "constraints": {"max_steps": 1, "max_tokens": 500, "timeout_seconds": 30},
    })
    print(f"[notify_customer] status={result.get('status')}")


def publish_completion(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    _publish_kafka_event("DagCompleted", ctx["correlation_id"], {
        "task_id": ctx["task_id"],
        "dag_id": "stolen_card_response_dag",
        "dag_run_id": context["dag_run"].run_id,
    })
    print(f"[publish_completion] task_id={ctx['task_id']} — DAG completed successfully")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="stolen_card_response_dag",
    description="Scenario A: deterministic stolen-card containment process",
    default_args=default_args,
    schedule_interval=None,  # Triggered externally by Integration Orchestrator
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud", "scenario-a"],
    max_active_runs=10,
) as dag:

    t1 = PythonOperator(task_id="load_task_context",           python_callable=load_task_context)
    t2 = PythonOperator(task_id="retrieve_customer_profile",   python_callable=retrieve_customer_profile)
    t3 = PythonOperator(task_id="retrieve_card_transactions",  python_callable=retrieve_card_and_transactions)
    t4 = PythonOperator(task_id="run_fraud_score",             python_callable=run_fraud_score)
    t5 = PythonOperator(task_id="evaluate_policy",             python_callable=evaluate_policy)
    t6 = PythonOperator(task_id="freeze_card",                 python_callable=freeze_card)
    t7 = PythonOperator(task_id="notify_customer",             python_callable=notify_customer)
    t8 = PythonOperator(task_id="publish_completion",          python_callable=publish_completion)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
