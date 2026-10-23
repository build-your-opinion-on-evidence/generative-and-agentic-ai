"""
airflow/dags/known_fraud_containment_dag.py
General fraud containment DAG — used for large transaction and suspicious patterns.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta

import httpx
from airflow import DAG
from airflow.operators.python import PythonOperator

_BANKING_API = os.environ.get("FAKE_BANKING_API_URL", "http://fake-banking-api:8001")
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


def _publish_event(event_type: str, correlation_id: str, data: dict) -> None:
    from confluent_kafka import Producer
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
    producer.produce(topic="fraud.orchestration-events.v1" if event_type == "DagCompleted" else "fraud.action-events.v1", key=correlation_id.encode(), value=json.dumps(envelope).encode())
    producer.flush()


def load_context(**context) -> None:
    conf = context["dag_run"].conf or {}
    task_id = conf.get("task_id", "unknown")
    ctx_data = conf.get("context", {})
    ti = context["ti"]
    ti.xcom_push(key="task_ctx", value={
        "task_id": task_id,
        "card_id": ctx_data.get("card_id"),
        "customer_id": ctx_data.get("customer_id"),
        "correlation_id": conf.get("correlation_id", ""),
        "risk_score": ctx_data.get("risk_score", 0),
    })
    print(f"[load_context] task_id={task_id}")


def get_customer_and_card(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    customer_id = ctx.get("customer_id")
    card_id = ctx.get("card_id")

    customer = _get(f"{_BANKING_API}/customers/{customer_id}") if customer_id else {}
    card = _get(f"{_BANKING_API}/cards/{card_id}") if card_id else {}
    transactions = _get(f"{_BANKING_API}/transactions/by-card/{card_id}?limit=30") if card_id else []

    ti.xcom_push(key="customer", value=customer)
    ti.xcom_push(key="card", value=card)
    ti.xcom_push(key="transactions", value=transactions)
    print(f"[get_customer_and_card] customer={customer.get('customer_id')} card_status={card.get('status')}")


def score_fraud(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")

    result = _post(f"{_FRAUD_SCORING_AGENT}/invoke", {
        "request_id": f"req-{uuid.uuid4().hex[:8]}",
        "task_id": ctx["task_id"],
        "correlation_id": ctx["correlation_id"],
        "agent_id": "fraud-scoring-agent",
        "prompt_version": "v1",
        "input": {
            "customer_id": ctx.get("customer_id"),
            "card_id": ctx.get("card_id"),
            "triggering_event": ctx,
        },
        "constraints": {"max_steps": 5, "max_tokens": 1200, "timeout_seconds": 45},
    })
    confidence = result.get("confidence", 0.5)
    ti.xcom_push(key="fraud_score_result", value=result)
    print(f"[score_fraud] status={result.get('status')} confidence={confidence}")


def check_policy(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")

    policy = _post(f"{_POLICY_ENGINE}/evaluate", {
        "task_id": ctx["task_id"],
        "correlation_id": ctx["correlation_id"],
        "action_type": "FREEZE_CARD",
        "action_payload": {"card_id": ctx.get("card_id")},
        "requested_by": "known-fraud-containment-dag",
    })
    ti.xcom_push(key="policy_decision", value=policy)
    print(f"[check_policy] decision={policy.get('decision')}")


def execute_containment(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    policy = ti.xcom_pull(key="policy_decision") or {}
    decision = policy.get("decision", "REQUIRE_APPROVAL")
    card_id = ctx.get("card_id")

    if decision == "AUTO_ALLOW" and card_id:
        result = _post(f"{_BANKING_API}/cards/{card_id}/freeze", {"reason": f"fraud_containment:{ctx['task_id']}"})
        print(f"[execute_containment] Card frozen: {result}")
        _publish_event("CardFrozen", ctx["correlation_id"], {"task_id": ctx["task_id"], "card_id": card_id})
    else:
        print(f"[execute_containment] Decision={decision} — no auto-action taken")


def send_notification(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    customer_id = ctx.get("customer_id")
    card_id = ctx.get("card_id")

    if not customer_id:
        print("[send_notification] No customer_id — skipping")
        return

    result = _post(f"{_NOTIFICATION_AGENT}/invoke", {
        "request_id": f"req-notif-{uuid.uuid4().hex[:6]}",
        "task_id": ctx["task_id"],
        "correlation_id": ctx["correlation_id"],
        "agent_id": "notification-agent",
        "prompt_version": "v1",
        "input": {
            "customer_id": customer_id,
            "card_id": card_id or "****",
            "notification_type": "FRAUD_ALERT",
            "channel": "push",
        },
        "constraints": {"max_steps": 1, "max_tokens": 300, "timeout_seconds": 15},
    })
    print(f"[send_notification] status={result.get('status')}")


def complete_dag(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    _publish_event("DagCompleted", ctx.get("correlation_id", ""), {
        "task_id": ctx["task_id"],
        "dag_id": "known_fraud_containment_dag",
        "dag_run_id": context["dag_run"].run_id,
    })
    print(f"[complete_dag] DAG finished for task_id={ctx['task_id']}")


with DAG(
    dag_id="known_fraud_containment_dag",
    description="General fraud containment for suspicious/large transactions",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud", "general"],
    max_active_runs=10,
) as dag:

    t1 = PythonOperator(task_id="load_context",          python_callable=load_context)
    t2 = PythonOperator(task_id="get_customer_and_card", python_callable=get_customer_and_card)
    t3 = PythonOperator(task_id="score_fraud",           python_callable=score_fraud)
    t4 = PythonOperator(task_id="check_policy",          python_callable=check_policy)
    t5 = PythonOperator(task_id="execute_containment",   python_callable=execute_containment)
    t6 = PythonOperator(task_id="send_notification",     python_callable=send_notification)
    t7 = PythonOperator(task_id="complete_dag",          python_callable=complete_dag)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
