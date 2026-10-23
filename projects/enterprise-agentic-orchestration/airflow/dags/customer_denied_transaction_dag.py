"""
airflow/dags/customer_denied_transaction_dag.py
DAG for handling a denied transaction and customer dispute resolution.
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
_NOTIFICATION_AGENT = os.environ.get("NOTIFICATION_AGENT_URL", "http://notification-agent:8015")
_KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

default_args = {
    "owner": "fraud-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


def _post(url: str, data: dict) -> dict:
    resp = httpx.post(url, json=data, timeout=30)
    resp.raise_for_status()
    return resp.json()


def load_and_verify(**context) -> None:
    conf = context["dag_run"].conf or {}
    ti = context["ti"]
    ctx = {
        "task_id": conf.get("task_id", "unknown"),
        "customer_id": conf.get("customer_id") or conf.get("context", {}).get("customer_id"),
        "card_id": conf.get("card_id") or conf.get("context", {}).get("card_id"),
        "transaction_id": conf.get("context", {}).get("transaction_id"),
        "correlation_id": conf.get("correlation_id", ""),
    }
    ti.xcom_push(key="task_ctx", value=ctx)
    print(f"[load_and_verify] task_id={ctx['task_id']}")


def check_card_status(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    card_id = ctx.get("card_id")
    if card_id:
        resp = httpx.get(f"{_BANKING_API}/cards/{card_id}", timeout=10)
        card = resp.json() if resp.status_code == 200 else {}
        ti.xcom_push(key="card", value=card)
        print(f"[check_card_status] card_id={card_id} status={card.get('status')}")


def notify_declined(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    customer_id = ctx.get("customer_id")
    if not customer_id:
        return

    result = _post(f"{_NOTIFICATION_AGENT}/invoke", {
        "request_id": f"req-{uuid.uuid4().hex[:6]}",
        "task_id": ctx["task_id"],
        "correlation_id": ctx["correlation_id"],
        "agent_id": "notification-agent",
        "prompt_version": "v1",
        "input": {
            "customer_id": customer_id,
            "card_id": ctx.get("card_id", "****"),
            "notification_type": "FRAUD_ALERT",
            "channel": "sms",
        },
        "constraints": {"max_steps": 1, "max_tokens": 300, "timeout_seconds": 15},
    })
    print(f"[notify_declined] status={result.get('status')}")


def mark_complete(**context) -> None:
    ti = context["ti"]
    ctx = ti.xcom_pull(key="task_ctx")
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": _KAFKA_BOOTSTRAP})
    p.produce("fraud.orchestration-events.v1", key=ctx["correlation_id"].encode(), value=json.dumps({
        "specversion": "1.0", "id": f"evt-{uuid.uuid4().hex[:8]}", "type": "DagCompleted",
        "source": "airflow-dag", "time": datetime.utcnow().isoformat() + "Z",
        "correlation_id": ctx["correlation_id"], "trace_id": f"trace-{uuid.uuid4().hex[:8]}",
        "schema_version": "1.0",
        "data": {"task_id": ctx["task_id"], "dag_id": "customer_denied_transaction_dag"},
    }).encode())
    p.flush()
    print(f"[mark_complete] task_id={ctx['task_id']}")


with DAG(
    dag_id="customer_denied_transaction_dag",
    description="Handle customer denied transaction dispute",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud", "denied-tx"],
    max_active_runs=10,
) as dag:

    t1 = PythonOperator(task_id="load_and_verify",    python_callable=load_and_verify)
    t2 = PythonOperator(task_id="check_card_status",  python_callable=check_card_status)
    t3 = PythonOperator(task_id="notify_declined",    python_callable=notify_declined)
    t4 = PythonOperator(task_id="mark_complete",      python_callable=mark_complete)

    t1 >> t2 >> t3 >> t4
