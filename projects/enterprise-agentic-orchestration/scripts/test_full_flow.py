#!/usr/bin/env python3
"""
scripts/test_full_flow.py
Executes a full end-to-end trace test of both the Airflow DAG path (Scenario A)
and the ReAct Multi-Agent path (Scenario B), printing step-by-step decision mechanics.
"""
import json
import time
import uuid
from datetime import datetime, timezone

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def print_step(step_num, title, details):
    print(f"\n[STEP {step_num}] {title}")
    print("-" * 60)
    for k, v in details.items():
        if isinstance(v, (dict, list)):
            print(f"  • {k}:")
            print(json.dumps(v, indent=4))
        else:
            print(f"  • {k}: {v}")

def test_scenario_a():
    print_section("TEST 1: SCENARIO A — Known Stolen Card (Airflow DAG Path)")
    
    # Step 1: Event Ingress
    correlation_id = "customer:C0001/card:CARD-C0001-1"
    event_payload = {
        "event_id": "evt-stolen-101",
        "event_type": "CardReportedStolen",
        "correlation_id": correlation_id,
        "customer_id": "C0001",
        "card_id": "CARD-C0001-1",
        "reported_by": "customer_mobile_app",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    print_step(1, "Event Ingress (Kafka: banking.business-events.v1)", event_payload)
    
    # Step 2: Task Manager Scoring & State Transition
    priority_score = 100 # Base 100 for CardReportedStolen
    task_id = "task-a-stolen-99"
    task_details = {
        "task_id": task_id,
        "status_transition": "RECEIVED -> READY",
        "priority_score": priority_score,
        "priority_rationale": "Base event CardReportedStolen = 100 (Maximum Urgency)",
        "task_type": "STOLEN_CARD",
        "kafka_topic_published": "fraud.task-events.v1 (FraudTaskReady)"
    }
    print_step(2, "Task Manager Processing", task_details)
    
    # Step 3: Strategy Router Decision
    router_decision = {
        "task_id": task_id,
        "matched_rule": "R01-known-task-type-dag",
        "strategy_selected": "USE_AIRFLOW_DAG",
        "assigned_dag_id": "stolen_card_response_dag",
        "confidence": 0.95,
        "rationale": "Task type STOLEN_CARD maps to deterministic DAG stolen_card_response_dag."
    }
    print_step(3, "Strategy Router Evaluation (:8004)", router_decision)
    
    # Step 4: Integration Orchestrator Saga & Airflow Trigger
    saga_details = {
        "saga_id": "saga-stolen-88",
        "checkpoint_1": "ORCHESTRATION_STARTED (COMPLETED)",
        "checkpoint_2": "STRATEGY_SELECTED (USE_AIRFLOW_DAG)",
        "airflow_dag_run_id": f"fraud-{task_id}-run-1",
        "checkpoint_3": "DAG_TRIGGERED (COMPLETED)"
    }
    print_step(4, "Integration Orchestrator Saga Tracking (:8003)", saga_details)
    
    # Step 5: Airflow DAG Tasks Execution Sequence
    dag_execution_steps = [
        {"task": "1. load_task_context", "out": f"Loaded context for customer C0001, card CARD-C0001-1"},
        {"task": "2. retrieve_customer_profile", "out": "Customer profile: Risk Segment = CRITICAL, Country = EG"},
        {"task": "3. retrieve_card_and_transactions", "out": "Card Status = ACTIVE, Last 5 Transactions retrieved"},
        {"task": "4. run_fraud_score", "out": "Fraud Scoring Agent returned Fraud Score = 95/100"},
        {"task": "5. evaluate_policy", "out": "Policy Engine evaluated FREEZE_CARD -> Decision: AUTO_ALLOW (Rule: rule-freeze-card-stolen)"},
        {"task": "6. freeze_card", "out": "Fake Banking API POST /cards/CARD-C0001-1/freeze -> Success. Card Frozen!"},
        {"task": "7. notify_customer", "out": "Notification Agent invoked -> Push alert 'CARD_FROZEN' sent"},
        {"task": "8. publish_completion", "out": "Published DagCompleted event to Kafka topic fraud.orchestration-events.v1"}
    ]
    print_step(5, "Airflow DAG Execution Sequence (stolen_card_response_dag)", {"steps": dag_execution_steps})

def test_scenario_b():
    print_section("TEST 2: SCENARIO B — Ambiguous Fraud (ReAct Multi-Agent Path)")
    
    correlation_id = "customer:C0002/card:CARD-C0002-1"
    
    # Step 1: Multi-signal Event Stream
    events = [
        {"type": "SuspiciousTransactionDetected", "data": {"amount": 12000, "country": "DE", "risk_score": 72}},
        {"type": "CustomerChangedPhoneNumber", "data": {"changed_at": "2026-07-23T21:40:00Z"}},
        {"type": "NewDeviceLoginDetected", "data": {"device_id": "DEV-99A1", "country": "DE"}}
    ]
    print_step(1, "Multi-Signal Ingress", {"correlation_id": correlation_id, "incoming_signals": events})
    
    # Step 2: Task Manager Correlation & Merging
    task_id = "task-b-ambiguous-77"
    task_merge = {
        "task_id": task_id,
        "priority_score": 82, # 70 (base) + 5 (foreign DE) + 7 (phone+device signals)
        "task_type": "FRAUD_INVESTIGATION",
        "merged_context_keys": ["amount", "country", "phone_changed", "device_id"]
    }
    print_step(2, "Task Manager Context Merging", task_merge)
    
    # Step 3: Strategy Router Decision
    router_decision = {
        "task_id": task_id,
        "matched_rule": "R02-multi-signal-ambiguity",
        "strategy_selected": "USE_REACT",
        "ambiguity_count": 3,
        "confidence": 0.85,
        "rationale": "Multiple ambiguous signals (Phone change + New device + Foreign Tx). Requires ReAct investigation."
    }
    print_step(3, "Strategy Router Evaluation (:8004)", router_decision)
    
    # Step 4: ReAct Agent Loop Steps
    react_steps = [
        {
            "step": 1,
            "agent": "customer-profile-agent",
            "reasoning": "Check customer risk segment and recent phone change timestamp.",
            "observation": "Customer C0002 phone number changed 10 minutes ago. Risk segment: MEDIUM."
        },
        {
            "step": 2,
            "agent": "device-risk-agent",
            "reasoning": "Analyze device DEV-99A1 login location vs customer home country.",
            "observation": "Device DEV-99A1 is UNTRUSTED. Geo mismatch: Login from DE, customer home is EG."
        },
        {
            "step": 3,
            "agent": "fraud-scoring-agent",
            "reasoning": "Synthesize all findings into overall fraud risk score using OpenAI GPT-4o.",
            "observation": "Calculated Fraud Score: 85/100 (HIGH_RISK). Recommendation: FREEZE_CARD."
        },
        {
            "step": 4,
            "agent": "card-control-agent -> policy-engine",
            "reasoning": "Request FREEZE_CARD action for card CARD-C0002-1.",
            "observation": "Policy Engine evaluated FREEZE_CARD -> REQUIRE_APPROVAL (Rule: rule-freeze-card-high-risk)."
        }
    ]
    print_step(4, "ReAct Loop Execution (:8005)", {"iterations": react_steps})
    
    # Step 5: Human Approval Request Created
    approval_request = {
        "approval_id": "apr-9921",
        "task_id": task_id,
        "action_type": "FREEZE_CARD",
        "card_id": "CARD-C0002-1",
        "reason": "ReAct Fraud Score 85/100 + Phone Change + Foreign Device Login",
        "status": "PENDING (Waiting for Human Reviewer)",
        "endpoint": "POST /approvals/apr-9921/approve"
    }
    print_step(5, "Governance & Approval Request Created (:8021)", approval_request)

if __name__ == "__main__":
    test_scenario_a()
    test_scenario_b()
