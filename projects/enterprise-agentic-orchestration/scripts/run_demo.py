#!/usr/bin/env python3
"""
scripts/run_demo.py
Interactive CLI demo runner for the Enterprise Agentic Fraud Orchestration Platform.
"""
import sys
import time
import httpx

SIMULATOR_URL = "http://localhost:8030"
TASK_MANAGER_URL = "http://localhost:8002"
ORCHESTRATOR_URL = "http://localhost:8003"
APPROVAL_URL = "http://localhost:8021"

def print_header(title: str):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def trigger_scenario(name: str):
    print_header(f"Triggering Scenario {name}")
    try:
        r = httpx.post(f"{SIMULATOR_URL}/scenarios/{name}", timeout=10)
        r.raise_for_status()
        res = r.json()
        print(f"[+] Response: {res}")
        return res
    except Exception as e:
        print(f"[-] Error triggering scenario {name}: {e}")
        return None

def monitor_tasks(seconds: int = 5):
    print(f"\nWaiting {seconds} seconds for events to process through Kafka & Task Manager...")
    time.sleep(seconds)
    try:
        r = httpx.get(f"{TASK_MANAGER_URL}/tasks", timeout=10)
        if r.status_code == 200:
            tasks = r.json()
            print_header(f"Recent Tasks ({len(tasks)} found)")
            for t in tasks[:5]:
                print(f" - ID: {t.get('task_id')} | Status: {t.get('status')} | Priority: {t.get('priority')} | Type: {t.get('task_type')}")
    except Exception as e:
        print(f"[-] Could not query tasks: {e}")

def list_pending_approvals():
    try:
        r = httpx.get(f"{APPROVAL_URL}/approvals", timeout=10)
        if r.status_code == 200:
            approvals = r.json()
            print_header(f"Pending Approvals ({len(approvals)} pending)")
            for a in approvals:
                print(f" - Approval ID: {a.get('approval_id')} | Task: {a.get('task_id')} | Action: {a.get('action_type')}")
    except Exception as e:
        print(f"[-] Could not query approvals: {e}")

def main():
    print_header("Enterprise Event-Driven Agentic AI Platform Demo")
    print("1. Scenario A: Known stolen-card process (Deterministic Airflow DAG)")
    print("2. Scenario B: Ambiguous fraud investigation (Dynamic ReAct Path)")
    print("3. Scenario C: Priority preemption (Critical Task Priority)")
    print("4. Scenario D: Idempotency deduplication")
    print("5. Scenario E: Failure & recovery simulation")
    print("6. Show Recent Tasks")
    print("7. Show Pending Approvals")

    if len(sys.argv) > 1:
        choice = sys.argv[1].upper()
    else:
        choice = input("\nSelect scenario/option (A-E / 6-7): ").strip().upper()

    if choice in ["A", "1"]:
        trigger_scenario("A")
        monitor_tasks(3)
    elif choice in ["B", "2"]:
        trigger_scenario("B")
        monitor_tasks(5)
    elif choice in ["C", "3"]:
        trigger_scenario("C")
        monitor_tasks(3)
    elif choice in ["D", "4"]:
        trigger_scenario("D")
        monitor_tasks(3)
    elif choice in ["E", "5"]:
        trigger_scenario("E")
        monitor_tasks(3)
    elif choice in ["6"]:
        monitor_tasks(0)
    elif choice in ["7"]:
        list_pending_approvals()
    else:
        print("Invalid selection.")

if __name__ == "__main__":
    main()
