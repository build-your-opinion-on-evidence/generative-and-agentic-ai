import urllib.request
import urllib.error
import json
import time

def do_req(url, method="GET", body=None):
    req = urllib.request.Request(url, method=method)
    if body:
        req.add_header('Content-Type', 'application/json')
        req.data = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, None

print("Testing Scenario A...")
_, data = do_req("http://localhost:8000/api/scenarios/A", "POST")
cid_a = data["correlation_id"]
print(f"CID A: {cid_a}")

for _ in range(15):
    time.sleep(1)
    status_code, d = do_req(f"http://localhost:8000/api/orchestrator/runs/{cid_a}")
    if status_code == 200 and d:
        print(f"A Status: {d.get('overall_status')}")
        if d.get("overall_status") in ("COMPLETED", "FAILED"):
            break

print("\nTesting Scenario B...")
_, data = do_req("http://localhost:8000/api/scenarios/B", "POST")
cid_b = data["correlation_id"]
print(f"CID B: {cid_b}")

for _ in range(30):
    time.sleep(1)
    status_code, d = do_req(f"http://localhost:8000/api/orchestrator/runs/{cid_b}")
    if status_code == 200 and d:
        status = d.get("overall_status")
        print(f"B Status: {status}")
        
        tr_code, tr = do_req(f"http://localhost:8000/api/orchestrator/runs/{cid_b}/trace?after_sequence=0")
        if tr_code == 200 and tr:
            events = tr.get("events", [])
            if events:
                print(f"  Got {len(events)} trace events.")
                for ev in events:
                    if ev['event_type'] == 'AgentSelected':
                        print(f"    - Agent: {ev['data'].get('selected_agent')}")
        
        if status in ("AWAITING_APPROVAL", "COMPLETED", "FAILED"):
            if status == "AWAITING_APPROVAL":
                print("Fetching approval...")
                ar_code, apps = do_req(f"http://localhost:8000/api/approvals/?correlation_id={cid_b}")
                if ar_code == 200 and apps:
                    for a in apps:
                        if a['status'] == 'PENDING':
                            print(f"Approving {a['approval_id']}...")
                            do_req(f"http://localhost:8000/api/approvals/{a['approval_id']}/decision", "POST", {"decision":"APPROVE", "reviewer_id":"tester"})
            else:
                break
