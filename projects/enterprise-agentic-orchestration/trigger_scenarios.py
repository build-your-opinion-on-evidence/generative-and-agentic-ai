import urllib.request
import time

def wait_for_api():
    print("Waiting for API Gateway to be ready...")
    for _ in range(60):
        try:
            req = urllib.request.Request("http://localhost:8000/healthz")
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    print("API Gateway is ready!")
                    return True
        except Exception:
            pass
        time.sleep(2)
    return False

def trigger(scenario):
    print(f"Triggering Scenario {scenario}...")
    req = urllib.request.Request(f"http://localhost:8000/api/scenarios/{scenario}", method="POST")
    try:
        with urllib.request.urlopen(req) as response:
            print(f"Scenario {scenario} triggered: HTTP {response.status}")
    except Exception as e:
        print(f"Failed to trigger Scenario {scenario}: {e}")

if wait_for_api():
    time.sleep(10) # Give python services time to fully boot after proxy is up
    trigger("A")
    time.sleep(2)
    trigger("B")
