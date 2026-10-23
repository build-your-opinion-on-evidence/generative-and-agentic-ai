#!/usr/bin/env python3
"""
scripts/publish_scenario.py
Triggers demo scenarios A, B, C, D, or E via the Event Simulator service.
"""
import sys
import argparse
import httpx

EVENT_SIMULATOR = "http://localhost:8030"

def main():
    parser = argparse.ArgumentParser(description="Trigger demo scenarios for Fraud Agentic System")
    parser.add_argument("--scenario", "-s", choices=["A", "B", "C", "D", "E"], required=True, help="Scenario to trigger (A-E)")
    args = parser.parse_args()

    url = f"{EVENT_SIMULATOR}/scenarios/{args.scenario.upper()}"
    print(f"Triggering Scenario {args.scenario.upper()} at {url}...")
    try:
        resp = httpx.post(url, timeout=10)
        resp.raise_for_status()
        print("Scenario triggered successfully:")
        print(resp.json())
    except Exception as e:
        print(f"Failed to trigger scenario: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
