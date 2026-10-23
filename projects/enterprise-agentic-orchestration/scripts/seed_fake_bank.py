#!/usr/bin/env python3
"""
scripts/seed_fake_bank.py
Seeds initial customer, account, card, and device data into the fake banking API.
"""
import sys
import httpx

BANKING_API = "http://localhost:8001"

def seed():
    print(f"Triggering seed script on {BANKING_API}...")
    try:
        resp = httpx.post(f"{BANKING_API}/seed", timeout=15)
        resp.raise_for_status()
        print("Successfully seeded Banking API database:")
        print(resp.json())
    except Exception as e:
        print(f"Error seeding database: {e}")
        sys.exit(1)

if __name__ == "__main__":
    seed()
