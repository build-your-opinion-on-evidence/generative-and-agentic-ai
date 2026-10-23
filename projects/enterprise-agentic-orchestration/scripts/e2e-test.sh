#!/usr/bin/env bash
set -e
echo "Running End-to-End Test for Scenario A and B..."
RES_A=$(curl -s -X POST http://localhost:8000/api/scenarios/A)
CID_A=$(echo $RES_A | grep -o '"correlation_id":"[^"]*' | cut -d'"' -f4)
if [ -z "$CID_A" ]; then echo "Scenario A Failed to Start"; exit 1; fi
echo "Started A: $CID_A"

RES_B=$(curl -s -X POST http://localhost:8000/api/scenarios/B)
CID_B=$(echo $RES_B | grep -o '"correlation_id":"[^"]*' | cut -d'"' -f4)
if [ -z "$CID_B" ]; then echo "Scenario B Failed to Start"; exit 1; fi
echo "Started B: $CID_B"

echo "End-to-end triggers passed."
