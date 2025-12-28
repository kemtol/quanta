#!/bin/bash
# Quick script to check compressed parts status

echo "=== Checking compressed parts for yesterday ==="
YESTERDAY=$(date -u -d "yesterday" +%Y-%m-%d)
echo "Target date: $YESTERDAY"
echo ""

# We'll use the worker's R2 binding via a temporary endpoint
# For now, let's just trigger and see the response
curl -s -X POST "https://fut-taping-agregator.mkemalw.workers.dev/compress?date=$YESTERDAY" | jq .

echo ""
echo "=== Sanity Info ==="
curl -s "https://fut-taping-agregator.mkemalw.workers.dev/r2-monitor-futures-comp" | jq .
