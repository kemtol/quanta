#!/bin/bash
# Check remaining raw files vs compressed parts

DATE="2025-12-18"
SYMBOL="ENQ"

echo "=== Compressed Parts for $SYMBOL on $DATE ==="
curl -s "https://fut-taping-agregator.mkemalw.workers.dev/list-parts?date=$DATE&symbol=$SYMBOL" | jq '.count'

echo ""
echo "=== Triggering another round to process remaining files ==="
curl -s -X POST "https://fut-taping-agregator.mkemalw.workers.dev/run-cron?mode=housekeeping&force=true"

echo ""
echo "Waiting 30 seconds..."
sleep 30

echo ""
echo "=== Checking status again ==="
curl -s "https://fut-taping-agregator.mkemalw.workers.dev/list-parts?date=$DATE&symbol=$SYMBOL" | jq
