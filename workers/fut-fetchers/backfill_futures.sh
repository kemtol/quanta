#!/bin/bash

# Backfill MNQ and MGC futures data from Yahoo Finance
# Runs multiple calls to avoid Cloudflare Worker CPU timeout
# Each call fetches 2 years max (~500 bars)

BASE_URL="https://fut-fetchers.mkemalw.workers.dev"

echo "=== Backfilling MNQ Futures ==="
echo ""

# MNQ - Call 1: Years 1-2 (most recent 2 years)
echo "[1/3] Fetching MNQ: Years 1-2 (2024-2025)..."
curl -s -X POST "${BASE_URL}/backfill-futures?ticker=MNQ&years=2" | jq
echo ""
sleep 3

# MNQ - Call 2: Older data (will get whatever Yahoo has beyond 2 years)
echo "[2/3] Fetching MNQ: Year 3..."
curl -s -X POST "${BASE_URL}/backfill-futures?ticker=MNQ&years=3" | jq || echo "Timeout or error (expected for 3+ years)"
echo ""
sleep 3

# MNQ - Call 3: Max available
echo "[3/3] Fetching MNQ: Year 4..."
curl -s -X POST "${BASE_URL}/backfill-futures?ticker=MNQ&years=4" | jq || echo "Timeout or error (expected for 4+ years)"
echo ""
sleep 3

echo "=== Backfilling MGC Futures ==="
echo ""

# MGC - Call 1: Years 1-2
echo "[1/3] Fetching MGC: Years 1-2 (2024-2025)..."
curl -s -X POST "${BASE_URL}/backfill-futures?ticker=MGC&years=2" | jq || echo "Timeout or error"
echo ""
sleep 3

# MGC - Call 2: Year 3
echo "[2/3] Fetching MGC: Year 3..."
curl -s -X POST "${BASE_URL}/backfill-futures?ticker=MGC&years=3" | jq || echo "Timeout or error (expected)"
echo ""
sleep 3

# MGC - Call 3: Year 4
echo "[3/3] Fetching MGC: Year 4..."
curl -s -X POST "${BASE_URL}/backfill-futures?ticker=MGC&years=4" | jq || echo "Timeout or error (expected)"
echo ""

echo "=== Backfill Complete ==="
echo ""
echo "Note: Some calls may timeout (Cloudflare limit ~50 sec)"
echo "Data that was successfully written is saved to R2"
echo "Check R2 bucket for raw_MNQ/ and raw_MGC/ directories"
