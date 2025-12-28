#!/bin/bash
# Quick check of consolidated features

WORKER_URL="https://fut-features.mkemalw.workers.dev"

echo "📊 Consolidating December 2024..."
RESULT=$(curl -s -X POST "$WORKER_URL/consolidate?month=2024-12")
echo "$RESULT" | jq

echo ""
echo "Note: To view the features.json file, you'll need to:"
echo "1. Access R2 bucket directly via Cloudflare dashboard"
echo "2. Or create a GET endpoint in the worker to fetch from R2"
echo "3. Or use wrangler r2 object get command"
