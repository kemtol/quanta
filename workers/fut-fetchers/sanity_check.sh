#!/bin/bash
# Sanity check script - verify backfilled data

WORKER_URL="https://fut-fetchers.mkemalw.workers.dev/sanity-check"

# Default values
START_DATE="${1:-2024-01-01}"
END_DATE="${2:-2025-12-16}"
SAMPLES="${3:-20}"

echo "🔍 Running sanity check..."
echo "   Date range: $START_DATE to $END_DATE"
echo "   Samples: $SAMPLES random dates"
echo ""

# Fetch and display summary
RESPONSE=$(curl -s "${WORKER_URL}?start=${START_DATE}&end=${END_DATE}&samples=${SAMPLES}")

echo "📊 Summary:"
echo "$RESPONSE" | jq -r '.sanity_check.summary | to_entries[] | "   \(.key): \(.value.valid)/\(.value.total) valid (\(.value.success_rate))"'

echo ""
echo "📅 Sampled dates:"
echo "$RESPONSE" | jq -r '.sanity_check.config.sampled_dates[]' | head -10
if [ $(echo "$RESPONSE" | jq -r '.sanity_check.config.sampled_dates | length') -gt 10 ]; then
    echo "   ... and more"
fi

echo ""

# Check if there are any issues
MISSING=$(echo "$RESPONSE" | jq '[.sanity_check.summary[].missing] | add')
INVALID=$(echo "$RESPONSE" | jq '[.sanity_check.summary[].invalid] | add')

if [ "$MISSING" -gt 0 ] || [ "$INVALID" -gt 0 ]; then
    echo "⚠️  Found issues:"
    echo "   Missing: $MISSING"
    echo "   Invalid: $INVALID"
    echo ""
    echo "🔎 Details:"
    echo "$RESPONSE" | jq -r '.sanity_check.details | to_entries[] | select(.value[] | (.exists == false or .valid == false)) | "   \(.key): \(.value[] | select(.exists == false or .valid == false) | "\(.date) - \(.error // "invalid")")"'
else
    echo "✅ All checks passed!"
fi
