#!/bin/bash

# Parallel bulk delete for old raw futures structure
# Runs deletes in background for faster cleanup

BUCKET="tape-data-futures"
PARALLEL_JOBS=20  # Run 20 deletes at once

echo "=== Cleaning up old futures raw data (PARALLEL) ==="
echo ""

# Function to delete files for a date range
delete_date_range_parallel() {
    local ticker=$1
    local start_date=$2
    local end_date=$3
    
    echo "Deleting ${ticker} from ${start_date} to ${end_date}..."
    
    current_date=$(date -d "$start_date" +%Y-%m-%d)
    end=$(date -d "$end_date" +%Y-%m-%d)
    
    count=0
    running=0
    
    while [ "$current_date" != "$end" ]; do
        # Get year/month/day components
        year=$(date -d "$current_date" +%Y)
        month=$(date -d "$current_date" +%m)
        yyyymmdd=$(date -d "$current_date" +%Y%m%d)
        
        # Build path
        path="raw_${ticker}/daily/${year}/${month}/${yyyymmdd}.json"
        
        # Delete in background
        (npx wrangler r2 object delete "${BUCKET}/${path}" 2>/dev/null && echo -n ".") &
        
        count=$((count + 1))
        running=$((running + 1))
        
        # Limit parallel jobs
        if [ $running -ge $PARALLEL_JOBS ]; then
            wait -n  # Wait for any one job to finish
            running=$((running - 1))
        fi
        
        # Next day
        current_date=$(date -I -d "$current_date + 1 day")
    done
    
    # Wait for remaining jobs
    wait
    
    echo ""
    echo "Processed ${count} ${ticker} files"
    echo ""
}

# Delete MNQ (Dec 2022 - Dec 2025) - parallel!
delete_date_range_parallel "MNQ" "2022-12-01" "2025-12-17"

# Delete MGC (Dec 2023 - Dec 2025) - parallel!
delete_date_range_parallel "MGC" "2023-12-01" "2025-12-17"

echo "Deleting old monthly files..."
npx wrangler r2 object delete "${BUCKET}/MNQ/202512.json" 2>/dev/null && echo "✓ Deleted MNQ/202512.json" &
npx wrangler r2 object delete "${BUCKET}/MGC/202512.json" 2>/dev/null && echo "✓ Deleted MGC/202512.json" &
wait

echo ""
echo "=== Cleanup Complete ===" 
echo ""
echo "Ready for clean backfill!"
echo "Run: ./backfill_futures.sh"
