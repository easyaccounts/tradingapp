#!/bin/bash

echo "Checking depth_200_snapshots data..."
echo ""

# Check total row count
echo "Total snapshots in database:"
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "SELECT COUNT(*) FROM depth_200_snapshots;"

echo ""
echo "Latest 5 snapshots:"
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    timestamp AT TIME ZONE 'Asia/Kolkata' as time_ist,
    instrument_name,
    best_bid,
    best_ask,
    spread,
    imbalance_ratio,
    total_bid_qty,
    total_ask_qty
FROM depth_200_snapshots 
ORDER BY timestamp DESC 
LIMIT 5;
"

echo ""
echo "Data collection stats (last 10 minutes):"
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    COUNT(*) as snapshots,
    MIN(timestamp AT TIME ZONE 'Asia/Kolkata') as first_snapshot,
    MAX(timestamp AT TIME ZONE 'Asia/Kolkata') as last_snapshot,
    ROUND(AVG(imbalance_ratio)::numeric, 2) as avg_imbalance,
    ROUND(AVG(spread)::numeric, 2) as avg_spread
FROM depth_200_snapshots 
WHERE timestamp > NOW() - INTERVAL '10 minutes';
"
