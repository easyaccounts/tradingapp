#!/bin/bash
# Analyze Depth Data Clusters - Shell Script Version
# Run: bash analyze_depth_clusters.sh

echo "Connecting to database..."

# Use docker exec to run psql inside TimescaleDB container
PSQL_CMD="docker exec tradingapp-timescaledb psql -U tradinguser -d tradingdb"

echo ""
echo "================================================================================"
echo "DEPTH CLUSTER ANALYSIS - TODAY'S DATA"
echo "================================================================================"
echo ""

# Get time range and data count
echo "Data Summary:"
$PSQL_CMD << EOF
SELECT 
    'First Record: ' || MIN(time)::text,
    'Last Record: ' || MAX(time)::text,
    'Total Records: ' || COUNT(*)::text,
    'Unique Snapshots: ' || COUNT(DISTINCT time)::text,
    'Duration (minutes): ' || EXTRACT(EPOCH FROM (MAX(time) - MIN(time)))/60::text
FROM depth_levels_200
WHERE time >= CURRENT_DATE;
EOF

echo ""
echo "Global Statistics:"
$PSQL_CMD << EOF
SELECT 
    'Average Orders per Level: ' || AVG(orders)::numeric(10,1)::text,
    'Max Orders Seen: ' || MAX(orders)::text,
    'Min Orders Seen: ' || MIN(orders)::text,
    'Total Levels Recorded: ' || COUNT(*)::text
FROM depth_levels_200
WHERE time >= CURRENT_DATE;
EOF

echo ""
echo "================================================================================"
echo "TOP 25 PRICE CLUSTERS (by average order concentration)"
echo "================================================================================"
echo ""

# Find top price clusters
$PSQL_CMD << EOF
WITH price_clusters AS (
    SELECT 
        ROUND(price * 2) / 2 as price_level,
        side,
        AVG(orders) as avg_orders,
        MAX(orders) as max_orders,
        AVG(quantity) as avg_quantity,
        COUNT(*) as appearances
    FROM depth_levels_200
    WHERE time >= CURRENT_DATE
    GROUP BY price_level, side
    HAVING AVG(orders) >= 20
),
global_avg AS (
    SELECT AVG(orders) as avg_all
    FROM depth_levels_200
    WHERE time >= CURRENT_DATE
)
SELECT 
    '₹' || price_level::numeric(10,2)::text as "Price",
    CASE WHEN side = 'bid' THEN 'BID' ELSE 'ASK' END as "Side",
    avg_orders::numeric(10,1)::text || ' orders' as "Avg Orders",
    max_orders::text || ' orders' as "Max Orders",
    avg_quantity::numeric(10,0)::text as "Avg Qty",
    appearances::text || 'x' as "Appearances",
    (avg_orders / ga.avg_all)::numeric(10,1)::text || 'x avg' as "Strength"
FROM price_clusters pc
CROSS JOIN global_avg ga
ORDER BY avg_orders DESC
LIMIT 25;
EOF

echo ""
echo "================================================================================"
echo "SUPPORT & RESISTANCE LEVELS (relative to current price)"
echo "================================================================================"
echo ""

# Get current price and nearby levels
$PSQL_CMD << EOF
WITH current_price AS (
    SELECT (best_bid + best_ask) / 2.0 as mid_price
    FROM depth_200_snapshots
    ORDER BY timestamp DESC
    LIMIT 1
),
price_clusters AS (
    SELECT 
        ROUND(price * 2) / 2 as price_level,
        side,
        AVG(orders) as avg_orders,
        COUNT(*) as appearances
    FROM depth_levels_200
    WHERE time >= CURRENT_DATE
    GROUP BY price_level, side
    HAVING AVG(orders) >= 20
)
SELECT 
    'Current Price: ₹' || cp.mid_price::numeric(10,2)::text as "Market Info"
FROM current_price cp
UNION ALL
SELECT '---SUPPORT LEVELS (below current price)---'
UNION ALL
SELECT 
    '  ₹' || pc.price_level::numeric(10,2)::text || 
    ' - ' || pc.avg_orders::numeric(10,0)::text || ' orders avg' ||
    ' (−₹' || (cp.mid_price - pc.price_level)::numeric(10,2)::text || ')'
FROM price_clusters pc, current_price cp
WHERE pc.price_level < cp.mid_price
ORDER BY pc.price_level DESC
LIMIT 5
UNION ALL
SELECT ''
UNION ALL
SELECT '---RESISTANCE LEVELS (above current price)---'
UNION ALL
SELECT 
    '  ₹' || pc.price_level::numeric(10,2)::text || 
    ' - ' || pc.avg_orders::numeric(10,0)::text || ' orders avg' ||
    ' (+₹' || (pc.price_level - cp.mid_price)::numeric(10,2)::text || ')'
FROM price_clusters pc, current_price cp
WHERE pc.price_level > cp.mid_price
ORDER BY pc.price_level ASC
LIMIT 5;
EOF

echo ""
echo "✓ Analysis complete"
echo ""
