-- 200-Level Market Depth Storage
-- Stores all 200 bid and ask levels for each snapshot

CREATE TABLE IF NOT EXISTS depth_levels (
    -- Timestamp
    time TIMESTAMPTZ NOT NULL,
    
    -- Instrument identification
    security_id INT NOT NULL,
    instrument_name TEXT,
    
    -- Depth side and level
    side VARCHAR(3) NOT NULL,  -- 'BID' or 'ASK'
    level INT NOT NULL,        -- 1-200 (1 = closest to mid price)
    
    -- Order book data at this level
    price NUMERIC(12, 2) NOT NULL,
    quantity INT NOT NULL,
    orders INT NOT NULL,
    
    -- Constraints
    PRIMARY KEY (time, security_id, side, level),
    CHECK (side IN ('BID', 'ASK')),
    CHECK (level >= 1 AND level <= 200)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable(
    'depth_levels',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_depth_levels_time_side 
ON depth_levels (time DESC, side);

CREATE INDEX IF NOT EXISTS idx_depth_levels_price 
ON depth_levels (security_id, price, time DESC);

CREATE INDEX IF NOT EXISTS idx_depth_levels_level
ON depth_levels (security_id, side, level, time DESC);

-- Index for order concentration queries
CREATE INDEX IF NOT EXISTS idx_depth_levels_orders
ON depth_levels (security_id, time DESC, orders DESC);

-- Compression policy (compress after 7 days)
ALTER TABLE depth_levels SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'security_id, side, level',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('depth_levels', INTERVAL '7 days');

-- Retention policy (drop data older than 60 days)
SELECT add_retention_policy('depth_levels', INTERVAL '60 days');

-- Create aggregated snapshots view for quick summary queries
CREATE MATERIALIZED VIEW IF NOT EXISTS depth_snapshots_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 second', time) AS bucket,
    security_id,
    side,
    COUNT(*) as levels_count,
    SUM(quantity) as total_quantity,
    SUM(orders) as total_orders,
    AVG(orders) as avg_orders_per_level,
    SUM(price * quantity) / NULLIF(SUM(quantity), 0) as vwap,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM depth_levels
GROUP BY bucket, security_id, side;

-- Refresh policy for materialized view (refresh every 10 seconds)
SELECT add_continuous_aggregate_policy('depth_snapshots_summary',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 second',
    schedule_interval => INTERVAL '10 seconds');

-- Useful queries examples:
COMMENT ON TABLE depth_levels IS 
'Stores 200-level market depth snapshots. 
Example queries:
1. Get all levels at specific time:
   SELECT * FROM depth_levels WHERE time = ''2025-12-29 14:19:00'' AND security_id = 49543;

2. Find price levels with most orders:
   SELECT price, AVG(orders) as avg_orders 
   FROM depth_levels 
   WHERE time BETWEEN ''14:19:00'' AND ''14:20:00'' 
   GROUP BY price 
   ORDER BY avg_orders DESC LIMIT 10;

3. Track order changes at specific price:
   SELECT time, side, orders, quantity 
   FROM depth_levels 
   WHERE price = 25950.00 
   ORDER BY time;
';
