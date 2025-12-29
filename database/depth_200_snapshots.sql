-- 200-Level Market Depth Snapshots Table
-- Stores aggregated metrics from 200-level orderbook data

CREATE TABLE IF NOT EXISTS depth_200_snapshots (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    security_id INTEGER NOT NULL,
    instrument_name VARCHAR(100),
    
    -- Top of book
    best_bid NUMERIC(12, 2),
    best_ask NUMERIC(12, 2),
    spread NUMERIC(12, 2),
    
    -- Aggregated quantities (sum of 200 levels)
    total_bid_qty INTEGER,
    total_ask_qty INTEGER,
    
    -- Order counts
    total_bid_orders INTEGER,
    total_ask_orders INTEGER,
    
    -- Order flow metrics
    imbalance_ratio NUMERIC(10, 4),
    avg_bid_order_size NUMERIC(12, 2),
    avg_ask_order_size NUMERIC(12, 2),
    
    -- Volume-weighted prices
    bid_vwap NUMERIC(12, 2),
    ask_vwap NUMERIC(12, 2),
    
    -- Liquidity concentration (level where 50% volume accumulated)
    bid_50pct_level INTEGER,
    ask_50pct_level INTEGER
);

-- Hypertable for time-series optimization
SELECT create_hypertable('depth_200_snapshots', 'timestamp', if_not_exists => TRUE);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_depth_200_timestamp ON depth_200_snapshots (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_depth_200_security ON depth_200_snapshots (security_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_depth_200_imbalance ON depth_200_snapshots (imbalance_ratio, timestamp DESC);

-- Retention policy: Keep 90 days of 200-depth data (high volume)
SELECT add_retention_policy('depth_200_snapshots', INTERVAL '90 days', if_not_exists => TRUE);

COMMENT ON TABLE depth_200_snapshots IS 'Aggregated metrics from 200-level market depth snapshots';
COMMENT ON COLUMN depth_200_snapshots.imbalance_ratio IS 'total_bid_qty / total_ask_qty - measures buying vs selling pressure';
COMMENT ON COLUMN depth_200_snapshots.bid_vwap IS 'Volume-weighted average price across 200 bid levels';
COMMENT ON COLUMN depth_200_snapshots.bid_50pct_level IS 'Level number where 50% of bid volume is concentrated (1-200)';
