-- 200-Level Market Depth Storage
-- Stores individual price levels from order book

CREATE TABLE IF NOT EXISTS depth_levels_200 (
    time TIMESTAMPTZ NOT NULL,
    security_id INT NOT NULL,
    instrument_name TEXT,
    
    -- Side and level identification
    side VARCHAR(3) NOT NULL,  -- 'BID' or 'ASK'
    level_num INT NOT NULL,     -- 1 to 200
    
    -- Level data
    price NUMERIC(12, 2) NOT NULL,
    quantity INT NOT NULL,
    orders INT NOT NULL,
    
    -- Composite primary key
    PRIMARY KEY (time, security_id, side, level_num)
);

-- Convert to TimescaleDB hypertable
SELECT create_hypertable(
    'depth_levels_200', 
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_depth_levels_security_time 
ON depth_levels_200 (security_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_depth_levels_price 
ON depth_levels_200 (security_id, price, time DESC);

CREATE INDEX IF NOT EXISTS idx_depth_levels_side_time 
ON depth_levels_200 (security_id, side, time DESC);

-- Index for finding levels with high order counts
CREATE INDEX IF NOT EXISTS idx_depth_levels_orders 
ON depth_levels_200 (security_id, orders DESC, time DESC) 
WHERE orders > 10;

-- Compression policy
ALTER TABLE depth_levels_200 SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'security_id, side',
    timescaledb.compress_orderby = 'time DESC, level_num'
);

-- Compress chunks older than 7 days
SELECT add_compression_policy('depth_levels_200', INTERVAL '7 days');

-- Retention policy - keep 60 days
SELECT add_retention_policy('depth_levels_200', INTERVAL '60 days');

-- Create view for easy querying of latest snapshot
CREATE OR REPLACE VIEW latest_depth_snapshot AS
SELECT 
    time,
    security_id,
    side,
    level_num,
    price,
    quantity,
    orders
FROM depth_levels_200
WHERE time = (SELECT MAX(time) FROM depth_levels_200)
ORDER BY side DESC, level_num;

COMMENT ON TABLE depth_levels_200 IS '200-level market depth data for order book analysis';
COMMENT ON COLUMN depth_levels_200.level_num IS 'Level number: 1 = best bid/ask, 200 = deepest level';
COMMENT ON COLUMN depth_levels_200.orders IS 'Number of orders at this price level';
