-- Trading Platform Database Initialization
-- TimescaleDB schema for high-frequency tick data

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- TICKS TABLE - High-frequency market data
-- ============================================================================
-- Aligned with KiteConnect WebSocket tick structure
-- Supports full market depth (5 levels) and derived metrics

CREATE TABLE ticks (
    -- Timestamps
    time TIMESTAMPTZ NOT NULL,
    last_trade_time TIMESTAMPTZ,
    
    -- Instrument identification
    instrument_token INT NOT NULL,
    trading_symbol TEXT,
    exchange TEXT,
    instrument_type TEXT,
    
    -- Price data
    last_price NUMERIC(12, 2),
    last_traded_quantity INT,
    average_traded_price NUMERIC(12, 2),
    
    -- Volume & Open Interest
    volume_traded BIGINT,
    oi BIGINT,
    oi_day_high BIGINT,
    oi_day_low BIGINT,
    
    -- OHLC (Day candle)
    day_open NUMERIC(12, 2),
    day_high NUMERIC(12, 2),
    day_low NUMERIC(12, 2),
    day_close NUMERIC(12, 2),
    
    -- Change metrics
    change NUMERIC(12, 2),
    change_percent NUMERIC(8, 4),
    
    -- Order book totals
    total_buy_quantity BIGINT,
    total_sell_quantity BIGINT,
    
    -- Market Depth - Bids (5 levels)
    -- Arrays store [level0, level1, level2, level3, level4]
    bid_prices NUMERIC(12, 2)[5],
    bid_quantities INT[5],
    bid_orders INT[5],
    
    -- Market Depth - Asks (5 levels)
    ask_prices NUMERIC(12, 2)[5],
    ask_quantities INT[5],
    ask_orders INT[5],
    
    -- Metadata
    tradable BOOLEAN DEFAULT TRUE,
    mode TEXT, -- 'quote', 'ltp', 'full'
    
    -- Derived fields (calculated during enrichment)
    bid_ask_spread NUMERIC(12, 2),      -- ask[0] - bid[0]
    mid_price NUMERIC(12, 2),           -- (bid[0] + ask[0]) / 2
    order_imbalance BIGINT,             -- total_buy_quantity - total_sell_quantity
    
    -- Primary key ensures no duplicates
    PRIMARY KEY (time, instrument_token)
);

-- Convert to TimescaleDB hypertable
-- Partition by time with 1-day chunks for optimal query performance
SELECT create_hypertable(
    'ticks', 
    'time', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ============================================================================
-- INDEXES - Optimized for common query patterns
-- ============================================================================

-- Most common query: get ticks for specific instrument ordered by time
CREATE INDEX idx_ticks_instrument_time 
ON ticks (instrument_token, time DESC);

-- Query by trading symbol (e.g., 'NIFTY24DECFUT')
CREATE INDEX idx_ticks_symbol_time 
ON ticks (trading_symbol, time DESC) 
WHERE trading_symbol IS NOT NULL;

-- Query by exchange (NSE, NFO, etc.)
CREATE INDEX idx_ticks_exchange_time 
ON ticks (exchange, time DESC)
WHERE exchange IS NOT NULL;

-- Index for analytics on volume and OI
CREATE INDEX idx_ticks_volume 
ON ticks (instrument_token, time DESC) 
WHERE volume_traded > 0;

-- ============================================================================
-- COMPRESSION - Reduce storage by ~10x after 7 days
-- ============================================================================

ALTER TABLE ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instrument_token',
    timescaledb.compress_orderby = 'time DESC'
);

-- Automatically compress chunks older than 7 days
SELECT add_compression_policy('ticks', INTERVAL '7 days');

-- ============================================================================
-- RETENTION - Automatically drop old data
-- ============================================================================

-- Remove data older than 90 days to manage storage
SELECT add_retention_policy('ticks', INTERVAL '90 days');

-- ============================================================================
-- INSTRUMENTS TABLE - Master data for all tradable instruments
-- ============================================================================

CREATE TABLE instruments (
    -- Primary identification
    instrument_token INT PRIMARY KEY,
    exchange_token INT,
    
    -- Symbol and naming
    trading_symbol TEXT NOT NULL,
    name TEXT,
    
    -- Classification
    exchange TEXT NOT NULL,              -- NSE, NFO, BSE, etc.
    segment TEXT,                        -- NFO-FUT, NFO-OPT, etc.
    instrument_type TEXT,                -- FUT, CE, PE, EQ
    
    -- Derivatives metadata
    expiry DATE,
    strike NUMERIC(12, 2),
    
    -- Trading parameters
    tick_size NUMERIC(10, 4),            -- Minimum price movement
    lot_size INT,                        -- Contract size
    
    -- Metadata
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for instrument lookups
CREATE INDEX idx_instruments_symbol 
ON instruments (trading_symbol);

CREATE INDEX idx_instruments_exchange 
ON instruments (exchange);

CREATE INDEX idx_instruments_type 
ON instruments (instrument_type);

CREATE INDEX idx_instruments_expiry 
ON instruments (expiry) 
WHERE expiry IS NOT NULL;

-- ============================================================================
-- CONTINUOUS AGGREGATES - Pre-computed views for fast analytics
-- ============================================================================

-- 1-minute OHLCV aggregates
CREATE MATERIALIZED VIEW ticks_1min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', time) AS bucket,
    instrument_token,
    trading_symbol,
    first(last_price, time) AS open,
    max(last_price) AS high,
    min(last_price) AS low,
    last(last_price, time) AS close,
    sum(last_traded_quantity) AS volume,
    last(oi, time) AS oi,
    avg(bid_ask_spread) AS avg_spread,
    avg(order_imbalance) AS avg_imbalance,
    count(*) AS tick_count
FROM ticks
GROUP BY bucket, instrument_token, trading_symbol
WITH NO DATA;

-- Refresh policy: update every 30 seconds for last 2 hours
SELECT add_continuous_aggregate_policy('ticks_1min',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds'
);

-- 5-minute OHLCV aggregates
CREATE MATERIALIZED VIEW ticks_5min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('5 minutes', time) AS bucket,
    instrument_token,
    trading_symbol,
    first(last_price, time) AS open,
    max(last_price) AS high,
    min(last_price) AS low,
    last(last_price, time) AS close,
    sum(last_traded_quantity) AS volume,
    last(oi, time) AS oi,
    avg(bid_ask_spread) AS avg_spread,
    avg(order_imbalance) AS avg_imbalance,
    count(*) AS tick_count
FROM ticks
GROUP BY bucket, instrument_token, trading_symbol
WITH NO DATA;

-- Refresh every 2 minutes for last 6 hours
SELECT add_continuous_aggregate_policy('ticks_5min',
    start_offset => INTERVAL '6 hours',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '2 minutes'
);

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to get latest tick for an instrument
CREATE OR REPLACE FUNCTION get_latest_tick(p_instrument_token INT)
RETURNS TABLE (
    time TIMESTAMPTZ,
    last_price NUMERIC,
    volume_traded BIGINT,
    oi BIGINT,
    bid_price NUMERIC,
    ask_price NUMERIC,
    spread NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.time,
        t.last_price,
        t.volume_traded,
        t.oi,
        t.bid_prices[1] AS bid_price,
        t.ask_prices[1] AS ask_price,
        t.bid_ask_spread AS spread
    FROM ticks t
    WHERE t.instrument_token = p_instrument_token
    ORDER BY t.time DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate VWAP over a time period
CREATE OR REPLACE FUNCTION calculate_vwap(
    p_instrument_token INT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
RETURNS NUMERIC AS $$
DECLARE
    vwap NUMERIC;
BEGIN
    SELECT 
        SUM(last_price * last_traded_quantity) / NULLIF(SUM(last_traded_quantity), 0)
    INTO vwap
    FROM ticks
    WHERE instrument_token = p_instrument_token
      AND time >= p_start_time
      AND time <= p_end_time
      AND last_traded_quantity > 0;
    
    RETURN COALESCE(vwap, 0);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- MONITORING VIEWS
-- ============================================================================

-- View for data ingestion monitoring
CREATE VIEW ingestion_stats AS
SELECT 
    time_bucket('1 minute', time) AS minute,
    COUNT(*) AS tick_count,
    COUNT(DISTINCT instrument_token) AS unique_instruments,
    AVG(bid_ask_spread) AS avg_spread,
    MAX(time) AS latest_tick_time
FROM ticks
WHERE time > NOW() - INTERVAL '1 hour'
GROUP BY minute
ORDER BY minute DESC;

-- View for storage usage
CREATE VIEW storage_stats AS
SELECT 
    hypertable_name,
    pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) AS table_size,
    pg_size_pretty(indexes_size(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) AS indexes_size,
    pg_size_pretty(total_bytes(format('%I.%I', hypertable_schema, hypertable_name)::regclass)) AS total_size
FROM timescaledb_information.hypertables
WHERE hypertable_name = 'ticks';

-- ============================================================================
-- GRANTS - Set up permissions
-- ============================================================================

-- Grant necessary permissions to the application user
GRANT SELECT, INSERT ON ticks TO tradinguser;
GRANT SELECT, INSERT, UPDATE ON instruments TO tradinguser;
GRANT SELECT ON ticks_1min TO tradinguser;
GRANT SELECT ON ticks_5min TO tradinguser;
GRANT SELECT ON ingestion_stats TO tradinguser;
GRANT SELECT ON storage_stats TO tradinguser;

-- ============================================================================
-- INITIAL DATA
-- ============================================================================

-- Insert sample instruments (will be updated by update_instruments.py)
INSERT INTO instruments (instrument_token, exchange_token, trading_symbol, name, exchange, instrument_type, lot_size)
VALUES 
    (260105, 1024, 'NIFTY24DECFUT', 'NIFTY', 'NFO', 'FUT', 50),
    (261897, 1023, 'BANKNIFTY24DECFUT', 'BANKNIFTY', 'NFO', 'FUT', 25),
    (265, 1, 'FINNIFTY24DECFUT', 'FINNIFTY', 'NFO', 'FUT', 40)
ON CONFLICT (instrument_token) DO NOTHING;

-- ============================================================================
-- MAINTENANCE
-- ============================================================================

-- Create a function to manually compress old chunks if needed
CREATE OR REPLACE FUNCTION compress_old_chunks(older_than INTERVAL DEFAULT INTERVAL '7 days')
RETURNS TEXT AS $$
DECLARE
    compressed_chunks INT;
BEGIN
    SELECT count(*) INTO compressed_chunks
    FROM compress_chunk(
        show_chunks('ticks', older_than => NOW() - older_than)
    );
    
    RETURN format('Compressed %s chunks older than %s', compressed_chunks, older_than);
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE 'Trading Platform Database Initialized Successfully!';
    RAISE NOTICE 'Hypertable: ticks (with compression and retention policies)';
    RAISE NOTICE 'Tables: instruments';
    RAISE NOTICE 'Continuous Aggregates: ticks_1min, ticks_5min';
    RAISE NOTICE 'Ready to ingest market data!';
END $$;
