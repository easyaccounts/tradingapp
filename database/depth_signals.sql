-- Signal Generator Database Schema
-- Tables for storing calculated trading signals and metrics

-- Table 1: Depth Signals (Core metrics every 10 seconds)
CREATE TABLE IF NOT EXISTS depth_signals (
    time TIMESTAMPTZ NOT NULL,
    security_id INT NOT NULL,
    current_price NUMERIC(12,2),
    
    -- Key levels (JSONB for flexibility)
    key_levels JSONB,  -- Array of {price, side, orders, strength, age_seconds, status}
    
    -- Absorptions detected
    absorptions JSONB,  -- Array of {price, side, orders_before, orders_now, reduction_pct}
    
    -- Pressure metrics at different timeframes
    pressure_30s NUMERIC(5,3),   -- Short-term
    pressure_60s NUMERIC(5,3),   -- Primary
    pressure_120s NUMERIC(5,3),  -- Context
    market_state VARCHAR(10),    -- bullish, bearish, neutral
    
    PRIMARY KEY (time, security_id)
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('depth_signals', 'time', if_not_exists => TRUE);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_depth_signals_time ON depth_signals (time DESC);
CREATE INDEX IF NOT EXISTS idx_depth_signals_state ON depth_signals (market_state, time DESC);
CREATE INDEX IF NOT EXISTS idx_depth_signals_security ON depth_signals (security_id, time DESC);

-- Enable compression (after 1 day)
ALTER TABLE depth_signals SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'security_id'
);

-- Add compression policy (compress data older than 1 day)
SELECT add_compression_policy('depth_signals', INTERVAL '1 day', if_not_exists => TRUE);

-- Add retention policy (keep 60 days)
SELECT add_retention_policy('depth_signals', INTERVAL '60 days', if_not_exists => TRUE);

-- Table 2: Signal Outcomes (For backtesting and learning)
CREATE TABLE IF NOT EXISTS signal_outcomes (
    signal_time TIMESTAMPTZ NOT NULL,
    security_id INT NOT NULL,
    signal_type VARCHAR(20),  -- 'key_level', 'absorption', 'pressure_change'
    signal_data JSONB,        -- Full signal details
    
    -- Price outcomes at different horizons
    price_entry NUMERIC(12,2),
    price_10s_later NUMERIC(12,2),
    price_30s_later NUMERIC(12,2),
    price_60s_later NUMERIC(12,2),
    price_5min_later NUMERIC(12,2),
    
    -- Calculated PnL (assumes 1 lot trade)
    pnl_10s NUMERIC(10,2),
    pnl_30s NUMERIC(10,2),
    pnl_60s NUMERIC(10,2),
    pnl_5min NUMERIC(10,2),
    
    -- Result classification
    result VARCHAR(10),  -- 'profitable', 'loss', 'neutral'
    
    PRIMARY KEY (signal_time, security_id, signal_type)
);

-- Convert to hypertable
SELECT create_hypertable('signal_outcomes', 'signal_time', if_not_exists => TRUE);

-- Indexes for analysis queries
CREATE INDEX IF NOT EXISTS idx_outcomes_type ON signal_outcomes (signal_type, signal_time DESC);
CREATE INDEX IF NOT EXISTS idx_outcomes_result ON signal_outcomes (result, signal_time DESC);

-- Retention policy (keep 90 days for backtesting)
SELECT add_retention_policy('signal_outcomes', INTERVAL '90 days', if_not_exists => TRUE);

-- View: Signal Performance Summary
CREATE OR REPLACE VIEW signal_performance AS
SELECT 
    signal_type,
    DATE(signal_time) as trade_date,
    COUNT(*) as total_signals,
    COUNT(*) FILTER (WHERE result = 'profitable') as winning_signals,
    COUNT(*) FILTER (WHERE result = 'loss') as losing_signals,
    ROUND(100.0 * COUNT(*) FILTER (WHERE result = 'profitable') / NULLIF(COUNT(*), 0), 2) as win_rate,
    ROUND(AVG(pnl_60s), 2) as avg_pnl_60s,
    ROUND(AVG(pnl_5min), 2) as avg_pnl_5min,
    ROUND(SUM(pnl_60s), 2) as total_pnl_60s,
    ROUND(SUM(pnl_5min), 2) as total_pnl_5min
FROM signal_outcomes
GROUP BY signal_type, DATE(signal_time)
ORDER BY trade_date DESC, signal_type;

COMMENT ON TABLE depth_signals IS 'Real-time trading signals calculated from 200-level order book depth';
COMMENT ON TABLE signal_outcomes IS 'Historical performance tracking of generated signals for backtesting';
COMMENT ON VIEW signal_performance IS 'Daily summary of signal performance by type';
