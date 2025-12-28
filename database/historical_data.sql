-- Historical OHLCV Data Table
-- Stores historical candle data for different timeframes and symbols

CREATE TABLE IF NOT EXISTS historical_data (
    -- Timestamps
    time TIMESTAMPTZ NOT NULL,
    
    -- Instrument identification
    instrument_token INT NOT NULL,
    trading_symbol TEXT NOT NULL,
    
    -- Timeframe (day, 60minute, 30minute, 15minute, minute, etc.)
    timeframe TEXT NOT NULL,
    
    -- OHLCV data
    open NUMERIC(12, 2) NOT NULL,
    high NUMERIC(12, 2) NOT NULL,
    low NUMERIC(12, 2) NOT NULL,
    close NUMERIC(12, 2) NOT NULL,
    volume BIGINT NOT NULL,
    
    -- Open Interest (for derivatives)
    oi BIGINT,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Primary key ensures no duplicates
    PRIMARY KEY (time, instrument_token, timeframe)
);

-- Convert to TimescaleDB hypertable for efficient time-series queries
SELECT create_hypertable(
    'historical_data', 
    'time', 
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_historical_instrument_time 
ON historical_data (instrument_token, timeframe, time DESC);

CREATE INDEX IF NOT EXISTS idx_historical_symbol_time 
ON historical_data (trading_symbol, timeframe, time DESC);

CREATE INDEX IF NOT EXISTS idx_historical_timeframe 
ON historical_data (timeframe, time DESC);

-- Note: Compression requires TimescaleDB 2.0+ with columnstore enabled
-- Uncomment if you want to enable compression:
-- ALTER TABLE historical_data SET (
--     timescaledb.compress,
--     timescaledb.compress_segmentby = 'instrument_token,timeframe'
-- );
-- SELECT add_compression_policy('historical_data', INTERVAL '30 days');

-- View for easy querying
CREATE OR REPLACE VIEW latest_historical_data AS
SELECT DISTINCT ON (instrument_token, timeframe)
    instrument_token,
    trading_symbol,
    timeframe,
    time,
    open,
    high,
    low,
    close,
    volume,
    oi
FROM historical_data
ORDER BY instrument_token, timeframe, time DESC;

-- Function to get historical data for backtesting
CREATE OR REPLACE FUNCTION get_historical_data(
    p_instrument_token INT,
    p_timeframe TEXT,
    p_from_date TIMESTAMPTZ,
    p_to_date TIMESTAMPTZ
)
RETURNS TABLE (
    result_time TIMESTAMPTZ,
    result_open NUMERIC,
    result_high NUMERIC,
    result_low NUMERIC,
    result_close NUMERIC,
    result_volume BIGINT,
    result_oi BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        h.time,
        h.open,
        h.high,
        h.low,
        h.close,
        h.volume,
        h.oi
    FROM historical_data h
    WHERE h.instrument_token = p_instrument_token
        AND h.timeframe = p_timeframe
        AND h.time >= p_from_date
        AND h.time <= p_to_date
    ORDER BY h.time ASC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE historical_data IS 'Historical OHLCV data for different timeframes and symbols';
COMMENT ON FUNCTION get_historical_data IS 'Retrieve historical data for backtesting';
