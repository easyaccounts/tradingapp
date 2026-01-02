-- Migration: Add Dhan security_id support to instruments table
-- Purpose: Enable Dhan API integration while maintaining Kite compatibility
-- Date: 2026-01-02

-- Add security_id column for Dhan API
ALTER TABLE instruments 
ADD COLUMN IF NOT EXISTS security_id TEXT;

-- Add index for fast lookups by security_id
CREATE INDEX IF NOT EXISTS idx_instruments_security_id 
ON instruments (security_id);

-- Add is_active column if it doesn't exist (for filtering stale instruments)
ALTER TABLE instruments 
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- Add source column to track data origin (kite/dhan)
ALTER TABLE instruments 
ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'kite';

-- Add comment explaining the mapping
COMMENT ON COLUMN instruments.security_id IS 'Dhan API Security ID (SEM_SMST_SECURITY_ID from Dhan instrument master)';
COMMENT ON COLUMN instruments.instrument_token IS 'KiteConnect instrument token OR generated token for Dhan instruments';
COMMENT ON COLUMN instruments.source IS 'Data source: kite, dhan, or manual';

-- Create a view for active instruments with both IDs
CREATE OR REPLACE VIEW active_instruments AS
SELECT 
    instrument_token,
    security_id,
    trading_symbol,
    name,
    exchange,
    segment,
    instrument_type,
    expiry,
    strike,
    tick_size,
    lot_size,
    source,
    last_updated
FROM instruments
WHERE is_active = TRUE;

COMMENT ON VIEW active_instruments IS 'Active instruments from all sources (Kite + Dhan)';
