-- Add orderflow toxicity columns to ticks table
-- Run this migration to add the 4 new toxicity metrics

ALTER TABLE ticks 
ADD COLUMN IF NOT EXISTS consumption_rate NUMERIC(12, 2),
ADD COLUMN IF NOT EXISTS flow_intensity NUMERIC(12, 2),
ADD COLUMN IF NOT EXISTS depth_toxicity_tick NUMERIC(8, 4),
ADD COLUMN IF NOT EXISTS kyle_lambda_tick NUMERIC(12, 6);

-- Verify columns were added
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'ticks' 
  AND column_name IN ('consumption_rate', 'flow_intensity', 'depth_toxicity_tick', 'kyle_lambda_tick')
ORDER BY ordinal_position;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Orderflow toxicity columns added successfully!';
    RAISE NOTICE 'New columns: consumption_rate, flow_intensity, depth_toxicity_tick, kyle_lambda_tick';
END $$;
