SELECT date_trunc('second', time) AS ts,
       COUNT(DISTINCT time) AS snapshots,
       COUNT(*) AS rows
FROM depth_levels_200
WHERE time >= TIMESTAMP '2026-01-01 09:00:00+00'  -- adjust window start
  AND time <  TIMESTAMP '2026-01-01 09:30:00+00'  -- adjust window end
GROUP BY 1
ORDER BY ts DESC
LIMIT 1200;