# 200-Level Depth Collector - VPS Deployment

## Updated on VPS

The code has been modified to save all 200 levels of market depth to TimescaleDB.

## Deploy Steps

### 1. Pull latest code on VPS
```bash
ssh root@82.180.144.255
cd /opt/tradingapp
git pull
```

### 2. Install Python dependencies
```bash
pip install -r depth_collector_requirements.txt
```

Or if using venv:
```bash
source venv/bin/activate
pip install -r depth_collector_requirements.txt
```

### 3. Set environment variables (if not already set)
```bash
export DB_HOST=pgbouncer
export DB_PORT=5432
export DB_NAME=tradingdb
export DB_USER=tradinguser
export DB_PASSWORD='5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
```

### 4. Run the collector
```bash
python dhan_200depth_websocket.py
```

## What It Does

### During Market Hours:
- Connects to Dhan 200-depth WebSocket API
- Receives 200 bid levels + 200 ask levels every ~200ms
- Saves aggregate metrics to CSV (backward compatible)
- **NEW**: Buffers and batch-inserts all 400 levels per snapshot to database

### Database Schema:
```sql
Table: depth_levels_200
- time: timestamp
- security_id: 49543 (NIFTY DEC FUT)
- side: 'BID' or 'ASK'
- level_num: 1-200
- price: actual price at that level
- quantity: total quantity
- orders: number of orders at that price
```

### Performance:
- Batch size: 400 rows (1 complete snapshot)
- Insert frequency: Every 2 snapshots (~400ms)
- Storage: ~400 rows/sec × 5400 seconds = ~2.16M rows per trading session
- Compressed size: ~100-200MB per day after compression

## Verify Data Collection

### Check if data is being saved:
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    side,
    COUNT(*) as total_levels,
    COUNT(DISTINCT time) as snapshots,
    MIN(time) as first_snapshot,
    MAX(time) as last_snapshot
FROM depth_levels_200
WHERE security_id = 49543
GROUP BY side;
"
```

### Sample query - Top 10 prices by order count:
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    time,
    side,
    price,
    orders,
    quantity
FROM depth_levels_200
WHERE security_id = 49543
  AND time > NOW() - INTERVAL '1 hour'
ORDER BY orders DESC
LIMIT 10;
"
```

### Sample query - View specific snapshot:
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    level_num,
    price,
    orders,
    quantity
FROM depth_levels_200
WHERE security_id = 49543
  AND side = 'BID'
  AND time = (SELECT MAX(time) FROM depth_levels_200)
ORDER BY level_num
LIMIT 20;
"
```

## Monitoring

The collector prints status every 100 snapshots:
```
[14:19:35] Snapshots: 500, Bid: ₹25,950.00, Ask: ₹25,951.50, Spread: ₹1.50, Imbalance: 1.05, DB Buffer: 0
```

- **Snapshots**: Total captured since start
- **DB Buffer**: How many rows waiting to be inserted (should be 0-400)

## Troubleshooting

### Database connection failed:
```bash
# Check if containers are running
docker ps

# Check database credentials
echo $DB_PASSWORD

# Test connection manually
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "SELECT version();"
```

### No data in database:
```bash
# Check table exists
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "\dt depth_levels_200"

# Check if collector is running
ps aux | grep dhan_200depth

# Check collector logs
tail -f /opt/tradingapp/dhan_200depth.log  # if you redirect output
```

## Storage Management

### Current retention: 60 days
Automatic cleanup via TimescaleDB policy.

### Check storage usage:
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    pg_size_pretty(pg_total_relation_size('depth_levels_200')) as total_size,
    pg_size_pretty(pg_relation_size('depth_levels_200')) as table_size,
    pg_size_pretty(pg_indexes_size('depth_levels_200')) as indexes_size;
"
```

### Compression status:
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    chunk_schema,
    chunk_name,
    compression_status,
    compressed_total_bytes,
    uncompressed_total_bytes
FROM timescaledb_information.compressed_chunk_stats
WHERE hypertable_name = 'depth_levels_200'
ORDER BY chunk_name DESC
LIMIT 5;
"
```
