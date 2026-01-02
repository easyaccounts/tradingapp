"""
Dhan Migration Testing Guide
Complete step-by-step guide for testing and deploying Dhan market feed

This document covers:
1. Pre-migration checklist
2. Database schema migration
3. Instrument synchronization
4. Configuration and deployment
5. Testing procedures
6. Rollback plan
"""

# STEP 1: PRE-MIGRATION CHECKLIST
# ================================

## 1.1 Verify Environment
# - Dhan API credentials available and tested
# - /app/data/dhan_token.json exists with valid token (not expired)
# - Database backups completed
# - Current Kite ingestion running smoothly
# - RabbitMQ and workers operational

# Check Dhan token validity:
# python -c "from services.ingestion.dhan_auth import check_dhan_token_validity; print('Valid' if check_dhan_token_validity() else 'Invalid')"


# STEP 2: DATABASE SCHEMA MIGRATION
# ==================================

## 2.1 Apply SQL Migration
# This adds security_id column and indexes for Dhan instruments
# File: database/add_dhan_security_id.sql
# 
# Run on VPS:
# psql postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb -f database/add_dhan_security_id.sql
# 
# Or via Docker:
# docker exec tradingapp-timescaledb psql -U tradinguser -d tradingdb -f /docker-entrypoint-initdb.d/add_dhan_security_id.sql

## 2.2 Verify Schema Changes
# psql postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb
# \d instruments
# Expected new columns:
#   - security_id: TEXT (Dhan identifier)
#   - is_active: BOOLEAN DEFAULT TRUE
#   - source: TEXT DEFAULT 'kite' ('kite' or 'dhan')
# \di idx_instruments_security_id
# SELECT * FROM active_instruments LIMIT 5;


# STEP 3: INSTRUMENT SYNCHRONIZATION
# ====================================

## 3.1 Download and Parse Dhan Instruments
# Option A: Auto-download from Dhan
# cd /opt/tradingapp
# python scripts/sync_dhan_instruments.py \
#   --db-url postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb \
#   --download \
#   --batch-size 2000

# Option B: Manual CSV upload (if auto-download fails)
# 1. Download: https://images.dhan.co/api-data/api-scrip-master.csv
# 2. Place in repo root as api-scrip-master.csv
# 3. Run sync:
# python scripts/sync_dhan_instruments.py --db-url postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb

## 3.2 Verify Synchronization
# psql postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb
# SELECT COUNT(*) FROM instruments WHERE source = 'dhan';
# Expected: ~228,000+ instruments from Dhan master
# 
# SELECT COUNT(*) FROM instruments WHERE source = 'kite';
# Expected: ~1500+ existing Kite instruments (unchanged)
# 
# SELECT * FROM instruments WHERE security_id IS NOT NULL LIMIT 5;
# Verify security_id values present


# STEP 4: CONFIGURATION
# =====================

## 4.1 Update .env File
# Set DATA_SOURCE to control ingestion source:
#   DATA_SOURCE=kite    # Default, use KiteConnect WebSocket
#   DATA_SOURCE=dhan    # Use Dhan market feed instead
# 
# Edit .env:
# DATA_SOURCE=kite

## 4.2 Verify Docker Compose
# docker-compose config | grep -A 10 "ingestion:"
# Should show DATA_SOURCE env variable passed to service

## 4.3 Test Dhan Authentication
# python -c "
# from services.ingestion.dhan_auth import get_dhan_credentials, get_websocket_url
# creds = get_dhan_credentials()
# print(f'Client ID: {creds[\"client_id\"]}')
# print(f'WebSocket URL: {get_websocket_url(creds[\"access_token\"], creds[\"client_id\"])}')
# "


# STEP 5: TESTING PROCEDURES
# ===========================

## 5.1 Unit Tests - Binary Parser
# python -c "
# from services.ingestion.dhan_parser import parse_packet
# import struct
# 
# # Create mock full packet (response code 8)
# header = struct.pack('<BhBi', 8, 171, 1, 49229)  # Code 8, Full, NSE_FNO, NIFTY FUT
# prices = struct.pack('<ffififiiffff', 
#   24500.0,           # last_price
#   1234.0,            # last_traded_qty (wrong type for demo)
#   1671234567,        # last_trade_time
#   24480.5,           # avg_price
#   500000,            # volume
#   2000000,           # sell_qty
#   2500000,           # buy_qty
#   15000000,          # oi
#   15100000,          # oi_high
#   14900000,          # oi_low
#   24400.0,           # day_open
#   24550.0,           # day_close
#   24600.0,           # day_high
#   24400.0)           # day_low
# 
# # Mock depth (5 levels)
# depth = struct.pack('<' + 'iihh' * 10, 
#   100000, 120000, 50, 60,  # Level 1
#   90000, 130000, 45, 65,   # Level 2
#   80000, 140000, 40, 70,   # Level 3
#   70000, 150000, 35, 75,   # Level 4
#   60000, 160000, 30, 80) + b'\\x00' * 80  # Level 5 + padding
# 
# packet = header + prices + depth
# result = parse_packet(packet)
# print('Parser test:', 'PASS' if result else 'FAIL')
# print(f'  Response Code: {result[\"response_code\"] if result else None}')
# print(f'  Security ID: {result[\"security_id\"] if result else None}')
# "

## 5.2 Unit Tests - Models
# python -c "
# from services.ingestion.models import DhanTick, DhanMarketDepthLevel
# from datetime import datetime
# 
# # Create sample tick
# tick = DhanTick(
#     response_code=8,
#     message_length=171,
#     exchange_segment='NSE_FNO',
#     exchange_segment_code=1,
#     security_id='49229',
#     last_price=24500.0,
#     last_traded_quantity=100,
#     last_trade_time=datetime.now(),
#     average_traded_price=24480.5,
#     volume_traded=500000,
#     total_sell_quantity=2000000,
#     total_buy_quantity=2500000,
#     day_open=24400.0,
#     day_close=24550.0,
#     day_high=24600.0,
#     day_low=24400.0,
#     oi=15000000,
#     depth=[DhanMarketDepthLevel(
#         bid_quantity=100000, ask_quantity=120000,
#         bid_orders=50, ask_orders=60,
#         bid_price=24498.0, ask_price=24502.0
#     )]
# )
# print('Model test:', 'PASS' if tick.security_id == '49229' else 'FAIL')
# "

## 5.3 Integration Test - Single Instrument
# 
# PROCEDURE:
# 1. Update config.py with single test instrument
# 2. Switch DATA_SOURCE to 'dhan' in .env
# 3. Start ingestion service:
#    cd /opt/tradingapp
#    python services/ingestion/main.py
# 
# 4. Verify in another terminal:
#    - Check logs for connection messages
#    - Monitor database ticks table for new entries
#    - Check RabbitMQ queue for published messages
# 
# EXPECTED OUTPUT (in logs):
# ```
# ingestion_service_starting data_source=dhan
# validating_configuration
# dhan_token_valid
# connecting_to_redis
# redis_connected
# loading_instruments_cache_from_database
# instruments_cache_loaded count=XXXX
# initializing_rabbitmq_publisher
# rabbitmq_publisher_ready
# initializing_dhan_websocket_client
# dhan_websocket_client_initialized
# starting_dhan_websocket_connection
# dhan_websocket_connected
# subscription_sent exchange=1 count=1 mode=full
# subscription_complete total_instruments=1
# [TICK] 49229 | LTP: 24500.0 | Volume: 500000
# ```
# 
# DATABASE VERIFICATION:
# psql postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb
# SELECT COUNT(*) FROM ticks WHERE instrument_token = (
#   SELECT instrument_token FROM instruments WHERE security_id = '49229'
# );
# Expected: > 0 (should see ticks within 1 minute)
# 
# SELECT last_price, volume_traded, oi FROM ticks 
# WHERE instrument_token = (SELECT instrument_token FROM instruments WHERE security_id = '49229')
# ORDER BY time DESC LIMIT 5;
# Expected: Recent ticks with increasing volume

## 5.4 Integration Test - Multiple Instruments
# 
# 1. Update config.py with multiple test instruments (5-10):
#    config.INSTRUMENTS = [
#        {'security_id': '49229', 'exchange_segment': 1},  # NIFTY FUT
#        {'security_id': '49230', 'exchange_segment': 1},  # Next contract
#        {'security_id': '13', 'exchange_segment': 0},     # NIFTY 50 INDEX
#    ]
# 
# 2. Start ingestion and let run for 5 minutes
# 
# 3. Verify data distribution:
# psql postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb
# SELECT instrument_token, COUNT(*) as tick_count 
# FROM ticks 
# WHERE time > NOW() - INTERVAL '5 minutes'
# GROUP BY instrument_token
# ORDER BY tick_count DESC;
# 
# Expected: All subscribed instruments have ticks (tick_count > 0)

## 5.5 Load Test - Full Rollout
# 
# 1. Switch to full instrument list in config.py
# 2. Run with DATA_SOURCE=dhan for 1 hour
# 3. Monitor metrics:
#    - CPU usage (should be 5-15%)
#    - Memory usage (should stabilize)
#    - Database insert rate (should maintain ~1000 ticks/sec)
#    - RabbitMQ queue depth (should stay < 1000)
#    - Tick ingestion latency (should be < 500ms)
# 
# 4. Database performance:
# SELECT 
#   COUNT(*) as total_ticks,
#   COUNT(DISTINCT instrument_token) as unique_instruments,
#   MAX(time) as latest_time,
#   MIN(time) as earliest_time
# FROM ticks
# WHERE time > NOW() - INTERVAL '1 hour';


# STEP 6: ROLLBACK PLAN
# =====================

## 6.1 Quick Rollback (If Issues Occur)
# Revert DATA_SOURCE in .env:
# DATA_SOURCE=kite
# 
# Restart ingestion:
# docker-compose restart ingestion
# 
# Or if running via PM2:
# pm2 restart tradingapp-ingestion

## 6.2 Data Cleanup (Optional)
# If issues require full reset:
# 
# # Preserve Kite data, remove Dhan data
# DELETE FROM ticks WHERE instrument_token IN (
#   SELECT instrument_token FROM instruments WHERE source = 'dhan'
# );
# 
# # Or remove security_id mappings
# UPDATE instruments SET security_id = NULL WHERE source = 'dhan';


# STEP 7: PRODUCTION DEPLOYMENT
# ==============================

## 7.1 Deployment Checklist
# - [x] Schema migration applied
# - [x] Instruments synchronized
# - [ ] Single instrument test PASSED
# - [ ] Multiple instrument test PASSED
# - [ ] Load test PASSED (1+ hour)
# - [ ] Dhan token validity confirmed
# - [ ] Rollback plan documented
# - [ ] Team notified of migration
# - [ ] Monitoring alerts configured
# - [ ] Database backups current

## 7.2 Gradual Rollout Strategy
# 1. Day 1: Run both Kite and Dhan in parallel (separate services)
# 2. Day 2: Compare data quality metrics
# 3. Day 3: Switch to Dhan as primary if metrics better
# 4. Day 7: Disable Kite ingestion if stable
# 5. Week 2+: Monitor for anomalies

## 7.3 Deploy Commands
# 
# Apply database migration:
# docker exec tradingapp-timescaledb psql -U tradinguser -d tradingdb \
#   -f /path/to/database/add_dhan_security_id.sql
# 
# Sync instruments (from repo root):
# python scripts/sync_dhan_instruments.py \
#   --db-url $DATABASE_URL \
#   --download
# 
# Switch to Dhan (edit .env or via container):
# docker-compose down
# # Edit .env: DATA_SOURCE=dhan
# docker-compose up -d --build


# STEP 8: MONITORING & METRICS
# ==============================

# Monitor these key metrics during Dhan operation:
# 
# 1. Ingestion Rate
#    SELECT COUNT(*) as ticks_per_minute FROM ticks 
#    WHERE time > NOW() - INTERVAL '1 minute';
#    Expected: 15000-25000 ticks/min (250-400 ticks/sec across 160+ instruments)
# 
# 2. Tick Latency
#    SELECT AVG(EXTRACT(EPOCH FROM (NOW() - last_trade_time)) * 1000) as latency_ms
#    FROM ticks
#    WHERE time > NOW() - INTERVAL '5 minutes';
#    Expected: < 500ms
# 
# 3. RabbitMQ Queue Depth
#    rabbitmqctl list_queues name messages
#    Expected: ticks queue < 5000 messages
# 
# 4. Database Connection Pool
#    SELECT count(*) FROM pg_stat_activity;
#    Expected: 5-15 active connections (depends on worker count)
# 
# 5. Error Rates
#    SELECT COUNT(*) FROM logs 
#    WHERE level = 'ERROR' AND time > NOW() - INTERVAL '5 minutes';
#    Expected: < 1 error per minute


# STEP 9: TROUBLESHOOTING
# =======================

# Q: Dhan WebSocket disconnects frequently
# A: Check token expiry, ensure /app/data/dhan_token.json is current
#    Watch for network issues, increase reconnect timeout
# 
# Q: Security ID not found errors
# A: Run sync_dhan_instruments.py with --download to refresh master
#    Check if security_id column is indexed: \di idx_instruments_security_id
# 
# Q: Ticks not flowing to database
# A: Verify RabbitMQ is running and accepting messages
#    Check workers are consuming from queue
#    Monitor database insert performance (BATCH_SIZE setting)
# 
# Q: High CPU usage
# A: Reduce number of subscribed instruments
#    Check if parser is spending time on malformed packets
#    Monitor WebSocket receive buffer usage
# 
# Q: Data quality issues (stale ticks, gaps)
# A: Verify Dhan WebSocket subscription confirmed
#    Check market hours (9:15-15:30 IST weekdays only)
#    Compare with Kite data source to identify issues


print(__doc__)
