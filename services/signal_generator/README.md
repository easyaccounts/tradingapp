# Signal Generator Service

Analyzes 200-level orderbook depth data from NIFTY futures and generates actionable trading signals delivered to Slack.

## Overview

This service subscribes to real-time depth snapshots via Redis pub/sub, maintains a 60-second rolling buffer, and calculates 3 proven institutional metrics:

1. **Key Price Levels** - Identifies significant support/resistance from order clustering
2. **Order Absorption** - Detects when large orders are being filled or pulled
3. **Market Pressure** - Quantifies buy/sell imbalance across multiple timeframes

## Architecture

```
depth-collector (WebSocket)
  ↓ Publishes to Redis
signal-generator (Subscriber)
  ↓ Calculates metrics
  ├─→ Slack (filtered alerts)
  ├─→ Redis (real-time state)
  └─→ TimescaleDB (historical signals)
```

## Metrics Details

### 1. Key Price Levels
- **Detection**: Order count >2.5x average
- **Persistence**: Must exist for 5+ seconds
- **Range**: Within ±100 points of current price
- **Alert threshold**: 3.0x average, 10+ seconds old

### 2. Order Absorption
- **Detection**: 60%+ order reduction over 30-60 seconds
- **Verification**: Price must test level (within 20 points)
- **Consistency**: Declining trend, not flickering
- **Alert threshold**: 70%+ reduction with price breakthrough

### 3. Market Pressure
- **Calculation**: (Bid orders - Ask orders) / Total orders
- **Windows**: 30s, 60s, 120s
- **Scope**: Top 20 levels near current price
- **States**: Bullish (>0.3), Bearish (<-0.3), Neutral
- **Alert threshold**: 0.4+ imbalance with state change

## Configuration

Environment variables:

```bash
REDIS_URL=redis://redis:6379/0           # Redis connection
DB_HOST=timescaledb                      # Database host
DB_PORT=5432                             # Database port
DB_NAME=tradingdb                        # Database name
DB_USER=tradinguser                      # Database user
DB_PASSWORD=your_password                # Database password
SLACK_WEBHOOK_URL=https://hooks.slack... # Slack webhook
SECURITY_ID=49543                        # NIFTY futures security ID
```

## Database Tables

### depth_signals
Stores calculated metrics every 10 seconds:
- `key_levels` (JSONB) - Array of tracked levels
- `absorptions` (JSONB) - Array of absorption events
- `pressure_30s/60s/120s` (NUMERIC) - Imbalance values
- `market_state` (VARCHAR) - bullish/bearish/neutral

### signal_outcomes
Backtesting table with price outcomes:
- Tracks 10s, 30s, 60s, 5min price changes
- Calculates PnL assuming entry at signal price
- Classifies win/loss/breakeven

### signal_performance (VIEW)
Aggregated performance metrics:
- Daily win rate by signal type
- Average PnL per signal
- Signal frequency

## Alert Behavior

**Frequency**: 5-15 alerts per day (not every 10s calculation)

**Deduplication**: 5-minute cooldown between similar alerts

**Filters**:
- Key levels: Only 3x+ strength, proven persistence
- Absorptions: Only 70%+ reduction with breakthrough
- Pressure: Only 0.4+ imbalance on state changes

## Running the Service

### Local Development
```bash
cd services/signal_generator
pip install -r requirements.txt

# Set environment variables
export REDIS_URL=redis://localhost:6379/0
export DB_HOST=localhost
export DB_PASSWORD=your_password
export SLACK_WEBHOOK_URL=https://hooks.slack...

python main.py
```

### Docker
```bash
# Build and start
docker-compose up -d signal-generator

# View logs
docker logs tradingapp-signal-generator -f

# Restart
docker-compose restart signal-generator
```

## Deployment Checklist

1. **Database Schema**
   ```bash
   docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb < database/depth_signals.sql
   ```

2. **Slack Webhook**
   - Create webhook at api.slack.com/messaging/webhooks
   - Add to `.env` file: `SLACK_WEBHOOK_URL=...`

3. **Update docker-compose.yml**
   - Already includes signal-generator service definition

4. **Build and Deploy**
   ```bash
   docker-compose build depth-collector signal-generator
   docker-compose up -d signal-generator
   ```

5. **Verify Operation**
   ```bash
   # Check logs for startup message
   docker logs tradingapp-signal-generator

   # Verify Redis pub/sub
   docker exec -it tradingapp-redis redis-cli
   SUBSCRIBE depth_snapshots:NIFTY

   # Check database inserts
   docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
   SELECT time, current_price, market_state,
          jsonb_array_length(key_levels) as levels,
          pressure_60s
   FROM depth_signals
   WHERE security_id = 49543
   ORDER BY time DESC LIMIT 5;"

   # Check Slack - should see startup message
   ```

## Monitoring

**Health Checks**:
- Startup/shutdown messages in Slack
- Database inserts every 10 seconds
- Redis state updates with 60s TTL

**Troubleshooting**:
```bash
# Check if depth-collector is publishing
docker exec -it tradingapp-redis redis-cli
SUBSCRIBE depth_snapshots:NIFTY
# Should see messages every 200ms

# Check database connectivity
docker exec -it tradingapp-signal-generator python -c "
import psycopg2
conn = psycopg2.connect(host='timescaledb', database='tradingdb', user='tradinguser', password='...')
print('✓ Database connected')
"

# Verify Slack webhook
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test message"}' \
  YOUR_SLACK_WEBHOOK_URL
```

## Performance

- **Memory**: ~100MB (rolling 60s buffer)
- **CPU**: Minimal (calculations every 10s)
- **Database**: ~8,640 rows/day (1 per 10s during market hours)
- **Redis**: Negligible (60s TTL on state)
- **Network**: ~5KB/s from Redis (top 20 levels)

## Tuning Parameters

Edit [metrics.py](metrics.py) to adjust:

```python
# Key level detection
threshold = avg_orders * 2.5  # Currently 2.5x average
max_distance = 100            # ±100 points from price
min_age = 5                   # 5 seconds persistence

# Absorption detection
reduction_threshold = 0.6     # 60% order reduction
price_tolerance = 20          # Within 20 points
window_seconds = 30           # 30-60s lookback

# Pressure calculation
imbalance_threshold = 0.3     # ±0.3 for state change
top_levels = 20               # Top 20 near price
windows = [30, 60, 120]       # Multiple timeframes
```

Edit [slack_alerts.py](slack_alerts.py) to adjust:

```python
cooldown_minutes = 5          # 5 min between similar alerts
level_strength_min = 3.0      # 3x for alert
level_age_min = 10            # 10s proven
absorption_min = 70           # 70% reduction
pressure_min = 0.4            # 0.4 imbalance
```

## Data Flow

1. **Collection** (depth-collector):
   - WebSocket receives 200 bid + 200 ask levels
   - Saves to `depth_levels_200` table (all 400 levels)
   - Publishes top 20 levels to Redis `depth_snapshots:NIFTY`

2. **Processing** (signal-generator):
   - Subscribes to Redis channel
   - Maintains 600-snapshot rolling buffer (60 seconds)
   - Every 10 seconds: calculate 3 metrics
   - Track level lifecycle (forming → active → breaking → broken)

3. **Publishing**:
   - **Redis**: Real-time state with 60s TTL
   - **Database**: Historical signals for backtesting
   - **Slack**: Filtered high-confidence alerts only

## Future Enhancements

Potential improvements:

- [ ] Machine learning for threshold optimization
- [ ] Multi-timeframe signal confluence
- [ ] Price action patterns (failed breakouts, retests)
- [ ] Volume profile integration
- [ ] Order flow toxicity scoring
- [ ] Adaptive thresholds based on volatility
- [ ] Backtesting engine with signal outcomes
- [ ] Performance dashboard in Grafana
- [ ] Multi-instrument support (BANKNIFTY, etc)
