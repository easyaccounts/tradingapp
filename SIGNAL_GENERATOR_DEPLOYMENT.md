# Signal Generator Deployment Guide

Complete step-by-step guide to deploy the trading signal system with Slack integration.

## Prerequisites

- VPS with Docker and docker-compose
- Existing tradingapp installation
- Slack workspace for alerts
- Market hours: Will run 9:00 AM - 3:40 PM IST (systemd timers already configured)

## Step 1: Create Slack Webhook (5 minutes)

1. Go to https://api.slack.com/messaging/webhooks
2. Click "Create your Slack app"
3. Choose "From scratch"
4. Name: "Trading Signals", select workspace
5. Click "Incoming Webhooks"
6. Toggle "Activate Incoming Webhooks" to **On**
7. Click "Add New Webhook to Workspace"
8. Select channel (e.g., `#trading-signals`)
9. Copy the webhook URL (starts with `https://hooks.slack.com/services/...`)

## Step 2: Update Environment Variables

SSH to VPS and edit `.env`:

```bash
ssh root@82.180.144.255
cd /opt/tradingapp
nano .env
```

Add this line:
```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

Save and exit (Ctrl+X, Y, Enter)

## Step 3: Execute Database Schema

Create the tables for storing signals:

```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb < database/depth_signals.sql
```

Verify tables created:
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "\dt depth_*"
```

Should see:
- `depth_levels_200`
- `depth_signals`
- `signal_outcomes`

## Step 4: Pull Latest Code

```bash
cd /opt/tradingapp
git pull origin main
```

## Step 5: Build Services

Rebuild depth-collector (now publishes to Redis) and build signal-generator:

```bash
docker-compose build depth-collector signal-generator
```

This will take 2-3 minutes.

## Step 6: Start Signal Generator

```bash
docker-compose up -d signal-generator
```

## Step 7: Verify Operation

### Check Logs
```bash
docker logs tradingapp-signal-generator -f
```

Should see:
```
Connecting to Redis...
✓ Redis connected: redis://redis:6379/0
Connecting to TimescaleDB...
✓ TimescaleDB connected
Initializing Slack alerter...
✓ All connections established
Subscribing to depth_snapshots:NIFTY...
✓ Subscribed
🚀 Signal Generator running...
```

Press Ctrl+C to stop following logs.

### Check Slack
You should see a message in your Slack channel:
```
🟢 Signal Generator Online
Monitoring NIFTY 200-level depth for trading signals
Started at 2025-01-28 09:00:15 IST
```

### Verify Redis Pub/Sub
Check that depth-collector is publishing:

```bash
docker exec -it tradingapp-redis redis-cli
```

In Redis CLI:
```
SUBSCRIBE depth_snapshots:NIFTY
```

Should see messages flowing every ~200ms. Press Ctrl+C to exit.

### Check Database Inserts
After 30-60 seconds, check if signals are being saved:

```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    time, 
    current_price, 
    market_state,
    jsonb_array_length(key_levels) as num_levels,
    pressure_60s
FROM depth_signals 
WHERE security_id = 49543 
ORDER BY time DESC 
LIMIT 5;
"
```

Should see recent rows (updated every 10 seconds).

## Step 8: Monitor During Market Hours

### Next Trading Day

Services will auto-start at 9:00 AM IST via systemd timer.

Watch for Slack alerts:
- Key levels: When strong support/resistance is detected
- Absorptions: When big orders are being filled
- Pressure changes: When buy/sell imbalance shifts

Expected frequency: 5-15 alerts per day (not every 10 seconds).

### Check Performance
```bash
# View today's signal performance
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT * FROM signal_performance 
WHERE signal_date = CURRENT_DATE;
"
```

## Troubleshooting

### No Slack Messages

1. Test webhook manually:
```bash
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test from VPS"}' \
  "YOUR_SLACK_WEBHOOK_URL"
```

2. Check if `SLACK_WEBHOOK_URL` is set:
```bash
docker exec tradingapp-signal-generator env | grep SLACK
```

3. Check logs for Slack errors:
```bash
docker logs tradingapp-signal-generator | grep -i slack
```

### No Data in Database

1. Check if depth-collector is running:
```bash
docker ps | grep depth-collector
```

2. Check depth-collector logs:
```bash
docker logs tradingapp-depth-collector --tail 50
```

3. Verify Redis is receiving data:
```bash
docker exec tradingapp-redis redis-cli PUBSUB CHANNELS
# Should show: depth_snapshots:NIFTY
```

4. Check signal-generator is subscribed:
```bash
docker exec tradingapp-redis redis-cli PUBSUB NUMSUB depth_snapshots:NIFTY
# Should show: 1
```

### Service Keeps Restarting

```bash
docker logs tradingapp-signal-generator --tail 100
```

Common issues:
- Database password incorrect
- Slack webhook URL malformed
- Redis not reachable

Fix and restart:
```bash
docker-compose restart signal-generator
```

### Too Many Alerts

Edit [slack_alerts.py](services/signal_generator/slack_alerts.py):

```python
# Increase cooldown
self.cooldown_minutes = 10  # Was 5

# Raise thresholds
if level['strength'] >= 4.0:  # Was 3.0
if absorption['reduction_pct'] >= 80:  # Was 70
if abs(pressure['60s']) >= 0.5:  # Was 0.4
```

Rebuild and restart:
```bash
docker-compose build signal-generator
docker-compose restart signal-generator
```

### Too Few Alerts

Edit [slack_alerts.py](services/signal_generator/slack_alerts.py):

```python
# Decrease cooldown
self.cooldown_minutes = 3  # Was 5

# Lower thresholds
if level['strength'] >= 2.5:  # Was 3.0
if absorption['reduction_pct'] >= 60:  # Was 70
if abs(pressure['60s']) >= 0.35:  # Was 0.4
```

Rebuild and restart as above.

## Maintenance

### View Recent Signals
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    time,
    current_price,
    market_state,
    jsonb_array_length(key_levels) as levels,
    jsonb_array_length(absorptions) as absorptions,
    pressure_30s,
    pressure_60s,
    pressure_120s
FROM depth_signals
WHERE security_id = 49543
  AND time > NOW() - INTERVAL '1 hour'
ORDER BY time DESC;
"
```

### Check Signal Performance
```bash
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT 
    signal_date,
    signal_type,
    total_signals,
    win_rate,
    total_pnl
FROM signal_performance
WHERE signal_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY signal_date DESC, signal_type;
"
```

### Stop Service
```bash
docker-compose stop signal-generator
```

You'll see a shutdown message in Slack:
```
🔴 Signal Generator Offline
Stopped at 2025-01-28 15:40:00 IST
```

### Update and Restart
```bash
cd /opt/tradingapp
git pull
docker-compose build signal-generator
docker-compose restart signal-generator
```

## System Architecture

```
Market Hours: 9:00 AM - 3:40 PM IST (systemd timers)
    ↓
depth-collector
├─→ Saves to depth_levels_200 (all 200 levels)
└─→ Publishes to Redis (top 20 levels)
    ↓
signal-generator
├─→ 60s rolling buffer
├─→ Calculate metrics every 10s
├─→ Save to depth_signals
├─→ Publish to Redis (state)
└─→ Slack alerts (filtered)
```

## Next Steps

After running for a few days:

1. **Review alert quality**: Are signals actionable?
2. **Check win rate**: Query `signal_performance` view
3. **Tune thresholds**: Adjust based on market conditions
4. **Add grafana dashboard**: Visualize signals and performance
5. **Backtest historical**: Use `signal_outcomes` table

## Support

For issues or questions:
1. Check logs: `docker logs tradingapp-signal-generator`
2. Review [README.md](services/signal_generator/README.md)
3. Verify systemd timers: `systemctl list-timers tradingapp-*`
4. Check database: Query `depth_signals` table
5. Test Slack webhook manually with curl

## Summary

You now have:
- ✅ Real-time orderbook data collection (200 levels)
- ✅ 3 institutional-grade trading metrics
- ✅ Slack alerts for actionable signals (5-15/day)
- ✅ Historical database for backtesting
- ✅ Auto start/stop at market hours
- ✅ Complete monitoring and troubleshooting tools

The system runs automatically during market hours and delivers high-quality trading signals directly to your phone via Slack mobile app.
