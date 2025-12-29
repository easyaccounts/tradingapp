# Signal Generator Quick Reference

Essential commands for daily operations.

## Deployment (First Time)

```bash
# 1. Add Slack webhook to .env
echo 'SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...' >> .env

# 2. Create database tables
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb < database/depth_signals.sql

# 3. Build and start
docker-compose build signal-generator depth-collector
docker-compose up -d signal-generator
```

## Daily Monitoring

```bash
# View live logs
docker logs tradingapp-signal-generator -f

# Check service status
docker ps | grep signal-generator

# View recent signals
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT time, current_price, market_state, 
       jsonb_array_length(key_levels) as levels
FROM depth_signals 
ORDER BY time DESC LIMIT 10;"
```

## Troubleshooting

```bash
# Restart service
docker-compose restart signal-generator

# Check if Redis is publishing
docker exec tradingapp-redis redis-cli PUBSUB CHANNELS
# Should show: depth_snapshots:NIFTY

# Test Slack webhook
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Test"}' \
  "$SLACK_WEBHOOK_URL"

# View errors
docker logs tradingapp-signal-generator --tail 100 | grep -i error
```

## Tuning

```bash
# Edit thresholds
nano services/signal_generator/slack_alerts.py
# or
nano services/signal_generator/metrics.py

# Rebuild and restart
docker-compose build signal-generator
docker-compose restart signal-generator
```

## Performance

```bash
# Today's signal count
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT signal_type, COUNT(*) 
FROM depth_signals, unnest(ARRAY['key_level', 'absorption', 'pressure']) as signal_type
WHERE time::date = CURRENT_DATE
GROUP BY signal_type;"

# Check win rate (after outcomes recorded)
docker exec -i tradingapp-timescaledb psql -U tradinguser -d tradingdb -c "
SELECT * FROM signal_performance 
WHERE signal_date >= CURRENT_DATE - 7;"
```

## Common Issues

**No Slack alerts**:
```bash
docker logs tradingapp-signal-generator | grep -i slack
docker exec tradingapp-signal-generator env | grep SLACK
```

**No database inserts**:
```bash
# Check if depth-collector is publishing
docker logs tradingapp-depth-collector --tail 20

# Check Redis subscription
docker exec tradingapp-redis redis-cli PUBSUB NUMSUB depth_snapshots:NIFTY
# Should show: 1
```

**Service crashed**:
```bash
docker logs tradingapp-signal-generator --tail 50
docker-compose restart signal-generator
```

## System Status

```bash
# All services status
docker-compose ps

# Market hours timers
systemctl list-timers tradingapp-*

# Check if running now
docker ps --filter name=tradingapp
```

## Update Code

```bash
cd /opt/tradingapp
git pull
docker-compose build signal-generator depth-collector
docker-compose restart signal-generator depth-collector
```

---

## Threshold Cheat Sheet

### Current Settings (Default)

**Key Levels**:
- Detection: 2.5x average orders
- Alert: 3.0x average, 10+ seconds old
- Cooldown: 5 minutes

**Absorptions**:
- Detection: 60% order reduction
- Alert: 70% reduction + breakthrough
- Cooldown: 5 minutes

**Pressure**:
- States: Bullish (>0.3), Bearish (<-0.3), Neutral
- Alert: 0.4+ imbalance on state change
- Cooldown: 5 minutes

### If Too Many Alerts

Increase thresholds:
- Key levels: 4.0x, 15+ seconds
- Absorptions: 80% reduction
- Pressure: 0.5+ imbalance
- Cooldown: 10 minutes

### If Too Few Alerts

Decrease thresholds:
- Key levels: 2.0x, 7+ seconds
- Absorptions: 50% reduction
- Pressure: 0.3+ imbalance
- Cooldown: 3 minutes

---

## File Locations

```
/opt/tradingapp/
├── services/signal_generator/
│   ├── main.py              # Service entry point
│   ├── metrics.py           # Metric calculations (TUNE HERE)
│   ├── slack_alerts.py      # Alert thresholds (TUNE HERE)
│   ├── tracking.py          # Level tracking logic
│   └── README.md            # Full documentation
├── database/
│   └── depth_signals.sql    # Database schema
├── systemd/
│   └── tradingapp-market-hours.service  # Auto start/stop
├── SIGNAL_GENERATOR_DEPLOYMENT.md  # Deployment guide
├── SIGNAL_GENERATOR_SUMMARY.md     # Implementation overview
└── SIGNAL_GENERATOR_QUICKREF.md    # This file
```

---

## Slack Message Examples

**Startup**:
```
🟢 Signal Generator Online
Monitoring NIFTY 200-level depth for trading signals
Started at 2025-01-28 09:00:15 IST
```

**Key Level**:
```
🟢 Strong SUPPORT Detected
Price: ₹23,450.00
Orders: 2,340
Strength: 3.8x avg
Age: 47s
```

**Absorption**:
```
⚡ RESISTANCE BREAKING THROUGH
Level: ₹23,500.00
Current: ₹23,512.00
Reduction: 78%
Before: 3,200 → 704 orders
```

**Pressure**:
```
🚀 Market Pressure: BULLISH
30s: +0.387
60s: +0.425
120s: +0.362
```

**Shutdown**:
```
🔴 Signal Generator Offline
Stopped at 2025-01-28 15:40:00 IST
```

---

## Expected Behavior

**Market Hours** (9 AM - 3:40 PM IST):
- Services auto-start via systemd timer
- Slack startup message
- 5-15 alerts during session
- Database rows every 10 seconds
- Slack shutdown message at 3:40 PM

**After Hours**:
- All services stopped
- No data collection
- No database inserts
- No Slack messages

**Weekend**:
- System idle
- Timers configured but no market data

---

## Support Resources

1. **Full documentation**: [README.md](services/signal_generator/README.md)
2. **Deployment guide**: [SIGNAL_GENERATOR_DEPLOYMENT.md](SIGNAL_GENERATOR_DEPLOYMENT.md)
3. **Implementation details**: [SIGNAL_GENERATOR_SUMMARY.md](SIGNAL_GENERATOR_SUMMARY.md)
4. **This quick reference**: [SIGNAL_GENERATOR_QUICKREF.md](SIGNAL_GENERATOR_QUICKREF.md)

---

**Last Updated**: January 2025  
**Version**: 1.0  
**Status**: Production Ready
