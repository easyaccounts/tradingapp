# Market Hours Automation Setup

This document explains how to run trading services only during market hours (9:00 AM - 3:40 PM IST).

## Services Affected

**Time-restricted (9:00 AM - 3:40 PM IST):**
- `ingestion` - Kite WebSocket market data
- `depth-collector` - Dhan 200-level depth
- `worker` - Celery worker for data processing

**24/7 Services (unchanged):**
- `api` - REST API for historical data
- `frontend` - Web interface
- `timescaledb`, `pgbouncer`, `redis`, `rabbitmq` - Infrastructure

---

## Approach 1: Systemd Timers (Recommended)

### Installation on VPS

```bash
# 1. Copy systemd files
sudo cp systemd/*.service /etc/systemd/system/
sudo cp systemd/*.timer /etc/systemd/system/

# 2. Reload systemd
sudo systemctl daemon-reload

# 3. Enable timers (will start automatically)
sudo systemctl enable tradingapp-start.timer
sudo systemctl enable tradingapp-stop.timer

# 4. Start timers immediately
sudo systemctl start tradingapp-start.timer
sudo systemctl start tradingapp-stop.timer

# 5. Verify timers are active
sudo systemctl list-timers tradingapp-*
```

### Expected Output
```
NEXT                        LEFT          LAST PASSED UNIT
Mon 2025-12-30 03:30:00 UTC 5h 15min left n/a  n/a    tradingapp-start.timer
Sun 2025-12-29 10:10:00 UTC 12h left      n/a  n/a    tradingapp-stop.timer
```

### Manual Control

```bash
# Manually start services (without waiting for timer)
sudo systemctl start tradingapp-market-hours.service

# Manually stop services
sudo systemctl stop tradingapp-market-hours.service

# Check service status
sudo systemctl status tradingapp-market-hours.service

# View logs
journalctl -u tradingapp-market-hours.service -f
```

### Modify Timings

Edit timer files if you need different times:
```bash
sudo nano /etc/systemd/system/tradingapp-start.timer
sudo nano /etc/systemd/system/tradingapp-stop.timer
sudo systemctl daemon-reload
sudo systemctl restart tradingapp-start.timer tradingapp-stop.timer
```

**Note:** Timer uses UTC. IST = UTC + 5:30
- 9:00 AM IST = 3:30 AM UTC
- 3:40 PM IST = 10:10 AM UTC

---

## Approach 2: Python Time Checks (Alternative)

Add market hours validation inside each service. Services will sleep when outside market hours.

### Modify each service's main.py

Add this helper module:

```python
# services/common/market_hours.py
from datetime import datetime, time
import pytz
import time as time_module

IST = pytz.timezone('Asia/Kolkata')
MARKET_START = time(9, 0)   # 9:00 AM
MARKET_END = time(15, 40)   # 3:40 PM

def is_market_hours():
    """Check if current time is within market hours (IST)"""
    now = datetime.now(IST)
    current_time = now.time()
    
    # Check if it's a weekday (Monday=0, Sunday=6)
    if now.weekday() > 4:  # Saturday or Sunday
        return False
    
    # Check if within trading hours
    return MARKET_START <= current_time <= MARKET_END

def wait_for_market_hours():
    """Sleep until market opens"""
    while not is_market_hours():
        now = datetime.now(IST)
        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Outside market hours. Sleeping...")
        time_module.sleep(300)  # Check every 5 minutes
```

Then in each service's main loop:

```python
from common.market_hours import is_market_hours, wait_for_market_hours

def main():
    print("Waiting for market hours...")
    wait_for_market_hours()
    
    while True:
        if not is_market_hours():
            print("Market closed. Stopping...")
            break
        
        # Your existing code here
        process_data()
```

**Pros:**
- No systemd configuration needed
- Services check time themselves
- Can add holiday calendar logic

**Cons:**
- Services still run (just idle) outside market hours
- Uses more resources than stopping completely

---

## Approach 3: Cron Jobs (Simple Alternative)

```bash
# Edit crontab
crontab -e

# Add these lines (IST times):
30 3 * * 1-5 cd /opt/tradingapp && /usr/bin/docker-compose up -d ingestion depth-collector worker
10 10 * * 1-5 cd /opt/tradingapp && /usr/bin/docker-compose stop ingestion depth-collector worker
```

**Pros:**
- Simple, familiar
- Works on any Linux system

**Cons:**
- Less robust than systemd (no dependency management)
- Harder to debug
- Doesn't handle failures well

---

## Recommended Setup

1. **Use Systemd Timers** for production (Approach 1)
2. **Add Python time checks** as safety net (Approach 2)

This dual approach ensures:
- Containers don't run unnecessarily (saves resources)
- Services self-stop if manually started outside market hours
- Protection against accidental 24/7 operation

---

## Testing

### Test Manual Start/Stop

```bash
# Start services manually
sudo systemctl start tradingapp-market-hours.service

# Check containers are running
docker ps | grep -E 'ingestion|depth-collector|worker'

# Stop services manually
sudo systemctl stop tradingapp-market-hours.service

# Verify they stopped
docker ps | grep -E 'ingestion|depth-collector|worker'
```

### Test Timer Schedule

```bash
# Dry-run: When will timers fire next?
systemctl list-timers tradingapp-*

# View timer logs
journalctl -u tradingapp-start.timer -u tradingapp-stop.timer --since today
```

### Simulate Market Hours (Testing)

Temporarily change timer to fire in 2 minutes:

```bash
# Edit start timer
sudo systemctl edit --full tradingapp-start.timer

# Change OnCalendar to:
# OnCalendar=*-*-* *:*:00
# (This fires every minute - for testing only!)

sudo systemctl daemon-reload
sudo systemctl restart tradingapp-start.timer

# Watch logs
journalctl -u tradingapp-market-hours.service -f
```

---

## Monitoring

### Check Service Status

```bash
# Are services running?
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Check systemd status
systemctl status tradingapp-market-hours.service

# View recent logs
journalctl -u tradingapp-market-hours.service -n 50
```

### Alert on Failures

Add to systemd service file:

```ini
[Service]
# ... existing config ...
OnFailure=tradingapp-alert@%n.service
```

Create alert service:

```bash
# /etc/systemd/system/tradingapp-alert@.service
[Unit]
Description=Trading App Failure Alert

[Service]
Type=oneshot
ExecStart=/usr/bin/mail -s "TradingApp Service Failure: %i" your@email.com
```

---

## Troubleshooting

### Services don't start at scheduled time

```bash
# Check timer status
systemctl status tradingapp-start.timer

# Check if timer is enabled
systemctl is-enabled tradingapp-start.timer

# Verify timer fires
journalctl -u tradingapp-start.timer --since "1 hour ago"
```

### Services don't stop at scheduled time

```bash
# Check stop timer
systemctl status tradingapp-stop.timer

# Manually trigger stop
sudo systemctl start tradingapp-stop.timer
```

### Timezone issues

```bash
# Check system timezone
timedatectl

# Verify UTC to IST conversion
# 9:00 AM IST = 3:30 AM UTC
# 3:40 PM IST = 10:10 AM UTC

# Test timer calculation
systemd-analyze calendar "03:30:00"
systemd-analyze calendar "10:10:00"
```

### Docker Compose not found

```bash
# Find docker-compose location
which docker-compose

# Update service file with correct path
sudo nano /etc/systemd/system/tradingapp-market-hours.service

# Change ExecStart path if needed
ExecStart=/usr/local/bin/docker-compose up -d ingestion depth-collector worker
```

---

## Holiday Management

For market holidays, you can:

### Option A: Disable timers temporarily

```bash
# Before holiday
sudo systemctl stop tradingapp-start.timer
sudo systemctl stop tradingapp-stop.timer

# After holiday
sudo systemctl start tradingapp-start.timer
sudo systemctl start tradingapp-stop.timer
```

### Option B: Add calendar exceptions

Edit timer file to skip specific dates:

```ini
[Timer]
OnCalendar=*-*-* 03:30:00
# Skip Republic Day (Jan 26)
OnCalendar=*-01-26 03:30:00
ExecCondition=/usr/bin/false
```

### Option C: Holiday calendar in Python

Add to `market_hours.py`:

```python
# NSE holidays 2025
HOLIDAYS = [
    '2025-01-26',  # Republic Day
    '2025-03-14',  # Holi
    '2025-04-10',  # Mahavir Jayanti
    # ... add more
]

def is_market_hours():
    now = datetime.now(IST)
    date_str = now.strftime('%Y-%m-%d')
    
    if date_str in HOLIDAYS:
        return False
    
    # ... rest of checks
```

---

## Cost Savings

Running services only during market hours (6.5 hours/day, 5 days/week):

- **Before:** 24/7 = 168 hours/week
- **After:** 6.5 × 5 = 32.5 hours/week
- **Savings:** ~80% reduction in compute time

**Estimated savings:**
- CPU usage: 80% reduction
- Network traffic: 80% reduction
- Database load: 80% reduction
- VPS costs: Potentially downgrade instance size
