# NIFTY Options Auto-Update

Automatically downloads and updates NIFTY options tokens from Kite API every Wednesday.

## What it does

1. Downloads all instruments from `https://api.kite.trade/instruments`
2. Filters for NFO-OPT segment with name="NIFTY" 
3. Extracts all instrument tokens
4. Updates the `INSTRUMENTS` variable in `.env` file
5. Creates backup before updating

## Setup on VPS

### 1. Install the systemd service and timer

```bash
cd /opt/tradingapp

# Create log directory
sudo mkdir -p /var/log/tradingapp

# Copy service files
sudo cp scripts/nifty-options-update.service /etc/systemd/system/
sudo cp scripts/nifty-options-update.timer /etc/systemd/system/

# Reload systemd
sudo systemctl daemon-reload

# Enable and start the timer
sudo systemctl enable nifty-options-update.timer
sudo systemctl start nifty-options-update.timer

# Check timer status
sudo systemctl status nifty-options-update.timer
```

### 2. Verify the timer is scheduled

```bash
# List all timers
sudo systemctl list-timers

# You should see:
# NEXT                         LEFT        LAST PASSED UNIT                         ACTIVATES
# Wed 2024-xx-xx 06:00:00 IST  x days left n/a  n/a    nifty-options-update.timer   nifty-options-update.service
```

### 3. Test the script manually

```bash
cd /opt/tradingapp
source venv/bin/activate
python scripts/update_nifty_options.py
```

### 4. Run the service manually (without waiting for Wednesday)

```bash
sudo systemctl start nifty-options-update.service

# Check logs
tail -f /var/log/tradingapp/nifty-options-update.log
```

### 5. After update, restart ingestion service

```bash
docker-compose restart ingestion
```

## Schedule

- **Runs:** Every Wednesday at 6:00 AM IST
- **Why Wednesday:** Weekly options expire on Thursdays, new contracts are created
- **Time:** 6:00 AM is before market opens (9:15 AM), ensuring fresh data

## Backups

Every time the script runs, it creates a backup:
- Location: `/opt/tradingapp/backups/`
- Format: `.env.backup.YYYYMMDD_HHMMSS`

## Logs

View logs:
```bash
tail -f /var/log/tradingapp/nifty-options-update.log
```

## Manual Execution

If you need to update tokens immediately:

```bash
cd /opt/tradingapp
source venv/bin/activate
python scripts/update_nifty_options.py
docker-compose restart ingestion
```

## Troubleshooting

**Timer not running:**
```bash
sudo systemctl status nifty-options-update.timer
sudo journalctl -u nifty-options-update.timer
```

**Service failed:**
```bash
sudo systemctl status nifty-options-update.service
sudo journalctl -u nifty-options-update.service
tail -f /var/log/tradingapp/nifty-options-update.log
```

**Change schedule:**
Edit `/etc/systemd/system/nifty-options-update.timer` and reload:
```bash
sudo systemctl daemon-reload
sudo systemctl restart nifty-options-update.timer
```

## Disable Auto-Update

```bash
sudo systemctl stop nifty-options-update.timer
sudo systemctl disable nifty-options-update.timer
```
