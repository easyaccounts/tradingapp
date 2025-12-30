# PM2 Migration Plan - Step by Step

## Current State Analysis

**Services running in Docker:**
- ✅ timescaledb, pgbouncer, redis, rabbitmq (KEEP in Docker)
- ✅ nginx, prometheus, grafana (KEEP in Docker)  
- ❌ api, ingestion, worker-1/2/3, depth-collector, signal-generator (MIGRATE to PM2)
- ❌ frontend (MIGRATE to direct Nginx serve)

**Critical Connection Points:**
- All Docker containers use network names: `timescaledb`, `redis`, `rabbitmq`, `pgbouncer`
- PM2 services will use: `localhost` instead
- Token files: `/opt/tradingapp/data` (already accessible from host)

## Environment Variables Changes Needed

**Current (.env with Docker):**
```
DB_HOST=pgbouncer  → DB_HOST=localhost
REDIS_HOST=redis   → REDIS_HOST=localhost  
RABBITMQ_HOST=rabbitmq → RABBITMQ_HOST=localhost
```

**Port mappings (already exposed):**
- PostgreSQL (TimescaleDB): 5432
- PgBouncer: 6432
- Redis: 6379
- RabbitMQ: 5672

## Migration Steps

### Phase 1: Preparation (NO DOWNTIME)

1. **Create PM2 venv on VPS:**
```bash
cd /opt/tradingapp
python3 -m venv venv
source venv/bin/activate
pip install -r services/api/requirements.txt
pip install -r services/ingestion/requirements.txt
pip install -r services/worker/requirements.txt
pip install -r services/depth_collector/depth_collector_requirements.txt
pip install -r services/signal_generator/requirements.txt
```

2. **Create .env.pm2 file:**
```bash
cp .env.example .env.pm2
# Edit to change docker hostnames to localhost
sed -i 's/DB_HOST=pgbouncer/DB_HOST=localhost/' .env.pm2
sed -i 's/REDIS_HOST=redis/REDIS_HOST=localhost/' .env.pm2  
sed -i 's/RABBITMQ_HOST=rabbitmq/RABBITMQ_HOST=localhost/' .env.pm2
sed -i 's/@pgbouncer:/@localhost:6432/' .env.pm2
sed -i 's/@redis:/@localhost:/' .env.pm2
sed -i 's/@rabbitmq:/@localhost:/' .env.pm2
# Update depth collector DB_HOST
echo "DB_HOST=localhost" >> .env.pm2
```

3. **Install PM2:**
```bash
npm install -g pm2
pm2 startup  # Create systemd service for PM2
```

### Phase 2: Test Services ONE at a time

**Test API first (safest):**
```bash
# Load environment
export $(cat .env.pm2 | xargs)

# Test API
cd /opt/tradingapp/services/api
/opt/tradingapp/venv/bin/python main.py
# Check http://localhost:8000/health
# Ctrl+C if works
```

**Test worker:**
```bash
cd /opt/tradingapp/services/worker
/opt/tradingapp/venv/bin/python consumer.py
# Should connect to RabbitMQ, Ctrl+C if works
```

**Test ingestion:**
```bash
cd /opt/tradingapp/services/ingestion
/opt/tradingapp/venv/bin/python main.py
# Should connect, Ctrl+C if works
```

### Phase 3: Deploy PM2 (CONTROLLED DOWNTIME)

**Stop Docker services:**
```bash
docker-compose stop ingestion worker-1 worker-2 worker-3 depth-collector signal-generator api frontend
```

**Start with PM2:**
```bash
pm2 start ecosystem.config.js
pm2 save  # Save process list
pm2 startup # Ensure starts on reboot
```

**Verify:**
```bash
pm2 status  # All should be "online"
pm2 logs --lines 50  # Check for errors
```

### Phase 4: Update Nginx for Frontend

**Edit nginx config to serve static files:**
```nginx
# Replace frontend proxy with direct serve
location / {
    root /opt/tradingapp/services/frontend/public;
    try_files $uri $uri/ /index.html;
    add_header Cache-Control "public, max-age=3600";
}
```

**Reload Nginx:**
```bash
docker exec tradingapp-nginx nginx -t
docker exec tradingapp-nginx nginx -s reload
```

### Phase 5: Update Systemd Timers

**Update timers to use PM2:**
```bash
# Edit tradingapp-market-hours.service
ExecStartPre=/usr/bin/pm2 stop ingestion depth-collector signal-generator worker-1 worker-2 worker-3
ExecStart=/usr/bin/pm2 start ingestion depth-collector signal-generator worker-1 worker-2 worker-3

# Edit tradingapp-market-hours-stop.service  
ExecStart=/usr/bin/pm2 stop ingestion depth-collector signal-generator worker-1 worker-2 worker-3
```

## Rollback Plan

**If something breaks:**
```bash
pm2 stop all
docker-compose up -d ingestion worker-1 worker-2 worker-3 depth-collector signal-generator api
# System back to original state
```

## Benefits After Migration

**Development workflow:**
```bash
# Edit code on Windows
git commit && git push

# On VPS
git pull  # PM2 auto-restarts services (if --watch enabled)
# OR
pm2 restart ingestion  # Manual restart (2 seconds)
```

**No more:**
- ❌ `docker-compose build` (2+ minutes)
- ❌ `docker-compose up -d` (30+ seconds)
- ❌ Image rebuilds
- ❌ Container recreation

## Testing Checklist

- [ ] PM2 services start successfully
- [ ] API responds at http://localhost:8000/health
- [ ] Ingestion connects to Kite WebSocket
- [ ] Workers consume from RabbitMQ
- [ ] Depth collector connects to Dhan
- [ ] Signal generator processes data
- [ ] Frontend loads at https://zopilot.in
- [ ] Timers trigger PM2 start/stop
- [ ] Logs visible via `pm2 logs`
- [ ] Auto-restart works on code change (with --watch)
