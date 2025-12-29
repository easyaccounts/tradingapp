# Trading Data Platform - AI Agent Guide

## Architecture Overview

**Message Flow:** Kite/Dhan WebSocket → Ingestion Service → RabbitMQ → Workers (3x) → TimescaleDB (via PgBouncer)

**Services:**
- `api/` - FastAPI REST (Kite/Dhan OAuth, orderflow queries)
- `ingestion/` - KiteConnect WebSocket consumer, publishes to RabbitMQ
- `depth_collector/` - Dhan 200-level orderbook WebSocket
- `worker/` - RabbitMQ consumer, batch writes to TimescaleDB (3 instances)
- `signal_generator/` - Real-time trading signal detection from depth data
- `frontend/` - Static HTML/JS dashboard

**Key Decision:** Workers use **RabbitMQ** (not Celery tasks) for tick ingestion because Celery's task overhead was too slow for 1000+ ticks/sec. See `services/worker/consumer.py` for direct pika usage.

## Authentication Patterns

**Token Storage (Shared Volume):**
- Kite: `/app/data/access_token.txt` (plain text)
- Dhan: `/app/data/dhan_token.json` (JSON with expiry)
- Volume mounted at `/opt/tradingapp/data` on host, `/app/data` in containers

**Getting Tokens:**
```python
# Kite (in containers)
from kite_auth import get_access_token
token = get_access_token(redis_url)  # File primary, Redis fallback

# Dhan (anywhere)
from dhan_auth import get_dhan_credentials
creds = get_dhan_credentials()  # Returns {'access_token': '...', 'client_id': '...'}
```

**OAuth Flows:**
- Kite: `routes/kite.py` - Standard 3-legged OAuth
- Dhan: `routes/dhan.py` - Custom consent app flow, auto-refresh support

## Database Connection

**Critical:** Connection strings differ for containers vs. host scripts.

**Containers:**
```python
DB_HOST=pgbouncer  # Docker service name
DB_PORT=5432       # Internal port
```

**Host Scripts (on VPS):**
```python
DB_HOST=localhost
DB_PORT=6432  # PgBouncer mapped port (use 5432 for direct TimescaleDB)
```

See `DATABASE_CONNECTION.md` for detailed patterns. Use conditional defaults:
```python
DB_HOST = os.getenv('DB_HOST', 'localhost')  # Works both places
```

## Data Processing Pipeline

**Batch Writing (Critical for Performance):**
- Workers use `psycopg2.extras.execute_batch()` with `page_size=1000`
- Buffer size: 400 ticks (~2 snapshots for depth data)
- See `services/worker/db_writer.py:insert_ticks_batch()` for COPY FROM pattern

**Tick Enrichment:**
- Market depth calculations in `db_writer.py:calculate_tick_metrics()`
- Order imbalance: `(bid_volume - ask_volume) / (bid_volume + ask_volume)`
- Spread calculation: Always use `last_price` if available

**Depth Data:**
- Dhan provides 200-level orderbook (bid/ask with price/qty/orders)
- Stored in `depth_levels_200` table with compound PK: `(time, security_id, side, level_num)`
- ON CONFLICT DO NOTHING because same timestamp can arrive multiple times

## Development Workflow

**No Rebuild Needed (Code on Host):**
- Standalone scripts: `dhan_*.py`, `scripts/*.py` - Edit and run directly
- These use `get_dhan_credentials()` to read tokens from `/opt/tradingapp/data/`

**Rebuild Required (Dockerized Services):**
```bash
git pull
docker-compose build api depth-collector  # Only changed services
docker-compose up -d api depth-collector
```

**Quick Deploy (All Services):**
```bash
docker-compose down
git pull
docker-compose up -d --build
```

**Volume Mount Issue:** Currently no dev volume mounts - every code change requires rebuild. Consider adding `docker-compose.dev.yml` with source mounts for faster iteration.

## Market Hours Automation

**Systemd Timers (on VPS):**
- `tradingapp-start.timer` - 9:00 AM IST (3:30 UTC)
- `tradingapp-stop.timer` - 3:40 PM IST (10:10 UTC)
- Only starts: ingestion, depth-collector, signal-generator, workers
- API/frontend/databases run 24/7

**Check Status:**
```bash
ssh root@82.180.144.255 "systemctl list-timers tradingapp-*"
```

## Critical Files

- `docker-compose.yml` - Service definitions, port mappings, volume binds
- `TOKEN_ACCESS.md` - Token file formats and usage patterns
- `DATABASE_CONNECTION.md` - Container vs. host DB connection guide
- `services/api/routes/dhan.py` - Dhan OAuth implementation (383 lines)
- `services/depth_collector/dhan_200depth_websocket.py` - 200-level depth ingestion
- `services/worker/db_writer.py` - Batch insertion logic and metric calculations

## Common Patterns

**Structured Logging:**
```python
import structlog
logger = structlog.get_logger()
logger.info("event_name", key=value, count=123)  # Use snake_case event names
```

**Environment Variables:**
All services read from `.env`. Critical vars: `DATABASE_URL`, `REDIS_URL`, `RABBITMQ_URL`, `KITE_API_KEY`, `DHAN_API_KEY`, `DHAN_API_SECRET`, `DHAN_CLIENT_ID`

**Docker Networking:**
- All services on `tradingapp-network`
- Inter-service communication uses service names (e.g., `pgbouncer:5432`)
- External access uses mapped ports (e.g., `localhost:6432` → PgBouncer)

## Troubleshooting

**Token Issues:**
```bash
# Check token files exist
docker exec tradingapp-api ls -la /app/data/

# Re-authenticate
Visit https://zopilot.in/ and click Kite/Dhan login buttons
```

**Database Connection:**
```bash
# Test from host
psql postgresql://tradinguser:PASSWORD@localhost:6432/tradingdb

# Test from container
docker exec tradingapp-api psql $DATABASE_URL -c "SELECT COUNT(*) FROM ticks;"
```

**Service Logs:**
```bash
docker logs tradingapp-depth-collector --tail 100 -f
journalctl -u tradingapp-market-hours.service -f  # Systemd timer logs
```

## Deployment Context

- **VPS:** 82.180.144.255 (Cloudflare proxied via zopilot.in)
- **SSL:** Origin certificate (Cloudflare → Origin), requires "Full" mode (not "Full strict")
- **Data Volume:** Bind mount at `/opt/tradingapp/data` for token persistence across recreates
