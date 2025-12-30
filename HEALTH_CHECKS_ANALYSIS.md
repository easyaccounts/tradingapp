# Health Checks Analysis for Trading Services

## Current Status
- ✅ API: Has health check (shows "Up (healthy)")
- ✅ RabbitMQ: Has health check (shows "Up (healthy)")
- ✅ Redis: Has health check (shows "Up (healthy)")
- ✅ TimescaleDB: Has health check (shows "Up (healthy)")
- ❌ Ingestion: NO health check (shows "Up" even when failing)
- ❌ Workers (1/2/3): NO health check (shows "Up" even when idle)
- ❌ Depth Collector: NO health check
- ❌ Signal Generator: NO health check

---

## Service Analysis

### 1. **Ingestion Service** (`services/ingestion/main.py`)

**Current Issues:**
- WebSocket can disconnect with 403 Forbidden (token expired)
- Service shows "Up" but no ticks flowing (failed after 50 reconnect attempts)
- No way to detect: connection lost, no data received, invalid token

**Health Check Requirements:**

| Metric | Check | Threshold | Severity |
|--------|-------|-----------|----------|
| WebSocket Connected | `kws.is_connected()` | Must be True | CRITICAL |
| Ticks Received | Last tick timestamp | < 60 seconds ago | CRITICAL |
| Token Validity | 403 error count | 0 in last 5 min | CRITICAL |
| Reconnect Attempts | Current attempts | < 10 | WARNING |
| Publisher Connection | RabbitMQ connection alive | Must be connected | CRITICAL |
| Tick Rate | Ticks per second | > 0 during market hours | WARNING |

**Implementation Approach:**
1. Add global state tracking: `last_tick_time`, `connection_status`, `error_count_403`
2. Expose HTTP health endpoint on port 8001 or write status to Redis
3. Docker HEALTHCHECK queries this endpoint or Redis key

**Code Changes Needed:**
- `kite_websocket.py`: Track `last_tick_time`, `websocket_connected` state
- `main.py`: Expose health status (HTTP endpoint or Redis key)
- `docker-compose.yml`: Add HEALTHCHECK

---

### 2. **Workers (1/2/3)** (`services/worker/consumer.py`)

**Current Issues:**
- No visibility into: RabbitMQ connection status, messages being processed
- Can be connected but stuck (no messages arriving)
- Database connection can fail silently

**Health Check Requirements:**

| Metric | Check | Threshold | Severity |
|--------|-------|-----------|----------|
| RabbitMQ Connected | Connection status | Must be connected | CRITICAL |
| Messages Processed | Last message timestamp | < 120 seconds ago | WARNING |
| Database Connection | Test query | Must succeed | CRITICAL |
| Batch Flush Success | Failed flushes count | < 5 in last hour | WARNING |
| Queue Consumer Active | Consumer tag exists | Must exist | CRITICAL |

**Implementation Approach:**
1. Track: `last_message_time`, `total_processed`, `failed_flushes`
2. Write status to Redis every 30 seconds
3. Docker HEALTHCHECK reads from Redis

**Code Changes Needed:**
- `consumer.py`: Add state tracking, write to Redis
- `docker-compose.yml`: Add HEALTHCHECK script

---

### 3. **Depth Collector** (`services/depth_collector/dhan_200depth_websocket.py`)

**Current Issues:**
- No tracking of: WebSocket connection, snapshots received, Dhan auth failures
- Similar to ingestion - can be "Up" but not collecting data

**Health Check Requirements:**

| Metric | Check | Threshold | Severity |
|--------|-------|-----------|----------|
| WebSocket Connected | Connection status | Must be True | CRITICAL |
| Snapshots Received | Last snapshot time | < 10 seconds ago | CRITICAL |
| Dhan Token Valid | Auth errors | 0 in last 5 min | CRITICAL |
| Database Writes | Last write time | < 60 seconds ago | WARNING |
| Bid/Ask Balance | Pending depth count | < 100 orphaned | WARNING |

**Implementation Approach:**
1. Track: `last_snapshot_time`, `ws_connected`, `auth_error_count`
2. Write to Redis or expose HTTP endpoint
3. Docker HEALTHCHECK

**Code Changes Needed:**
- Add health state tracking
- Expose health endpoint or write to Redis
- `docker-compose.yml`: Add HEALTHCHECK

---

### 4. **Signal Generator** (`services/signal_generator/main.py`)

**Current Issues:**
- No visibility into: Redis subscription status, signal calculations
- Can be running but not generating signals

**Health Check Requirements:**

| Metric | Check | Threshold | Severity |
|--------|-------|-----------|----------|
| Redis Subscribed | PubSub connection | Must be active | CRITICAL |
| Snapshots Received | Buffer size | > 0 during market hours | WARNING |
| Calculations Running | Last calc time | < 60 seconds ago | WARNING |
| Database Connection | Connection alive | Must be connected | CRITICAL |
| Slack Connection | Alert send success | Check if webhook works | WARNING |

**Implementation Approach:**
1. Track: `last_snapshot_time`, `last_calc_time`, `redis_connected`
2. Write to Redis
3. Docker HEALTHCHECK

**Code Changes Needed:**
- Add state tracking
- Write health status to Redis
- `docker-compose.yml`: Add HEALTHCHECK

---

## Implementation Strategy

### Phase 1: Add Health State Tracking (Internal)
For each service, add global variables:
```python
health_status = {
    'last_activity': datetime.now(),
    'connected': False,
    'error_count': 0,
    'processed_count': 0,
    'status': 'starting'  # starting, healthy, degraded, unhealthy
}
```

### Phase 2: Expose Health Information
Two options:

**Option A: HTTP Endpoint** (More standard)
```python
from flask import Flask, jsonify
health_app = Flask('health')

@health_app.route('/health')
def health():
    return jsonify(health_status), 200 if health_status['status'] == 'healthy' else 503
```

**Option B: Redis Key** (Simpler, no extra port)
```python
redis_client.setex(f'health:{service_name}', 60, json.dumps(health_status))
```

### Phase 3: Docker HEALTHCHECK
Add to `docker-compose.yml`:

**For HTTP endpoint:**
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8001/health"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s
```

**For Redis key:**
```yaml
healthcheck:
  test: ["CMD-SHELL", "python -c \"import redis, json, sys; r=redis.from_url('$REDIS_URL'); s=json.loads(r.get('health:ingestion') or '{}'); sys.exit(0 if s.get('status')=='healthy' else 1)\""]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s
```

---

## Recommended Approach

**For Production:** Use **HTTP health endpoints** because:
1. Standard practice (Kubernetes, Docker Swarm expect HTTP)
2. No external dependency (doesn't need Redis to check health)
3. Can return detailed JSON with metrics
4. Tools like Prometheus can scrape metrics from same endpoint

**Quick Win:** Use **Redis keys** for now because:
1. Redis already running
2. Simpler to implement (no Flask/HTTP server needed)
3. Can query from external monitoring scripts
4. Less resource overhead

---

## Priority Order

1. **Ingestion** - CRITICAL (single point of failure for all tick data)
2. **Workers** - HIGH (data loss if not processing)
3. **Depth Collector** - HIGH (orderbook data critical for analysis)
4. **Signal Generator** - MEDIUM (alerts only, not data loss)

---

## Next Steps

1. Choose approach: HTTP endpoints or Redis keys
2. Implement health tracking in ingestion service first (highest priority)
3. Test health check during failure scenarios
4. Roll out to other services
5. Add monitoring dashboard (Grafana) to visualize health status
