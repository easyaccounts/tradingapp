# Database as Single Source of Truth - Implementation Summary

## ✅ Changes Made

### 1. Ingestion Service - Database-Driven
**Files Modified:**
- `services/ingestion/config.py` - Now queries Postgres for active instruments
- `services/ingestion/enricher.py` - Loads metadata from Postgres (Redis fallback)
- `services/ingestion/main.py` - Passes DATABASE_URL to enricher
- `services/ingestion/requirements.txt` - Added psycopg2-binary
- `docker-compose.yml` - Added DATABASE_URL to ingestion service env

**What Changed:**
```python
# OLD: Read from .env
instruments_str = self._get_required("INSTRUMENTS")
self.INSTRUMENTS = self._parse_instruments(instruments_str)

# NEW: Query database
self.INSTRUMENTS = self._load_instruments_from_db()
# SELECT instrument_token FROM instruments WHERE is_active = TRUE
```

### 2. Enricher - Postgres Primary, Redis Fallback
**Before:**
```python
def load_instruments_cache(redis_client) -> Dict:
    # Load from Redis only
```

**After:**
```python
def load_instruments_cache(database_url, redis_client=None) -> Dict:
    # Try Postgres first
    # Fallback to Redis if DB fails
```

### 3. Sync Script - Marks Instruments Active
**Enhanced `sync_instruments.py`:**
```python
def mark_instruments_active(tokens):
    # UPDATE instruments SET is_active = FALSE  (clear all)
    # UPDATE instruments SET is_active = TRUE WHERE token IN (...)
```

**Flow:**
1. Download all instruments
2. Store in Postgres + Redis
3. Filter (nifty/banknifty/all)
4. **Mark filtered as active in DB**
5. Update .env (backward compat - now optional)

## 🔄 New Architecture

### Before (Multi-Source):
```
.env INSTRUMENTS=1,2,3
     ↓
config.py reads .env
     ↓
main.py subscribes
     ↓
enricher.py loads from Redis
```

### After (DB Single Source):
```
Postgres instruments table (is_active column)
     ↓
config.py queries DB
     ↓
main.py subscribes
     ↓
enricher.py loads from DB (Redis fallback)
```

## 🚀 Usage

### 1. Run Sync Script (One-Time Setup):
```bash
# This will:
# - Download 129K instruments
# - Store in Postgres + Redis
# - Mark NIFTY instruments as active
# - Update .env (optional)

python scripts/sync_instruments.py --filter nifty
```

### 2. No Restart Needed (Future):
```bash
# Modify active instruments via SQL:
psql -U tradinguser -d tradingdb -c "
  UPDATE instruments SET is_active = TRUE 
  WHERE name = 'BANKNIFTY' AND segment IN ('NFO-OPT', 'NFO-FUT')
"

# Ingestion reloads instruments on next restart or via signal
```

### 3. Current Behavior:
```bash
# For now, still requires restart:
docker compose restart ingestion
```

## 📊 Benefits

| Aspect | Before | After |
|--------|--------|-------|
| **Source of Truth** | .env file (static) | Postgres (dynamic) |
| **Modification** | Edit file + restart | SQL UPDATE (no restart needed in future) |
| **Consistency** | .env + Redis + Postgres (3 sources) | Postgres only (1 source) |
| **Fallback** | None | Redis if DB fails |
| **Flexibility** | Manual .env editing | SQL queries or API |

## 🛡️ Backward Compatibility

### Kept Working:
- ✅ .env INSTRUMENTS still updated (optional)
- ✅ Redis still populated (for fallback)
- ✅ Old scripts still work

### Graceful Degradation:
```python
# If DB connection fails:
1. Try Redis fallback
2. Try .env fallback
3. Fail with clear error message
```

## 📋 Migration Steps

### Step 1: Push Code
```bash
git add .
git commit -m "DB as single source of truth for instruments"
git push
```

### Step 2: Pull on VPS
```bash
cd /opt/tradingapp
git pull
```

### Step 3: Run Sync Script
```bash
python scripts/sync_instruments.py --filter nifty
```

This will:
- Store 129K instruments in Postgres
- Mark 1,491 NIFTY instruments as `is_active = TRUE`
- Store in Redis (fallback)
- Update .env (compatibility)

### Step 4: Rebuild & Restart
```bash
docker compose build ingestion
docker compose restart ingestion
```

### Step 5: Verify
```bash
# Check logs
docker logs tradingapp-ingestion | grep "instruments_loaded_from_db"

# Should see: instruments_loaded_from_db count=1491
```

## 🔧 API/Future Enhancements

### Add HTTP Endpoint (Optional):
```python
# services/api/routes/instruments.py
@router.post("/instruments/activate")
async def activate_instruments(tokens: List[int]):
    # UPDATE instruments SET is_active = TRUE WHERE token IN (...)
    # Signal ingestion to reload (via Redis pub/sub or HTTP)
    return {"activated": len(tokens)}
```

### Auto-Reload (Optional):
```python
# services/ingestion/main.py
def reload_instruments_on_signal():
    # Listen for SIGHUP or Redis message
    # Reload instruments_cache without restart
```

## 🧪 Testing

### Verify DB Query Works:
```bash
docker exec tradingapp-ingestion python -c "
from config import config
print(f'Loaded {len(config.INSTRUMENTS)} instruments from DB')
print(config.INSTRUMENTS[:10])
"
```

### Verify Enricher Loads from DB:
```bash
docker exec tradingapp-ingestion python -c "
from enricher import load_instruments_cache
from config import config
import redis

redis_client = redis.from_url(config.REDIS_URL)
cache = load_instruments_cache(config.DATABASE_URL, redis_client)
print(f'Loaded {len(cache)} instruments metadata from DB')
"
```

## 📝 SQL Useful Commands

### Check Active Instruments:
```sql
SELECT COUNT(*) FROM instruments WHERE is_active = TRUE;
```

### See Active Instruments:
```sql
SELECT instrument_token, trading_symbol, name, segment 
FROM instruments 
WHERE is_active = TRUE 
ORDER BY name, expiry NULLS LAST
LIMIT 20;
```

### Activate BANKNIFTY:
```sql
UPDATE instruments SET is_active = TRUE 
WHERE name = 'BANKNIFTY' AND segment IN ('NFO-OPT', 'NFO-FUT');
```

### Deactivate All:
```sql
UPDATE instruments SET is_active = FALSE;
```

## ⚠️ Important Notes

1. **is_active column required** - Added by `sync_instruments.py` automatically
2. **.env INSTRUMENTS no longer used** by ingestion (only fallback)
3. **Redis optional** - Only for fallback if DB fails
4. **Restart still needed** - Auto-reload not implemented yet
5. **Backward compatible** - Old behavior available via .env fallback

## 🎯 Next Steps

- [x] Implement DB-driven config
- [x] Update enricher to use DB
- [x] Add is_active marking in sync script
- [ ] Test on VPS
- [ ] Remove .env INSTRUMENTS dependency completely
- [ ] Add HTTP API for instrument management
- [ ] Implement auto-reload without restart
- [ ] Add instruments management UI

---

**Status**: ✅ Implemented, ready for testing
**Breaking Changes**: None (backward compatible)
**Benefits**: Single source of truth, no restart needed (future)
