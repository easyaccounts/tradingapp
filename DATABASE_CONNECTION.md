# Database Connection Guide

## Overview

The database has two access points:
- **TimescaleDB** (Direct): Port 5432
- **PgBouncer** (Connection pooler): Port 6432 → TimescaleDB:5432

## Connection from Docker Containers

Containers communicate via the `tradingapp-network` Docker network and use service names:

**Environment Variables:**
```bash
DB_HOST=pgbouncer          # Service name (Docker network)
DB_PORT=5432               # PgBouncer's internal port
DB_NAME=tradingdb
DB_USER=tradinguser
DB_PASSWORD=5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=

# Full connection string
DATABASE_URL=postgresql://tradinguser:5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=@pgbouncer:5432/tradingdb
```

**Python Usage (Inside Containers):**
```python
import psycopg2
import os

# Option 1: Use DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)

# Option 2: Individual params
conn = psycopg2.connect(
    host=os.getenv('DB_HOST', 'pgbouncer'),
    port=os.getenv('DB_PORT', '5432'),
    database=os.getenv('DB_NAME', 'tradingdb'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
```

## Connection from VPS Host

Host scripts cannot resolve Docker service names, so use `localhost` with **mapped ports**:

**Port Mappings (from docker-compose.yml):**
- TimescaleDB: `5432:5432` → localhost:5432
- PgBouncer: `6432:5432` → localhost:6432

**Environment Variables for Host Scripts:**
```bash
DB_HOST=localhost          # Use localhost, not pgbouncer
DB_PORT=6432               # Use mapped port (6432 for PgBouncer)
DB_NAME=tradingdb
DB_USER=tradinguser
DB_PASSWORD=5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=

# Full connection string for host
DATABASE_URL=postgresql://tradinguser:5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=@localhost:6432/tradingdb
```

**Python Usage (On VPS Host):**
```python
import psycopg2
import os

# Option 1: Direct connection via PgBouncer (recommended for pooling)
conn = psycopg2.connect(
    host='localhost',
    port=6432,          # PgBouncer mapped port
    database='tradingdb',
    user='tradinguser',
    password='5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
)

# Option 2: Direct to TimescaleDB (bypass pooling)
conn = psycopg2.connect(
    host='localhost',
    port=5432,          # TimescaleDB mapped port
    database='tradingdb',
    user='tradinguser',
    password='5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
)

# Option 3: Use environment with fallback to localhost
conn = psycopg2.connect(
    host=os.getenv('DB_HOST', 'localhost'),  # Fallback to localhost for host scripts
    port=int(os.getenv('DB_PORT', '6432')),  # Use 6432 for PgBouncer on host
    database=os.getenv('DB_NAME', 'tradingdb'),
    user=os.getenv('DB_USER', 'tradinguser'),
    password=os.getenv('DB_PASSWORD')
)
```

## Recommendations

### For Docker Containers:
- ✅ Use `DB_HOST=pgbouncer` (service name resolution)
- ✅ Use `DB_PORT=5432` (internal container port)
- ✅ Use `DATABASE_URL` from environment

### For VPS Host Scripts:
- ✅ Use `DB_HOST=localhost`
- ✅ Use `DB_PORT=6432` (PgBouncer) for connection pooling
- ✅ Use `DB_PORT=5432` (TimescaleDB) only for direct access
- ✅ Set environment variables before running scripts:
  ```bash
  export DB_HOST=localhost
  export DB_PORT=6432
  python your_script.py
  ```

### For Cross-Environment Scripts:
Use conditional defaults that work in both environments:
```python
# Works in containers (pgbouncer) and on host (localhost)
DB_HOST = os.getenv('DB_HOST', 'localhost')  # Default to localhost for host scripts
DB_PORT = os.getenv('DB_PORT', '6432')       # Default to PgBouncer port

# Inside container: env vars will be "pgbouncer:5432"
# On host: defaults will be "localhost:6432"
```

## PgBouncer Configuration

**Pool Settings:**
- Mode: `transaction` (connection released after each transaction)
- Max client connections: 1000
- Default pool size: 25
- Reserve pool size: 5

**Use PgBouncer (port 6432) for:**
- High-frequency short queries
- Worker services processing batches
- Scripts with many sequential queries

**Use TimescaleDB directly (port 5432) for:**
- Long-running queries
- Temporary table operations
- Session-specific settings
- Prepared statements across multiple queries
