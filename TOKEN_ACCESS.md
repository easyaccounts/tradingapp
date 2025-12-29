# Token Access Guide

## Token Storage Location

All authentication tokens are stored in a shared bind-mounted directory:

- **Docker containers:** `/app/data/`
- **VPS host:** `/opt/tradingapp/data/`

## Kite Connect Token

**File:** `access_token.txt`  
**Format:** Plain text (32-character string)  
**Validity:** 24 hours

**Usage in Python:**
```python
# From containers or host
with open('/app/data/access_token.txt', 'r') as f:  # or /opt/tradingapp/data/access_token.txt
    kite_token = f.read().strip()

# Using kite_auth helper (ingestion service)
from kite_auth import get_access_token
token = get_access_token(redis_url)  # Falls back to Redis if file not found
```

## Dhan OAuth Token

**File:** `dhan_token.json`  
**Format:** JSON with metadata  
**Validity:** ~24 hours (auto-refresh capable)

**Structure:**
```json
{
  "access_token": "eyJ0eXAi...",
  "expiry": "2025-12-31T00:45:49",
  "client_id": "1109719771",
  "generated_at": "2025-12-29T19:15:49.568089"
}
```

**Usage in Python:**
```python
# Using DhanAuthManager (recommended)
from dhan_auth import get_dhan_credentials
creds = get_dhan_credentials()
access_token = creds['access_token']
client_id = creds['client_id']

# Manual read
import json
with open('/app/data/dhan_token.json', 'r') as f:
    data = json.load(f)
    access_token = data['access_token']
```

## Environment Variables Required

```bash
# Kite
KITE_API_KEY=your_api_key
KITE_API_SECRET=your_api_secret

# Dhan
DHAN_API_KEY=your_api_key
DHAN_API_SECRET=your_api_secret
DHAN_CLIENT_ID=your_client_id
```

## Re-authentication

- **Kite:** Visit `https://yourdomain.com/` → Login button
- **Dhan:** Visit `https://yourdomain.com/` → Dhan Login button

Tokens are automatically written to the shared directory after OAuth flow completion.
