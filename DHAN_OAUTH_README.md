# Dhan OAuth Integration

Complete OAuth implementation for Dhan API authentication, matching the KiteConnect pattern for a consistent user experience.

## Overview

This implementation provides:
- **Web-based OAuth flow**: Users authenticate via browser (no daily token generation)
- **Long-lived tokens**: Access tokens valid for 12 months with auto-refresh capability
- **Consistent UX**: Matches existing KiteConnect authentication flow
- **Dual provider support**: Users can connect both Kite and Dhan accounts

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Browser   │─────▶│  FastAPI     │─────▶│  Dhan Auth  │
│   (User)    │◀─────│  (Backend)   │◀─────│  Service    │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │  Token Store │
                     │ File + Redis │
                     └──────────────┘
```

## OAuth Flow (3 Steps)

### Step 1: Generate Consent

**User Action**: Clicks "Connect Dhan" button on website

**Backend**: `GET /api/dhan/login-url`
```python
# API calls Dhan auth service
POST https://auth.dhan.co/app/generate-consent
Body: {
    "apiKey": "YOUR_API_KEY",
    "secretKey": "YOUR_API_SECRET",
    "redirectUrl": "https://zopilot.in/api/dhan/callback"
}

# Response
{
    "status": "success",
    "data": {
        "consentAppId": "abc123...",
        "expiryTime": "2024-01-01T10:05:00"
    }
}
```

**Backend**: Stores `consentAppId` in Redis (5 min TTL) and returns browser URL:
```json
{
    "login_url": "https://auth.dhan.co/login/consentApp-login?consentAppId=abc123...",
    "consent_app_id": "abc123...",
    "expiry": "2024-01-01T10:05:00"
}
```

**Result**: User's browser redirected to Dhan login page

### Step 2: User Authentication (Browser)

**User Action**: 
1. Enters Dhan credentials on `auth.dhan.co`
2. Authorizes app access
3. Dhan redirects to callback URL

**Redirect**: 
```
https://zopilot.in/api/dhan/callback?tokenId=xyz789...
```

### Step 3: Consume Consent

**Backend**: `GET /api/dhan/callback?tokenId=xyz789...`
```python
# Retrieve consentAppId from Redis
consent_app_id = redis.get("dhan_consent_app_id")

# Exchange tokenId for access token
GET https://auth.dhan.co/app/consumeApp-consent
Params: {
    "tokenId": "xyz789...",
    "consentAppId": "abc123...",
    "apiKey": "YOUR_API_KEY"
}

# Response
{
    "status": "success",
    "data": {
        "accessToken": "eyJhbGc...",
        "expiryTime": "2025-01-01T00:00:00",
        "clientName": "John Doe"
    }
}
```

**Backend**: 
1. Stores token in file (`/app/data/dhan_token.json`)
2. Caches in Redis with 24h TTL
3. Redirects to success page:
```
https://zopilot.in/success.html?provider=dhan
```

## Token Storage

### File Format (`/app/data/dhan_token.json`)
```json
{
    "access_token": "eyJhbGc...",
    "expiry": "2025-01-01T00:00:00",
    "client_id": "DHAN12345",
    "client_name": "John Doe",
    "generated_at": "2024-01-01T10:00:00"
}
```

### Redis Keys
- `dhan_access_token`: Access token (24h TTL)
- `dhan_token_expiry`: Expiry timestamp (24h TTL)
- `dhan_client_name`: Client name (24h TTL)
- `dhan_consent_app_id`: Temporary consent ID (5 min TTL)
- `dhan_token_id`: Token ID for reference (30 day TTL)

## API Endpoints

### 1. Generate Login URL
```http
GET /api/dhan/login-url
```

**Response**:
```json
{
    "login_url": "https://auth.dhan.co/login/consentApp-login?consentAppId=...",
    "consent_app_id": "abc123...",
    "expiry": "2024-01-01T10:05:00"
}
```

**Errors**:
- `500`: Failed to generate consent (API key invalid, network error)

### 2. OAuth Callback
```http
GET /api/dhan/callback?tokenId=xyz789
```

**Response**: Redirects to `/success.html?provider=dhan`

**Errors**:
- `400`: Missing tokenId parameter
- `401`: Invalid tokenId or expired consent
- `500`: Failed to consume consent

### 3. Check Status
```http
GET /api/dhan/status
```

**Response**:
```json
{
    "authenticated": true,
    "client_name": "John Doe",
    "expiry": "2025-01-01T00:00:00",
    "source": "redis"
}
```

### 4. Get Token
```http
GET /api/dhan/token
```

**Response**:
```json
{
    "access_token": "eyJhbGc...",
    "expiry": "2025-01-01T00:00:00"
}
```

**Note**: This endpoint is for backend services only. Do not expose publicly.

## Setup Instructions

### 1. Register App on Dhan Console

1. Visit https://api-console.dhan.co/
2. Create new app or use existing
3. Configure:
   - **App Name**: Trading Platform
   - **Redirect URL**: `https://zopilot.in/api/dhan/callback`
   - **Permissions**: Market data, Orders (as needed)
4. Note down:
   - API Key
   - API Secret
   - Client ID

### 2. Configure Environment Variables

Add to `.env` file:
```bash
# Dhan API Configuration
DHAN_API_KEY=your_dhan_api_key_here
DHAN_API_SECRET=your_dhan_api_secret_here
DHAN_CLIENT_ID=your_dhan_client_id_here
DHAN_REDIRECT_URL=https://zopilot.in/api/dhan/callback

# Domain (must match redirect URL)
DOMAIN=zopilot.in
ENVIRONMENT=production
```

### 3. Restart Services

```bash
docker-compose restart api frontend
```

### 4. Test OAuth Flow

1. Visit https://zopilot.in/
2. Click "Connect Dhan" button
3. Login with Dhan credentials
4. Verify redirect to success page
5. Check token stored:
   ```bash
   docker exec api cat /app/data/dhan_token.json
   ```

## Frontend Integration

### Login Page

The login page (`/login.html`) supports both providers via URL parameter:
```
/login.html?provider=kite   # KiteConnect login
/login.html?provider=dhan   # Dhan login
```

Dynamic UI updates based on provider:
- Page title: "Kite Login" or "Dhan Login"
- Button text: "Login with Kite" or "Login with Dhan"
- API endpoint: `/api/kite/login-url` or `/api/dhan/login-url`

### Success Page

The success page (`/success.html`) displays provider-specific status:
```
/success.html?provider=kite   # Shows Kite connection status
/success.html?provider=dhan   # Shows Dhan connection status
```

Features:
- Provider name in message
- Token expiry (days for Dhan, hours for Kite)
- User/client identification
- Ingestion service status

### Index Page

Two separate buttons for clarity:
```html
<a href="/login.html?provider=kite" class="btn">🔐 Connect Kite</a>
<a href="/login.html?provider=dhan" class="btn">🔐 Connect Dhan</a>
```

## Usage in Services

### Depth Collector

The depth collector automatically uses the OAuth token:

```python
from dhan_auth import get_dhan_credentials

# Get credentials (reads from /app/data/dhan_token.json)
creds = get_dhan_credentials()

# Use in Dhan client
dhan_client = dhanhq(
    creds["client_id"],
    creds["access_token"]
)
```

**Auto-refresh**: Token automatically refreshed if expired (using `DhanAuthManager`)

### Standalone Scripts

Copy `dhan_auth.py` to script directory:
```python
from dhan_auth import get_dhan_credentials

creds = get_dhan_credentials()
print(f"Authenticated as: {creds['client_name']}")
```

## Token Lifecycle

### Validity Period
- **Initial token**: 12 months from generation
- **Refresh token**: Not applicable (Dhan uses long-lived tokens)
- **Session token**: Can be renewed via `/RenewToken` endpoint (implemented in DhanAuthManager)

### Auto-Refresh Logic

When token expires:
1. `DhanAuthManager.get_valid_token()` checks expiry (1-hour buffer)
2. If expired, calls `refresh_token()`
3. New token generated and stored
4. Returns valid token to caller

### Manual Refresh

If needed, force refresh:
```bash
docker exec api python -c "
from routes.dhan import write_token_to_file
from dhan_auth import DhanAuthManager

manager = DhanAuthManager()
new_token = manager.refresh_token()
print('Token refreshed:', new_token[:20] + '...')
"
```

## Security Best Practices

### 1. Environment Variables
- ✅ Store credentials in `.env` (not committed to git)
- ✅ Use strong, unique values for each environment
- ✅ Rotate API secrets periodically

### 2. Token Storage
- ✅ File permissions: `600` (owner read/write only)
- ✅ Volume mount: `/app/data` (persistent across restarts)
- ✅ Backup: Include in regular backup schedule

### 3. HTTPS Only
- ✅ Enforce HTTPS in production (nginx config)
- ✅ Redirect URL must use HTTPS
- ✅ HSTS headers enabled

### 4. Redis Security
- ✅ Password-protected Redis connection
- ✅ TTL on all cached tokens (24h max)
- ✅ No sensitive data in Redis logs

### 5. API Endpoints
- ✅ `/api/dhan/token` endpoint: Internal use only
- ✅ Rate limiting on authentication endpoints
- ✅ Log all authentication attempts

## Troubleshooting

### Issue: "Failed to generate consent"

**Possible causes**:
- Invalid API key or secret
- Network connectivity to auth.dhan.co
- Redirect URL mismatch

**Solution**:
```bash
# Check environment variables
docker exec api env | grep DHAN

# Test API connectivity
docker exec api curl -X POST https://auth.dhan.co/app/generate-consent \
  -H "Content-Type: application/json" \
  -d '{"apiKey":"YOUR_KEY","secretKey":"YOUR_SECRET","redirectUrl":"YOUR_URL"}'

# Verify redirect URL matches Dhan console
```

### Issue: "Token expired" immediately after login

**Possible causes**:
- System clock out of sync
- Timezone mismatch in expiry parsing

**Solution**:
```bash
# Check system time
docker exec api date

# Sync time (on host)
sudo ntpdate pool.ntp.org

# Verify token expiry
docker exec api python -c "
import json
with open('/app/data/dhan_token.json') as f:
    token = json.load(f)
    print('Token expires:', token['expiry'])
"
```

### Issue: "Consent app ID not found in Redis"

**Possible causes**:
- Redis connection failure
- Consent expired (5 min limit)
- User took too long to login

**Solution**:
```bash
# Check Redis connectivity
docker exec api redis-cli -u $REDIS_URL ping

# Increase consent TTL (in dhan.py)
redis_client.setex("dhan_consent_app_id", 600, consent_app_id)  # 10 minutes

# Clear Redis cache if needed
docker exec api redis-cli -u $REDIS_URL FLUSHDB
```

### Issue: "Authentication successful but no data ingestion"

**Possible causes**:
- Depth collector not using OAuth token
- Token file permissions
- Service restart needed

**Solution**:
```bash
# Verify token file exists and readable
docker exec depth-collector ls -lah /app/data/dhan_token.json

# Check depth collector logs
docker logs depth-collector --tail 50

# Restart depth collector
docker-compose restart depth-collector

# Test token manually
docker exec depth-collector python -c "
from dhan_auth import get_dhan_credentials
creds = get_dhan_credentials()
print('Using token:', creds['access_token'][:20] + '...')
"
```

## Migration from Manual Tokens

If currently using manual token generation:

### Before (Manual)
```python
# In .env
DHAN_ACCESS_TOKEN=manually_generated_token

# In code
access_token = os.getenv("DHAN_ACCESS_TOKEN")
client = dhanhq(client_id, access_token)
```

### After (OAuth)
```python
# In .env (once)
DHAN_API_KEY=your_api_key
DHAN_API_SECRET=your_api_secret
DHAN_CLIENT_ID=your_client_id

# In code (auto-managed)
from dhan_auth import get_dhan_credentials
creds = get_dhan_credentials()
client = dhanhq(creds["client_id"], creds["access_token"])
```

### Migration Steps
1. Backup existing `.env` file
2. Add Dhan OAuth variables to `.env`
3. Update all services to use `get_dhan_credentials()`
4. Test OAuth flow in browser
5. Remove manual `DHAN_ACCESS_TOKEN` from `.env`
6. Deploy changes

## Comparison: Kite vs Dhan OAuth

| Feature | KiteConnect | Dhan |
|---------|-------------|------|
| **OAuth Steps** | 2-step (request token → access token) | 3-step (consent → tokenId → access token) |
| **Token Validity** | 24 hours | 12 months |
| **Daily Login** | Yes | No |
| **Auto-Refresh** | No (manual re-auth) | Yes (via /RenewToken) |
| **Token Storage** | File only | File + Redis |
| **Callback URL** | `/api/kite/callback` | `/api/dhan/callback` |
| **Status Endpoint** | `/api/kite/status` | `/api/dhan/status` |
| **UI Integration** | `?provider=kite` | `?provider=dhan` |

## Files Modified

### Backend (API)
- ✅ `services/api/routes/dhan.py` (NEW - 385 lines)
- ✅ `services/api/routes/__init__.py` (UPDATED - added dhan import)
- ✅ `services/api/main.py` (UPDATED - registered dhan router)

### Frontend
- ✅ `services/frontend/public/index.html` (UPDATED - added Dhan button)
- ✅ `services/frontend/public/login.html` (UPDATED - dual provider support)
- ✅ `services/frontend/public/success.html` (UPDATED - provider-specific status)

### Auth Manager
- ✅ `services/depth_collector/dhan_auth.py` (EXISTING - used by depth collector)
- ✅ `dhan_auth.py` (EXISTING - copy for standalone scripts)

### Configuration
- ✅ `.env.example` (UPDATED - added Dhan variables)
- ✅ `DHAN_OAUTH_README.md` (NEW - this file)

## Next Steps

1. ✅ **Deploy**: Push changes to VPS
2. ✅ **Configure**: Update `.env` with Dhan credentials
3. ✅ **Test**: Complete OAuth flow in browser
4. ✅ **Monitor**: Check depth collector uses OAuth token
5. ✅ **Document**: Update main README with Dhan support

## Support

For issues or questions:
- Check [DHAN_AUTH_SETUP.md](DHAN_AUTH_SETUP.md) for detailed setup
- Review Dhan API docs: https://dhanhq.co/docs/
- Check logs: `docker logs api` or `docker logs depth-collector`

---

**Last Updated**: 2024-01-01  
**Version**: 1.0.0  
**Status**: Production Ready ✅
