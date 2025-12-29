# Dhan API Key Authentication Setup Guide

## Overview

Migrated from **manual 24-hour tokens** to **API key-based authentication** with:
- ✅ 12-month API key validity (vs 24 hours)
- ✅ Auto token refresh
- ✅ File-based token caching
- ✅ No daily manual login required
- ✅ Graceful fallback to manual token

---

## Quick Setup (Choose One Method)

### Method 1: API Key (Recommended) - One-time setup

**Step 1: Generate API Key & Secret**

1. Login to https://web.dhan.co
2. Go to: My Profile → Access DhanHQ APIs
3. Toggle to **"API key"**
4. Fill in:
   - **App Name**: `Trading App Depth Collector`
   - **Redirect URL**: `http://localhost:8080/callback` (or your callback URL)
   - **Postback URL**: (optional)
5. Click **Generate**
6. Copy and save:
   - `API Key`
   - `API Secret`
   - `Client ID`

**Step 2: Update Environment Variables**

SSH to VPS and edit `.env`:
```bash
ssh root@82.180.144.255
cd /opt/tradingapp
nano .env
```

Add/update these lines:
```bash
# Dhan API Key Authentication (recommended)
DHAN_API_KEY=your_api_key_here
DHAN_API_SECRET=your_api_secret_here
DHAN_CLIENT_ID=your_client_id

# Optional: Remove old manual token variables
# DHAN_ACCESS_TOKEN=...  # Can remove this line
```

Save and exit (Ctrl+X, Y, Enter)

**Step 3: One-Time OAuth Flow**

The first time you start the service, it will print instructions:

```bash
cd /opt/tradingapp
docker-compose build depth-collector
docker-compose up depth-collector
```

You'll see output like:
```
===============================================================================
MANUAL STEP REQUIRED (one-time)
===============================================================================

1. Open this URL in browser:
   https://auth.dhan.co/login/consentApp-login?consentAppId=abc123...

2. Login with your Dhan credentials

3. After successful login, you'll be redirected with tokenId

4. Copy the tokenId from URL and set as environment variable:
   export DHAN_TOKEN_ID=<your_token_id>

5. Restart the service
===============================================================================
```

**Step 4: Complete OAuth**

1. Open the URL from above in your browser
2. Login with your Dhan credentials (OTP/TOTP)
3. After redirect, copy the `tokenId` from URL
   - URL will look like: `http://localhost:8080/callback?tokenId=xyz789...`
   - Copy just the `xyz789...` part

**Step 5: Save Token ID and Restart**

```bash
cd /opt/tradingapp
nano .env
```

Add this line:
```bash
DHAN_TOKEN_ID=xyz789_your_token_id_here
```

Save and restart:
```bash
docker-compose restart depth-collector
```

**Done!** Service will now:
- Load token from cache
- Auto-refresh before expiry
- No manual intervention needed for 12 months

---

### Method 2: Manual Token (Current Method) - Daily renewal

Keep using current method if you don't want to do OAuth setup:

**Daily routine**:
1. Login to https://web.dhan.co
2. My Profile → Access DhanHQ APIs
3. Generate "Access Token" (24-hour validity)
4. Update `.env` with new token:
   ```bash
   DHAN_ACCESS_TOKEN=new_token_here
   ```
5. Restart: `docker-compose restart depth-collector`

---

## Token Storage Location

Tokens are cached in: `/app/data/dhan_token.json` (inside container)

**To persist across container restarts**, add volume in `docker-compose.yml`:

```yaml
depth-collector:
  volumes:
    - dhan_tokens:/app/data  # Add this line
```

And in volumes section:
```yaml
volumes:
  dhan_tokens:
```

---

## Verification

### Check Authentication Status

```bash
# View logs
docker logs tradingapp-depth-collector --tail 50

# Should see:
# ✓ Authenticated as Client ID: 1100003626
# ✓ Using cached token (expires: 2025-12-30 09:00:00)
```

### Test Token Validity

```bash
# Check token file
docker exec tradingapp-depth-collector cat /app/data/dhan_token.json

# Should show:
# {
#   "access_token": "eyJ...",
#   "expiry": "2025-12-30T09:00:00",
#   "client_id": "1100003626",
#   "generated_at": "2025-12-29T10:15:30"
# }
```

### Manual Token Test

```bash
docker exec tradingapp-depth-collector python -c "
from dhan_auth import get_dhan_credentials
creds = get_dhan_credentials()
print(f'Token: {creds[\"access_token\"][:50]}...')
print(f'Client: {creds[\"client_id\"]}')
"
```

---

## Troubleshooting

### Issue: "Manual OAuth step required"

**Cause**: First-time setup or tokenId not provided

**Solution**: Follow Step 3-5 above to complete one-time OAuth

---

### Issue: "Token expired or missing"

**Cause**: Token file doesn't exist or is expired

**Solution**: 
- If using API key: Will auto-regenerate (may need OAuth)
- If using manual token: Update `DHAN_ACCESS_TOKEN` in `.env`

```bash
nano .env  # Update token
docker-compose restart depth-collector
```

---

### Issue: "Authentication failed"

**Cause**: Invalid credentials

**Solution**: Verify environment variables
```bash
docker exec tradingapp-depth-collector env | grep DHAN
```

Should show:
```
DHAN_API_KEY=...
DHAN_API_SECRET=...
DHAN_CLIENT_ID=...
DHAN_TOKEN_ID=...  (if completed OAuth)
```

If missing, update `.env` and restart.

---

### Issue: Token not persisting across restarts

**Cause**: No volume mounted for `/app/data`

**Solution**: Add volume to docker-compose.yml (see "Token Storage Location" above)

---

## Setup TOTP (Optional - Fully Automated)

For completely hands-free operation:

**Step 1: Enable TOTP on Dhan**
1. Login to https://web.dhan.co
2. DhanHQ Trading APIs → Setup TOTP
3. Scan QR with Google Authenticator / Authy
4. Confirm with first TOTP code

**Step 2: Use dhanhq SDK (Advanced)**

Currently, the basic implementation requires manual browser login once. For fully automated TOTP-based auth, you would need to:

1. Install dhanhq SDK: `pip install dhanhq`
2. Modify `dhan_auth.py` to use SDK
3. Provide TOTP secret for automated code generation

This is optional and can be implemented later if needed.

---

## Migration Checklist

- [ ] Generate API Key & Secret from web.dhan.co
- [ ] Update `.env` with `DHAN_API_KEY`, `DHAN_API_SECRET`, `DHAN_CLIENT_ID`
- [ ] Rebuild container: `docker-compose build depth-collector`
- [ ] Start and complete one-time OAuth
- [ ] Save `DHAN_TOKEN_ID` to `.env`
- [ ] Restart: `docker-compose restart depth-collector`
- [ ] Verify logs show successful authentication
- [ ] Monitor for 24 hours to confirm auto-refresh works
- [ ] Remove `DHAN_ACCESS_TOKEN` from `.env` (optional)

---

## Benefits After Migration

| Aspect | Before | After |
|--------|--------|-------|
| **Token Validity** | 24 hours | 12 months (API key) |
| **Daily Manual Work** | Yes (regenerate token) | No |
| **OAuth Setup** | Never | Once (initial) |
| **Auto Refresh** | No | Yes (every 23 hours) |
| **Service Restart** | Breaks without new token | Continues with cached token |
| **Monitoring Needed** | Daily | None |

---

## Fallback Plan

If API key authentication fails, the system automatically falls back to manual token mode:

```bash
# Temporarily use manual token
export DHAN_ACCESS_TOKEN=your_24hr_token
docker-compose restart depth-collector
```

Service will use manual token until API key setup is completed.

---

## Support

**Dhan Documentation**:
- API Key Setup: https://dhanhq.co/docs/v2/authentication/#api-key-secret
- TOTP Setup: https://dhanhq.co/docs/v2/authentication/#setup-totp

**Check Service Status**:
```bash
# Logs
docker logs tradingapp-depth-collector -f

# Token status
docker exec tradingapp-depth-collector cat /app/data/dhan_token.json | python -m json.tool

# Environment
docker exec tradingapp-depth-collector env | grep DHAN
```

---

## Summary

✅ **Implemented**: Reusable auth module with file-based token caching and auto-refresh  
✅ **Setup Required**: One-time OAuth flow (10 minutes)  
✅ **Maintenance**: None for 12 months  
✅ **Fallback**: Manual token method still supported  

After initial setup, your depth-collector will run unattended with automatic token management!
