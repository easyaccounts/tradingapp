# Dhan OAuth Testing Checklist

## Pre-Deployment Testing (Local)

### API Endpoints

#### 1. Test Login URL Generation
```bash
# Start services
docker-compose up -d api

# Test endpoint
curl http://localhost:8000/api/dhan/login-url

# Expected response:
# {
#   "login_url": "https://auth.dhan.co/login/consentApp-login?consentAppId=...",
#   "consent_app_id": "...",
#   "expiry": "2024-01-01T10:05:00"
# }
```

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### 2. Test Status Endpoint (No Token)
```bash
curl http://localhost:8000/api/dhan/status

# Expected response:
# {
#   "authenticated": false,
#   "source": "none"
# }
```

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### 3. Test Token Endpoint (No Token)
```bash
curl http://localhost:8000/api/dhan/token

# Expected response:
# {
#   "error": "No Dhan token found"
# }
# Status: 404
```

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### Frontend Pages

#### 4. Test Index Page
```
URL: http://localhost/
```

**Checks**:
- [ ] Page loads without errors
- [ ] "Connect Kite" button visible
- [ ] "Connect Dhan" button visible
- [ ] Both buttons have correct href: `/login.html?provider=kite` and `/login.html?provider=dhan`
- [ ] Footer mentions "Powered by KiteConnect, Dhan & TimescaleDB"

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### 5. Test Kite Login Page
```
URL: http://localhost/login.html?provider=kite
```

**Checks**:
- [ ] Page title: "Kite Login - Trading Platform"
- [ ] Heading: "Kite Login"
- [ ] Subtitle mentions "Zerodha"
- [ ] Button text: "Login with Kite"
- [ ] Info box mentions "Zerodha's login page" and "Kite credentials"

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### 6. Test Dhan Login Page
```
URL: http://localhost/login.html?provider=dhan
```

**Checks**:
- [ ] Page title: "Dhan Login - Trading Platform"
- [ ] Heading: "Dhan Login"
- [ ] Subtitle mentions "Dhan"
- [ ] Button text: "Login with Dhan"
- [ ] Info box mentions "Dhan's login page" and "Dhan credentials"

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### 7. Test Success Page (Kite)
```
URL: http://localhost/success.html?provider=kite
```

**Checks**:
- [ ] Success message mentions "Kite"
- [ ] Fetches from `/api/kite/status`
- [ ] Shows connection status
- [ ] Displays token expiry in hours/minutes format

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### 8. Test Success Page (Dhan)
```
URL: http://localhost/success.html?provider=dhan
```

**Checks**:
- [ ] Success message mentions "Dhan"
- [ ] Fetches from `/api/dhan/status`
- [ ] Shows connection status
- [ ] Displays token expiry in days format (when token exists)

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

## Production Deployment Testing

### 9. VPS Access
```bash
ssh root@82.180.144.255
```

**Checks**:
- [ ] SSH connection successful
- [ ] Navigate to `/opt/tradingapp`
- [ ] Git repository up to date
- [ ] Docker services running

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 10. Environment Variables
```bash
docker exec api env | grep DHAN
```

**Checks**:
- [ ] DHAN_API_KEY set
- [ ] DHAN_API_SECRET set
- [ ] DHAN_CLIENT_ID set
- [ ] DHAN_REDIRECT_URL=https://zopilot.in/api/dhan/callback
- [ ] DOMAIN=zopilot.in
- [ ] ENVIRONMENT=production

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 11. Dhan Console Configuration

**Visit**: https://api-console.dhan.co/

**Checks**:
- [ ] App exists with correct name
- [ ] Redirect URL: `https://zopilot.in/api/dhan/callback` (exact match)
- [ ] App is active/enabled
- [ ] API key matches .env value
- [ ] Required permissions enabled (market data, orders)

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 12. Production API Endpoints

#### Test Login URL (Production)
```bash
curl https://zopilot.in/api/dhan/login-url
```

**Expected**: JSON with `login_url` field

**Status**: [ ] Pass [ ] Fail

**Response**: _____________________________________

---

#### Test Status Endpoint (Production)
```bash
curl https://zopilot.in/api/dhan/status
```

**Expected**: JSON with `authenticated` field (false initially)

**Status**: [ ] Pass [ ] Fail

**Response**: _____________________________________

---

### 13. Full OAuth Flow (Production)

**Steps**:
1. [ ] Visit https://zopilot.in/
2. [ ] Page loads with both "Connect Kite" and "Connect Dhan" buttons
3. [ ] Click "Connect Dhan" button
4. [ ] Redirected to https://zopilot.in/login.html?provider=dhan
5. [ ] Page shows "Dhan Login" heading
6. [ ] Click "Login with Dhan" button
7. [ ] Loading spinner appears
8. [ ] Redirected to https://auth.dhan.co/login/consentApp-login?consentAppId=...
9. [ ] Dhan login page loads
10. [ ] Enter Dhan credentials (username + password)
11. [ ] 2FA verification (if enabled)
12. [ ] Authorize app access
13. [ ] Redirected to https://zopilot.in/api/dhan/callback?tokenId=...
14. [ ] Backend processes callback (may take 2-3 seconds)
15. [ ] Redirected to https://zopilot.in/success.html?provider=dhan
16. [ ] Success page shows:
    - [ ] "Your Dhan account is now connected"
    - [ ] Connection Status: ✓ Connected
    - [ ] Token Expiry: Shows days remaining (~365 days)
    - [ ] User ID: Shows client name
    - [ ] Ingestion Status: ✓ Active (after 2 seconds)

**Status**: [ ] Pass [ ] Fail

**Issues encountered**: _____________________________________

---

### 14. Token Storage Verification

#### Check token file exists
```bash
ssh root@82.180.144.255
docker exec api cat /app/data/dhan_token.json
```

**Expected structure**:
```json
{
    "access_token": "eyJhbGc...",
    "expiry": "2025-01-01T00:00:00",
    "client_id": "DHAN12345",
    "client_name": "John Doe",
    "generated_at": "2024-01-01T10:00:00"
}
```

**Checks**:
- [ ] File exists
- [ ] Contains access_token (long JWT string)
- [ ] Expiry is ~12 months in future
- [ ] client_id matches Dhan account
- [ ] client_name shows user's name

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### Check Redis cache
```bash
docker exec api redis-cli -u $REDIS_URL KEYS "dhan_*"
```

**Expected keys**:
- `dhan_access_token`
- `dhan_token_expiry`
- `dhan_client_name`
- `dhan_token_id`

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 15. Depth Collector Integration

#### Check depth collector can read token
```bash
docker exec depth-collector python -c "
from dhan_auth import get_dhan_credentials
creds = get_dhan_credentials()
print('Authenticated:', 'access_token' in creds)
print('Client:', creds.get('client_name', 'N/A'))
print('Token length:', len(creds.get('access_token', '')))
"
```

**Expected output**:
```
Authenticated: True
Client: John Doe
Token length: 200+ (JWT token)
```

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

#### Restart depth collector
```bash
docker-compose restart depth-collector
docker logs depth-collector --tail 50
```

**Checks**:
- [ ] No authentication errors
- [ ] Shows "Successfully authenticated" or similar
- [ ] WebSocket connection established
- [ ] Data ingestion starts

**Status**: [ ] Pass [ ] Fail

**Logs**: _____________________________________

---

### 16. Re-authentication Test

**Purpose**: Verify existing token gets replaced

**Steps**:
1. [ ] Note current token expiry date
2. [ ] Complete OAuth flow again (repeat test #13)
3. [ ] Check token file for new expiry date
4. [ ] Verify new token is different from old token

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 17. Concurrent Kite + Dhan Test

**Purpose**: Verify both providers work independently

**Steps**:
1. [ ] Connect Kite account (existing flow)
2. [ ] Verify Kite token stored: `/app/data/access_token.txt`
3. [ ] Connect Dhan account (new flow)
4. [ ] Verify Dhan token stored: `/app/data/dhan_token.json`
5. [ ] Check both status endpoints:
   ```bash
   curl https://zopilot.in/api/kite/status
   curl https://zopilot.in/api/dhan/status
   ```
6. [ ] Verify both show `"authenticated": true`

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

## Error Handling Tests

### 18. Invalid API Credentials

**Setup**: Temporarily set wrong API key in .env

**Test**:
```bash
# Update .env
DHAN_API_KEY=invalid_key

# Restart API
docker-compose restart api

# Try OAuth flow
curl https://zopilot.in/api/dhan/login-url
```

**Expected**: Error message, HTTP 500

**Status**: [ ] Pass [ ] Fail

**Error message**: _____________________________________

**Restore**: Set correct API key and restart

---

### 19. Expired Consent (Timeout Test)

**Steps**:
1. [ ] Click "Login with Dhan" button
2. [ ] Wait on Dhan login page for > 5 minutes (don't login)
3. [ ] Try to login after timeout
4. [ ] Verify error message shown

**Expected**: "Consent expired" or similar error

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 20. Network Failure Simulation

**Test**: Disconnect from internet during OAuth flow

**Steps**:
1. [ ] Start OAuth flow
2. [ ] Disconnect network after redirect to Dhan
3. [ ] Try to complete login
4. [ ] Verify appropriate error handling

**Expected**: Error message, retry option

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

## Performance Tests

### 21. OAuth Flow Timing

**Measure**:
- Time from clicking button to redirect: _______ seconds
- Time on Dhan login page (user entry): _______ seconds
- Time from callback to success page: _______ seconds
- Total OAuth flow time: _______ seconds

**Expected**: < 10 seconds total (excluding user entry)

**Status**: [ ] Acceptable [ ] Too Slow

---

### 22. Status Endpoint Performance

**Test**:
```bash
time curl https://zopilot.in/api/dhan/status
```

**Response time**: _______ ms

**Expected**: < 100ms

**Status**: [ ] Pass [ ] Fail

---

### 23. Concurrent Users Simulation

**Test**: Multiple users OAuth simultaneously

**Setup**: Use multiple browsers/devices

**Steps**:
1. [ ] User 1 starts OAuth flow
2. [ ] User 2 starts OAuth flow (before User 1 completes)
3. [ ] Both complete authentication
4. [ ] Verify both get correct tokens

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

## Security Tests

### 24. Token File Permissions

**Check**:
```bash
docker exec api ls -lah /app/data/dhan_token.json
```

**Expected permissions**: `-rw-------` (600) or `-rw-r--r--` (644)

**Status**: [ ] Pass [ ] Fail

**Actual permissions**: _____________________________________

---

### 25. HTTPS Enforcement

**Test**: Try HTTP access
```bash
curl http://zopilot.in/api/dhan/login-url
```

**Expected**: Redirect to HTTPS or connection refused

**Status**: [ ] Pass [ ] Fail

**Response**: _____________________________________

---

### 26. Token Not Exposed in Logs

**Check**:
```bash
docker logs api --tail 200 | grep -i "eyJ"
```

**Expected**: No JWT tokens in logs

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 27. CORS Policy

**Test**: Cross-origin request from unauthorized domain

**Expected**: CORS error or request blocked

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

## Regression Tests

### 28. Kite OAuth Still Works

**Verify**: Existing Kite OAuth not affected by Dhan changes

**Steps**:
1. [ ] Visit https://zopilot.in/login.html?provider=kite
2. [ ] Complete Kite OAuth flow
3. [ ] Verify Kite token stored
4. [ ] Check `/api/kite/status` works

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 29. Dashboard Still Accessible

**Test**:
```
URL: https://zopilot.in/dashboard.html
```

**Checks**:
- [ ] Page loads
- [ ] No JavaScript errors
- [ ] Market data displays (if services running)

**Status**: [ ] Pass [ ] Fail

**Notes**: _____________________________________

---

### 30. Existing Services Unaffected

**Check each service**:
```bash
docker-compose ps
```

**Verify running**:
- [ ] postgres
- [ ] redis
- [ ] rabbitmq
- [ ] api (✓ modified)
- [ ] frontend (✓ modified)
- [ ] worker
- [ ] depth-collector
- [ ] signal-generator

**Status**: [ ] Pass [ ] Fail

**Issues**: _____________________________________

---

## Final Checklist

### Documentation
- [ ] DHAN_OAUTH_README.md complete
- [ ] DHAN_OAUTH_IMPLEMENTATION_SUMMARY.md complete
- [ ] .env.example updated
- [ ] Main README updated (if needed)

### Code Quality
- [ ] No errors in `get_errors` output
- [ ] All routes registered correctly
- [ ] Frontend pages tested
- [ ] API endpoints tested

### Deployment
- [ ] Code pushed to repository
- [ ] VPS updated with latest code
- [ ] Services restarted
- [ ] Environment variables configured

### Testing Summary
- Total tests: 30
- Passed: ___
- Failed: ___
- Skipped: ___

### Sign-off
- Tested by: _____________________
- Date: _____________________
- Environment: [ ] Local [ ] VPS [ ] Both
- Ready for production: [ ] Yes [ ] No

### Notes
_________________________________________________________
_________________________________________________________
_________________________________________________________
_________________________________________________________

---

## Emergency Rollback

If critical issues found:

```bash
ssh root@82.180.144.255
cd /opt/tradingapp

# Rollback code
git revert HEAD
git push

# Rebuild and restart
docker-compose build api
docker-compose restart api frontend

# Verify services
docker-compose ps
```

**Rollback executed**: [ ] Yes [ ] No  
**Reason**: _____________________________________
