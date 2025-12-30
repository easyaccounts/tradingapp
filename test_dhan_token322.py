from dhan_auth import get_dhan_credentials
import requests
import base64
import json

print("=" * 80)
print("DHAN TOKEN TEST")
print("=" * 80)

creds = get_dhan_credentials()
print('\n✓ Credentials loaded:')
print(f'  Token (first 30 chars): {creds["access_token"][:30]}...')
print(f'  Token (last 10 chars): ...{creds["access_token"][-10:]}')
print(f'  Client ID from file: {creds["client_id"]}')

# Decode JWT token to see what's inside
try:
    token_parts = creds["access_token"].split('.')
    if len(token_parts) >= 2:
        # Decode payload (add padding if needed)
        payload = token_parts[1]
        padding = len(payload) % 4
        if padding:
            payload += '=' * (4 - padding)
        decoded = base64.urlsafe_b64decode(payload)
        token_data = json.loads(decoded)
        print(f'\n✓ JWT Token decoded:')
        print(f'  dhanClientId in token: {token_data.get("dhanClientId")}')
        print(f'  Issuer: {token_data.get("iss")}')
        print(f'  Expires (epoch): {token_data.get("exp")}')
        
        if token_data.get("dhanClientId") != creds["client_id"]:
            print(f'\n✗ WARNING: Client ID mismatch!')
            print(f'  Token has: {token_data.get("dhanClientId")}')
            print(f'  File has: {creds["client_id"]}')
except Exception as e:
    print(f'\n✗ Could not decode token: {e}')

if not creds['client_id']:
    print('\n✗ ERROR: Client ID is None/empty!')
    print('  This will cause 401 errors with Dhan API')
    exit(1)

print('\n' + "=" * 80)
print('Testing with Market Quote API (LTP)...')
print("=" * 80)

url = 'https://api.dhan.co/v2/marketfeed/ltp'

# Test 1: Standard approach (token in header)
print('\nTest 1: Token in access-token header')
headers1 = {
    'access-token': creds['access_token'],
    'Content-Type': 'application/json'
}
payload = {'NSE_FNO': ['49543']}

try:
    resp = requests.post(url, json=payload, headers=headers1)
    print(f'Status Code: {resp.status_code}')
    print(f'Response: {resp.text}')
except Exception as e:
    print(f'Error: {e}')

# Test 2: With dhanClientId in header (CORRECT per Dhan docs!)
print('\nTest 2: Token + dhanClientId in headers (CORRECT FORMAT)')
headers2 = {
    'access-token': creds['access_token'],
    'dhanClientId': creds['client_id'],
    'Content-Type': 'application/json'
}

try:
    resp = requests.post(url, json=payload, headers=headers2)
    print(f'Status Code: {resp.status_code}')
    print(f'Response: {resp.text}')
    
    if resp.status_code == 200:
        print('✓ SUCCESS! This is the correct format')
except Exception as e:
    print(f'Error: {e}')

# Test 3: Check profile to see data subscription status
print('\nTest 3: Profile API (check data subscription)')
profile_url = 'https://api.dhan.co/v2/profile'
headers3 = {
    'access-token': creds['access_token'],
    'Content-Type': 'application/json'
}

try:
    resp = requests.get(profile_url, headers=headers3)
    print(f'Status Code: {resp.status_code}')
    if resp.status_code == 200:
        print('✓ Token is valid!')
        profile = resp.json()
        print(f'\nProfile Data:')
        print(f'  Client ID: {profile.get("dhanClientId")}')
        print(f'  Token Validity: {profile.get("tokenValidity")}')
        print(f'  Data Plan: {profile.get("dataPlan")}')
        print(f'  Data Validity: {profile.get("dataValidity")}')
        print(f'  Active Segments: {profile.get("activeSegment")}')
        
        if profile.get("dataPlan") != "Active":
            print('\n✗ WARNING: Data Plan is NOT Active!')
            print('  This explains why market data APIs are failing')
            print('  Activate data subscription at web.dhan.co')
    else:
        print(f'Response: {resp.text}')
except Exception as e:
    print(f'Error: {e}')

print("=" * 80)
