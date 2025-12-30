from dhan_auth import get_dhan_credentials
import requests

creds = get_dhan_credentials()
print('Token (first 30 chars):', creds['access_token'][:30] + '...')
print('Client ID:', creds['client_id'])
print()
print('Testing with Market Quote API...')

url = 'https://api.dhan.co/v2/marketfeed/ltp'
headers = {
    'access-token': creds['access_token'],
    'Content-Type': 'application/json'
}
payload = {'NSE_FNO': ['49543']}

try:
    resp = requests.post(url, json=payload, headers=headers)
    print('Status Code:', resp.status_code)
    print('Response:', resp.text)
except Exception as e:
    print('Error:', e)
