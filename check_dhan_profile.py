import requests
import os
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')

print(f"CLIENT_ID: {CLIENT_ID}")
print()

# Check profile/fund limits
url = "https://api.dhan.co/v2/fundlimit"
headers = {
    'access-token': ACCESS_TOKEN,
    'Content-Type': 'application/json'
}

print("Checking Dhan account profile...")
response = requests.get(url, headers=headers)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")
print()

# Check if there's a data subscriptions endpoint
print("=" * 60)
print("Checking data subscriptions...")
url2 = "https://api.dhan.co/v2/edis/profile"
response2 = requests.get(url2, headers=headers)
print(f"Profile Status: {response2.status_code}")
print(f"Profile Response: {response2.text}")
