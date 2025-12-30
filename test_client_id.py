from dhan_auth import get_dhan_credentials
import os

print("=" * 60)
print("CLIENT_ID from environment:", os.getenv("DHAN_CLIENT_ID"))
creds = get_dhan_credentials()
print("CLIENT_ID from credentials:", creds.get("client_id"))
print("=" * 60)
