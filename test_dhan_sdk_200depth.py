from dhanhq import marketfeed
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')

print(f"CLIENT_ID: {CLIENT_ID}")
print(f"Testing 200-depth with official Dhan SDK...")
print()

# Instruments to subscribe
instruments = [(marketfeed.NSE_FNO, "49229")]  # NIFTY JAN 2026 FUT

# Callback for market data
def on_message(instance, message):
    print(f"[MESSAGE] Received: {message}")

# Callback for open
def on_open(instance):
    print("[OPEN] WebSocket connected via SDK!")

# Callback for error
def on_error(instance, error):
    print(f"[ERROR] {error}")

# Callback for close
def on_close(instance):
    print("[CLOSE] WebSocket disconnected")

# Create feed instance with 200-level depth
print("Initializing MarketFeed with depth_200=True...")
try:
    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        instruments,
        subscription_code=marketfeed.Depth,  # or try marketfeed.FullMarketDepth
        depth_200=True  # Request 200-level depth
    )
    
    feed.on_message = on_message
    feed.on_open = on_open
    feed.on_error = on_error
    feed.on_close = on_close
    
    print("Connecting to Dhan WebSocket via SDK...")
    feed.run_forever()
    
except Exception as e:
    print(f"Error: {e}")
    print("\nTrying alternative approach...")
    
    # Try without depth_200 parameter in case it's not supported
    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        instruments,
        subscription_code=marketfeed.FullMarketDepth
    )
    
    feed.on_message = on_message
    feed.on_open = on_open
    feed.on_error = on_error
    feed.on_close = on_close
    
    print("Connecting...")
    feed.run_forever()
