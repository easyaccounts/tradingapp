from dhanhq import marketfeed
import os
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')

print(f"CLIENT_ID: {CLIENT_ID}")
print()

# Instruments to subscribe - NIFTY JAN 2026 FUT
instruments = [(marketfeed.NSE_FNO, "49229")]

# Callback for market data
def on_message(instance, message):
    print(f"[MESSAGE] {message}")

# Callback for open
def on_connect(instance):
    print("[CONNECTED] WebSocket opened via SDK")
    # Try subscribing to Depth (19) which might be the 20-level depth
    print("Subscribing to Depth feed...")

# Callback for error
def on_error(instance, error):
    print(f"[ERROR] {error}")

# Test 1: Try Depth (19) - might be 20-level depth
print("Test 1: Creating DhanFeed with Depth subscription...")
try:
    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        instruments,
        version='v2'  # Try v2 in case it supports more features
    )
    
    # Check if there's a subscribe method
    print(f"DhanFeed methods: {[m for m in dir(feed) if not m.startswith('_')]}")
    print()
    
    # Subscribe to depth
    if hasattr(feed, 'subscribe_depth'):
        print("Found subscribe_depth method")
        feed.subscribe_depth()
    elif hasattr(feed, 'subscribe'):
        print("Found subscribe method, trying with Depth code...")
        feed.subscribe(marketfeed.Depth)
    
    feed.on_message = on_message
    feed.on_connect = on_connect
    feed.on_error = on_error
    
    print("Connecting...")
    feed.run_forever()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
