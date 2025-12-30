import websocket
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
SECURITY_ID = '49229'  # NIFTY JAN 2026 FUT

print(f"Testing 20-Level Depth (more commonly available)")
print(f"CLIENT_ID: {CLIENT_ID}")
print(f"SECURITY_ID: {SECURITY_ID}")
print()

def on_message(ws, message):
    print(f"[MESSAGE] Received {len(message)} bytes")
    if isinstance(message, bytes):
        print(f"[MESSAGE] Binary message: {message[:50].hex()}...")
    else:
        print(f"[MESSAGE] Text message: {message}")

def on_error(ws, error):
    print(f"[ERROR] {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"[CLOSE] Status: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    print("[OPEN] WebSocket connected!")
    
    # Subscribe to 20-level depth (different format - has InstrumentList)
    subscription = {
        'RequestCode': 23,
        'InstrumentCount': 1,
        'InstrumentList': [
            {
                'ExchangeSegment': 'NSE_FNO',
                'SecurityId': SECURITY_ID
            }
        ]
    }
    
    print(f"[SUBSCRIBE] Sending: {json.dumps(subscription, indent=2)}")
    ws.send(json.dumps(subscription))
    print("[SUBSCRIBE] Subscription sent, waiting for data...")

if __name__ == "__main__":
    # Build WebSocket URL for 20-level depth
    ws_url = (
        f"wss://depth-api-feed.dhan.co/twentydepth?"
        f"token={ACCESS_TOKEN}&"
        f"clientId={CLIENT_ID}&"
        f"authType=2"
    )
    
    print(f"[CONNECT] Connecting to Dhan 20-Level Depth WebSocket...")
    print(f"[CONNECT] URL: wss://depth-api-feed.dhan.co/twentydepth?token=...&clientId={CLIENT_ID}&authType=2")
    print()
    
    # Create WebSocket connection
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run forever
    ws.run_forever()
