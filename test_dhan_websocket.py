import websocket
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')
SECURITY_ID = '1333'  # RELIANCE (NSE_EQ - always active)

print(f"ACCESS_TOKEN: {ACCESS_TOKEN[:20]}..." if ACCESS_TOKEN else "None")
print(f"CLIENT_ID: {CLIENT_ID}")
print(f"SECURITY_ID: {SECURITY_ID}")
print()

def on_message(ws, message):
    print(f"[MESSAGE] Received {len(message)} bytes")
    if isinstance(message, bytes):
        print(f"[MESSAGE] Binary message: {message[:50]}...")
    else:
        print(f"[MESSAGE] Text message: {message}")

def on_error(ws, error):
    print(f"[ERROR] {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"[CLOSE] Status: {close_status_code}, Message: {close_msg}")

def on_open(ws):
    print("[OPEN] WebSocket connected!")
    
    # Subscribe to 200-level depth
    subscription = {
        'RequestCode': 23,
        'ExchangeSegment': 'NSE_EQ',
        'SecurityId': SECURITY_ID
    }
    
    print(f"[SUBSCRIBE] Sending: {subscription}")
    ws.send(json.dumps(subscription))
    print("[SUBSCRIBE] Subscription sent, waiting for data...")

if __name__ == "__main__":
    # Build WebSocket URL
    ws_url = (
        f"wss://full-depth-api.dhan.co/twohundreddepth?"
        f"token={ACCESS_TOKEN}&"
        f"clientId={CLIENT_ID}&"
        f"authType=2"
    )
    
    print(f"[CONNECT] Connecting to Dhan 200-Depth WebSocket...")
    print(f"[CONNECT] URL: wss://full-depth-api.dhan.co/twohundreddepth?token=...&clientId={CLIENT_ID}&authType=2")
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
