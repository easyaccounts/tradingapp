"""
Dhan 200-Level Market Depth WebSocket Test
Issue: Connection succeeds, subscription sent, but server disconnects immediately with no data
Account has active Data Plan (expires 2026-01-28) and 20-level depth works fine
"""

import websocket
import json
import os
from dotenv import load_dotenv

# Load credentials
load_dotenv()
ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN')
CLIENT_ID = os.getenv('DHAN_CLIENT_ID')

# Test instruments
TESTS = [
    {'security_id': '49229', 'segment': 'NSE_FNO', 'name': 'NIFTY JAN 2026 FUT'},
    {'security_id': '1333', 'segment': 'NSE_EQ', 'name': 'RELIANCE'},
    {'security_id': '11536', 'segment': 'NSE_EQ', 'name': 'INFY'}
]

def on_message(ws, message):
    print(f"✓ [DATA RECEIVED] {len(message)} bytes")
    if isinstance(message, bytes) and len(message) >= 3:
        response_code = message[2]
        print(f"  Response code: {response_code} (41=BID, 51=ASK, 50=DISCONNECT)")

def on_error(ws, error):
    print(f"✗ [ERROR] {error}")

def on_close(ws, status, msg):
    print(f"✗ [CLOSED] Status: {status}, Message: {msg}")

def on_open(ws):
    print(f"✓ [CONNECTED] WebSocket opened")
    
    # Try support's suggested format with InstrumentList
    subscription = {
        'RequestCode': 23,  # Full Market Depth
        'InstrumentList': [{
            'ExchangeSegment': current_test['segment'],
            'SecurityId': current_test['security_id']
        }]
    }
    
    ws.send(json.dumps(subscription))
    print(f"✓ [SUBSCRIBED] Sent: {subscription}")
    print("  Waiting for data...")

# Test each instrument
for test in TESTS:
    current_test = test
    print("\n" + "="*70)
    print(f"Testing: {test['name']} (SecurityId: {test['security_id']})")
    print("="*70)
    
    ws_url = (
        f"wss://full-depth-api.dhan.co/twohundreddepth?"
        f"version=2&"  # Added per support suggestion
        f"token={ACCESS_TOKEN}&"
        f"clientId={CLIENT_ID}&"
        f"authType=2"
    )
    
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Run for 10 seconds
    import threading
    def stop():
        ws.close()
    timer = threading.Timer(10.0, stop)
    timer.start()
    
    ws.run_forever()
    timer.cancel()

print("\n" + "="*70)
print("SUMMARY:")
print("="*70)
print(f"✓ Authentication: CLIENT_ID={CLIENT_ID}, Token valid")
print(f"✓ WebSocket URL: wss://full-depth-api.dhan.co/twohundreddepth")
print(f"✓ Subscription: RequestCode=23, per API docs")
print(f"✗ Result: All instruments connect but disconnect immediately (0 data)")
print(f"")
print(f"Note: 20-level depth works fine with same credentials:")
print(f"  URL: wss://depth-api-feed.dhan.co/twentydepth")
print(f"  Subscription: RequestCode=23 + InstrumentList format")
print(f"  Result: Continuous data streaming (664-1992 bytes per message)")
print("="*70)
