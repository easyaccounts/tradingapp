#!/usr/bin/env python3
"""
Dhan API script to fetch 200-level market depth for NIFTY JAN 2026 FUT (Security ID: 49229)
Uses WebSocket connection as REST API doesn't support market depth
"""

import json
import websocket
import time
import threading
import struct
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Add the services path to import DhanAuthManager
import sys
sys.path.append('services/depth_collector')

try:
    from dhan_auth import DhanAuthManager, get_dhan_credentials
except ImportError:
    print("ERROR: Could not import DhanAuthManager. Make sure dhan_auth.py is available.")
    DhanAuthManager = None
    
    # Fallback function if dhan_auth is not available
    def get_dhan_credentials():
        """Fallback function to load credentials from environment or token file"""
        try:
            import json
            token_file = '/opt/tradingapp/data/dhan_token.json'
            if os.path.exists(token_file):
                with open(token_file, 'r') as f:
                    data = json.load(f)
                    return data.get('access_token'), data.get('client_id')
        except Exception as e:
            print(f"Error loading token file: {e}")
        
        # Try environment variables as fallback
        access_token = os.getenv('DHAN_ACCESS_TOKEN')
        client_id = os.getenv('DHAN_CLIENT_ID')
        return access_token, client_id

# Feed Response Codes (from Dhan API docs)
RESPONSE_BID_DEPTH = 41
RESPONSE_ASK_DEPTH = 51
RESPONSE_DISCONNECT = 50

def parse_response_header_200depth(data):
    """Parse 200-depth response header (12 bytes)"""
    if len(data) < 12:
        return None

    try:
        message_length = struct.unpack('<H', data[0:2])[0]  # uint16
        response_code = data[2]  # byte
        exchange_segment = data[3]  # byte
        security_id = struct.unpack('<I', data[4:8])[0]  # uint32
        num_rows = struct.unpack('<I', data[8:12])[0]  # uint32

        return {
            'message_length': message_length,
            'response_code': response_code,
            'exchange_segment': exchange_segment,
            'security_id': security_id,
            'num_rows': num_rows
        }
    except Exception as e:
        print(f"Error parsing header: {e}")
        return None

def parse_depth_packet_200(data):
    """Parse 200-level depth packet (bid or ask)"""
    header = parse_response_header_200depth(data[:12])

    if not header:
        return None

    depth_side = 'BID' if header['response_code'] == RESPONSE_BID_DEPTH else 'ASK'
    num_rows = header['num_rows']

    # Parse depth levels (16 bytes each: 8 bytes price, 4 bytes qty, 4 bytes orders)
    levels = []
    offset = 12

    for i in range(num_rows):
        if offset + 16 > len(data):
            break

        price = struct.unpack('<d', data[offset:offset+8])[0]  # float64
        quantity = struct.unpack('<I', data[offset+8:offset+12])[0]  # uint32
        orders = struct.unpack('<I', data[offset+12:offset+16])[0]  # uint32

        levels.append({
            'level': i + 1,
            'price': price,
            'quantity': quantity,
            'orders': orders
        })

        offset += 16

    return {
        'header': header,
        'side': depth_side,
        'num_rows': num_rows,
        'levels': levels
    }

def get_market_depth_websocket(access_token, client_id, security_id):
    """Fetch 200-level market depth using WebSocket connection"""

    # WebSocket URL for 20-level depth (more reliable for testing)
    ws_url = f"wss://depth-api-feed.dhan.co/twentydepth?version=2&token={access_token}&clientId={client_id}&authType=2"

    depth_data = {'bid': [], 'ask': []}
    received_data = False
    pending_bid = None
    pending_ask = None

    def on_message(ws, message):
        nonlocal depth_data, received_data, pending_bid, pending_ask
        try:
            print(f"Received message: {len(message)} bytes, first 10 bytes: {[hex(b) for b in message[:10]]}")

            # Parse binary message
            if len(message) < 12:
                print(f"Message too short: {len(message)} bytes")
                return

            # Check if this is a combined message (bid + ask stacked together)
            # Single packet is 3212 bytes (12 header + 200*16 levels)
            # Combined message would be 6424 bytes (two packets stacked)
            if len(message) == 6424:
                print("Received combined bid+ask message")
                # Parse first half as BID
                bid_depth = parse_depth_packet_200(message[:3212])
                # Parse second half as ASK
                ask_depth = parse_depth_packet_200(message[3212:])

                if bid_depth and ask_depth:
                    depth_data['bid'] = bid_depth['levels']
                    depth_data['ask'] = ask_depth['levels']
                    received_data = True
                    ws.close()
                return

            # Otherwise parse as separate messages with response codes
            response_code = message[2]
            print(f"Response code: {response_code}")

            if response_code == RESPONSE_BID_DEPTH:
                bid_depth = parse_depth_packet_200(message)
                if bid_depth:
                    pending_bid = bid_depth
                    print(f"Received BID depth: {bid_depth['num_rows']} levels")

            elif response_code == RESPONSE_ASK_DEPTH:
                ask_depth = parse_depth_packet_200(message)
                if ask_depth:
                    pending_ask = ask_depth
                    print(f"Received ASK depth: {ask_depth['num_rows']} levels")

            else:
                print(f"Unknown response code: {response_code}, message length: {len(message)}")

            # When we have both bid and ask, save and close
            if pending_bid and pending_ask:
                depth_data['bid'] = pending_bid['levels']
                depth_data['ask'] = pending_ask['levels']
                received_data = True
                ws.close()

        except Exception as e:
            print(f"Error parsing message: {e}")
            import traceback
            traceback.print_exc()

    def on_error(ws, error):
        print(f"WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        print("WebSocket connection closed")

    def on_open(ws):
        print("WebSocket connection opened")

        # Test connection with a ping
        try:
            ws.send("", websocket.ABNF.OPCODE_PING)
            print("Sent ping to test connection")
        except Exception as e:
            print(f"Error sending ping: {e}")

        # Subscribe to the security
        subscribe_message = {
            "RequestCode": 23,
            "ExchangeSegment": "NSE_FNO",
            "SecurityId": str(security_id)
        }

        try:
            ws.send(json.dumps(subscribe_message))
            print(f"Subscribed to security {security_id}")
        except Exception as e:
            print(f"Error sending subscription: {e}")

    def on_pong(ws, data):
        print("Received pong - connection is alive")

    # Create WebSocket connection
    ws = websocket.WebSocketApp(ws_url,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close,
                              on_open=on_open,
                              on_pong=on_pong)

    # Run WebSocket in a thread with timeout
    ws_thread = threading.Thread(target=ws.run_forever)
    ws_thread.daemon = True
    ws_thread.start()

    # Wait for data or timeout
    timeout = 15
    start_time = time.time()

    while not received_data and (time.time() - start_time) < timeout:
        time.sleep(0.1)

    if not received_data:
        print("Timeout: No depth data received")
        return None

    return depth_data

def display_depth_data(depth_data):
    """Display the 200-level market depth data"""

    if not depth_data or len(depth_data) == 0:
        print("No depth data received")
        return

    # Get the first (and only) security data
    security_data = depth_data[0]

    print(f"Security ID: {security_data.get('security_id')}")
    print(f"Trading Symbol: {security_data.get('trading_symbol')}")
    print(f"Last Price: {security_data.get('last_price')}")
    print(f"Net Change: {security_data.get('net_change')}")
    print(f"Volume: {security_data.get('volume')}")
    print()

    # Display bid side (buy orders)
    bid_depth = security_data.get('bid', [])
    print(f"Bid Depth (Top 10 of {len(bid_depth)} levels):")
    print("Price\t\tQuantity\tOrders")
    print("-" * 40)

    for i, level in enumerate(bid_depth[:10]):
        price = level.get('price', 0)
        quantity = level.get('quantity', 0)
        orders = level.get('orders', 0)
        print(f"{price:.2f}\t\t{quantity}\t\t{orders}")

    print()

    # Display ask side (sell orders)
    ask_depth = security_data.get('ask', [])
    print(f"Ask Depth (Top 10 of {len(ask_depth)} levels):")
    print("Price\t\tQuantity\tOrders")
    print("-" * 40)

    for i, level in enumerate(ask_depth[:10]):
        price = level.get('price', 0)
        quantity = level.get('quantity', 0)
        orders = level.get('orders', 0)
        print(f"{price:.2f}\t\t{quantity}\t\t{orders}")

    print()
    print(f"Total Bid Levels: {len(bid_depth)}")
    print(f"Total Ask Levels: {len(ask_depth)}")

def main():
    """Main function to fetch and display market depth"""

    if DhanAuthManager is None:
        print("DhanAuthManager not available. Using manual token loading...")
        # Fallback to manual loading
        access_token, client_id = get_dhan_credentials()
        if not access_token or not client_id:
            print("Failed to load Dhan credentials")
            return
    else:
        # Use DhanAuthManager for proper authentication
        try:
            auth_manager = DhanAuthManager(
                token_file='dhan_token.json'  # Use local file
            )
            access_token = auth_manager.get_valid_token()
            client_id = auth_manager.client_id
            print(f"✓ Authentication successful (expires: {auth_manager.token_expiry.strftime('%Y-%m-%d %H:%M:%S')})")
        except Exception as e:
            print(f"Authentication failed: {e}")
            return

    # Security ID for NIFTY DEC 2025 FUT (older contract that might still be available)
    security_id = 49543  # Try December contract instead of January

    print(f"Fetching 200-level market depth for Security ID: {security_id}")
    print("=" * 60)

    print(f"Using Client ID: {client_id}")
    print()

    # Fetch market depth
    depth_data = get_market_depth_websocket(access_token, client_id, security_id)

    if depth_data:
        display_depth_data(depth_data)
    else:
        print("Failed to fetch market depth data")

if __name__ == "__main__":
    main()