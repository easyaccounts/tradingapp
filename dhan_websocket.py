import websocket
import json
import struct
import csv
from datetime import datetime
import pytz
import time
from dhan_auth import get_dhan_credentials

# Load OAuth credentials
print("Loading Dhan OAuth credentials...")
credentials = get_dhan_credentials()
ACCESS_TOKEN = credentials['access_token']
CLIENT_ID = credentials['client_id']
print(f"✓ Authenticated with Client ID: {CLIENT_ID}")
SECURITY_ID = "49543"  # December NIFTY futures

# CSV file to log ticks
csv_file = "dhan_nifty_futures_ticks.csv"
ist = pytz.timezone('Asia/Kolkata')

# Feed Request Codes
TICKER_DATA = 15  # Request code for ticker data
QUOTE_DATA = 17   # Request code for quote data  
FULL_DATA = 21    # Request code for full data (with market depth)

# Feed Response Codes
RESPONSE_TICKER = 2
RESPONSE_QUOTE = 4
RESPONSE_OI = 5
RESPONSE_PREV_CLOSE = 6
RESPONSE_FULL = 8
RESPONSE_DISCONNECT = 50

# Exchange Segment Codes
NSE_FNO = 2  # NSE Futures & Options

# Global variables
ws = None
tick_count = 0
start_time = None

def parse_response_header(data):
    """Parse 8-byte response header"""
    if len(data) < 8:
        return None
    
    response_code = data[0]
    message_length = struct.unpack('<H', data[1:3])[0]  # Little endian
    exchange_segment = data[3]
    security_id = struct.unpack('<I', data[4:8])[0]  # Little endian
    
    return {
        'response_code': response_code,
        'message_length': message_length,
        'exchange_segment': exchange_segment,
        'security_id': security_id
    }

def parse_ticker_packet(data):
    """Parse ticker packet (response code 2)"""
    if len(data) < 17:
        return None
    
    header = parse_response_header(data[:8])
    ltp = struct.unpack('<f', data[8:12])[0]
    ltt_epoch = struct.unpack('<I', data[12:16])[0]
    
    ltt = datetime.fromtimestamp(ltt_epoch, tz=ist)
    
    return {
        **header,
        'type': 'ticker',
        'ltp': ltp,
        'ltt': ltt,
        'ltt_epoch': ltt_epoch
    }

def parse_quote_packet(data):
    """Parse quote packet (response code 4)"""
    if len(data) < 51:
        return None
    
    header = parse_response_header(data[:8])
    ltp = struct.unpack('<f', data[8:12])[0]
    ltq = struct.unpack('<H', data[12:14])[0]
    ltt_epoch = struct.unpack('<I', data[14:18])[0]
    atp = struct.unpack('<f', data[18:22])[0]
    volume = struct.unpack('<I', data[22:26])[0]
    total_sell_qty = struct.unpack('<I', data[26:30])[0]
    total_buy_qty = struct.unpack('<I', data[30:34])[0]
    open_price = struct.unpack('<f', data[34:38])[0]
    close_price = struct.unpack('<f', data[38:42])[0]
    high_price = struct.unpack('<f', data[42:46])[0]
    low_price = struct.unpack('<f', data[46:50])[0]
    
    ltt = datetime.fromtimestamp(ltt_epoch, tz=ist)
    
    return {
        **header,
        'type': 'quote',
        'ltp': ltp,
        'ltq': ltq,
        'ltt': ltt,
        'ltt_epoch': ltt_epoch,
        'atp': atp,
        'volume': volume,
        'total_sell_qty': total_sell_qty,
        'total_buy_qty': total_buy_qty,
        'open': open_price,
        'close': close_price,
        'high': high_price,
        'low': low_price
    }

def parse_oi_packet(data):
    """Parse OI packet (response code 5)"""
    if len(data) < 12:  # Changed from 13 to 12
        return None
    
    header = parse_response_header(data[:8])
    oi = struct.unpack('<I', data[8:12])[0]
    
    return {
        **header,
        'type': 'oi',
        'oi': oi
    }

def parse_prev_close_packet(data):
    """Parse previous close packet (response code 6)"""
    if len(data) < 17:
        return None
    
    header = parse_response_header(data[:8])
    prev_close = struct.unpack('<f', data[8:12])[0]
    prev_oi = struct.unpack('<I', data[12:16])[0]
    
    return {
        **header,
        'type': 'prev_close',
        'prev_close': prev_close,
        'prev_oi': prev_oi
    }

def parse_full_packet(data):
    """Parse full packet with market depth (response code 8)"""
    if len(data) < 162:  # Changed from 163 to 162
        return None
    
    header = parse_response_header(data[:8])
    ltp = struct.unpack('<f', data[8:12])[0]
    ltq = struct.unpack('<H', data[12:14])[0]
    ltt_epoch = struct.unpack('<I', data[14:18])[0]
    atp = struct.unpack('<f', data[18:22])[0]
    volume = struct.unpack('<I', data[22:26])[0]
    total_sell_qty = struct.unpack('<I', data[26:30])[0]
    total_buy_qty = struct.unpack('<I', data[30:34])[0]
    oi = struct.unpack('<I', data[34:38])[0]
    oi_day_high = struct.unpack('<I', data[38:42])[0]
    oi_day_low = struct.unpack('<I', data[42:46])[0]
    open_price = struct.unpack('<f', data[46:50])[0]
    close_price = struct.unpack('<f', data[50:54])[0]
    high_price = struct.unpack('<f', data[54:58])[0]
    low_price = struct.unpack('<f', data[58:62])[0]
    
    ltt = datetime.fromtimestamp(ltt_epoch, tz=ist)
    
    # Parse 5 levels of market depth (20 bytes each)
    depth = []
    for i in range(5):
        offset = 62 + (i * 20)
        bid_qty = struct.unpack('<I', data[offset:offset+4])[0]
        ask_qty = struct.unpack('<I', data[offset+4:offset+8])[0]
        bid_orders = struct.unpack('<H', data[offset+8:offset+10])[0]
        ask_orders = struct.unpack('<H', data[offset+10:offset+12])[0]
        bid_price = struct.unpack('<f', data[offset+12:offset+16])[0]
        ask_price = struct.unpack('<f', data[offset+16:offset+20])[0]
        
        depth.append({
            'bid_qty': bid_qty,
            'ask_qty': ask_qty,
            'bid_orders': bid_orders,
            'ask_orders': ask_orders,
            'bid_price': bid_price,
            'ask_price': ask_price
        })
    
    return {
        **header,
        'type': 'full',
        'ltp': ltp,
        'ltq': ltq,
        'ltt': ltt,
        'ltt_epoch': ltt_epoch,
        'atp': atp,
        'volume': volume,
        'total_sell_qty': total_sell_qty,
        'total_buy_qty': total_buy_qty,
        'oi': oi,
        'oi_day_high': oi_day_high,
        'oi_day_low': oi_day_low,
        'open': open_price,
        'close': close_price,
        'high': high_price,
        'low': low_price,
        'depth': depth
    }

def parse_binary_message(data):
    """Parse binary message based on response code"""
    if len(data) < 8:
        return None
    
    response_code = data[0]
    
    parsers = {
        RESPONSE_TICKER: parse_ticker_packet,
        RESPONSE_QUOTE: parse_quote_packet,
        RESPONSE_OI: parse_oi_packet,
        RESPONSE_PREV_CLOSE: parse_prev_close_packet,
        RESPONSE_FULL: parse_full_packet
    }
    
    parser = parsers.get(response_code)
    if parser:
        return parser(data)
    
    return None

def log_tick_to_csv(tick):
    """Log tick data to CSV file"""
    global tick_count
    
    # Create CSV with headers if first tick
    if tick_count == 0:
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            headers = ['time', 'type', 'ltp', 'volume', 'oi', 'ltq', 'atp', 
                      'total_buy_qty', 'total_sell_qty', 'open', 'high', 'low', 'close',
                      'bid_price_1', 'bid_qty_1', 'bid_orders_1',
                      'ask_price_1', 'ask_qty_1', 'ask_orders_1',
                      'bid_price_2', 'bid_qty_2', 'ask_price_2', 'ask_qty_2',
                      'bid_price_3', 'bid_qty_3', 'ask_price_3', 'ask_qty_3',
                      'bid_price_4', 'bid_qty_4', 'ask_price_4', 'ask_qty_4',
                      'bid_price_5', 'bid_qty_5', 'ask_price_5', 'ask_qty_5']
            writer.writerow(headers)
    
    # Prepare row data
    now = datetime.now(ist)
    row = [
        now.strftime('%Y-%m-%d %H:%M:%S.%f'),
        tick.get('type', ''),
        tick.get('ltp', ''),
        tick.get('volume', ''),
        tick.get('oi', ''),
        tick.get('ltq', ''),
        tick.get('atp', ''),
        tick.get('total_buy_qty', ''),
        tick.get('total_sell_qty', ''),
        tick.get('open', ''),
        tick.get('high', ''),
        tick.get('low', ''),
        tick.get('close', '')
    ]
    
    # Add market depth if available
    if 'depth' in tick:
        for i in range(5):
            depth_level = tick['depth'][i]
            if i == 0:
                row.extend([
                    depth_level['bid_price'],
                    depth_level['bid_qty'],
                    depth_level['bid_orders'],
                    depth_level['ask_price'],
                    depth_level['ask_qty'],
                    depth_level['ask_orders']
                ])
            else:
                row.extend([
                    depth_level['bid_price'],
                    depth_level['bid_qty'],
                    depth_level['ask_price'],
                    depth_level['ask_qty']
                ])
    else:
        # Fill with empty values
        row.extend([''] * 22)
    
    # Write to CSV
    with open(csv_file, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(row)
    
    tick_count += 1

def on_message(ws, message):
    """Handle incoming WebSocket messages"""
    global tick_count, start_time
    
    if start_time is None:
        start_time = time.time()
    
    try:
        # Check if it's a text message (JSON) or binary
        if isinstance(message, str):
            print(f"Received text message: {message}")
            return
        
        # Print raw binary data for debugging (first message only)
        if tick_count == 0:
            print(f"First binary message received ({len(message)} bytes)")
            print(f"First 20 bytes: {message[:20].hex()}")
        
        # Parse binary message
        tick = parse_binary_message(message)
        
        if tick:
            log_tick_to_csv(tick)
            
            # Print first few ticks for verification
            if tick_count < 5:
                print(f"Tick #{tick_count+1}: Type={tick['type']}, LTP=₹{tick.get('ltp', 0):,.2f}, "
                      f"Volume={tick.get('volume', 0):,}, OI={tick.get('oi', 0):,}")
            
            # Print periodic updates
            elif tick_count % 100 == 0:
                elapsed = time.time() - start_time
                tps = tick_count / elapsed if elapsed > 0 else 0
                print(f"[{datetime.now(ist).strftime('%H:%M:%S')}] Received {tick_count} ticks | "
                      f"Type: {tick['type']} | LTP: ₹{tick.get('ltp', 0):,.2f} | "
                      f"Volume: {tick.get('volume', 0):,} | OI: {tick.get('oi', 0):,} | "
                      f"Rate: {tps:.1f} ticks/sec")
        else:
            print(f"Could not parse message (length: {len(message)}, first byte: {message[0] if len(message) > 0 else 'empty'})")
    
    except Exception as e:
        print(f"Error parsing message: {e}")
        import traceback
        traceback.print_exc()

def on_error(ws, error):
    """Handle WebSocket errors"""
    print(f"WebSocket Error: {error}")
    import traceback
    if error:
        traceback.print_exc()
    else:
        print("Error object is None - connection might have been rejected or closed by server")

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket close"""
    print(f"\n{'='*80}")
    print(f"WebSocket Connection Closed")
    print(f"Status Code: {close_status_code}")
    print(f"Message: {close_msg}")
    print(f"Total ticks received: {tick_count}")
    if start_time:
        elapsed = time.time() - start_time
        print(f"Session duration: {elapsed:.1f} seconds")
        print(f"Average rate: {tick_count/elapsed:.1f} ticks/sec")
    print(f"Data saved to: {csv_file}")
    print(f"{'='*80}")

def on_open(ws):
    """Handle WebSocket connection opened"""
    print(f"\n{'='*80}")
    print(f"DHAN WebSocket Connected")
    print(f"Instrument: NIFTY Futures (Security ID: {SECURITY_ID})")
    print(f"Time: {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"Subscribing to FULL data (with market depth)...")
    print(f"{'='*80}\n")
    
    # Subscribe to FULL data (request code 21)
    # Note: SecurityId should be string type as per docs
    subscribe_message = {
        "RequestCode": FULL_DATA,
        "InstrumentCount": 1,
        "InstrumentList": [
            {
                "ExchangeSegment": "NSE_FNO",
                "SecurityId": str(SECURITY_ID)  # Ensure string type
            }
        ]
    }
    
    try:
        ws.send(json.dumps(subscribe_message))
        print(f"Subscription request sent: {subscribe_message}")
        print(f"Listening for ticks... (Press Ctrl+C to stop)")
        print(f"Note: If markets are closed or in lunch break, no ticks will be received.\n")
    except Exception as e:
        print(f"Error sending subscription: {e}")
        import traceback
        traceback.print_exc()

def start_websocket():
    """Start WebSocket connection"""
    global ws
    
    # Build WebSocket URL
    ws_url = f"wss://api-feed.dhan.co?version=2&token={ACCESS_TOKEN}&clientId={CLIENT_ID}&authType=2"
    
    print(f"\n{'='*80}")
    print(f"DHAN API WebSocket Client")
    print(f"Starting connection...")
    print(f"{'='*80}")
    
    # Create WebSocket connection
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    
    # Run WebSocket
    try:
        ws.run_forever()
    except KeyboardInterrupt:
        print("\n\nStopping WebSocket...")
        ws.close()

if __name__ == "__main__":
    try:
        start_websocket()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
