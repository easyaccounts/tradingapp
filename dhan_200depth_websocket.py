import websocket
import json
import struct
import csv
from datetime import datetime
import pytz
import time

# Configuration
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3MDc2MDI1LCJpYXQiOjE3NjY5ODk2MjUsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5NzE5NzcxIn0.YjrUkXL5YlXWBBvoiXaMBnbHGcnWFX73jgCn9iH95uTjdfcIRMwm7nR5v3ZSn0BCwFha5qPhLI7d_cRAsnpT-w"
CLIENT_ID = "1109719771"
SECURITY_ID = "49543"  # December NIFTY futures

# CSV file to log depth data
csv_file = "dhan_200depth_nifty_futures.csv"
ist = pytz.timezone('Asia/Kolkata')

# Feed Response Codes
RESPONSE_BID_DEPTH = 41
RESPONSE_ASK_DEPTH = 51
RESPONSE_DISCONNECT = 50

# Global variables
ws = None
depth_snapshots = []
snapshot_count = 0
start_time = None

def parse_response_header_200depth(data):
    """Parse 12-byte response header for 200-depth"""
    if len(data) < 12:
        return None
    
    message_length = struct.unpack('<H', data[0:2])[0]  # Little endian
    response_code = data[2]
    exchange_segment = data[3]
    security_id = struct.unpack('<I', data[4:8])[0]
    num_rows = struct.unpack('<I', data[8:12])[0]
    
    return {
        'message_length': message_length,
        'response_code': response_code,
        'exchange_segment': exchange_segment,
        'security_id': security_id,
        'num_rows': num_rows
    }

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

def analyze_depth_snapshot(bid_depth, ask_depth):
    """Analyze a complete depth snapshot"""
    if not bid_depth or not ask_depth:
        return None
    
    bid_levels = bid_depth['levels']
    ask_levels = ask_depth['levels']
    
    if len(bid_levels) == 0 or len(ask_levels) == 0:
        return None
    
    # Basic metrics
    best_bid = bid_levels[0]['price']
    best_ask = ask_levels[0]['price']
    spread = best_ask - best_bid
    
    # Total depth quantities
    total_bid_qty = sum(level['quantity'] for level in bid_levels)
    total_ask_qty = sum(level['quantity'] for level in ask_levels)
    
    # Total orders
    total_bid_orders = sum(level['orders'] for level in bid_levels)
    total_ask_orders = sum(level['orders'] for level in ask_levels)
    
    # Imbalance ratio
    imbalance_ratio = total_bid_qty / total_ask_qty if total_ask_qty > 0 else 0
    
    # Average order sizes
    avg_bid_order_size = total_bid_qty / total_bid_orders if total_bid_orders > 0 else 0
    avg_ask_order_size = total_ask_qty / total_ask_orders if total_ask_orders > 0 else 0
    
    # Volume-weighted average prices (VWAP) for depth
    bid_vwap = sum(l['price'] * l['quantity'] for l in bid_levels) / total_bid_qty if total_bid_qty > 0 else 0
    ask_vwap = sum(l['price'] * l['quantity'] for l in ask_levels) / total_ask_qty if total_ask_qty > 0 else 0
    
    # Distribution analysis - find where majority of volume sits
    bid_50pct_qty = total_bid_qty * 0.5
    ask_50pct_qty = total_ask_qty * 0.5
    
    cumulative_bid = 0
    bid_50pct_level = 0
    for i, level in enumerate(bid_levels):
        cumulative_bid += level['quantity']
        if cumulative_bid >= bid_50pct_qty:
            bid_50pct_level = i + 1
            break
    
    cumulative_ask = 0
    ask_50pct_level = 0
    for i, level in enumerate(ask_levels):
        cumulative_ask += level['quantity']
        if cumulative_ask >= ask_50pct_qty:
            ask_50pct_level = i + 1
            break
    
    return {
        'timestamp': datetime.now(ist),
        'best_bid': best_bid,
        'best_ask': best_ask,
        'spread': spread,
        'total_bid_qty': total_bid_qty,
        'total_ask_qty': total_ask_qty,
        'total_bid_orders': total_bid_orders,
        'total_ask_orders': total_ask_orders,
        'imbalance_ratio': imbalance_ratio,
        'avg_bid_order_size': avg_bid_order_size,
        'avg_ask_order_size': avg_ask_order_size,
        'bid_vwap': bid_vwap,
        'ask_vwap': ask_vwap,
        'bid_50pct_level': bid_50pct_level,
        'ask_50pct_level': ask_50pct_level,
        'bid_depth': bid_levels,
        'ask_depth': ask_levels
    }

def save_snapshot_to_csv(snapshot):
    """Save depth snapshot to CSV"""
    global snapshot_count
    
    # Create CSV with headers if first snapshot
    if snapshot_count == 0:
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            headers = ['timestamp', 'best_bid', 'best_ask', 'spread', 
                      'total_bid_qty', 'total_ask_qty', 
                      'total_bid_orders', 'total_ask_orders',
                      'imbalance_ratio', 'avg_bid_order_size', 'avg_ask_order_size',
                      'bid_vwap', 'ask_vwap',
                      'bid_50pct_level', 'ask_50pct_level']
            writer.writerow(headers)
    
    # Write snapshot
    with open(csv_file, 'a', newline='') as f:
        writer = csv.writer(f)
        row = [
            snapshot['timestamp'].strftime('%Y-%m-%d %H:%M:%S.%f'),
            snapshot['best_bid'],
            snapshot['best_ask'],
            snapshot['spread'],
            snapshot['total_bid_qty'],
            snapshot['total_ask_qty'],
            snapshot['total_bid_orders'],
            snapshot['total_ask_orders'],
            snapshot['imbalance_ratio'],
            snapshot['avg_bid_order_size'],
            snapshot['avg_ask_order_size'],
            snapshot['bid_vwap'],
            snapshot['ask_vwap'],
            snapshot['bid_50pct_level'],
            snapshot['ask_50pct_level']
        ]
        writer.writerow(row)
    
    snapshot_count += 1

# Buffer for pairing bid/ask packets
pending_bid = None
pending_ask = None

def on_message(ws, message):
    """Handle incoming WebSocket messages"""
    global start_time, pending_bid, pending_ask, snapshot_count
    
    if start_time is None:
        start_time = time.time()
    
    try:
        # Parse binary message
        if len(message) < 12:
            return
        
        # Check if this is a combined message (bid + ask stacked together)
        # Single packet is 3212 bytes (12 header + 200*16 levels)
        # Combined message would be 6424 bytes (two packets stacked)
        if len(message) == 6424:
            # Parse first half as BID
            bid_depth = parse_depth_packet_200(message[:3212])
            # Parse second half as ASK  
            ask_depth = parse_depth_packet_200(message[3212:])
            
            if bid_depth and ask_depth:
                snapshot = analyze_depth_snapshot(bid_depth, ask_depth)
                if snapshot:
                    save_snapshot_to_csv(snapshot)
                    depth_snapshots.append(snapshot)
                    
                    # Print summary every 100 snapshots
                    if snapshot_count % 100 == 0:
                        print(f"[{snapshot['timestamp'].strftime('%H:%M:%S')}] Snapshots: {snapshot_count}, Bid: ₹{snapshot['best_bid']:,.2f}, Ask: ₹{snapshot['best_ask']:,.2f}, Spread: ₹{snapshot['spread']:.2f}, Imbalance: {snapshot['imbalance_ratio']:.2f}")
            return
        
        # Otherwise parse as separate messages with response codes
        response_code = message[2]
        
        if response_code == RESPONSE_BID_DEPTH:
            bid_depth = parse_depth_packet_200(message)
            if bid_depth:
                pending_bid = bid_depth
                if snapshot_count < 3:
                    print(f"Received BID depth: {bid_depth['num_rows']} levels, Best bid: ₹{bid_depth['levels'][0]['price']:.2f}")
        
        elif response_code == RESPONSE_ASK_DEPTH:
            ask_depth = parse_depth_packet_200(message)
            if ask_depth:
                pending_ask = ask_depth
                if snapshot_count < 3:
                    print(f"Received ASK depth: {ask_depth['num_rows']} levels, Best ask: ₹{ask_depth['levels'][0]['price']:.2f}")
        
        else:
            if snapshot_count < 3:
                print(f"Unknown response code: {response_code}, message length: {len(message)}")
        
        # When we have both bid and ask, analyze and save
        if pending_bid and pending_ask:
            snapshot = analyze_depth_snapshot(pending_bid, pending_ask)
            if snapshot:
                save_snapshot_to_csv(snapshot)
                depth_snapshots.append(snapshot)
                
                # Print summary every 100 snapshots
                if snapshot_count % 100 == 0:
                    print(f"[{snapshot['timestamp'].strftime('%H:%M:%S')}] Snapshots: {snapshot_count}, Bid: ₹{snapshot['best_bid']:,.2f}, Ask: ₹{snapshot['best_ask']:,.2f}, Spread: ₹{snapshot['spread']:.2f}, Imbalance: {snapshot['imbalance_ratio']:.2f}")
            
            # Reset buffers
            pending_bid = None
            pending_ask = None
    
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

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket close"""
    print(f"\n{'='*80}")
    print(f"WebSocket Connection Closed")
    print(f"Total snapshots captured: {snapshot_count}")
    if start_time:
        elapsed = time.time() - start_time
        print(f"Session duration: {elapsed:.1f} seconds")
    print(f"Data saved to: {csv_file}")
    print(f"{'='*80}")

def on_open(ws):
    """Handle WebSocket connection opened"""
    print(f"\n{'='*80}")
    print(f"DHAN 200-DEPTH WebSocket Connected")
    print(f"Instrument: NIFTY Futures (Security ID: {SECURITY_ID})")
    print(f"Time: {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"Subscribing to 200-level market depth...")
    print(f"{'='*80}\n")
    
    # Subscribe to 200-depth (request code 23)
    subscribe_message = {
        "RequestCode": 23,
        "ExchangeSegment": "NSE_FNO",
        "SecurityId": str(SECURITY_ID)
    }
    
    try:
        ws.send(json.dumps(subscribe_message))
        print(f"Subscription request sent: {subscribe_message}")
        print(f"Waiting for depth data... (Press Ctrl+C to stop)\n")
    except Exception as e:
        print(f"Error sending subscription: {e}")

def start_websocket():
    """Start WebSocket connection"""
    global ws
    
    # Build WebSocket URL for 200-depth
    ws_url = f"wss://full-depth-api.dhan.co/twohundreddepth?token={ACCESS_TOKEN}&clientId={CLIENT_ID}&authType=2"
    
    print(f"\n{'='*80}")
    print(f"DHAN 200-DEPTH API WebSocket Client")
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
