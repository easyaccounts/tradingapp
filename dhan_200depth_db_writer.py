import websocket
import json
import struct
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import pytz
import time
import os
from dhan_auth import get_dhan_credentials

# Load OAuth credentials
print("Loading Dhan OAuth credentials...")
credentials = get_dhan_credentials()
ACCESS_TOKEN = credentials['access_token']
CLIENT_ID = credentials['client_id']
print(f"✓ Authenticated with Client ID: {CLIENT_ID}")
SECURITY_ID = "49543"  # December NIFTY futures

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'trading'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

ist = pytz.timezone('Asia/Kolkata')

# Feed Response Codes
RESPONSE_BID_DEPTH = 41
RESPONSE_ASK_DEPTH = 51
RESPONSE_DISCONNECT = 50

# Global variables
ws = None
snapshot_count = 0
start_time = None
db_conn = None
batch_buffer = []
BATCH_SIZE = 100  # Insert every 100 snapshots (200 levels × 2 sides × 100 = 40,000 rows)

def get_db_connection():
    """Get database connection"""
    global db_conn
    try:
        if db_conn is None or db_conn.closed:
            db_conn = psycopg2.connect(**DB_CONFIG)
            db_conn.autocommit = False
            print("✓ Connected to TimescaleDB")
        return db_conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

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

def save_depth_to_db(timestamp, security_id, instrument_name, bid_levels, ask_levels):
    """Save full 200-level depth to TimescaleDB"""
    global batch_buffer, snapshot_count
    
    # Prepare rows for batch insert
    rows = []
    
    # Add bid levels
    for level_data in bid_levels:
        rows.append((
            timestamp,
            security_id,
            instrument_name,
            'BID',
            level_data['level'],
            level_data['price'],
            level_data['quantity'],
            level_data['orders']
        ))
    
    # Add ask levels
    for level_data in ask_levels:
        rows.append((
            timestamp,
            security_id,
            instrument_name,
            'ASK',
            level_data['level'],
            level_data['price'],
            level_data['quantity'],
            level_data['orders']
        ))
    
    # Add to batch buffer
    batch_buffer.extend(rows)
    
    # Insert batch when buffer is full
    if len(batch_buffer) >= BATCH_SIZE * 400:  # 400 rows per snapshot (200 bid + 200 ask)
        flush_batch_to_db()

def flush_batch_to_db():
    """Flush batched rows to database"""
    global batch_buffer
    
    if not batch_buffer:
        return
    
    conn = get_db_connection()
    if not conn:
        print("⚠ Database not available, discarding batch")
        batch_buffer = []
        return
    
    try:
        cursor = conn.cursor()
        
        # Batch insert using execute_batch for performance
        insert_query = """
            INSERT INTO depth_levels 
            (time, security_id, instrument_name, side, level, price, quantity, orders)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, security_id, side, level) DO NOTHING
        """
        
        execute_batch(cursor, insert_query, batch_buffer, page_size=1000)
        conn.commit()
        
        print(f"✓ Inserted {len(batch_buffer)} depth level rows to database")
        batch_buffer = []
        
    except Exception as e:
        print(f"Database insert error: {e}")
        conn.rollback()
        batch_buffer = []
        import traceback
        traceback.print_exc()

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
        
        timestamp = datetime.now(ist)
        
        # Check if this is a combined message (bid + ask stacked together)
        if len(message) == 6424:
            # Parse first half as BID
            bid_depth = parse_depth_packet_200(message[:3212])
            # Parse second half as ASK  
            ask_depth = parse_depth_packet_200(message[3212:])
            
            if bid_depth and ask_depth:
                # Save to database
                save_depth_to_db(
                    timestamp,
                    bid_depth['header']['security_id'],
                    'NIFTY DEC 2025 FUT',
                    bid_depth['levels'],
                    ask_depth['levels']
                )
                
                snapshot_count += 1
                
                # Print summary every 100 snapshots
                if snapshot_count % 100 == 0:
                    best_bid = bid_depth['levels'][0]['price'] if bid_depth['levels'] else 0
                    best_ask = ask_depth['levels'][0]['price'] if ask_depth['levels'] else 0
                    spread = best_ask - best_bid
                    print(f"[{timestamp.strftime('%H:%M:%S')}] Snapshots: {snapshot_count}, "
                          f"Bid: ₹{best_bid:,.2f}, Ask: ₹{best_ask:,.2f}, Spread: ₹{spread:.2f}")
            return
        
        # Otherwise parse as separate messages with response codes
        response_code = message[2]
        
        if response_code == RESPONSE_BID_DEPTH:
            bid_depth = parse_depth_packet_200(message)
            if bid_depth:
                pending_bid = bid_depth
                if snapshot_count < 3:
                    print(f"Received BID depth: {bid_depth['num_rows']} levels")
        
        elif response_code == RESPONSE_ASK_DEPTH:
            ask_depth = parse_depth_packet_200(message)
            if ask_depth:
                pending_ask = ask_depth
                if snapshot_count < 3:
                    print(f"Received ASK depth: {ask_depth['num_rows']} levels")
        
        # When we have both bid and ask, save to database
        if pending_bid and pending_ask:
            save_depth_to_db(
                timestamp,
                pending_bid['header']['security_id'],
                'NIFTY DEC 2025 FUT',
                pending_bid['levels'],
                pending_ask['levels']
            )
            
            snapshot_count += 1
            
            # Print summary every 100 snapshots
            if snapshot_count % 100 == 0:
                best_bid = pending_bid['levels'][0]['price'] if pending_bid['levels'] else 0
                best_ask = pending_ask['levels'][0]['price'] if pending_ask['levels'] else 0
                spread = best_ask - best_bid
                print(f"[{timestamp.strftime('%H:%M:%S')}] Snapshots: {snapshot_count}, "
                      f"Bid: ₹{best_bid:,.2f}, Ask: ₹{best_ask:,.2f}, Spread: ₹{spread:.2f}")
            
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

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket close"""
    print(f"\nWebSocket Connection Closed")
    print(f"Status Code: {close_status_code}")
    print(f"Message: {close_msg}")
    
    # Flush remaining batch
    flush_batch_to_db()
    
    # Close database connection
    global db_conn
    if db_conn:
        db_conn.close()
        print("✓ Database connection closed")
    
    # Print statistics
    if start_time:
        duration = time.time() - start_time
        print(f"\nSession Statistics:")
        print(f"Duration: {duration:.1f} seconds")
        print(f"Total Snapshots: {snapshot_count}")
        print(f"Average Rate: {snapshot_count/duration:.2f} snapshots/second")

def on_open(ws):
    """Handle WebSocket open"""
    print("WebSocket Connection Opened")
    
    # Subscribe to 200-level depth feed
    subscription = {
        "RequestCode": 23,  # 200-level depth subscription
        "InstrumentCount": 1,
        "InstrumentList": [
            {
                "ExchangeSegment": 2,  # NSE FO
                "SecurityId": SECURITY_ID
            }
        ]
    }
    
    # Send as binary (struct format)
    request_code = 23
    instrument_count = 1
    exchange_segment = 2
    security_id = int(SECURITY_ID)
    
    # Pack: RequestCode (1 byte), InstrumentCount (1 byte), then for each instrument: ExchangeSegment (1 byte) + SecurityId (4 bytes)
    message = struct.pack('<BB', request_code, instrument_count)
    message += struct.pack('<BI', exchange_segment, security_id)
    
    ws.send(message, opcode=websocket.ABNF.OPCODE_BINARY)
    print(f"Subscribed to 200-level depth for Security ID: {SECURITY_ID}")

def start_websocket():
    """Start WebSocket connection"""
    global ws
    
    # Test database connection first
    conn = get_db_connection()
    if not conn:
        print("❌ Cannot connect to database. Please check your database configuration.")
        return
    
    # WebSocket URL
    ws_url = f"wss://api-feed.dhan.co?version=2&token={ACCESS_TOKEN}&clientId={CLIENT_ID}&authType=2"
    
    print(f"\n{'='*70}")
    print(f"Starting Dhan 200-Level Market Depth WebSocket")
    print(f"{'='*70}")
    print(f"Security ID: {SECURITY_ID} (NIFTY DEC 2025 FUT)")
    print(f"Database: {DB_CONFIG['database']}@{DB_CONFIG['host']}")
    print(f"Batch Size: {BATCH_SIZE} snapshots")
    print(f"Storage: TimescaleDB (60-day retention)")
    print(f"{'='*70}\n")
    
    # Create WebSocket connection
    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    
    # Run forever with auto-reconnect
    try:
        ws.run_forever(
            ping_interval=30,
            ping_timeout=10
        )
    except KeyboardInterrupt:
        print("\nStopping WebSocket...")
        flush_batch_to_db()
        if db_conn:
            db_conn.close()

if __name__ == "__main__":
    start_websocket()
