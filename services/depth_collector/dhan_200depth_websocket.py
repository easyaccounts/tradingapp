import websocket
import json
import struct
import os
import psycopg2
from datetime import datetime
import pytz
import time
import redis
from dhan_auth import get_dhan_credentials

# Configuration from environment variables
SECURITY_ID = os.getenv('SECURITY_ID', '49543')  # December NIFTY futures
INSTRUMENT_NAME = os.getenv('INSTRUMENT_NAME', 'NIFTY DEC 2025 FUT')

# Dhan credentials (obtained via auth manager)
ACCESS_TOKEN = None
CLIENT_ID = None

# Database configuration
DB_HOST = os.getenv('DB_HOST', 'postgres')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'tradingdb')
DB_USER = os.getenv('DB_USER', 'tradinguser')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Redis configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://redis:6379/0')

ist = pytz.timezone('Asia/Kolkata')

# Feed Response Codes
RESPONSE_BID_DEPTH = 41
RESPONSE_ASK_DEPTH = 51
RESPONSE_DISCONNECT = 50

# Global variables
ws = None
db_conn = None
db_cursor = None
redis_client = None
snapshot_count = 0
start_time = None

def get_db_connection():
    """Establish database connection with retry logic"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=10
            )
            conn.autocommit = True
            print(f"✓ Database connected: {DB_HOST}:{DB_PORT}/{DB_NAME}")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Database connection failed (attempt {attempt+1}/{max_retries}): {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"✗ Failed to connect to database after {max_retries} attempts")
                raise

def get_redis_connection():
    """Establish Redis connection with retry logic"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            client = redis.from_url(REDIS_URL, decode_responses=True)
            client.ping()  # Test connection
            print(f"✓ Redis connected: {REDIS_URL}")
            return client
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Redis connection failed (attempt {attempt+1}/{max_retries}): {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"⚠ Failed to connect to Redis after {max_retries} attempts - continuing without Redis")
                return None

# Global buffer for batch inserts of 200 levels
depth_levels_buffer = []
BATCH_SIZE = 400  # 200 bids + 200 asks per snapshot

def insert_depth_levels_batch(cursor, records):
    """Batch insert depth levels with conflict handling"""
    from psycopg2.extras import execute_batch
    
    insert_query = """
        INSERT INTO depth_levels_200 (time, security_id, side, level_num, price, quantity, orders)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (time, security_id, side, level_num) DO UPDATE SET
            price = EXCLUDED.price,
            quantity = EXCLUDED.quantity,
            orders = EXCLUDED.orders
    """
    
    try:
        execute_batch(cursor, insert_query, records, page_size=400)
        print(f"✓ Inserted {len(records)} depth level records")
    except Exception as e:
        print(f"✗ Error in batch insert: {e}")

def save_depth_levels_to_db(cursor, bid_depth, ask_depth, timestamp_utc, security_id):
    """Save individual 200 bid and 200 ask levels to database with batching"""
    global depth_levels_buffer
    
    # Add all bid levels to buffer
    for i, level in enumerate(bid_depth, start=1):
        depth_levels_buffer.append((
            timestamp_utc,
            int(security_id),
            'BID',
            i,
            level['price'],
            level['quantity'],
            level['orders']
        ))
    
    # Add all ask levels to buffer
    for i, level in enumerate(ask_depth, start=1):
        depth_levels_buffer.append((
            timestamp_utc,
            int(security_id),
            'ASK',
            i,
            level['price'],
            level['quantity'],
            level['orders']
        ))
    
    # If buffer reaches batch size, insert
    if len(depth_levels_buffer) >= BATCH_SIZE:
        insert_depth_levels_batch(cursor, depth_levels_buffer)
        depth_levels_buffer.clear()

def parse_response_header_200depth(data):
    """Parse 12-byte response header for 200-depth"""
    if len(data) < 12:
        return None
    
    message_length = struct.unpack('<H', data[0:2])[0]
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
    if not header or header['num_rows'] == 0:
        return []
    
    depth_data = []
    offset = 12
    
    for i in range(min(header['num_rows'], 200)):
        if offset + 16 > len(data):
            break
        
        price = struct.unpack('<d', data[offset:offset+8])[0]
        quantity = struct.unpack('<I', data[offset+8:offset+12])[0]
        orders = struct.unpack('<I', data[offset+12:offset+16])[0]
        
        depth_data.append({
            'level': i + 1,
            'price': price,
            'quantity': quantity,
            'orders': orders
        })
        
        offset += 16
    
    return depth_data

def analyze_depth_snapshot(bid_depth, ask_depth):
    """Calculate aggregated metrics from 200-level depth"""
    if not bid_depth or not ask_depth:
        return None
    
    # Top of book
    best_bid = bid_depth[0]['price']
    best_ask = ask_depth[0]['price']
    spread = best_ask - best_bid
    
    # Total quantities and orders
    total_bid_qty = sum(level['quantity'] for level in bid_depth)
    total_ask_qty = sum(level['quantity'] for level in ask_depth)
    total_bid_orders = sum(level['orders'] for level in bid_depth)
    total_ask_orders = sum(level['orders'] for level in ask_depth)
    
    # Imbalance ratio
    imbalance_ratio = total_bid_qty / total_ask_qty if total_ask_qty > 0 else 0
    
    # Average order sizes
    avg_bid_order_size = total_bid_qty / total_bid_orders if total_bid_orders > 0 else 0
    avg_ask_order_size = total_ask_qty / total_ask_orders if total_ask_orders > 0 else 0
    
    # Volume-weighted average prices
    bid_vwap = sum(level['price'] * level['quantity'] for level in bid_depth) / total_bid_qty if total_bid_qty > 0 else 0
    ask_vwap = sum(level['price'] * level['quantity'] for level in ask_depth) / total_ask_qty if total_ask_qty > 0 else 0
    
    # 50% volume concentration levels
    bid_50pct_qty = total_bid_qty * 0.5
    ask_50pct_qty = total_ask_qty * 0.5
    
    bid_cumulative = 0
    bid_50pct_level = 0
    for level in bid_depth:
        bid_cumulative += level['quantity']
        bid_50pct_level += 1
        if bid_cumulative >= bid_50pct_qty:
            break
    
    ask_cumulative = 0
    ask_50pct_level = 0
    for level in ask_depth:
        ask_cumulative += level['quantity']
        ask_50pct_level += 1
        if ask_cumulative >= ask_50pct_qty:
            break
    
    return {
        'best_bid': round(best_bid, 2),
        'best_ask': round(best_ask, 2),
        'spread': round(spread, 2),
        'total_bid_qty': total_bid_qty,
        'total_ask_qty': total_ask_qty,
        'total_bid_orders': total_bid_orders,
        'total_ask_orders': total_ask_orders,
        'imbalance_ratio': round(imbalance_ratio, 4),
        'avg_bid_order_size': round(avg_bid_order_size, 2),
        'avg_ask_order_size': round(avg_ask_order_size, 2),
        'bid_vwap': round(bid_vwap, 2),
        'ask_vwap': round(ask_vwap, 2),
        'bid_50pct_level': bid_50pct_level,
        'ask_50pct_level': ask_50pct_level
    }

def save_snapshot_to_db(snapshot):
    """Save aggregated snapshot to database"""
    global db_cursor
    
    try:
        # Use current timestamp in IST (converted to UTC for storage)
        timestamp_ist = datetime.now(ist)
        timestamp_utc = timestamp_ist.astimezone(pytz.UTC)
        
        insert_query = """
            INSERT INTO depth_200_snapshots (
                timestamp, security_id, instrument_name,
                best_bid, best_ask, spread,
                total_bid_qty, total_ask_qty,
                total_bid_orders, total_ask_orders,
                imbalance_ratio,
                avg_bid_order_size, avg_ask_order_size,
                bid_vwap, ask_vwap,
                bid_50pct_level, ask_50pct_level
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s
            )
        """
        
        db_cursor.execute(insert_query, (
            timestamp_utc,
            int(SECURITY_ID),
            INSTRUMENT_NAME,
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
        ))
        
    except Exception as e:
        print(f"Error saving to database: {e}")
        # Try to reconnect
        try:
            global db_conn
            db_conn = get_db_connection()
            db_cursor = db_conn.cursor()
        except Exception as reconnect_error:
            print(f"Failed to reconnect to database: {reconnect_error}")

def on_open(ws):
    """WebSocket connection opened"""
    global start_time
    start_time = time.time()
    
    print("=" * 80)
    print("DHAN 200-DEPTH WebSocket Connected")
    print(f"Instrument: {INSTRUMENT_NAME} (Security ID: {SECURITY_ID})")
    print(f"Time: {datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')} IST")
    print(f"Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("Subscribing to 200-level market depth...")
    print("=" * 80)
    print()
    
    subscription_request = {
        'RequestCode': 23,
        'ExchangeSegment': 'NSE_FNO',
        'SecurityId': str(SECURITY_ID)  # Must be string per Dhan API docs
    }
    
    ws.send(json.dumps(subscription_request))
    print(f"Subscription request sent: {subscription_request}")
    print("Waiting for depth data... (Press Ctrl+C to stop)")
    print()

def on_message(ws, message):
    """Process incoming WebSocket message"""
    global snapshot_count, db_cursor
    
    try:
        if isinstance(message, bytes):
            # Log raw message details for debugging
            print(f"[DEBUG] Received binary message: {len(message)} bytes")
            
            # Log first few bytes to inspect message type
            if len(message) >= 12:
                response_code = message[2]
                print(f"[DEBUG] Response code: {response_code} (41=BID, 51=ASK, 50=DISCONNECT)")
                
                # Check for disconnect with reason code
                if response_code == RESPONSE_DISCONNECT:
                    if len(message) >= 14:
                        disconnect_code = struct.unpack('<H', message[12:14])[0]
                        print(f"[ERROR] Server sent disconnect code: {disconnect_code}")
                        print(f"[ERROR] Check https://dhanhq.co/docs/v2/annexure/#data-api-error for meaning")
                    ws.close()
                    return
            
            # Check for combined bid+ask packet (6424 bytes)
            if len(message) == 6424:
                print(f"[DEBUG] Processing 200-level depth packet")
                bid_depth = parse_depth_packet_200(message[:3212])
                ask_depth = parse_depth_packet_200(message[3212:])
                
                if bid_depth and ask_depth:
                    # Use current timestamp in IST (converted to UTC for storage)
                    timestamp_ist = datetime.now(ist)
                    timestamp_utc = timestamp_ist.astimezone(pytz.UTC)
                    
                    # Save individual 200 levels to database
                    save_depth_levels_to_db(db_cursor, bid_depth, ask_depth, timestamp_utc, int(SECURITY_ID))
                    
                    # Publish to Redis for signal-generator (top 20 levels for efficiency)
                    if redis_client:
                        try:
                            current_price = bid_depth[0]['price']  # Use best bid as current price
                            snapshot_data = {
                                'timestamp': timestamp_utc.isoformat(),
                                'current_price': current_price,
                                'bids': [{'price': lvl['price'], 'quantity': lvl['quantity'], 'orders': lvl['orders']} 
                                        for lvl in bid_depth[:20]],
                                'asks': [{'price': lvl['price'], 'quantity': lvl['quantity'], 'orders': lvl['orders']} 
                                        for lvl in ask_depth[:20]]
                            }
                            redis_client.publish('depth_snapshots:NIFTY', json.dumps(snapshot_data))
                        except Exception as redis_error:
                            # Don't let Redis errors stop data collection
                            if snapshot_count % 1000 == 0:  # Print error occasionally, not every time
                                print(f"Redis publish error: {redis_error}")
                    
                    snapshot_count += 1
                    
                    # Print progress every 100 snapshots
                    if snapshot_count % 100 == 0:
                        timestamp_str = datetime.now(ist).strftime('%H:%M:%S')
                        best_bid = bid_depth[0]['price']
                        best_ask = ask_depth[0]['price']
                        spread = best_ask - best_bid
                        print(f"[{timestamp_str}] Snapshots: {snapshot_count}, "
                              f"Bid: ₹{best_bid:,.2f}, "
                              f"Ask: ₹{best_ask:,.2f}, "
                              f"Spread: ₹{spread:.2f}")
            else:
                print(f"[DEBUG] Unexpected message length: {len(message)} bytes (expected 6424 for 200-level)")
                # Dump first 20 bytes in hex for analysis
                hex_dump = ' '.join(f'{b:02x}' for b in message[:min(20, len(message))])
                print(f"[DEBUG] First bytes (hex): {hex_dump}")
        else:
            # Text message (JSON response?)
            print(f"[DEBUG] Received text message: {message}")
        
    except Exception as e:
        print(f"Error parsing message: {e}")
        import traceback
        traceback.print_exc()

def on_error(ws, error):
    """Handle WebSocket errors"""
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket connection close"""
    global start_time, snapshot_count, db_conn, db_cursor, depth_levels_buffer
    
    print(f"[DEBUG] WebSocket closed - Status: {close_status_code}, Message: {close_msg}")
    
    # Flush any remaining depth levels in buffer
    if depth_levels_buffer and db_cursor:
        print(f"Flushing remaining {len(depth_levels_buffer)} depth level records...")
        insert_depth_levels_batch(db_cursor, depth_levels_buffer)
        depth_levels_buffer.clear()
    
    if start_time:
        duration = time.time() - start_time
    else:
        duration = 0
    
    print()
    print("=" * 80)
    print("WebSocket Connection Closed")
    print(f"Total snapshots captured: {snapshot_count}")
    print(f"Session duration: {duration:.1f} seconds")
    print(f"Data saved to database: depth_levels_200 (200 bid + 200 ask levels per snapshot)")
    print("=" * 80)
    
    # Close database connection
    if db_conn:
        db_conn.close()
        print("Database connection closed")
    
    # Close Redis connection
    if redis_client:
        redis_client.close()
        print("Redis connection closed")

def main():
    """Main function to start WebSocket connection with auto-reconnect"""
    global ws, db_conn, db_cursor, redis_client, ACCESS_TOKEN, CLIENT_ID
    
    # Get Dhan credentials (auto-handles token refresh)
    print("Authenticating with Dhan API...")
    try:
        credentials = get_dhan_credentials()
        ACCESS_TOKEN = credentials['access_token']
        CLIENT_ID = credentials['client_id']
        print(f"✓ Authenticated as Client ID: {CLIENT_ID}")
    except Exception as e:
        print(f"ERROR: Failed to authenticate with Dhan API: {e}")
        print("\nSetup instructions:")
        print("Option 1 - API Key (recommended):")
        print("  Set: DHAN_API_KEY, DHAN_API_SECRET, DHAN_CLIENT_ID")
        print("  Follow one-time OAuth flow, then auto-refresh")
        print("\nOption 2 - Manual Token:")
        print("  Set: DHAN_ACCESS_TOKEN, DHAN_CLIENT_ID")
        print("  Renew token daily from web.dhan.co")
        return
    
    # Validate configuration
    if not ACCESS_TOKEN or not CLIENT_ID:
        print("ERROR: Authentication failed - no valid credentials")
        return
    
    if not DB_PASSWORD:
        print("ERROR: DB_PASSWORD must be set")
        return
    
    # Connect to database
    try:
        db_conn = get_db_connection()
        db_cursor = db_conn.cursor()
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return
    
    # Connect to Redis
    redis_client = get_redis_connection()
    
    # WebSocket URL
    ws_url = (
        f"wss://full-depth-api.dhan.co/twohundreddepth?"
        f"token={ACCESS_TOKEN}&"
        f"clientId={CLIENT_ID}&"
        f"authType=2"
    )
    
    print()
    print("=" * 80)
    print("DHAN 200-DEPTH API WebSocket Client")
    print("Starting connection with auto-reconnect...")
    print("=" * 80)
    print()
    
    # Reconnection with exponential backoff
    retry_count = 0
    max_retries = 10
    base_delay = 5
    max_delay = 300  # 5 minutes
    
    while retry_count < max_retries:
        try:
            # Create and start WebSocket connection
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            # Run forever - server sends ping every 10s, websocket library auto-responds with pong
            # Server disconnects if no pong within 40s (per Dhan docs)
            ws.run_forever()
            
            # If we got data, reset retry counter
            if snapshot_count > 0:
                retry_count = 0
            else:
                retry_count += 1
            
            # Calculate backoff delay
            if retry_count > 0:
                delay = min(base_delay * (2 ** retry_count), max_delay)
                print(f"\nReconnection attempt {retry_count}/{max_retries} in {delay}s...")
                time.sleep(delay)
            else:
                # Brief pause before reconnecting after successful session
                print("\nReconnecting in 5s...")
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n\nReceived interrupt signal, shutting down...")
            break
        except Exception as e:
            retry_count += 1
            print(f"\nUnexpected error: {e}")
            if retry_count < max_retries:
                delay = min(base_delay * (2 ** retry_count), max_delay)
                print(f"Reconnection attempt {retry_count}/{max_retries} in {delay}s...")
                time.sleep(delay)
            else:
                print(f"Max retries ({max_retries}) reached. Exiting...")
                break

if __name__ == "__main__":
    main()
