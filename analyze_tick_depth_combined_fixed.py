#!/usr/bin/env python3
"""
Combine tick volume with orderbook depth changes
FIXED TIMEZONE HANDLING:
- Ticks table: Stores IST timestamps (enricher.py converts UTC to IST)
- Depth table: Stores UTC timestamps (depth collector converts IST to UTC)
- Server: Running in UTC timezone
"""

import os
import psycopg2
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# Configuration
TICK_INSTRUMENT = 12602626  # Nifty Future
DEPTH_SECURITY = 49229       # Nifty Future
TODAY = '2025-12-31'

# Time windows to analyze (IST)
WINDOWS = [
    ('09:43:00', '09:47:00'),
    ('10:27:00', '10:33:00'),
    ('10:51:00', '10:57:00'),
    ('11:32:00', '11:36:00'),
    ('11:55:00', '12:00:00'),
    ('13:09:00', '13:14:00'),
    ('13:33:00', '13:37:00'),
    ('14:27:00', '14:32:00'),
    ('14:42:00', '14:46:00'),
]


def ist_to_utc_time(ist_time_str):
    """Convert IST time string to UTC time string (subtract 5:30)"""
    ist_dt = datetime.strptime(f"{TODAY} {ist_time_str}", "%Y-%m-%d %H:%M:%S")
    utc_dt = ist_dt - timedelta(hours=5, minutes=30)
    return utc_dt.strftime("%Y-%m-%d %H:%M:%S")


def classify_signal(volume, price_change, delta_bid, delta_ask, bid_qty, ask_qty):
    """
    Classify market behavior based on volume + depth + price action
    
    Bullish Absorption: High volume + ASK quantity decreases + Price up
    Bearish Absorption: High volume + BID quantity decreases + Price down
    Accumulation: Volume + BID increases (buying pressure building)
    Distribution: Volume + ASK increases (selling pressure building)
    """
    
    if volume < 1:
        return "No Volume"
    
    # Strong thresholds
    strong_volume = volume > 50
    big_bid_drop = delta_bid < -5000
    big_ask_drop = delta_ask < -5000
    big_bid_add = delta_bid > 5000
    big_ask_add = delta_ask > 5000
    price_up = price_change > 0.5
    price_down = price_change < -0.5
    
    # Bullish Absorption: Volume takes out asks, price moves up
    if strong_volume and big_ask_drop and price_up:
        return "🟢 BULLISH ABSORPTION"
    
    # Bearish Absorption: Volume takes out bids, price moves down
    if strong_volume and big_bid_drop and price_down:
        return "🔴 BEARISH ABSORPTION"
    
    # Accumulation: Adding to bids with volume
    if strong_volume and big_bid_add:
        return "🟡 Accumulation (Bid)"
    
    # Distribution: Adding to asks with volume
    if strong_volume and big_ask_add:
        return "🟠 Distribution (Ask)"
    
    # Weaker signals
    if volume > 10 and delta_ask < -1000 and price_change > 0:
        return "🟢 Bullish (Weak)"
    
    if volume > 10 and delta_bid < -1000 and price_change < 0:
        return "🔴 Bearish (Weak)"
    
    if volume > 10:
        return "⚪ Volume (Neutral)"
    
    return "⚫ Quiet"


def main():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 6432)),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD')
    )
    cursor = conn.cursor()
    
    for window_start_ist, window_end_ist in WINDOWS:
        print(f"\n{'='*140}")
        print(f"Window: {window_start_ist} - {window_end_ist} IST")
        print(f"{'='*140}")
        
        # Convert IST window to UTC for depth queries
        window_start_utc = ist_to_utc_time(window_start_ist)
        window_end_utc = ist_to_utc_time(window_end_ist)
        
        # Step 1: Get tick data (ticks table stores IST timestamps)
        tick_query = """
            SELECT 
                DATE_TRUNC('second', time) as second_ist,
                SUM(volume_delta) as total_volume,
                MIN(last_price) as first_price,
                MAX(last_price) as last_price,
                COUNT(*) as tick_count
            FROM ticks
            WHERE instrument_token = %s
                AND time >= %s::timestamp
                AND time < %s::timestamp
            GROUP BY DATE_TRUNC('second', time)
            ORDER BY second_ist
        """
        
        cursor.execute(tick_query, (
            TICK_INSTRUMENT,
            f"{TODAY} {window_start_ist}",
            f"{TODAY} {window_end_ist}"
        ))
        
        tick_rows = cursor.fetchall()
        print(f"✓ Loaded {len(tick_rows)} seconds of tick data")
        
        # Step 2: Get depth data (depth table stores UTC timestamps)
        depth_query = """
            SELECT 
                DATE_TRUNC('second', time) as second_utc,
                side,
                SUM(quantity) as total_qty,
                SUM(orders) as total_orders,
                COUNT(DISTINCT time) as snapshot_count
            FROM depth_levels_200
            WHERE security_id = %s
                AND time >= %s::timestamp
                AND time < %s::timestamp
            GROUP BY DATE_TRUNC('second', time), side
            ORDER BY second_utc, side
        """
        
        cursor.execute(depth_query, (
            DEPTH_SECURITY,
            window_start_utc,
            window_end_utc
        ))
        
        depth_rows = cursor.fetchall()
        print(f"✓ Loaded {len(depth_rows)} depth aggregations")
        
        # Organize depth data by second (convert UTC to IST for matching)
        depth_by_second = defaultdict(lambda: {'BID': {'qty': 0, 'orders': 0}, 'ASK': {'qty': 0, 'orders': 0}})
        for row in depth_rows:
            second_utc = row[0]
            # Convert UTC to IST for matching with tick data
            second_ist = second_utc + timedelta(hours=5, minutes=30)
            side = row[1]
            depth_by_second[second_ist][side] = {
                'qty': row[2] or 0,
                'orders': row[3] or 0,
                'snapshots': row[4]
            }
        
        # Organize tick data
        tick_by_second = {}
        for row in tick_rows:
            second_ist = row[0]
            tick_by_second[second_ist] = {
                'volume': row[1] or 0,
                'first_price': row[2] or 0,
                'last_price': row[3] or 0,
                'tick_count': row[4]
            }
        
        # Get all unique seconds
        all_seconds = sorted(set(tick_by_second.keys()) | set(depth_by_second.keys()))
        
        if not all_seconds:
            print("⚠️  No data found for this window\n")
            continue
        
        # Analyze combined data
        print(f"\n{'Time':<10} {'Price':<8} {'ΔP':<7} {'Vol':<7} {'BID Qty':<12} {'ASK Qty':<12} {'ΔBid':<10} {'ΔAsk':<10} {'Signal':<25}")
        print("-" * 140)
        
        prev_bid_qty = None
        prev_ask_qty = None
        classifications = defaultdict(int)
        
        for second in all_seconds:
            time_str = second.strftime('%H:%M:%S')
            
            # Get tick data
            tick = tick_by_second.get(second, {})
            volume = tick.get('volume', 0)
            first_price = tick.get('first_price', 0)
            last_price = tick.get('last_price', 0)
            price_change = last_price - first_price if (last_price and first_price) else 0
            
            # Get depth data
            depth = depth_by_second.get(second, {'BID': {}, 'ASK': {}})
            bid_qty = depth['BID'].get('qty', 0)
            ask_qty = depth['ASK'].get('qty', 0)
            
            # Calculate depth deltas
            delta_bid = (bid_qty - prev_bid_qty) if prev_bid_qty is not None else 0
            delta_ask = (ask_qty - prev_ask_qty) if prev_ask_qty is not None else 0
            
            # Classify signal
            signal = classify_signal(volume, price_change, delta_bid, delta_ask, bid_qty, ask_qty)
            classifications[signal] += 1
            
            # Format output
            price_str = f"{last_price:.2f}" if last_price > 0 else "N/A"
            price_change_str = f"{price_change:+.2f}" if last_price > 0 else "N/A"
            delta_bid_str = f"{delta_bid:+,}" if prev_bid_qty is not None else "N/A"
            delta_ask_str = f"{delta_ask:+,}" if prev_ask_qty is not None else "N/A"
            
            print(f"{time_str:<10} {price_str:<8} {price_change_str:<7} {volume:<7,} {bid_qty:<12,} {ask_qty:<12,} {delta_bid_str:<10} {delta_ask_str:<10} {signal:<25}")
            
            # Update previous values
            if bid_qty > 0:
                prev_bid_qty = bid_qty
            if ask_qty > 0:
                prev_ask_qty = ask_qty
        
        # Summary
        print(f"\n{'Signal':<30} {'Count':>6} {'%':>6}")
        print("-" * 45)
        total = len(all_seconds)
        for signal, count in sorted(classifications.items(), key=lambda x: x[1], reverse=True):
            pct = (count / total * 100) if total > 0 else 0
            print(f"{signal:<30} {count:>6} {pct:>5.1f}%")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
