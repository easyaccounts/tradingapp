#!/usr/bin/env python3
"""
Combine tick volume with orderbook depth changes for window 10:27-10:33 IST
Classify each second as: Bullish Absorption / Bearish Absorption / Accumulation / Distribution / Neutral
"""

import os
import psycopg2
from datetime import datetime, date
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 6432)),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD')
    )


def analyze_tick_depth_combined():
    """Analyze tick volume + depth changes for 10:27-10:33 IST window"""
    
    tick_security_id = 12602626
    depth_security_id = 49229
    
    start_time_ist = "10:27"
    end_time_ist = "10:33"
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"\n{'='*120}")
    print(f"TICK + DEPTH COMBINED ANALYSIS")
    print(f"Window: {start_time_ist}-{end_time_ist} IST")
    print(f"Tick Security: {tick_security_id} | Depth Security: {depth_security_id}")
    print(f"Date: {date.today()}")
    print(f"{'='*120}\n")
    
    # Step 1: Get tick data (volume traded per second)
    tick_query = """
        SELECT 
            DATE_TRUNC('second', time AT TIME ZONE 'Asia/Kolkata') as second_ist,
            MIN(last_price) as first_price,
            MAX(last_price) as last_price,
            SUM(volume) as total_volume,
            COUNT(*) as tick_count
        FROM ticks
        WHERE security_id = %s
            AND DATE(time AT TIME ZONE 'Asia/Kolkata') = %s
            AND (time AT TIME ZONE 'Asia/Kolkata')::time >= %s::time
            AND (time AT TIME ZONE 'Asia/Kolkata')::time < %s::time
        GROUP BY DATE_TRUNC('second', time AT TIME ZONE 'Asia/Kolkata')
        ORDER BY second_ist
    """
    
    cursor.execute(tick_query, (tick_security_id, date.today(), start_time_ist, end_time_ist))
    tick_data = {}
    
    for row in cursor.fetchall():
        second, first_price, last_price, volume, tick_count = row
        tick_data[second] = {
            'first_price': float(first_price) if first_price else 0,
            'last_price': float(last_price) if last_price else 0,
            'volume': int(volume) if volume else 0,
            'tick_count': tick_count,
            'price_change': float(last_price - first_price) if (last_price and first_price) else 0
        }
    
    print(f"Loaded {len(tick_data)} seconds of tick data\n")
    
    # Step 2: Get depth snapshots per second
    depth_query = """
        SELECT 
            DATE_TRUNC('second', time AT TIME ZONE 'Asia/Kolkata') as second_ist,
            side,
            SUM(quantity) as total_qty,
            SUM(orders) as total_orders,
            COUNT(*) as snapshot_count
        FROM depth_levels_200
        WHERE security_id = %s
            AND DATE(time AT TIME ZONE 'Asia/Kolkata') = %s
            AND (time AT TIME ZONE 'Asia/Kolkata')::time >= %s::time
            AND (time AT TIME ZONE 'Asia/Kolkata')::time < %s::time
            AND orders > 0
        GROUP BY DATE_TRUNC('second', time AT TIME ZONE 'Asia/Kolkata'), side
        ORDER BY second_ist, side
    """
    
    cursor.execute(depth_query, (depth_security_id, date.today(), start_time_ist, end_time_ist))
    
    depth_by_second = defaultdict(lambda: {'BID': {}, 'ASK': {}})
    
    for row in cursor.fetchall():
        second, side, total_qty, total_orders, snapshot_count = row
        depth_by_second[second][side] = {
            'total_qty': int(total_qty) if total_qty else 0,
            'total_orders': int(total_orders) if total_orders else 0,
            'snapshot_count': snapshot_count
        }
    
    cursor.close()
    conn.close()
    
    print(f"Loaded {len(depth_by_second)} seconds of depth data\n")
    
    # Step 3: Combine and analyze
    sorted_seconds = sorted(set(tick_data.keys()) | set(depth_by_second.keys()))
    
    if not sorted_seconds:
        print("No data found for this window")
        return
    
    print(f"{'Time (IST)':<12} {'Price':<10} {'ΔPrice':<10} {'Volume':<10} {'BID Qty':<12} {'ASK Qty':<12} {'ΔBid':<10} {'ΔAsk':<10} {'Signal':<25}")
    print("-" * 140)
    
    prev_bid_qty = None
    prev_ask_qty = None
    
    classifications = defaultdict(int)
    
    for second in sorted_seconds:
        time_str = second.strftime('%H:%M:%S')
        
        # Get tick data
        tick = tick_data.get(second, {})
        price = tick.get('last_price', 0)
        price_change = tick.get('price_change', 0)
        volume = tick.get('volume', 0)
        
        # Get depth data
        depth = depth_by_second.get(second, {'BID': {}, 'ASK': {}})
        bid_qty = depth['BID'].get('total_qty', 0)
        ask_qty = depth['ASK'].get('total_qty', 0)
        
        # Calculate depth deltas
        delta_bid = bid_qty - prev_bid_qty if prev_bid_qty is not None else 0
        delta_ask = ask_qty - prev_ask_qty if prev_ask_qty is not None else 0
        
        # Classify the signal
        signal = classify_signal(volume, price_change, delta_bid, delta_ask, bid_qty, ask_qty)
        classifications[signal] += 1
        
        # Print row
        delta_bid_str = f"{delta_bid:+,}" if prev_bid_qty is not None else "N/A"
        delta_ask_str = f"{delta_ask:+,}" if prev_ask_qty is not None else "N/A"
        price_str = f"{price:.2f}" if price > 0 else "N/A"
        price_change_str = f"{price_change:+.2f}" if price > 0 else "N/A"
        
        print(f"{time_str:<12} {price_str:<10} {price_change_str:<10} {volume:<10,} {bid_qty:<12,} {ask_qty:<12,} {delta_bid_str:<10} {delta_ask_str:<10} {signal:<25}")
        
        # Update previous values
        prev_bid_qty = bid_qty if bid_qty > 0 else prev_bid_qty
        prev_ask_qty = ask_qty if ask_qty > 0 else prev_ask_qty
    
    # Summary
    print(f"\n{'='*120}")
    print("SIGNAL CLASSIFICATION SUMMARY")
    print(f"{'='*120}\n")
    
    total_seconds = len(sorted_seconds)
    for signal, count in sorted(classifications.items(), key=lambda x: x[1], reverse=True):
        pct = (count / total_seconds * 100) if total_seconds > 0 else 0
        print(f"{signal:<30}: {count:>4} seconds ({pct:>5.1f}%)")


def classify_signal(volume, price_change, delta_bid, delta_ask, bid_qty, ask_qty):
    """
    Classify orderbook + volume behavior
    
    Rules:
    - Bullish Absorption: Volume > 0, ASK depth decreases, price up
    - Bearish Absorption: Volume > 0, BID depth decreases, price down
    - Accumulation: BID depth increases, low volume
    - Distribution: ASK depth increases, low volume
    - Neutral: No significant change
    """
    
    volume_threshold = 100  # Minimum volume to consider as active
    price_threshold = 0.5   # Minimum price change
    depth_threshold = 1000  # Minimum depth change
    
    has_volume = volume >= volume_threshold
    price_up = price_change >= price_threshold
    price_down = price_change <= -price_threshold
    
    ask_decreased = delta_ask <= -depth_threshold
    bid_decreased = delta_bid <= -depth_threshold
    ask_increased = delta_ask >= depth_threshold
    bid_increased = delta_bid >= depth_threshold
    
    # Primary signals (with volume)
    if has_volume and ask_decreased and price_up:
        return "🟢 BULLISH ABSORPTION"
    
    if has_volume and bid_decreased and price_down:
        return "🔴 BEARISH ABSORPTION"
    
    if has_volume and ask_decreased and not price_up:
        return "🟡 ASK SWEEP (weak up)"
    
    if has_volume and bid_decreased and not price_down:
        return "🟡 BID SWEEP (weak down)"
    
    # Secondary signals (depth changes without volume)
    if bid_increased and not has_volume:
        return "🔵 ACCUMULATION (BID+)"
    
    if ask_increased and not has_volume:
        return "🟠 DISTRIBUTION (ASK+)"
    
    # Replenishment patterns
    if has_volume and ask_increased:
        return "⚠️ ASK REPLENISHMENT"
    
    if has_volume and bid_increased:
        return "⚠️ BID REPLENISHMENT"
    
    # Default
    if has_volume:
        return "⚪ ACTIVE (no pattern)"
    
    return "⚪ NEUTRAL"


if __name__ == "__main__":
    analyze_tick_depth_combined()
