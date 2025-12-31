#!/usr/bin/env python3
"""
Combine tick volume with orderbook depth changes
IMPROVED VERSION:
- Aggregate depth data at tick timestamps (not per second)
- Calculate price delta from previous tick
- Weight signal % by volume (not count)
"""

import os
import psycopg2
from datetime import timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

# Configuration
TICK_INSTRUMENT = 12602626  # Nifty Future
DEPTH_SECURITY = 49229       # Nifty Future
TODAY = '2025-12-31'
WINDOW_START = '10:27:00'
WINDOW_END = '10:33:00'


def classify_signal(volume, price_change, delta_bid, delta_ask, bid_qty, ask_qty):
    """
    Classify using Imbalance Ratio = (|ΔBid| - |ΔAsk|) / (|ΔBid| + |ΔAsk|)
    
    Positive ratio: Bid side moving stronger (accumulation)
    Negative ratio: Ask side moving stronger (distribution)
    
    Ratio > +0.8:  Bid dominates 4.3x (extreme accumulation)
    Ratio < -0.8:  Ask dominates 4.3x (extreme distribution)
    Ratio > +0.6:  Bid dominates 2.5x (strong accumulation)
    Ratio < -0.6:  Ask dominates 2.5x (strong distribution)
    """
    
    if volume < 1:
        return "No Volume"
    
    # Calculate imbalance ratio
    abs_bid_delta = abs(delta_bid)
    abs_ask_delta = abs(delta_ask)
    total_delta = abs_bid_delta + abs_ask_delta
    
    if total_delta == 0:
        return "⚫ Quiet"
    
    imbalance_ratio = (abs_bid_delta - abs_ask_delta) / total_delta
    
    # Thresholds
    strong_volume = volume > 50
    extreme_ratio = abs(imbalance_ratio) > 0.8  # One side 4.3x stronger
    strong_ratio = abs(imbalance_ratio) > 0.6   # One side 2.5x stronger
    medium_ratio = abs(imbalance_ratio) > 0.4   # One side 1.7x stronger
    
    big_price_up = price_change > 0.5
    big_price_down = price_change < -0.5
    price_up = price_change > 0
    price_down = price_change < 0
    
    # EXTREME SIGNALS (One side collapses > 80% while other barely moves)
    if extreme_ratio > 0.8 and strong_volume and big_price_up:
        # ASK side collapsing (imbalance > 0.8 = BID stronger = ask weakening), price up
        return "🟢 BULLISH ABSORPTION"
    
    if extreme_ratio > 0.8 and strong_volume and big_price_down:
        # ASK extremely weak but price still down = weak signal
        return "🔴 Bearish Pressure"
    
    if imbalance_ratio < -0.8 and strong_volume and big_price_down:
        # BID side collapsing (imbalance < -0.8 = ASK stronger = bid weakening), price down
        return "🔴 BEARISH ABSORPTION"
    
    if imbalance_ratio < -0.8 and strong_volume and big_price_up:
        # BID extremely weak but price still up = weak signal
        return "🟢 Bullish Pressure"
    
    # STRONG DIRECTIONAL (One side 2.5x stronger than other)
    if imbalance_ratio > 0.6 and strong_volume:
        # BID side dominating (accumulation)
        if price_up or delta_bid > 3000:
            return "🟡 Accumulation (Bid)"
        else:
            return "🟡 Bid Pressure"
    
    if imbalance_ratio < -0.6 and strong_volume:
        # ASK side dominating (distribution)
        if price_down or delta_ask > 3000:
            return "🟠 Distribution (Ask)"
        else:
            return "🟠 Ask Pressure"
    
    # MEDIUM DIRECTIONAL (One side 1.7x stronger)
    if imbalance_ratio > 0.4 and strong_volume and big_price_up:
        return "🟢 Bullish Pressure"
    
    if imbalance_ratio < -0.4 and strong_volume and big_price_down:
        return "🔴 Bearish Pressure"
    
    # WEAK SIGNALS
    if imbalance_ratio > 0.3 and volume > 10 and price_up:
        return "🟢 Bullish (Weak)"
    
    if imbalance_ratio < -0.3 and volume > 10 and price_down:
        return "🔴 Bearish (Weak)"
    
    # BALANCED VOLUME
    if volume > 10:
        return "⚪ Volume (Neutral)"
    
    return "⚫ Quiet"


def ist_to_utc_time(ist_time_str):
    """Convert IST time string to UTC time string (subtract 5:30)"""
    from datetime import datetime
    ist_dt = datetime.strptime(f"{TODAY} {ist_time_str}", "%Y-%m-%d %H:%M:%S")
    utc_dt = ist_dt - timedelta(hours=5, minutes=30)
    return utc_dt.strftime("%Y-%m-%d %H:%M:%S")


def main():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 6432)),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD')
    )
    cursor = conn.cursor()
    
    print(f"\n{'='*140}")
    print(f"Window: {WINDOW_START} - {WINDOW_END} IST")
    print(f"{'='*140}")
    
    # Convert IST window to UTC for depth queries
    window_start_utc = ist_to_utc_time(WINDOW_START)
    window_end_utc = ist_to_utc_time(WINDOW_END)
    
    # Step 1: Get all tick events (ticks table stores IST timestamps)
    tick_query = """
        SELECT 
            time,
            last_price,
            volume_delta,
            oi
        FROM ticks
        WHERE instrument_token = %s
            AND time >= %s::timestamp
            AND time < %s::timestamp
        ORDER BY time
    """
    
    cursor.execute(tick_query, (
        TICK_INSTRUMENT,
        f"{TODAY} {WINDOW_START}",
        f"{TODAY} {WINDOW_END}"
    ))
    
    ticks = cursor.fetchall()
    print(f"✓ Loaded {len(ticks)} tick events")
    
    # Step 2: Get all depth snapshots (depth table stores UTC timestamps)
    depth_query = """
        SELECT 
            time,
            side,
            SUM(quantity) as total_qty,
            SUM(orders) as total_orders
        FROM depth_levels_200
        WHERE security_id = %s
            AND time >= %s::timestamp
            AND time < %s::timestamp
            AND orders > 0
        GROUP BY time, side
        ORDER BY time, side
    """
    
    cursor.execute(depth_query, (
        DEPTH_SECURITY,
        window_start_utc,
        window_end_utc
    ))
    
    depth_snapshots = cursor.fetchall()
    print(f"✓ Loaded {len(depth_snapshots)} depth snapshots\n")
    
    # Organize depth by timestamp (convert UTC to IST)
    depth_by_time = defaultdict(lambda: {'BID': 0, 'ASK': 0})
    for row in depth_snapshots:
        time_utc = row[0]
        time_ist = time_utc + timedelta(hours=5, minutes=30)
        side = row[1]
        qty = row[2] or 0
        depth_by_time[time_ist][side] = qty
    
    # Get all unique depth timestamps (sorted)
    all_depth_times = sorted(depth_by_time.keys())
    
    # Step 3: For each tick, aggregate depth between this tick and previous tick
    print(f"{'Time':<12} {'Price':<10} {'ΔPrice':<10} {'Volume':<10} {'BID Qty':<12} {'ASK Qty':<12} {'ΔBid':<12} {'ΔAsk':<12} {'Signal':<25}")
    print("-" * 140)
    
    prev_price = None
    prev_bid_qty = None
    prev_ask_qty = None
    prev_tick_time = None
    
    classifications = defaultdict(lambda: {'count': 0, 'volume': 0})
    
    for i, tick in enumerate(ticks):
        tick_time = tick[0]
        price = tick[1]
        volume = tick[2] or 0
        
        # Calculate price change from previous tick
        price_change = (price - prev_price) if prev_price is not None else 0
        
        # Find depth snapshots between previous tick and current tick
        if prev_tick_time is None:
            # First tick - use depth snapshot closest to this time
            relevant_depth_times = [t for t in all_depth_times if t <= tick_time]
        else:
            # Get depth snapshots between prev_tick_time and tick_time
            relevant_depth_times = [t for t in all_depth_times if prev_tick_time < t <= tick_time]
        
        # Average depth across relevant snapshots
        if relevant_depth_times:
            bid_qty_sum = sum(depth_by_time[t]['BID'] for t in relevant_depth_times)
            ask_qty_sum = sum(depth_by_time[t]['ASK'] for t in relevant_depth_times)
            bid_qty = bid_qty_sum // len(relevant_depth_times)
            ask_qty = ask_qty_sum // len(relevant_depth_times)
        else:
            bid_qty = prev_bid_qty if prev_bid_qty is not None else 0
            ask_qty = prev_ask_qty if prev_ask_qty is not None else 0
        
        # Calculate depth deltas
        delta_bid = (bid_qty - prev_bid_qty) if prev_bid_qty is not None else 0
        delta_ask = (ask_qty - prev_ask_qty) if prev_ask_qty is not None else 0
        
        # Classify signal
        signal = classify_signal(volume, price_change, delta_bid, delta_ask, bid_qty, ask_qty)
        classifications[signal]['count'] += 1
        classifications[signal]['volume'] += volume
        
        # Format output
        time_str = tick_time.strftime('%H:%M:%S.%f')[:-3]  # Include milliseconds
        price_str = f"{price:.2f}" if price else "N/A"
        price_change_str = f"{price_change:+.2f}" if prev_price is not None else "N/A"
        delta_bid_str = f"{delta_bid:+,}" if prev_bid_qty is not None else "N/A"
        delta_ask_str = f"{delta_ask:+,}" if prev_ask_qty is not None else "N/A"
        
        print(f"{time_str:<12} {price_str:<10} {price_change_str:<10} {volume:<10,} {bid_qty:<12,} {ask_qty:<12,} {delta_bid_str:<12} {delta_ask_str:<12} {signal:<25}")
        
        # Update previous values
        prev_price = price
        prev_tick_time = tick_time
        if bid_qty > 0:
            prev_bid_qty = bid_qty
        if ask_qty > 0:
            prev_ask_qty = ask_qty
    
    # Summary - weighted by volume
    print(f"\n{'='*140}")
    print("SIGNAL CLASSIFICATION SUMMARY (Volume-Weighted)")
    print(f"{'='*140}\n")
    
    total_volume = sum(data['volume'] for data in classifications.values())
    total_ticks = sum(data['count'] for data in classifications.values())
    
    print(f"{'Signal':<30} {'Ticks':>8} {'Volume':>12} {'% (Vol)':>10}")
    print("-" * 65)
    
    for signal, data in sorted(classifications.items(), key=lambda x: x[1]['volume'], reverse=True):
        count = data['count']
        volume = data['volume']
        vol_pct = (volume / total_volume * 100) if total_volume > 0 else 0
        print(f"{signal:<30} {count:>8} {volume:>12,} {vol_pct:>9.1f}%")
    
    print(f"\n{'TOTAL':<30} {total_ticks:>8} {total_volume:>12,} {'100.0':>9}%")
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
