#!/usr/bin/env python3
"""
Analyze orderbook behavior during specific time windows for security 49229
Shows bid/ask pressure, volume changes, and order fragmentation per window
"""

import os
import psycopg2
from datetime import datetime, date, time
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


# Time windows in IST (will be converted to UTC for DB query)
TIME_WINDOWS = [
    ("09:43", "09:47"),
    ("10:27", "10:33"),
    ("11:16", "11:19"),
    ("11:38", "11:42"),
    ("12:06", "12:09"),
    ("12:42", "12:46"),
    ("12:57", "13:01"),
    ("13:56", "14:00"),
    ("14:42", "14:46"),
]


def ist_to_utc(time_str):
    """Convert IST time string (HH:MM) to UTC"""
    h, m = map(int, time_str.split(':'))
    # IST is UTC+5:30, so subtract 5:30
    total_minutes = h * 60 + m - (5 * 60 + 30)
    
    if total_minutes < 0:
        total_minutes += 24 * 60  # Previous day
    
    utc_h = total_minutes // 60
    utc_m = total_minutes % 60
    return f"{utc_h:02d}:{utc_m:02d}"


def analyze_orderbook_windows(security_id=49229):
    """Analyze orderbook behavior for each time window"""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print(f"\n{'='*120}")
    print(f"ORDERBOOK BEHAVIOR ANALYSIS - Security ID: {security_id}")
    print(f"Date: {date.today()} (IST)")
    print(f"{'='*120}\n")
    
    for start_ist, end_ist in TIME_WINDOWS:
        start_utc = ist_to_utc(start_ist)
        end_utc = ist_to_utc(end_ist)
        
        print(f"\n{'='*120}")
        print(f"WINDOW: {start_ist}-{end_ist} IST ({start_utc}-{end_utc} UTC)")
        print(f"{'='*120}")
        
        # Query depth data for this window
        query = """
            SELECT 
                side,
                price,
                quantity,
                orders,
                time AT TIME ZONE 'Asia/Kolkata' as time_ist
            FROM depth_levels_200
            WHERE security_id = %s
                AND DATE(time AT TIME ZONE 'Asia/Kolkata') = %s
                AND (time AT TIME ZONE 'Asia/Kolkata')::time >= %s::time
                AND (time AT TIME ZONE 'Asia/Kolkata')::time <= %s::time
                AND orders > 0
            ORDER BY time ASC, side, price DESC
        """
        
        cursor.execute(query, (security_id, date.today(), start_ist, end_ist))
        rows = cursor.fetchall()
        
        if not rows:
            print(f"  No data available for this window")
            continue
        
        # Organize by time snapshot, then by side
        snapshots = defaultdict(lambda: {'BID': {}, 'ASK': {}})
        
        for side, price, qty, order_count, time_ist in rows:
            snapshot_time = time_ist.strftime('%H:%M:%S')
            if price not in snapshots[snapshot_time][side]:
                snapshots[snapshot_time][side][price] = []
            snapshots[snapshot_time][side][price].append({
                'qty': qty,
                'orders': order_count
            })
        
        # Analyze each snapshot
        analyze_window_snapshots(snapshots, start_ist, end_ist)
    
    cursor.close()
    conn.close()


def analyze_window_snapshots(snapshots, start_ist, end_ist):
    """Analyze snapshots within a time window"""
    
    sorted_times = sorted(snapshots.keys())
    
    if not sorted_times:
        print("  No data in window")
        return
    
    # Statistics across all snapshots in window
    bid_quantities = []
    ask_quantities = []
    bid_orders = []
    ask_orders = []
    
    for snapshot_time in sorted_times:
        bid_data = snapshots[snapshot_time]['BID']
        ask_data = snapshots[snapshot_time]['ASK']
        
        for price_levels in bid_data.values():
            for level in price_levels:
                bid_quantities.append(level['qty'])
                bid_orders.append(level['orders'])
        
        for price_levels in ask_data.values():
            for level in price_levels:
                ask_quantities.append(level['qty'])
                ask_orders.append(level['orders'])
    
    if not (bid_quantities or ask_quantities):
        print("  No valid data in window")
        return
    
    # Calculate statistics
    bid_avg_qty = sum(bid_quantities) / len(bid_quantities) if bid_quantities else 0
    ask_avg_qty = sum(ask_quantities) / len(ask_quantities) if ask_quantities else 0
    
    bid_avg_orders = sum(bid_orders) / len(bid_orders) if bid_orders else 0
    ask_avg_orders = sum(ask_orders) / len(ask_orders) if ask_orders else 0
    
    bid_total_qty = sum(bid_quantities) if bid_quantities else 0
    ask_total_qty = sum(ask_quantities) if ask_quantities else 0
    
    # Calculate imbalance
    total_qty = bid_total_qty + ask_total_qty
    if total_qty > 0:
        bid_pressure = (bid_total_qty - ask_total_qty) / total_qty
    else:
        bid_pressure = 0
    
    print(f"\n  Snapshot Count: {len(sorted_times)}")
    print(f"  Time Range: {sorted_times[0]} - {sorted_times[-1]}")
    
    print(f"\n  BID Side:")
    print(f"    Total Qty:      {bid_total_qty:,}")
    print(f"    Avg Qty/Level:  {bid_avg_qty:,.0f}")
    print(f"    Avg Orders:     {bid_avg_orders:.1f}")
    print(f"    Snapshots:      {len(bid_quantities)}")
    
    print(f"\n  ASK Side:")
    print(f"    Total Qty:      {ask_total_qty:,}")
    print(f"    Avg Qty/Level:  {ask_avg_qty:,.0f}")
    print(f"    Avg Orders:     {ask_avg_orders:.1f}")
    print(f"    Snapshots:      {len(ask_quantities)}")
    
    print(f"\n  Imbalance:")
    if bid_pressure > 0:
        print(f"    Bias: BULLISH ({bid_pressure:+.3f})")
    elif bid_pressure < 0:
        print(f"    Bias: BEARISH ({bid_pressure:+.3f})")
    else:
        print(f"    Bias: NEUTRAL")
    
    print(f"    BID/ASK Ratio:  {bid_total_qty / ask_total_qty:.2f}x" if ask_total_qty > 0 else "    BID/ASK Ratio:  N/A")
    
    # Show extremes
    print(f"\n  Extremes in Window:")
    if bid_quantities:
        print(f"    Max BID Qty:    {max(bid_quantities):,}")
        print(f"    Min BID Qty:    {min(bid_quantities):,}")
    if ask_quantities:
        print(f"    Max ASK Qty:    {max(ask_quantities):,}")
        print(f"    Min ASK Qty:    {min(ask_quantities):,}")


if __name__ == "__main__":
    analyze_orderbook_windows(security_id=49229)
