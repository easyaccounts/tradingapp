#!/usr/bin/env python3
"""
Analyze Order Cluster Evolution Over Time
Shows how price levels and order concentrations change throughout the trading session
"""

import psycopg2
import os
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': os.getenv('DB_NAME', 'tradingdb'),
    'user': os.getenv('DB_USER', 'tradinguser'),
    'password': os.getenv('DB_PASSWORD')
}

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def get_time_windows(conn, window_minutes=1):
    """Get list of time windows for analysis"""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT MIN(time), MAX(time)
        FROM depth_levels_200
        WHERE time >= CURRENT_DATE
    """)
    
    min_time, max_time = cursor.fetchone()
    cursor.close()
    
    if not min_time or not max_time:
        return []
    
    windows = []
    current = min_time
    while current < max_time:
        next_time = current + timedelta(minutes=window_minutes)
        windows.append((current, next_time))
        current = next_time
    
    return windows

def get_clusters_for_window(conn, start_time, end_time, top_n=10):
    """Get top price clusters for a specific time window"""
    cursor = conn.cursor()
    
    # Debug: Check raw data in this window
    cursor.execute("""
        SELECT side, COUNT(*), AVG(orders), MAX(orders)
        FROM depth_levels_200
        WHERE time >= %s AND time < %s AND price > 0
        GROUP BY side
    """, (start_time, end_time))
    debug_rows = cursor.fetchall()
    if debug_rows:
        for row in debug_rows:
            print(f"  DEBUG [{start_time.strftime('%H:%M')}]: {row[0].upper()} - {row[1]} records, avg orders={row[2]:.2f}, max={row[3]}")
    
    query = """
    WITH window_data AS (
        SELECT 
            ROUND(price * 2) / 2 as price_level,
            side,
            AVG(orders) as avg_orders,
            MAX(orders) as max_orders,
            AVG(quantity) as avg_quantity,
            COUNT(*) as appearances
        FROM depth_levels_200
        WHERE time >= %s AND time < %s
          AND price > 0
        GROUP BY price_level, side
        HAVING AVG(orders) >= 1
    )
    SELECT 
        price_level,
        side,
        avg_orders,
        max_orders,
        avg_quantity,
        appearances
    FROM window_data
    ORDER BY avg_orders DESC
    LIMIT %s
    """
    
    cursor.execute(query, (start_time, end_time, top_n))
    rows = cursor.fetchall()
    cursor.close()
    
    clusters = []
    for row in rows:
        clusters.append({
            'price': row[0],
            'side': row[1],
            'avg_orders': row[2],
            'max_orders': row[3],
            'avg_quantity': row[4],
            'appearances': row[5]
        })
    
    return clusters

def track_level_over_time(conn, price_level, tolerance=0.5):
    """Track how a specific price level's orders change over time"""
    cursor = conn.cursor()
    
    query = """
    SELECT 
        DATE_TRUNC('minute', time) as minute,
        AVG(orders) as avg_orders,
        MAX(orders) as max_orders,
        AVG(quantity) as avg_quantity,
        COUNT(*) as count
    FROM depth_levels_200
    WHERE time >= CURRENT_DATE
        AND price BETWEEN %s AND %s
    GROUP BY minute
    ORDER BY minute
    """
    
    cursor.execute(query, (price_level - tolerance, price_level + tolerance))
    rows = cursor.fetchall()
    cursor.close()
    
    return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]

def analyze_evolution(conn, window_minutes=2, top_n=5):
    """Main evolution analysis - separate BID and ASK tracking"""
    print("\n" + "="*100)
    print("ORDER CLUSTER EVOLUTION - Time Series Analysis (BID vs ASK)")
    print("="*100)
    
    windows = get_time_windows(conn, window_minutes)
    
    if not windows:
        print("No data found")
        return
    
    print(f"\nAnalyzing {len(windows)} time windows ({window_minutes}-minute intervals)")
    print(f"Period: {windows[0][0].strftime('%H:%M')} to {windows[-1][1].strftime('%H:%M')} UTC\n")
    
    # Track persistent levels separately for BID and ASK
    bid_level_appearances = defaultdict(list)
    ask_level_appearances = defaultdict(list)
    
    print("="*100)
    print("TOP BID LEVELS (Support) - Over Time")
    print("="*100)
    print(f"{'Time':^12} | {'Price':^10} | {'Avg Orders':^12} | {'Max Orders':^12} | {'Avg Qty':^12}")
    print("-"*100)
    
    for i, (start, end) in enumerate(windows):
        clusters = get_clusters_for_window(conn, start, end, top_n * 2)  # Get more to ensure both sides
        
        # Find top BID
        bid_clusters = [c for c in clusters if c['side'] == 'bid']
        if bid_clusters:
            top_bid = bid_clusters[0]
            time_label = start.strftime('%H:%M')
            
            print(f"{time_label:^12} | ₹{top_bid['price']:>8.2f} | {top_bid['avg_orders']:>12.1f} | "
                  f"{top_bid['max_orders']:>12} | {top_bid['avg_quantity']:>12,.0f}")
            
            # Track this level
            level_key = round(top_bid['price'], 1)
            bid_level_appearances[level_key].append({
                'time': start,
                'avg_orders': top_bid['avg_orders'],
                'max_orders': top_bid['max_orders']
            })
    
    print("\n" + "="*100)
    print("TOP ASK LEVELS (Resistance) - Over Time")
    print("="*100)
    print(f"{'Time':^12} | {'Price':^10} | {'Avg Orders':^12} | {'Max Orders':^12} | {'Avg Qty':^12}")
    print("-"*100)
    
    for i, (start, end) in enumerate(windows):
        clusters = get_clusters_for_window(conn, start, end, top_n * 2)
        
        # Find top ASK
        ask_clusters = [c for c in clusters if c['side'] == 'ask']
        if ask_clusters:
            top_ask = ask_clusters[0]
            time_label = start.strftime('%H:%M')
            
            print(f"{time_label:^12} | ₹{top_ask['price']:>8.2f} | {top_ask['avg_orders']:>12.1f} | "
                  f"{top_ask['max_orders']:>12} | {top_ask['avg_quantity']:>12,.0f}")
            
            # Track this level
            level_key = round(top_ask['price'], 1)
            ask_level_appearances[level_key].append({
                'time': start,
                'avg_orders': top_ask['avg_orders'],
                'max_orders': top_ask['max_orders']
            })
    
    print("\n" + "="*100)
    print("PERSISTENT SUPPORT LEVELS (BID - appeared in 5+ windows)")
    print("="*100)
    
    persistent_bids = [(level, data) for level, data in bid_level_appearances.items() if len(data) >= 5]
    persistent_bids.sort(key=lambda x: len(x[1]), reverse=True)
    
    if persistent_bids:
        for price, data in persistent_bids[:10]:
            avg_strength = sum(d['avg_orders'] for d in data) / len(data)
            max_strength = max(d['max_orders'] for d in data)
            first_seen = data[0]['time'].strftime('%H:%M')
            last_seen = data[-1]['time'].strftime('%H:%M')
            
            print(f"₹{price:.1f} - Appeared {len(data)}x from {first_seen} to {last_seen}")
            print(f"  Avg strength: {avg_strength:.1f} orders, Peak: {max_strength} orders")
    else:
        print("No persistent BID levels found")
    
    print("\n" + "="*100)
    print("PERSISTENT RESISTANCE LEVELS (ASK - appeared in 5+ windows)")
    print("="*100)
    
    persistent_asks = [(level, data) for level, data in ask_level_appearances.items() if len(data) >= 5]
    persistent_asks.sort(key=lambda x: len(x[1]), reverse=True)
    
    if persistent_asks:
        for price, data in persistent_asks[:10]:
            avg_strength = sum(d['avg_orders'] for d in data) / len(data)
            max_strength = max(d['max_orders'] for d in data)
            first_seen = data[0]['time'].strftime('%H:%M')
            last_seen = data[-1]['time'].strftime('%H:%M')
            
            print(f"₹{price:.1f} - Appeared {len(data)}x from {first_seen} to {last_seen}")
            print(f"  Avg strength: {avg_strength:.1f} orders, Peak: {max_strength} orders")
    else:
        print("No persistent ASK levels found")
    
    print("\n" + "="*100)

def analyze_level_breakdown(conn, price_level, window_minutes=2):
    """Analyze a specific level's evolution (for detecting absorption)"""
    print(f"\n" + "="*100)
    print(f"DETAILED ANALYSIS: ₹{price_level} Level Evolution")
    print("="*100)
    
    timeline = track_level_over_time(conn, price_level, tolerance=1.0)
    
    if not timeline:
        print(f"No data found for ₹{price_level}")
        return
    
    print(f"\n{'Time':^12} | {'Avg Orders':^12} | {'Max Orders':^12} | {'Avg Qty':^12} | {'Change':^12}")
    print("-"*100)
    
    prev_orders = None
    for minute, avg_orders, max_orders, avg_qty, count in timeline:
        change = ""
        if prev_orders is not None:
            diff = avg_orders - prev_orders
            if abs(diff) > 0.5:
                arrow = "↑" if diff > 0 else "↓"
                change = f"{arrow} {abs(diff):.1f}"
        
        print(f"{minute.strftime('%H:%M'):^12} | {avg_orders:>12.1f} | {max_orders:>12} | "
              f"{avg_qty:>12,.0f} | {change:^12}")
        
        prev_orders = avg_orders
    
    # Detect absorption (significant decline)
    if len(timeline) >= 3:
        initial_orders = timeline[0][1]  # avg_orders from first window
        final_orders = timeline[-1][1]
        decline_pct = (initial_orders - final_orders) / initial_orders * 100 if initial_orders > 0 else 0
        
        print("\n" + "-"*100)
        if decline_pct > 50:
            print(f"⚠ ABSORPTION DETECTED: Orders declined {decline_pct:.0f}% from {initial_orders:.1f} to {final_orders:.1f}")
        elif decline_pct < -50:
            print(f"📈 ACCUMULATION: Orders increased {abs(decline_pct):.0f}% from {initial_orders:.1f} to {final_orders:.1f}")
        else:
            print(f"Level stable: {decline_pct:+.0f}% change")
    
    print("="*100)

def main():
    """Main execution"""
    conn = connect_db()
    
    try:
        # Overall evolution analysis
        analyze_evolution(conn, window_minutes=2, top_n=3)
        
        # Detailed analysis of specific levels (example: major resistance)
        # analyze_level_breakdown(conn, 26100.0)
        
    finally:
        conn.close()
        print("\n✓ Analysis complete\n")

if __name__ == '__main__':
    main()
