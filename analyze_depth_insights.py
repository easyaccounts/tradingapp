#!/usr/bin/env python3
"""
Market Depth Analysis - Actionable Trading Insights
Analyzes 20-level orderbook data to identify support/resistance, absorption, and order flow
"""

import psycopg2
from datetime import datetime, timedelta
from collections import defaultdict
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Connect to TimescaleDB"""
    return psycopg2.connect(
        host='localhost',
        port=5432,
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD')
    )

def get_time_range(conn):
    """Get the time range of available data"""
    cur = conn.cursor()
    cur.execute("""
        SELECT MIN(time), MAX(time)
        FROM depth_levels_200
        WHERE time::date = CURRENT_DATE
    """)
    min_time, max_time = cur.fetchone()
    cur.close()
    return min_time, max_time

def get_snapshots(conn, limit=None):
    """Get list of unique timestamps (snapshots)"""
    cur = conn.cursor()
    query = """
        SELECT DISTINCT time
        FROM depth_levels_200
        WHERE time::date = CURRENT_DATE
        ORDER BY time
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    snapshots = [row[0] for row in cur.fetchall()]
    cur.close()
    return snapshots

def get_snapshot_data(conn, timestamp):
    """Get full depth data for a specific timestamp"""
    cur = conn.cursor()
    cur.execute("""
        SELECT level_num, side, price, quantity, orders
        FROM depth_levels_200
        WHERE time >= %s AND time < %s + INTERVAL '1 second'
          AND price > 0
        ORDER BY side DESC, level_num
    """, (timestamp, timestamp))
    
    rows = cur.fetchall()
    cur.close()
    
    bids = []
    asks = []
    
    for row in rows:
        level_data = {
            'level': row[0],
            'price': float(row[2]),
            'quantity': int(row[3]),
            'orders': int(row[4])
        }
        
        if row[1] == 'bid':
            bids.append(level_data)
        else:
            asks.append(level_data)
    
    return bids, asks

def analyze_snapshot_imbalance(bids, asks):
    """Analyze order imbalance in a snapshot"""
    total_bid_orders = sum(b['orders'] for b in bids)
    total_ask_orders = sum(a['orders'] for a in asks)
    
    total_bid_qty = sum(b['quantity'] for b in bids)
    total_ask_qty = sum(a['quantity'] for a in asks)
    
    # Calculate imbalance (-100 to +100, positive = bullish)
    total_orders = total_bid_orders + total_ask_orders
    if total_orders > 0:
        order_imbalance = ((total_bid_orders - total_ask_orders) / total_orders) * 100
    else:
        order_imbalance = 0
    
    return {
        'bid_orders': total_bid_orders,
        'ask_orders': total_ask_orders,
        'bid_qty': total_bid_qty,
        'ask_qty': total_ask_qty,
        'order_imbalance': order_imbalance
    }

def find_strongest_levels(bids, asks, top_n=3):
    """Find strongest bid and ask levels by order count"""
    # Sort by orders descending
    top_bids = sorted(bids, key=lambda x: x['orders'], reverse=True)[:top_n]
    top_asks = sorted(asks, key=lambda x: x['orders'], reverse=True)[:top_n]
    
    return top_bids, top_asks

def track_level_evolution(conn, sample_interval_seconds=60):
    """Track how key price levels evolve over time"""
    snapshots = get_snapshots(conn)
    
    if len(snapshots) < 2:
        print("Not enough data for evolution analysis")
        return
    
    # Sample snapshots at regular intervals
    sampled = []
    last_time = None
    for ts in snapshots:
        if last_time is None or (ts - last_time).total_seconds() >= sample_interval_seconds:
            sampled.append(ts)
            last_time = ts
    
    print(f"\n{'='*100}")
    print(f"LEVEL EVOLUTION TRACKING - {len(sampled)} snapshots sampled every {sample_interval_seconds}s")
    print(f"{'='*100}\n")
    
    # Track each price level's order count over time
    level_history = defaultdict(list)
    
    for i, ts in enumerate(sampled):
        bids, asks = get_snapshot_data(conn, ts)
        
        # Track all levels with orders
        for bid in bids:
            if bid['orders'] > 0:
                level_history[('bid', bid['price'])].append({
                    'time': ts,
                    'orders': bid['orders'],
                    'quantity': bid['quantity'],
                    'snapshot_idx': i
                })
        
        for ask in asks:
            if ask['orders'] > 0:
                level_history[('ask', ask['price'])].append({
                    'time': ts,
                    'orders': ask['orders'],
                    'quantity': ask['quantity'],
                    'snapshot_idx': i
                })
    
    # Find persistent levels (appeared in 30%+ of snapshots with ≥5 orders at peak)
    persistent_threshold = len(sampled) * 0.3
    persistent_levels = []
    
    for (side, price), history in level_history.items():
        if len(history) >= persistent_threshold:
            max_orders = max(h['orders'] for h in history)
            avg_orders = sum(h['orders'] for h in history) / len(history)
            
            if max_orders >= 5:  # At least hit 5 orders at some point
                persistent_levels.append({
                    'side': side,
                    'price': price,
                    'appearances': len(history),
                    'max_orders': max_orders,
                    'avg_orders': avg_orders,
                    'history': history
                })
    
    # Sort by max orders
    persistent_levels.sort(key=lambda x: x['max_orders'], reverse=True)
    
    print(f"PERSISTENT LEVELS (appeared in ≥{int(persistent_threshold)} snapshots with ≥5 orders peak):")
    print(f"{'-'*100}")
    
    for level in persistent_levels[:15]:
        side_label = 'SUPPORT' if level['side'] == 'bid' else 'RESISTANCE'
        
        # Check for absorption or accumulation
        history = level['history']
        if len(history) >= 3:
            early_avg = sum(h['orders'] for h in history[:len(history)//3]) / (len(history)//3)
            late_avg = sum(h['orders'] for h in history[-len(history)//3:]) / (len(history)//3)
            
            change_pct = ((late_avg - early_avg) / early_avg * 100) if early_avg > 0 else 0
            
            if change_pct < -40:
                signal = "⚠️  ABSORPTION (weakening)"
            elif change_pct > 40:
                signal = "📈 ACCUMULATION (strengthening)"
            else:
                signal = "━  STABLE"
        else:
            signal = "━  STABLE"
        
        first_seen = history[0]['time'].strftime('%H:%M:%S')
        last_seen = history[-1]['time'].strftime('%H:%M:%S')
        
        print(f"₹{level['price']:>10.2f} {side_label:>10} | Peak: {level['max_orders']:>2} orders | "
              f"Avg: {level['avg_orders']:>4.1f} | Seen: {level['appearances']:>3}x | "
              f"{first_seen} → {last_seen}")
        print(f"  {signal}")
        
        # Show evolution timeline for top 5
        if len(persistent_levels[:5]) > 0 and level in persistent_levels[:5]:
            print(f"  Timeline: ", end="")
            for h in history[::max(1, len(history)//10)]:  # Show ~10 points
                print(f"{h['time'].strftime('%H:%M')}({h['orders']})", end=" ")
            print()
        print()
    
    return level_history, persistent_levels

def analyze_order_flow_snapshots(conn, num_snapshots=20):
    """Analyze recent snapshots to show order flow dynamics"""
    snapshots = get_snapshots(conn)
    
    if len(snapshots) < 2:
        print("Not enough snapshots")
        return
    
    # Take last N snapshots
    recent_snapshots = snapshots[-num_snapshots:]
    
    print(f"\n{'='*100}")
    print(f"ORDER FLOW ANALYSIS - Last {len(recent_snapshots)} Snapshots")
    print(f"{'='*100}\n")
    
    print(f"{'Time':^10} | {'Bid Orders':>12} | {'Ask Orders':>12} | {'Imbalance':>12} | "
          f"{'Top Bid Lvl':>15} | {'Top Ask Lvl':>15}")
    print(f"{'-'*100}")
    
    for ts in recent_snapshots:
        bids, asks = get_snapshot_data(conn, ts)
        
        if not bids and not asks:
            continue
        
        imbalance = analyze_snapshot_imbalance(bids, asks)
        
        # Find strongest levels
        top_bid_str = "N/A"
        top_ask_str = "N/A"
        
        if bids:
            top_bid = max(bids, key=lambda x: x['orders'])
            top_bid_str = f"₹{top_bid['price']:.2f}({top_bid['orders']})"
        
        if asks:
            top_ask = max(asks, key=lambda x: x['orders'])
            top_ask_str = f"₹{top_ask['price']:.2f}({top_ask['orders']})"
        
        time_str = ts.strftime('%H:%M:%S')
        imb_str = f"{imbalance['order_imbalance']:+.1f}%"
        
        # Signal based on imbalance
        if imbalance['order_imbalance'] > 20:
            signal = "🟢 BULLISH"
        elif imbalance['order_imbalance'] < -20:
            signal = "🔴 BEARISH"
        else:
            signal = "━  NEUTRAL"
        
        print(f"{time_str:^10} | {imbalance['bid_orders']:>12} | {imbalance['ask_orders']:>12} | "
              f"{imb_str:>12} | {top_bid_str:>15} | {top_ask_str:>15} {signal}")

def analyze_depth_profile(conn):
    """Analyze average depth profile across all snapshots"""
    cur = conn.cursor()
    
    print(f"\n{'='*100}")
    print(f"AVERAGE DEPTH PROFILE BY LEVEL")
    print(f"{'='*100}\n")
    
    # Get average orders per level for BID
    cur.execute("""
        SELECT 
            level_num,
            AVG(orders) as avg_orders,
            AVG(quantity) as avg_quantity,
            COUNT(*) as snapshots
        FROM depth_levels_200
        WHERE time::date = CURRENT_DATE
          AND side = 'bid'
          AND price > 0
        GROUP BY level_num
        ORDER BY level_num
    """)
    
    bid_results = cur.fetchall()
    
    # Get average orders per level for ASK
    cur.execute("""
        SELECT 
            level_num,
            AVG(orders) as avg_orders,
            AVG(quantity) as avg_quantity,
            COUNT(*) as snapshots
        FROM depth_levels_200
        WHERE time::date = CURRENT_DATE
          AND side = 'ask'
          AND price > 0
        GROUP BY level_num
        ORDER BY level_num
    """)
    
    ask_results = cur.fetchall()
    cur.close()
    
    if bid_results:
        print("BID SIDE (Support):")
        print(f"{'Level':>5} | {'Avg Orders':>12} | {'Avg Quantity':>15} | {'Snapshots':>12}")
        print(f"{'-'*60}")
        for row in bid_results:
            print(f"{row[0]:>5} | {row[1]:>12.2f} | {row[2]:>15,.0f} | {row[3]:>12}")
    else:
        print("BID SIDE (Support): No data available")
    
    if ask_results:
        print("\nASK SIDE (Resistance):")
        print(f"{'Level':>5} | {'Avg Orders':>12} | {'Avg Quantity':>15} | {'Snapshots':>12}")
        print(f"{'-'*60}")
        for row in ask_results:
            print(f"{row[0]:>5} | {row[1]:>12.2f} | {row[2]:>15,.0f} | {row[3]:>12}")
    else:
        print("\nASK SIDE (Resistance): No data available")

def main():
    conn = get_db_connection()
    
    try:
        min_time, max_time = get_time_range(conn)
        
        if not min_time or not max_time:
            print("No data found for today")
            return
        
        duration = (max_time - min_time).total_seconds() / 60
        print(f"\n{'='*100}")
        print(f"MARKET DEPTH ANALYSIS")
        print(f"{'='*100}")
        print(f"Period: {min_time.strftime('%Y-%m-%d %H:%M:%S')} to {max_time.strftime('%H:%M:%S')} UTC")
        print(f"Duration: {duration:.1f} minutes")
        print(f"{'='*100}")
        
        # 1. Show recent order flow
        analyze_order_flow_snapshots(conn, num_snapshots=50)
        
        # 2. Track level evolution (sample every 60 seconds)
        track_level_evolution(conn, sample_interval_seconds=60)
        
        # 3. Show average depth profile
        analyze_depth_profile(conn)
        
        print(f"\n{'='*100}")
        print("✓ Analysis complete")
        print(f"{'='*100}\n")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
