#!/usr/bin/env python3
"""
Market Depth Analysis - Actionable Trading Insights
Analyzes 20-level orderbook data to identify support/resistance, absorption, and order flow
"""

import psycopg2
from datetime import datetime, timedelta
from collections import defaultdict
import os
import json
from dotenv import load_dotenv

load_dotenv()

STATE_FILE = '/opt/tradingapp/data/depth_analysis_state.json'

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

def get_current_price(conn):
    """Get current best bid and ask prices"""
    cur = conn.cursor()
    
    # Get most recent timestamp
    cur.execute("""
        SELECT MAX(time) FROM depth_levels_200 WHERE time::date = CURRENT_DATE
    """)
    latest_time = cur.fetchone()[0]
    
    if not latest_time:
        cur.close()
        return None, None, None
    
    # Get best bid (level 1 bid)
    cur.execute("""
        SELECT price FROM depth_levels_200
        WHERE time >= %s AND time < %s + INTERVAL '1 second'
          AND side = 'BID' AND level_num = 1 AND price > 0
        ORDER BY time DESC LIMIT 1
    """, (latest_time, latest_time))
    bid_result = cur.fetchone()
    best_bid = float(bid_result[0]) if bid_result else None
    
    # Get best ask (level 1 ask)
    cur.execute("""
        SELECT price FROM depth_levels_200
        WHERE time >= %s AND time < %s + INTERVAL '1 second'
          AND side = 'ASK' AND level_num = 1 AND price > 0
        ORDER BY time DESC LIMIT 1
    """, (latest_time, latest_time))
    ask_result = cur.fetchone()
    best_ask = float(ask_result[0]) if ask_result else None
    
    cur.close()
    
    # Calculate mid price
    if best_bid and best_ask:
        mid_price = (best_bid + best_ask) / 2
    else:
        mid_price = None
    
    return mid_price, best_bid, best_ask

def load_previous_state():
    """Load previous analysis state"""
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None

def save_current_state(state):
    """Save current analysis state"""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

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
        
        if row[1].lower() == 'bid':
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

def track_level_evolution(conn, sample_interval_seconds=10, current_price=None):
    """Track how key price levels evolve over time"""
    snapshots = get_snapshots(conn)
    
    if len(snapshots) < 2:
        print("Not enough data for evolution analysis")
        return None, None
    
    # Sample snapshots at regular intervals
    sampled = []
    last_time = None
    for ts in snapshots:
        if last_time is None or (ts - last_time).total_seconds() >= sample_interval_seconds:
            sampled.append(ts)
            last_time = ts
    
    print(f"\n{'='*100}")
    print(f"LEVEL EVOLUTION TRACKING - {len(sampled)} sample points at {sample_interval_seconds}-second intervals")
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
            max_quantity = max(h['quantity'] for h in history)
            avg_quantity = sum(h['quantity'] for h in history) / len(history)
            
            if max_orders >= 5:  # At least hit 5 orders at some point
                persistent_levels.append({
                    'side': side,
                    'price': price,
                    'appearances': len(history),
                    'max_orders': max_orders,
                    'avg_orders': avg_orders,
                    'max_quantity': max_quantity,
                    'avg_quantity': avg_quantity,
                    'history': history
                })
    
    # Sort by average quantity (highest volume levels)
    persistent_levels.sort(key=lambda x: x['avg_quantity'], reverse=True)
    
    print(f"PERSISTENT LEVELS (appeared in ≥{int(persistent_threshold)} snapshots with ≥5 orders peak):")
    print(f"⭐ = Strongest level - used for trade setup in KEY INSIGHTS section below")
    print(f"{'-'*100}")
    
    # Track which is the strongest of each side
    bid_levels = [l for l in persistent_levels if l['side'] == 'bid']
    ask_levels = [l for l in persistent_levels if l['side'] == 'ask']
    strongest_support = bid_levels[0] if bid_levels else None
    strongest_resistance = ask_levels[0] if ask_levels else None
    
    for level in persistent_levels[:15]:
        side_label = 'SUPPORT' if level['side'] == 'bid' else 'RESISTANCE'
        
        # Mark strongest levels
        is_strongest = (level == strongest_support or level == strongest_resistance)
        marker = "⭐" if is_strongest else "  "
        
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
        
        print(f"{marker} ₹{level['price']:>10.2f} {side_label:>10} | Peak: {level['max_orders']:>2} orders | "
              f"Avg: {level['avg_orders']:>4.1f} | Seen: {level['appearances']:>3}x | "
              f"{first_seen} → {last_seen}")
        print(f"  Qty: Peak {level['max_quantity']:,} | Avg {level['avg_quantity']:,.0f}")
        print(f"  {signal}")
        
        # Show executed trade volume for strongest (starred) levels only
        if is_strongest:
            # NIFTY JAN 2026 FUT Kite instrument_token (different from Dhan security_id 49229)
            NIFTY_JAN_FUT_TOKEN = 12602626
            vol_data = get_volume_at_price(conn, level['price'], NIFTY_JAN_FUT_TOKEN)
            if vol_data['tested']:
                # Level was tested with real trades
                if vol_data['total_volume'] >= 1000:  # Significant volume
                    delta_direction = "🟢" if vol_data['net_delta'] > 0 else "🔴" if vol_data['net_delta'] < 0 else "⚪"
                    validation = "✅ TESTED" if vol_data['total_volume'] >= 5000 else "⚠️  Lightly tested"
                    print(f"  📊 TRADED: {vol_data['trades']} trades | {vol_data['total_volume']:,} contracts")
                    print(f"     {delta_direction} Buy: {vol_data['buy_volume']:,} ({vol_data['buy_pct']:.0f}%) | "
                          f"Sell: {vol_data['sell_volume']:,} ({vol_data['sell_pct']:.0f}%) | "
                          f"Delta: {vol_data['net_delta']:+,} {validation}")
                else:
                    print(f"  📊 TRADED: {vol_data['total_volume']:,} contracts (low volume - use caution ⚠️)")
            else:
                print(f"  ⚠️  UNTESTED: No trades executed at this level today")
        
        # Show distance from current price
        if current_price:
            distance = level['price'] - current_price
            if abs(distance) < 50:  # Only show if within 50 points
                position = "ABOVE" if distance > 0 else "BELOW"
                print(f"  📍 {abs(distance):.2f} points {position} current price")
        
        # Show evolution timeline for top 5
        if len(persistent_levels[:5]) > 0 and level in persistent_levels[:5]:
            print(f"  Timeline: ", end="")
            for h in history[::max(1, len(history)//10)]:  # Show ~10 points
                print(f"{h['time'].strftime('%H:%M')}({h['orders']})", end=" ")
            print()
        print()
    
    return level_history, persistent_levels

def get_volume_at_price(conn, price, instrument_token, price_range=2.0):
    """
    Analyze executed trade volume at a specific price level
    
    Args:
        conn: Database connection
        price: Target price level
        instrument_token: Kite instrument token (e.g., 12602626 for NIFTY JAN FUT)
        price_range: +/- range to include (default ±2 points)
    
    Returns:
        Dict with trade count, total volume, buy/sell breakdown, and net delta
    """
    cur = conn.cursor()
    
    # Query ticks table for executed trades near this price
    # Using time::date = CURRENT_DATE to match orderbook analysis (full day)
    # Note: time is stored in UTC
    # Ticks table uses Kite instrument_token (different from depth_levels_200 security_id)
    cur.execute("""
        SELECT 
            COUNT(*) as trades,
            COALESCE(SUM(volume_delta), 0) as total_volume,
            COALESCE(SUM(CASE WHEN aggressor_side = 'BUY' THEN volume_delta ELSE 0 END), 0) as buy_volume,
            COALESCE(SUM(CASE WHEN aggressor_side = 'SELL' THEN volume_delta ELSE 0 END), 0) as sell_volume,
            COALESCE(SUM(cvd_change), 0) as net_delta
        FROM ticks
        WHERE time::date = CURRENT_DATE
          AND instrument_token = %s
          AND last_price BETWEEN %s AND %s
          AND volume_delta > 0
    """, (instrument_token, price - price_range, price + price_range))
    
    row = cur.fetchone()
    cur.close()
    
    if row and row[0] > 0:
        trades, total_volume, buy_volume, sell_volume, net_delta = row
        
        # Calculate percentages
        buy_pct = (buy_volume / total_volume * 100) if total_volume > 0 else 0
        sell_pct = (sell_volume / total_volume * 100) if total_volume > 0 else 0
        
        return {
            'trades': int(trades),
            'total_volume': int(total_volume),
            'buy_volume': int(buy_volume),
            'sell_volume': int(sell_volume),
            'buy_pct': buy_pct,
            'sell_pct': sell_pct,
            'net_delta': int(net_delta),
            'tested': True
        }
    else:
        return {
            'trades': 0,
            'total_volume': 0,
            'buy_volume': 0,
            'sell_volume': 0,
            'buy_pct': 0,
            'sell_pct': 0,
            'net_delta': 0,
            'tested': False
        }

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
          AND side = 'BID'
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
          AND side = 'ASK'
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

def analyze_changes(current_levels, previous_state, current_price):
    """Analyze changes from previous run"""
    if not previous_state:
        return
    
    prev_levels = previous_state.get('levels', {})
    prev_price = previous_state.get('price')
    prev_imbalance = previous_state.get('imbalance')
    
    print(f"\n{'='*100}")
    print(f"CHANGES FROM LAST ANALYSIS")
    print(f"{'='*100}\n")
    
    # Price change
    if prev_price and current_price:
        price_change = current_price - prev_price
        print(f"PRICE MOVEMENT: ₹{prev_price:.2f} → ₹{current_price:.2f} ({price_change:+.2f})\n")
    
    # Track changes in levels
    new_levels = []
    strengthening = []
    weakening = []
    
    for level in current_levels:
        key = f"{level['side']}_{level['price']:.2f}"
        current_orders = level['avg_orders']
        
        if key in prev_levels:
            prev_orders = prev_levels[key]['avg_orders']
            change_pct = ((current_orders - prev_orders) / prev_orders * 100) if prev_orders > 0 else 0
            
            if change_pct > 40:
                strengthening.append((level, prev_orders, current_orders, change_pct))
            elif change_pct < -40:
                weakening.append((level, prev_orders, current_orders, change_pct))
        else:
            if level['max_orders'] >= 5:
                new_levels.append(level)
    
    if new_levels:
        print("NEW LEVELS DETECTED:")
        for level in new_levels[:3]:
            side_label = 'SUPPORT' if level['side'] == 'bid' else 'RESISTANCE'
            print(f"  ₹{level['price']:.2f} {side_label} - Peak {level['max_orders']} orders")
        print()
    
    if strengthening:
        print("STRENGTHENING LEVELS (ACCUMULATION):")
        for level, prev, curr, pct in strengthening[:3]:
            side_label = 'SUPPORT' if level['side'] == 'bid' else 'RESISTANCE'
            print(f"  ₹{level['price']:.2f} {side_label} - Orders {prev:.1f} → {curr:.1f} ({pct:+.0f}%)")
        print()
    
    if weakening:
        print("WEAKENING LEVELS (ABSORPTION):")
        for level, prev, curr, pct in weakening[:3]:
            side_label = 'SUPPORT' if level['side'] == 'bid' else 'RESISTANCE'
            print(f"  ⚠️  ₹{level['price']:.2f} {side_label} - Orders {prev:.1f} → {curr:.1f} ({pct:.0f}%)")
        print()

def generate_key_insights(conn, current_price, persistent_levels, latest_time=None):
    """Generate actionable insights - returns (insights_text, insights_data)"""
    output_lines = []
    insights_data = {}
    
    output_lines.append(f"\n{'='*100}")
    output_lines.append(f"KEY INSIGHTS")
    output_lines.append(f"{'='*100}\n")
    
    if not current_price:
        output_lines.append("Unable to generate insights - current price not available")
        print('\n'.join(output_lines))
        return '\n'.join(output_lines), insights_data
    
    # Show timestamp
    if latest_time:
        ist_time = latest_time + timedelta(hours=5, minutes=30)
        output_lines.append(f"🎯 PRICE ACTION: ₹{current_price:.2f} (as of {ist_time.strftime('%H:%M:%S')} IST)")
        insights_data['timestamp_ist'] = ist_time.strftime('%H:%M:%S IST')
    
    insights_data['current_price'] = current_price
    
    supports = [l for l in persistent_levels if l['side'] == 'bid' and l['price'] < current_price]
    resistances = [l for l in persistent_levels if l['side'] == 'ask' and l['price'] > current_price]
    
    # Sort by avg_quantity (same logic as PERSISTENT LEVELS section)
    # Also filter to be within reasonable range (±50 points)
    supports = [s for s in supports if current_price - s['price'] <= 50]
    resistances = [r for r in resistances if r['price'] - current_price <= 50]
    
    supports.sort(key=lambda x: x['avg_quantity'], reverse=True)  # Strongest by volume
    resistances.sort(key=lambda x: x['avg_quantity'], reverse=True)  # Strongest by volume
    
    nearest_support = supports[0] if supports else None
    nearest_resistance = resistances[0] if resistances else None
    
    # Get recent imbalance
    snapshots = get_snapshots(conn)
    if snapshots:
        recent_ts = snapshots[-1]
        bids, asks = get_snapshot_data(conn, recent_ts)
        imbalance = analyze_snapshot_imbalance(bids, asks)
        imb_pct = imbalance['order_imbalance']
        
        # Determine bias
        if imb_pct > 20:
            bias = "BULLISH"
            bias_icon = "🟢"
        elif imb_pct < -20:
            bias = "BEARISH"
            bias_icon = "🔴"
        else:
            bias = "NEUTRAL"
            bias_icon = "━"
        
        if not latest_time:
            output_lines.append(f"🎯 PRICE ACTION: ₹{current_price:.2f}")
        
        insights_data['bias'] = bias
        insights_data['bias_icon'] = bias_icon
        insights_data['imbalance_pct'] = imb_pct
        insights_data['nearest_support'] = nearest_support
        insights_data['nearest_resistance'] = nearest_resistance
        
        if nearest_support:
            distance = current_price - nearest_support['price']
            output_lines.append(f"   Trading {distance:.2f} points ABOVE ₹{nearest_support['price']:.2f} support ({nearest_support['max_orders']} orders peak)")
        
        if nearest_resistance:
            distance = nearest_resistance['price'] - current_price
            output_lines.append(f"   Next resistance at ₹{nearest_resistance['price']:.2f} ({distance:.2f} points away, {nearest_resistance['max_orders']} orders)")
        
        output_lines.append(f"\n📊 MARKET BIAS: {bias_icon} {bias} (Order imbalance: {imb_pct:+.1f}%)")
        
        # Check for weakening resistance
        if nearest_resistance:
            history = nearest_resistance.get('history', [])
            if len(history) >= 3:
                early_avg = sum(h['orders'] for h in history[:len(history)//3]) / (len(history)//3)
                late_avg = sum(h['orders'] for h in history[-len(history)//3:]) / (len(history)//3)
                change_pct = ((late_avg - early_avg) / early_avg * 100) if early_avg > 0 else 0
                
                if change_pct < -40:
                    output_lines.append(f"\n⚠️  WATCH: ₹{nearest_resistance['price']:.2f} resistance showing ABSORPTION ({change_pct:.0f}% decline) - Breakout likely")
                    insights_data['absorption_warning'] = {
                        'price': nearest_resistance['price'],
                        'change_pct': change_pct
                    }
        
        # Trade setup
        if nearest_support and nearest_resistance and bias == "BULLISH":
            entry = nearest_support['price'] + 3  # Enter above support
            stop = nearest_support['price'] - 5
            target = nearest_resistance['price']
            risk = entry - stop
            reward = target - entry
            
            # Only show if setup makes sense (positive reward)
            if reward > 0 and risk > 0:
                rr_ratio = reward / risk
                
                output_lines.append(f"\n✅ LONG SETUP:")
                output_lines.append(f"   Entry: Above ₹{entry:.2f} (above {nearest_support['price']:.2f} support)")
                output_lines.append(f"   Stop: ₹{stop:.2f} (Risk: {risk:.2f} points)")
                output_lines.append(f"   Target: ₹{target:.2f} (Reward: {reward:.2f} points)")
                output_lines.append(f"   Risk/Reward: 1:{rr_ratio:.1f}")
                
                insights_data['trade_setup'] = {
                    'direction': 'LONG',
                    'entry': entry,
                    'stop': stop,
                    'target': target,
                    'risk': risk,
                    'reward': reward,
                    'rr_ratio': rr_ratio
                }
        
        elif nearest_support and nearest_resistance and bias == "BEARISH":
            entry = nearest_resistance['price'] - 3  # Enter below resistance
            stop = nearest_resistance['price'] + 5
            target = nearest_support['price']
            risk = stop - entry
            reward = entry - target
            
            # Only show if setup makes sense
            if reward > 0 and risk > 0:
                rr_ratio = reward / risk
                
                output_lines.append(f"\n✅ SHORT SETUP:")
                output_lines.append(f"   Entry: Below ₹{entry:.2f} (below {nearest_resistance['price']:.2f} resistance)")
                output_lines.append(f"   Stop: ₹{stop:.2f} (Risk: {risk:.2f} points)")
                output_lines.append(f"   Target: ₹{target:.2f} (Reward: {reward:.2f} points)")
                output_lines.append(f"   Risk/Reward: 1:{rr_ratio:.1f}")
                
                insights_data['trade_setup'] = {
                    'direction': 'SHORT',
                    'entry': entry,
                    'stop': stop,
                    'target': target,
                    'risk': risk,
                    'reward': reward,
                    'rr_ratio': rr_ratio
                }
    
    # Print output (for CLI usage)
    output_text = '\n'.join(output_lines)
    print(output_text)
    
    return output_text, insights_data

def get_analysis_data():
    """
    Get structured analysis data for programmatic use (e.g., Slack bot)
    Returns dict with:
        - strongest_levels: List of strongest bid/ask levels with full details
        - key_insights: Dict with price action, bias, trade setup
        - key_insights_text: Formatted text of KEY INSIGHTS section
    """
    conn = get_db_connection()
    
    try:
        min_time, max_time = get_time_range(conn)
        
        if not min_time or not max_time:
            return None
        
        # Get current price
        current_price, best_bid, best_ask = get_current_price(conn)
        
        # Get timestamp of current price
        cur = conn.cursor()
        cur.execute("SELECT MAX(time) FROM depth_levels_200 WHERE time::date = CURRENT_DATE")
        latest_time = cur.fetchone()[0]
        cur.close()
        
        # Track level evolution
        level_history, persistent_levels = track_level_evolution(conn, sample_interval_seconds=10, current_price=current_price)
        
        if not persistent_levels:
            return None
        
        # Get strongest levels (starred in output)
        bid_levels = [l for l in persistent_levels if l['side'] == 'bid']
        ask_levels = [l for l in persistent_levels if l['side'] == 'ask']
        strongest_support = bid_levels[0] if bid_levels else None
        strongest_resistance = ask_levels[0] if ask_levels else None
        
        strongest_levels = []
        if strongest_support:
            strongest_levels.append(strongest_support)
        if strongest_resistance:
            strongest_levels.append(strongest_resistance)
        
        # Generate key insights
        insights_text, insights_data = generate_key_insights(conn, current_price, persistent_levels, latest_time)
        
        return {
            'current_price': current_price,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'latest_time': latest_time,
            'strongest_levels': strongest_levels,
            'persistent_levels': persistent_levels[:15],  # Top 15
            'key_insights': insights_data,
            'key_insights_text': insights_text
        }
        
    finally:
        conn.close()

def main():
    conn = get_db_connection()
    
    try:
        min_time, max_time = get_time_range(conn)
        
        if not min_time or not max_time:
            print("No data found for today")
            return
        
        # Get current price
        current_price, best_bid, best_ask = get_current_price(conn)
        
        # Get timestamp of current price
        cur = conn.cursor()
        cur.execute("SELECT MAX(time) FROM depth_levels_200 WHERE time::date = CURRENT_DATE")
        latest_time = cur.fetchone()[0]
        cur.close()
        
        # Load previous state
        previous_state = load_previous_state()
        
        duration = (max_time - min_time).total_seconds() / 60
        now = datetime.now()
        
        print(f"\n{'='*100}")
        print(f"MARKET DEPTH ANALYSIS - {now.strftime('%Y-%m-%d %H:%M:%S')} IST")
        print(f"{'='*100}")
        
        if current_price and latest_time:
            # Convert UTC to IST for display
            ist_time = latest_time + timedelta(hours=5, minutes=30)
            print(f"CURRENT PRICE: ₹{current_price:.2f} (Bid: ₹{best_bid:.2f} | Ask: ₹{best_ask:.2f})")
            print(f"as of {ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST ({latest_time.strftime('%H:%M:%S')} UTC)")
            if previous_state and previous_state.get('price'):
                prev_price = previous_state['price']
                price_change = current_price - prev_price
                print(f"CHANGE: {price_change:+.2f} from last analysis (₹{prev_price:.2f})")
        
        print(f"Period: {min_time.strftime('%Y-%m-%d %H:%M:%S')} to {max_time.strftime('%H:%M:%S')} UTC")
        print(f"Duration: {duration:.1f} minutes")
        print(f"{'='*100}")
        
        # 1. Show recent order flow
        analyze_order_flow_snapshots(conn, num_snapshots=50)
        
        # 2. Track level evolution (sample every 10 seconds)
        level_history, persistent_levels = track_level_evolution(conn, sample_interval_seconds=10, current_price=current_price)
        
        # 3. Show average depth profile
        analyze_depth_profile(conn)
        
        # 4. Analyze changes from previous run
        if persistent_levels:
            analyze_changes(persistent_levels, previous_state, current_price)
        
        # 5. Generate key insights
        if persistent_levels:
            insights_text, insights_data = generate_key_insights(conn, current_price, persistent_levels, latest_time)
        
        # Save current state for next run
        if persistent_levels and current_price:
            current_state = {
                'timestamp': now.isoformat(),
                'price': current_price,
                'levels': {
                    f"{l['side']}_{l['price']:.2f}": {
                        'price': l['price'],
                        'side': l['side'],
                        'avg_orders': l['avg_orders'],
                        'max_orders': l['max_orders']
                    } for l in persistent_levels
                }
            }
            save_current_state(current_state)
        
        print(f"\n{'='*100}")
        print("✓ Analysis complete")
        print(f"{'='*100}\n")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
