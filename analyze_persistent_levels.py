"""
Analyze persistent key levels from today's market depth data
Shows how strong levels evolved and how price interacted with them
"""
import os
import psycopg2
import json
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


def load_todays_signals():
    """Load all depth signals from today"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    today = date.today()
    query = """
        SELECT time, current_price, key_levels, absorptions, 
               pressure_60s, market_state
        FROM depth_signals
        WHERE DATE(time) = %s
        ORDER BY time ASC
    """
    
    cursor.execute(query, (today,))
    rows = cursor.fetchall()
    
    signals = []
    for row in rows:
        signals.append({
            'time': row[0],
            'price': float(row[1]),
            'key_levels': row[2] if row[2] else [],
            'absorptions': row[3] if row[3] else [],
            'pressure_60s': float(row[4]) if row[4] else 0,
            'market_state': row[5]
        })
    
    cursor.close()
    conn.close()
    
    return signals


def group_levels(all_levels, tolerance=2):
    """Group similar price levels within tolerance range"""
    grouped = defaultdict(list)
    
    for level_data in all_levels:
        level_price = level_data['price']
        matched = False
        
        # Check if this level matches any existing group
        for group_price in list(grouped.keys()):
            if abs(level_price - group_price) <= tolerance:
                grouped[group_price].append(level_data)
                matched = True
                break
        
        if not matched:
            grouped[level_price].append(level_data)
    
    return grouped


def analyze_level_persistence(grouped_levels):
    """Calculate persistence metrics for each grouped level"""
    results = []
    
    for base_price, occurrences in grouped_levels.items():
        if not occurrences:
            continue
        
        # Time metrics
        first_seen = min(o['time'] for o in occurrences)
        last_seen = max(o['time'] for o in occurrences)
        duration_seconds = (last_seen - first_seen).total_seconds()
        duration_minutes = duration_seconds / 60
        
        # Strength metrics
        strengths = [o['strength'] for o in occurrences]
        avg_strength = sum(strengths) / len(strengths)
        max_strength = max(strengths)
        
        # Order metrics
        orders = [o['orders'] for o in occurrences]
        avg_orders = sum(orders) / len(orders)
        max_orders = max(orders)
        
        # Side (support/resistance)
        sides = [o['side'] for o in occurrences]
        side = max(set(sides), key=sides.count)  # Most common
        
        # Tests (how many times it appeared)
        tests = len(occurrences)
        
        results.append({
            'price': base_price,
            'side': side,
            'first_seen': first_seen,
            'last_seen': last_seen,
            'duration_min': duration_minutes,
            'tests': tests,
            'avg_strength': avg_strength,
            'max_strength': max_strength,
            'avg_orders': avg_orders,
            'max_orders': max_orders
        })
    
    # Sort by duration (most persistent first)
    results.sort(key=lambda x: x['duration_min'], reverse=True)
    return results


def analyze_price_action(signals, level_price, tolerance=2):
    """Analyze how price interacted with a specific level"""
    touches = 0
    breakouts = []
    rejections = []
    
    for i, signal in enumerate(signals):
        price = signal['price']
        
        # Check if price is near level
        if abs(price - level_price) <= tolerance:
            touches += 1
            
            # Look ahead to see if breakout or rejection
            if i < len(signals) - 5:  # Need at least 5 more candles
                future_prices = [s['price'] for s in signals[i+1:i+6]]
                avg_future = sum(future_prices) / len(future_prices)
                
                if avg_future > level_price + tolerance:
                    breakouts.append(('UPSIDE', signal['time']))
                elif avg_future < level_price - tolerance:
                    breakouts.append(('DOWNSIDE', signal['time']))
                else:
                    rejections.append(signal['time'])
    
    return {
        'touches': touches,
        'breakouts': breakouts,
        'rejections': len(rejections)
    }


def main():
    print("=" * 100)
    print(" " * 30 + "PERSISTENT LEVEL ANALYSIS - TODAY")
    print("=" * 100)
    
    # Load data
    print("\nLoading today's depth signals...")
    signals = load_todays_signals()
    
    if not signals:
        print("✗ No data found for today. Is signal-generator running?")
        return
    
    print(f"✓ Loaded {len(signals)} snapshots")
    print(f"  Time range: {signals[0]['time'].strftime('%H:%M:%S')} to {signals[-1]['time'].strftime('%H:%M:%S')}")
    print(f"  Price range: ₹{min(s['price'] for s in signals):.2f} to ₹{max(s['price'] for s in signals):.2f}")
    
    # Extract all key levels with timestamps
    print("\nExtracting key levels...")
    all_levels = []
    for signal in signals:
        for level in signal['key_levels']:
            all_levels.append({
                'time': signal['time'],
                'price': float(level['price']),
                'side': level['side'],
                'strength': float(level['strength']),
                'orders': int(level['orders'])
            })
    
    print(f"✓ Found {len(all_levels)} level observations")
    
    # Group similar levels
    print("\nGrouping levels (±2 point tolerance)...")
    grouped = group_levels(all_levels, tolerance=2)
    print(f"✓ Identified {len(grouped)} distinct price levels")
    
    # Analyze persistence
    print("\nAnalyzing persistence metrics...")
    persistent_levels = analyze_level_persistence(grouped)
    
    # Display results
    print("\n" + "=" * 100)
    print("TOP PERSISTENT LEVELS (Sorted by Duration)")
    print("=" * 100)
    
    current_price = signals[-1]['price']
    
    for i, level in enumerate(persistent_levels[:15], 1):  # Top 15
        side_emoji = "🟢" if level['side'] == 'support' else "🔴"
        distance = level['price'] - current_price
        distance_str = f"{distance:+.0f}" if distance != 0 else "CURRENT"
        
        print(f"\n{i}. {side_emoji} ₹{level['price']:.2f} ({level['side'].upper()}) [{distance_str} pts from CMP]")
        print(f"   Duration: {level['duration_min']:.0f} minutes ({level['first_seen'].strftime('%H:%M')} → {level['last_seen'].strftime('%H:%M')})")
        print(f"   Tests: {level['tests']} | Avg Strength: {level['avg_strength']:.1f}x | Max Strength: {level['max_strength']:.1f}x")
        print(f"   Orders: {level['avg_orders']:.0f} avg, {level['max_orders']} max")
        
        # Analyze price action
        action = analyze_price_action(signals, level['price'], tolerance=2)
        if action['breakouts']:
            for direction, time in action['breakouts']:
                print(f"   ⚡ BREAKOUT {direction} at {time.strftime('%H:%M:%S')}")
        if action['rejections'] > 0:
            print(f"   🔄 {action['rejections']} rejections (level held)")
    
    # Current market state
    print("\n" + "=" * 100)
    print("CURRENT MARKET STATE")
    print("=" * 100)
    
    latest = signals[-1]
    print(f"\nTime: {latest['time'].strftime('%H:%M:%S')}")
    print(f"Price: ₹{latest['price']:.2f}")
    print(f"Pressure (60s): {latest['pressure_60s']:+.3f}")
    print(f"State: {latest['market_state'].upper()}")
    
    # Find nearest levels
    above_levels = [l for l in persistent_levels if l['price'] > current_price and l['duration_min'] > 5]
    below_levels = [l for l in persistent_levels if l['price'] < current_price and l['duration_min'] > 5]
    
    if above_levels:
        nearest_resistance = min(above_levels, key=lambda x: x['price'])
        print(f"\n🔴 Nearest Resistance: ₹{nearest_resistance['price']:.2f} (+{nearest_resistance['price'] - current_price:.0f} pts)")
        print(f"   Duration: {nearest_resistance['duration_min']:.0f} min | Strength: {nearest_resistance['max_strength']:.1f}x")
    
    if below_levels:
        nearest_support = max(below_levels, key=lambda x: x['price'])
        print(f"🟢 Nearest Support: ₹{nearest_support['price']:.2f} ({nearest_support['price'] - current_price:.0f} pts)")
        print(f"   Duration: {nearest_support['duration_min']:.0f} min | Strength: {nearest_support['max_strength']:.1f}x")
    
    # Price movement summary
    print("\n" + "=" * 100)
    print("PRICE MOVEMENT SUMMARY")
    print("=" * 100)
    
    start_price = signals[0]['price']
    end_price = signals[-1]['price']
    change = end_price - start_price
    change_pct = (change / start_price) * 100
    
    high_price = max(s['price'] for s in signals)
    low_price = min(s['price'] for s in signals)
    range_price = high_price - low_price
    
    print(f"\nOpen: ₹{start_price:.2f}")
    print(f"Current: ₹{end_price:.2f}")
    print(f"Change: ₹{change:+.2f} ({change_pct:+.2f}%)")
    print(f"High: ₹{high_price:.2f}")
    print(f"Low: ₹{low_price:.2f}")
    print(f"Range: ₹{range_price:.2f} points")
    
    # Pressure summary
    bullish_periods = sum(1 for s in signals if s['pressure_60s'] > 0.2)
    bearish_periods = sum(1 for s in signals if s['pressure_60s'] < -0.2)
    neutral_periods = len(signals) - bullish_periods - bearish_periods
    
    print(f"\nPressure Distribution:")
    print(f"  Bullish (>0.2): {bullish_periods} snapshots ({100*bullish_periods/len(signals):.1f}%)")
    print(f"  Bearish (<-0.2): {bearish_periods} snapshots ({100*bearish_periods/len(signals):.1f}%)")
    print(f"  Neutral: {neutral_periods} snapshots ({100*neutral_periods/len(signals):.1f}%)")
    
    print("\n" + "=" * 100)
    print("Analysis Complete!")
    print("=" * 100)


if __name__ == '__main__':
    main()
