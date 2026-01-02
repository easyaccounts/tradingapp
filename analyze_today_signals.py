#!/usr/bin/env python3
"""
Analyze Today's Depth Signals
Provides insights on level evolution, breakouts, and market behavior
"""

import os
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from collections import defaultdict
import pytz
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '6432')),
    'database': os.getenv('DB_NAME', 'tradingdb'),
    'user': os.getenv('DB_USER', 'tradinguser'),
    'password': os.getenv('DB_PASSWORD')
}

IST = pytz.timezone('Asia/Kolkata')


def get_today_signals():
    """Fetch today's depth signals"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get today's data (IST)
    query = """
    SELECT 
        time,
        current_price,
        key_levels,
        absorptions,
        pressure_30s,
        pressure_60s,
        pressure_120s,
        market_state
    FROM depth_signals
    WHERE time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata')
    ORDER BY time ASC
    """
    
    cursor.execute(query)
    data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return data


def analyze_level_lifecycle(signals):
    """Track how individual levels evolved over time"""
    level_history = defaultdict(list)
    
    for record in signals:
        time = record['time'].astimezone(IST)
        price = float(record['current_price'])
        levels = record['key_levels'] if record['key_levels'] else []
        
        for level in levels:
            key = (level['price'], level['side'])
            level_history[key].append({
                'time': time,
                'current_price': price,
                'tests': level['tests'],
                'strength': level['strength'],
                'age_seconds': level['age_seconds'],
                'distance': level['distance']
            })
    
    return level_history


def find_breakouts(level_history, signals):
    """Identify levels that broke vs held"""
    breakouts = []
    held_levels = []
    
    for (price, side), history in level_history.items():
        if len(history) < 3:
            continue
        
        first_record = history[0]
        last_record = history[-1]
        max_tests = max(h['tests'] for h in history)
        avg_strength = sum(h['strength'] for h in history) / len(history)
        
        # Check if level broke
        if side == 'resistance':
            broke = any(h['current_price'] > price + 5 for h in history[len(history)//2:])
        else:  # support
            broke = any(h['current_price'] < price - 5 for h in history[len(history)//2:])
        
        level_data = {
            'price': price,
            'side': side,
            'first_seen': first_record['time'],
            'last_seen': last_record['time'],
            'duration_minutes': (last_record['time'] - first_record['time']).total_seconds() / 60,
            'max_tests': max_tests,
            'avg_strength': round(avg_strength, 1),
            'broke': broke
        }
        
        if broke:
            breakouts.append(level_data)
        elif max_tests >= 10:  # Only include held levels that were significantly tested
            held_levels.append(level_data)
    
    return breakouts, held_levels


def analyze_market_modes(signals):
    """Identify breakout vs mean reversion periods"""
    modes = []
    current_mode = None
    mode_start = None
    price_start = None
    price_high = None
    price_low = None
    
    for i, record in enumerate(signals):
        time = record['time'].astimezone(IST)
        price = float(record['current_price'])
        levels = record['key_levels'] if record['key_levels'] else []
        
        # Count fresh levels (age < 2 min) vs stale levels (tests > 10)
        fresh_levels = sum(1 for l in levels if l['age_seconds'] < 120)
        stale_levels = sum(1 for l in levels if l['tests'] > 10)
        
        # Determine mode
        if fresh_levels >= 2 and i > 0:
            mode = 'breakout'
        elif stale_levels >= 2:
            mode = 'range'
        else:
            mode = 'neutral'
        
        # Track mode changes
        if mode != current_mode:
            if current_mode is not None:
                modes.append({
                    'mode': current_mode,
                    'start_time': mode_start,
                    'end_time': time,
                    'duration_minutes': (time - mode_start).total_seconds() / 60,
                    'price_start': price_start,
                    'price_end': price,
                    'price_high': price_high,
                    'price_low': price_low,
                    'price_change': round(price - price_start, 2),
                    'price_range': round(price_high - price_low, 2)
                })
            
            current_mode = mode
            mode_start = time
            price_start = price
            price_high = price
            price_low = price
        else:
            # Update range
            price_high = max(price_high, price)
            price_low = min(price_low, price)
    
    return modes


def analyze_pressure_shifts(signals):
    """Track significant pressure changes"""
    shifts = []
    prev_state = None
    
    for record in signals:
        time = record['time'].astimezone(IST)
        price = float(record['current_price'])
        state = record['market_state']
        pressure = record['pressure_60s']
        
        if state != prev_state and prev_state is not None:
            shifts.append({
                'time': time,
                'price': price,
                'from_state': prev_state,
                'to_state': state,
                'pressure_60s': float(pressure) if pressure else 0
            })
        
        prev_state = state
    
    return shifts


def print_report(signals):
    """Generate pressure analysis report"""
    if not signals:
        print("❌ No signals found for today")
        return
    
    print("="*80)
    print(f"📊 PRESSURE ANALYSIS - {datetime.now(IST).strftime('%Y-%m-%d')}")
    print("="*80)
    
    # Basic stats
    first_signal = signals[0]
    last_signal = signals[-1]
    start_time = first_signal['time'].astimezone(IST)
    end_time = last_signal['time'].astimezone(IST)
    start_price = float(first_signal['current_price'])
    end_price = float(last_signal['current_price'])
    
    print(f"\n⏰ Session: {start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}")
    print(f"💰 Price: ₹{start_price:.2f} → ₹{end_price:.2f} ({end_price - start_price:+.2f})")
    print(f"📈 Total signals: {len(signals)}\n")
    
    # Pressure shifts
    print("="*80)
    print("⚡ PRESSURE CHANGES OVER TIME")
    print("="*80)
    
    shifts = analyze_pressure_shifts(signals)
    
    if shifts:
        print(f"\nDetected {len(shifts)} major pressure shift(s):\n")
        for i, shift in enumerate(shifts, 1):
            arrow = "🟢" if shift['to_state'] == 'bullish' else "🔴" if shift['to_state'] == 'bearish' else "⚪"
            print(f"{i}. {arrow} {shift['from_state'].upper()} → {shift['to_state'].upper()}")
            print(f"   Time: {shift['time'].strftime('%H:%M:%S')}")
            print(f"   Price: ₹{shift['price']:.2f}")
            print(f"   Pressure (60s): {shift['pressure_60s']:+.3f}")
    else:
        print("\n   No significant pressure shifts detected - market stayed in one state")
    
    # Detailed pressure timeline
    print("\n" + "="*80)
    print("📈 PRESSURE TIMELINE (every 30 seconds)")
    print("="*80 + "\n")
    
    # Sample every 30 seconds
    sampled_signals = signals[::3] if len(signals) > 30 else signals
    
    print(f"{'Time':<10} {'Price':<10} {'Pressure 30s':<15} {'Pressure 60s':<15} {'Pressure 120s':<15} {'State':<12}")
    print("-"*80)
    
    for record in sampled_signals:
        time = record['time'].astimezone(IST).strftime('%H:%M:%S')
        price = float(record['current_price'])
        p30 = float(record['pressure_30s']) if record['pressure_30s'] else 0
        p60 = float(record['pressure_60s']) if record['pressure_60s'] else 0
        p120 = float(record['pressure_120s']) if record['pressure_120s'] else 0
        state = record['market_state']
        
        p30_str = f"{p30:+.3f}"
        p60_str = f"{p60:+.3f}"
        p120_str = f"{p120:+.3f}"
        
        print(f"{time:<10} ₹{price:<9.2f} {p30_str:<15} {p60_str:<15} {p120_str:<15} {state:<12}")
    
    # Summary statistics
    print("\n" + "="*80)
    print("📊 PRESSURE STATISTICS")
    print("="*80)
    
    pressure_60s_values = [float(s['pressure_60s']) for s in signals if s['pressure_60s'] is not None]
    
    if pressure_60s_values:
        avg_pressure = sum(pressure_60s_values) / len(pressure_60s_values)
        max_pressure = max(pressure_60s_values)
        min_pressure = min(pressure_60s_values)
        
        bullish_count = sum(1 for s in signals if s['market_state'] == 'bullish')
        bearish_count = sum(1 for s in signals if s['market_state'] == 'bearish')
        neutral_count = sum(1 for s in signals if s['market_state'] == 'neutral')
        
        print(f"\nPressure (60s window):")
        print(f"  Average: {avg_pressure:+.3f}")
        print(f"  Max (Most Bullish): {max_pressure:+.3f}")
        print(f"  Min (Most Bearish): {min_pressure:+.3f}")
        print(f"  Range: {max_pressure - min_pressure:.3f}")
        
        print(f"\nMarket State Distribution:")
        print(f"  🟢 Bullish: {bullish_count} samples ({100*bullish_count/len(signals):.1f}%)")
        print(f"  🔴 Bearish: {bearish_count} samples ({100*bearish_count/len(signals):.1f}%)")
        print(f"  ⚪ Neutral: {neutral_count} samples ({100*neutral_count/len(signals):.1f}%)")
        
        # Determine overall market bias
        if bullish_count > bearish_count:
            bias = "BULLISH"
            icon = "🟢"
        elif bearish_count > bullish_count:
            bias = "BEARISH"
            icon = "🔴"
        else:
            bias = "NEUTRAL"
            icon = "⚪"
        
        print(f"\n{icon} Overall Bias: {bias}")
    
    print("\n" + "="*80)


def main():
    print("Fetching today's depth signals...\n")
    
    try:
        signals = get_today_signals()
        print_report(signals)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
