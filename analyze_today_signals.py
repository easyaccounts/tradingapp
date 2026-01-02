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
    """Generate comprehensive report"""
    if not signals:
        print("❌ No signals found for today")
        return
    
    print("="*80)
    print(f"📊 DEPTH SIGNALS ANALYSIS - {datetime.now(IST).strftime('%Y-%m-%d')}")
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
    print(f"📈 Total signals: {len(signals)}")
    
    # Level lifecycle analysis
    print("\n" + "="*80)
    print("🎯 KEY LEVEL ANALYSIS")
    print("="*80)
    
    level_history = analyze_level_lifecycle(signals)
    breakouts, held_levels = find_breakouts(level_history, signals)
    
    print(f"\n✅ BREAKOUTS ({len(breakouts)} levels)")
    print("-" * 80)
    for i, level in enumerate(sorted(breakouts, key=lambda x: x['first_seen']), 1):
        print(f"\n{i}. {level['side'].upper()} @ ₹{level['price']:.2f}")
        print(f"   First seen: {level['first_seen'].strftime('%H:%M:%S')}")
        print(f"   Broke after: {level['duration_minutes']:.1f} minutes")
        print(f"   Max tests: {level['max_tests']}")
        print(f"   Avg strength: {level['avg_strength']}x")
    
    print(f"\n\n🛑 LEVELS THAT HELD ({len(held_levels)} levels)")
    print("-" * 80)
    for i, level in enumerate(sorted(held_levels, key=lambda x: x['max_tests'], reverse=True), 1):
        print(f"\n{i}. {level['side'].upper()} @ ₹{level['price']:.2f}")
        print(f"   Duration: {level['duration_minutes']:.1f} minutes")
        print(f"   Max tests: {level['max_tests']} ← Rejection strength")
        print(f"   Avg strength: {level['avg_strength']}x")
    
    # Market modes
    print("\n" + "="*80)
    print("🔄 MARKET MODE ANALYSIS")
    print("="*80)
    
    modes = analyze_market_modes(signals)
    
    for i, mode_data in enumerate(modes, 1):
        icon = "🚀" if mode_data['mode'] == 'breakout' else "📦" if mode_data['mode'] == 'range' else "⚖️"
        print(f"\n{i}. {icon} {mode_data['mode'].upper()}")
        print(f"   Time: {mode_data['start_time'].strftime('%H:%M')} - {mode_data['end_time'].strftime('%H:%M')} ({mode_data['duration_minutes']:.1f} min)")
        print(f"   Price: ₹{mode_data['price_start']:.2f} → ₹{mode_data['price_end']:.2f} ({mode_data['price_change']:+.2f})")
        print(f"   Range: ₹{mode_data['price_range']:.2f}")
    
    # Pressure shifts
    print("\n" + "="*80)
    print("⚡ PRESSURE SHIFT ANALYSIS")
    print("="*80)
    
    shifts = analyze_pressure_shifts(signals)
    
    if shifts:
        for i, shift in enumerate(shifts, 1):
            arrow = "🟢" if shift['to_state'] == 'bullish' else "🔴" if shift['to_state'] == 'bearish' else "⚪"
            print(f"\n{i}. {arrow} {shift['from_state'].upper()} → {shift['to_state'].upper()}")
            print(f"   Time: {shift['time'].strftime('%H:%M:%S')}")
            print(f"   Price: ₹{shift['price']:.2f}")
            print(f"   Pressure: {shift['pressure_60s']:+.3f}")
    else:
        print("\n   No significant pressure shifts detected")
    
    # Trading insights
    print("\n" + "="*80)
    print("💡 TRADING INSIGHTS")
    print("="*80)
    
    breakout_count = sum(1 for m in modes if m['mode'] == 'breakout')
    range_count = sum(1 for m in modes if m['mode'] == 'range')
    
    if breakout_count > range_count:
        print("\n   📈 Today was TREND DAY - Breakout strategy would work")
        print("   ✅ Trade: Fresh resistances with low tests → Buy breakouts")
        print("   ❌ Avoid: Fading levels or mean reversion")
    else:
        print("\n   📊 Today was RANGE DAY - Mean reversion would work")
        print("   ✅ Trade: Levels with 10+ tests → Fade extremes")
        print("   ❌ Avoid: Chasing breakouts")
    
    if len(breakouts) > 0:
        avg_breakout_time = sum(b['duration_minutes'] for b in breakouts) / len(breakouts)
        print(f"\n   ⏱️  Average breakout time: {avg_breakout_time:.1f} minutes")
        print(f"   💡 Levels typically hold for {avg_breakout_time:.0f} min before breaking")
    
    if len(held_levels) > 0:
        strongest_level = max(held_levels, key=lambda x: x['max_tests'])
        print(f"\n   🏆 Strongest level: {strongest_level['side'].upper()} @ ₹{strongest_level['price']:.2f}")
        print(f"       Rejected {strongest_level['max_tests']} times!")
    
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
