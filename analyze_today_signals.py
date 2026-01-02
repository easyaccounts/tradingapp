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


def get_state_from_pressure(pressure_60s):
    """Determine market state based on pressure value"""
    if pressure_60s is None:
        return 'neutral'
    p = float(pressure_60s)
    if p > 0.3:
        return 'bullish'
    elif p < -0.3:
        return 'bearish'
    else:
        return 'neutral'


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


def find_high_qty_levels(signals):
    """Find key levels with peak or avg qty > 15k"""
    high_qty_levels = {}
    
    for record in signals:
        time = record['time'].astimezone(IST)
        price = float(record['current_price'])
        levels = record['key_levels'] if record['key_levels'] else []
        
        for level in levels:
            # Check if peak_qty or avg_qty exceeds 15k
            peak_qty = level.get('peak_qty', 0)
            avg_qty = level.get('avg_qty', 0)
            
            if peak_qty > 15000 or avg_qty > 15000:
                level_key = (round(level['price'], 2), level['side'])
                
                if level_key not in high_qty_levels:
                    high_qty_levels[level_key] = {
                        'price': level['price'],
                        'side': level['side'],
                        'peak_qty': peak_qty,
                        'avg_qty': avg_qty,
                        'tests': level['tests'],
                        'strength': level['strength'],
                        'first_seen': time,
                        'distance': level['distance'],
                        'age_seconds': level['age_seconds']
                    }
    
    return high_qty_levels


def validate_level_holds(signals, high_qty_levels):
    """Validate if high-qty levels actually hold as support/resistance"""
    signal_list = list(signals)
    validated = []
    
    for level_key, level_info in high_qty_levels.items():
        level_price = level_info['price']
        side = level_info['side']  # 'bid' or 'ask'
        
        # Find first time price tests this level
        first_test_idx = None
        for i, sig in enumerate(signal_list):
            current_price = float(sig['current_price'])
            
            if side == 'bid':  # Support level - price should not go below
                if current_price <= level_price:
                    first_test_idx = i
                    break
            else:  # Resistance level - price should not go above
                if current_price >= level_price:
                    first_test_idx = i
                    break
        
        if first_test_idx is None or first_test_idx >= len(signal_list) - 12:
            continue
        
        # Check next 12 signals (2 minutes) to see if level held
        test_time = signal_list[first_test_idx]['time'].astimezone(IST)
        test_price = float(signal_list[first_test_idx]['current_price'])
        
        future_prices = [float(signal_list[j]['current_price']) 
                        for j in range(first_test_idx, min(first_test_idx + 13, len(signal_list)))]
        
        max_penetration = 0
        min_price_after = min(future_prices) if future_prices else test_price
        max_price_after = max(future_prices) if future_prices else test_price
        
        # Check if level held
        if side == 'bid':  # Support - check if price bounced up
            min_after_test = min(future_prices[1:]) if len(future_prices) > 1 else test_price
            max_penetration = level_price - min_after_test  # How far below it went
            level_held = min_after_test >= level_price
        else:  # Resistance - check if price bounced down
            max_after_test = max(future_prices[1:]) if len(future_prices) > 1 else test_price
            max_penetration = max_after_test - level_price  # How far above it went
            level_held = max_after_test <= level_price
        
        validated.append({
            **level_info,
            'test_time': test_time,
            'test_price': test_price,
            'price_min_after': min_price_after,
            'price_max_after': max_price_after,
            'penetration_points': max_penetration,
            'level_held': level_held
        })
    
    return validated



    """Find price levels where 30s pressure crosses ±0.3 thresholds"""
    crossings = []
    prev_pressure_30s = None
    
    for record in signals:
        time = record['time'].astimezone(IST)
        price = float(record['current_price'])
        pressure_30s = float(record['pressure_30s']) if record['pressure_30s'] else 0
        
        if prev_pressure_30s is not None:
            # Check for crossing above 0.3 (bullish threshold)
            if prev_pressure_30s <= 0.3 and pressure_30s > 0.3:
                crossings.append({
                    'type': 'bullish_breakout',
                    'time': time,
                    'price': price,
                    'pressure': pressure_30s,
                    'direction': '↑ BULLISH THRESHOLD CROSSED'
                })
            
            # Check for crossing below -0.3 (bearish threshold)
            elif prev_pressure_30s >= -0.3 and pressure_30s < -0.3:
                crossings.append({
                    'type': 'bearish_breakout',
                    'time': time,
                    'price': price,
                    'pressure': pressure_30s,
                    'direction': '↓ BEARISH THRESHOLD CROSSED'
                })
        
        prev_pressure_30s = pressure_30s
    
    return crossings



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


def analyze_pressure_price_correlation(signals):
    """Analyze how price moved with pressure DELTA (rate of change)"""
    pressure_segments = []
    current_segment = None
    prev_pressure = None
    
    for i, record in enumerate(signals):
        time = record['time'].astimezone(IST)
        price = float(record['current_price'])
        pressure = float(record['pressure_60s']) if record['pressure_60s'] else 0
        
        # Calculate pressure delta
        if prev_pressure is not None:
            pressure_delta = pressure - prev_pressure
        else:
            pressure_delta = 0
        
        # Determine delta direction
        if pressure_delta > 0.05:
            delta_state = 'strengthening'  # Pressure increasing (more buying)
        elif pressure_delta < -0.05:
            delta_state = 'weakening'      # Pressure decreasing (less buying)
        else:
            delta_state = 'stable'         # Pressure stable
        
        # Start new segment or continue
        if current_segment is None or delta_state != current_segment['delta_state']:
            if current_segment is not None:
                pressure_segments.append(current_segment)
            
            current_segment = {
                'delta_state': delta_state,
                'start_time': time,
                'start_price': price,
                'start_pressure': pressure,
                'prices': [price],
                'pressures': [pressure],
                'deltas': [pressure_delta],
                'times': [time]
            }
        else:
            current_segment['prices'].append(price)
            current_segment['pressures'].append(pressure)
            current_segment['deltas'].append(pressure_delta)
            current_segment['times'].append(time)
        
        prev_pressure = pressure
    
    # Add final segment
    if current_segment is not None:
        pressure_segments.append(current_segment)
    
    # Calculate metrics for each segment
    for segment in pressure_segments:
        segment['end_time'] = segment['times'][-1]
        segment['end_price'] = segment['prices'][-1]
        segment['end_pressure'] = segment['pressures'][-1]
        segment['duration_sec'] = (segment['end_time'] - segment['start_time']).total_seconds()
        segment['price_move'] = segment['end_price'] - segment['start_price']
        segment['price_change_pct'] = (segment['price_move'] / segment['start_price']) * 100
        segment['avg_pressure'] = sum(segment['pressures']) / len(segment['pressures'])
        segment['avg_delta'] = sum(segment['deltas']) / len(segment['deltas'])
        segment['max_delta'] = max(segment['deltas'])
        segment['min_delta'] = min(segment['deltas'])
        segment['price_high'] = max(segment['prices'])
        segment['price_low'] = min(segment['prices'])
        segment['price_range'] = segment['price_high'] - segment['price_low']
    
    return pressure_segments


def analyze_pressure_shifts(signals):
    """Track significant pressure changes"""
    shifts = []
    prev_state = None
    
    for record in signals:
        time = record['time'].astimezone(IST)
        price = float(record['current_price'])
        pressure = record['pressure_60s']
        state = get_state_from_pressure(pressure)
        
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
    """Generate high-qty level analysis report"""
    if not signals:
        print("❌ No signals found for today")
        return
    
    print("="*90)
    print(f"📊 HIGH-QUANTITY LEVEL ANALYSIS - {datetime.now(IST).strftime('%Y-%m-%d')}")
    print("="*90)
    
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
    
    # FIND HIGH-QTY LEVELS
    print("="*90)
    print("🎯 KEY LEVELS WITH PEAK/AVG QTY > 15k")
    print("="*90)
    
    high_qty_levels = find_high_qty_levels(signals)
    
    if high_qty_levels:
        print(f"\nIdentified {len(high_qty_levels)} high-quantity levels\n")
        
        # Validate if they hold
        validated_levels = validate_level_holds(signals, high_qty_levels)
        
        if validated_levels:
            # Sort by side (bid first) then by price descending
            bid_levels = sorted([l for l in validated_levels if l['side'] == 'bid'], 
                               key=lambda x: x['price'], reverse=True)
            ask_levels = sorted([l for l in validated_levels if l['side'] == 'ask'], 
                               key=lambda x: x['price'])
            
            print(f"{'Level':<10} {'Side':<6} {'Peak Qty':<12} {'Avg Qty':<12} {'Tests':<8} {'Held?':<8} {'Penetration':<14}")
            print("-"*90)
            
            for level in bid_levels + ask_levels:
                icon = "📍" if level['level_held'] else "❌"
                side_icon = "🔵" if level['side'] == 'bid' else "🔴"
                held_text = "✅ YES" if level['level_held'] else f"NO ({level['penetration_points']:.2f}₹)"
                
                print(f"₹{level['price']:<9.2f} {side_icon} Bid   {level['peak_qty']:<11,.0f} {level['avg_qty']:<11,.0f} {level['tests']:<7} {held_text:<13} {level['penetration_points']:<14.2f}")
            
            # Summary statistics
            print("\n📊 Level Hold Statistics:")
            held = [l for l in validated_levels if l['level_held']]
            broken = [l for l in validated_levels if not l['level_held']]
            
            if held:
                hold_rate = (len(held) / len(validated_levels)) * 100
                print(f"   ✅ Held levels: {len(held)} ({hold_rate:.0f}%)")
                print(f"      Avg penetration (when held): {sum(l['penetration_points'] for l in held) / len(held):.2f}₹")
            
            if broken:
                break_rate = (len(broken) / len(validated_levels)) * 100
                print(f"   ❌ Broken levels: {len(broken)} ({break_rate:.0f}%)")
                print(f"      Avg penetration (when broken): {sum(l['penetration_points'] for l in broken) / len(broken):.2f}₹")
            
            # High-confidence levels (held with no penetration)
            clean_holds = [l for l in held if l['penetration_points'] < 2]
            if clean_holds:
                print(f"\n🏆 High-Confidence Levels (clean holds, <2₹ penetration):")
                for level in clean_holds:
                    print(f"   ₹{level['price']:.2f} ({level['side'].upper()}) - Peak Qty: {level['peak_qty']:,.0f}")
        else:
            print("   No levels with valid test data")
    else:
        print("\n   No levels with peak/avg qty > 15k found")
    
    print("\n" + "="*90)



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
