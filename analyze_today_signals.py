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


def find_pressure_threshold_crossings(signals):
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


def validate_threshold_signals(signals):
    """Validate if price actually reverses at threshold crossings (2 minutes later)"""
    crossings = find_pressure_threshold_crossings(signals)
    
    if not crossings:
        return []
    
    # Get price data after each crossing
    signal_list = list(signals)
    validated = []
    
    for i, crossing in enumerate(crossings):
        crossing_time = crossing['time']
        crossing_price = crossing['price']
        
        # Find the signal in the full list
        signal_idx = None
        for j, sig in enumerate(signal_list):
            if sig['time'].astimezone(IST) == crossing_time:
                signal_idx = j
                break
        
        if signal_idx is None or signal_idx >= len(signal_list) - 12:
            continue
        
        # Get prices for next 12 signals (~120 seconds = 2 minutes forward)
        future_prices = [float(signal_list[signal_idx + k]['current_price']) 
                        for k in range(1, min(13, len(signal_list) - signal_idx))]
        
        if not future_prices:
            continue
        
        max_future = max(future_prices)
        min_future = min(future_prices)
        final_future = future_prices[-1]
        
        # Determine if signal was correct
        if crossing['type'] == 'bearish_breakout':
            # Bearish signal should have price go down
            signal_worked = final_future < crossing_price
            move = final_future - crossing_price
        else:  # bullish_breakout
            # Bullish signal should have price go up
            signal_worked = final_future > crossing_price
            move = final_future - crossing_price
        
        validated.append({
            **crossing,
            'price_low_after': min_future,
            'price_high_after': max_future,
            'price_final': final_future,
            'price_move_after': move,
            'signal_correct': signal_worked,
            'signal_idx': signal_idx
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
    
    # PRESSURE THRESHOLD CROSSINGS
    print("="*80)
    print("🎯 PRESSURE THRESHOLD CROSSINGS (30s pressure ±0.3)")
    print("="*80)
    
    validated_crossings = validate_threshold_signals(signals)
    
    if validated_crossings:
        print(f"\nDetected {len(validated_crossings)} threshold crossings (with validation):\n")
        print(f"{'Time':<10} {'Type':<20} {'Price Level':<14} {'Price After 30s':<16} {'Result':<12}")
        print("-"*75)
        
        for crossing in validated_crossings:
            icon = "🟢" if crossing['type'] == 'bullish_breakout' else "🔴"
            crossing_type = crossing['direction']
            result_icon = "✅" if crossing['signal_correct'] else "❌"
            result = f"{result_icon} {crossing['price_move_after']:+.2f}"
            
            print(f"{crossing['time'].strftime('%H:%M:%S'):<10} {icon} {crossing_type:<18} ₹{crossing['price']:<13.2f} ₹{crossing['price_final']:<15.2f} {result:<12}")
        
        # Group by type and validate
        bullish_crossings = [c for c in validated_crossings if c['type'] == 'bullish_breakout']
        bearish_crossings = [c for c in validated_crossings if c['type'] == 'bearish_breakout']
        
        print(f"\n📊 Signal Validation Summary:")
        
        if bullish_crossings:
            bullish_correct = sum(1 for c in bullish_crossings if c['signal_correct'])
            bullish_accuracy = (bullish_correct / len(bullish_crossings)) * 100 if bullish_crossings else 0
            print(f"   🟢 Bullish Signals: {len(bullish_crossings)} total, {bullish_correct} correct ({bullish_accuracy:.0f}% accuracy)")
            print(f"      Average price move after signal: {sum(c['price_move_after'] for c in bullish_crossings) / len(bullish_crossings):+.2f}")
        
        if bearish_crossings:
            bearish_correct = sum(1 for c in bearish_crossings if c['signal_correct'])
            bearish_accuracy = (bearish_correct / len(bearish_crossings)) * 100 if bearish_crossings else 0
            print(f"   🔴 Bearish Signals: {len(bearish_crossings)} total, {bearish_correct} correct ({bearish_accuracy:.0f}% accuracy)")
            print(f"      Average price move after signal: {sum(c['price_move_after'] for c in bearish_crossings) / len(bearish_crossings):+.2f}")
        
        # Level ranges
        print(f"\n📍 Level Ranges:")
        if bullish_crossings:
            prices = [c['price'] for c in bullish_crossings]
            print(f"   🟢 Bullish Prices: {', '.join([f'₹{p:.2f}' for p in sorted(prices)])}")
        
        if bearish_crossings:
            prices = [c['price'] for c in bearish_crossings]
            print(f"   🔴 Bearish Prices: {', '.join([f'₹{p:.2f}' for p in sorted(prices)])}")
    else:
        print("\n   No threshold crossings detected")
    
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
