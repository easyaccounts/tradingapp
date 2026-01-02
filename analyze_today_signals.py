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
            # Check if peak_quantity or avg_quantity exceeds 15k
            peak_qty = level.get('peak_quantity', 0)
            avg_qty = level.get('avg_quantity', 0)
            
            if peak_qty > 15000 or avg_qty > 15000:
                level_key = (round(level['price'], 2), level['side'])
                
                if level_key not in high_qty_levels:
                    high_qty_levels[level_key] = {
                        'price': level['price'],
                        'side': level['side'],
                        'peak_qty': peak_qty,
                        'avg_qty': avg_qty,
                        'tests': level.get('tests', 0),
                        'strength': level.get('strength', 0),
                        'first_seen': time,
                        'distance': level.get('distance', 0),
                        'age_seconds': level.get('age_seconds', 0)
                    }
    
    return high_qty_levels


def analyze_level_formation(signals, high_qty_levels):
    """Analyze each level at its formation time and intent over 10 minutes"""
    signal_list = list(signals)
    level_analysis = []
    
    for level_key, level_info in high_qty_levels.items():
        level_price = level_info['price']
        side = level_info['side']
        
        # Find when this level first appeared in the signals
        first_appearance_idx = None
        for i, sig in enumerate(signal_list):
            levels = sig['key_levels'] if sig['key_levels'] else []
            for lvl in levels:
                if abs(lvl['price'] - level_price) < 0.1:  # Same level
                    first_appearance_idx = i
                    break
            if first_appearance_idx is not None:
                break
        
        if first_appearance_idx is None:
            continue
        
        # Get price at formation
        formation_signal = signal_list[first_appearance_idx]
        formation_time = formation_signal['time'].astimezone(IST)
        formation_price = float(formation_signal['current_price'])
        
        # Calculate distance and type
        distance = level_price - formation_price
        
        if abs(distance) <= 5:
            level_type = "AT MARKET"
            aggression = "🔥 AGGRESSIVE"
        elif distance < 0:
            level_type = "BELOW MARKET"
            aggression = "🛡️ SUPPORT"
        else:
            level_type = "ABOVE MARKET"
            aggression = "🛡️ RESISTANCE"
        
        # Check next 60 signals for confirmation (10 minutes)
        future_prices = []
        for j in range(first_appearance_idx + 1, min(first_appearance_idx + 61, len(signal_list))):
            future_prices.append(float(signal_list[j]['current_price']))
        
        if not future_prices:
            continue
        
        final_price = future_prices[-1]
        price_move = final_price - formation_price
        max_price = max(future_prices)
        min_price = min(future_prices)
        
        # Determine if formation intent was confirmed
        if side == 'support':
            # Support at formation time = buyers expecting dip
            intent_confirmed = price_move > 0  # Price went up = dip didn't happen, but bullish
        else:  # resistance
            # Resistance at formation time = sellers expecting continuation
            intent_confirmed = price_move > 0  # Price went up = resistance broken, bullish
        
        level_analysis.append({
            'price': level_price,
            'side': side,
            'peak_qty': level_info['peak_qty'],
            'formation_time': formation_time,
            'formation_price': formation_price,
            'distance_at_formation': distance,
            'level_type': level_type,
            'aggression': aggression,
            'price_move_after_10min': price_move,
            'final_price_after_10min': final_price,
            'price_high_10min': max_price,
            'price_low_10min': min_price,
            'intent_confirmed': intent_confirmed
        })
    
    return level_analysis


def validate_level_holds(signals, high_qty_levels):
    """Analyze if high-qty level breaks result in momentum continuation"""
    signal_list = list(signals)
    validated = []
    
    for level_key, level_info in high_qty_levels.items():
        level_price = level_info['price']
        side = level_info['side']  # 'support' or 'resistance'
        
        # Find when price actually TESTS the level in the expected direction
        # For support: price must go DOWN to or below the level
        # For resistance: price must go UP to or above the level
        first_test_idx = None
        
        for i, sig in enumerate(signal_list):
            current_price = float(sig['current_price'])
            
            if side == 'support':
                # Support must be tested by price going DOWN
                # Only count if this is part of a downward move (price was higher before)
                if i > 0 and current_price <= level_price:
                    prev_price = float(signal_list[i-1]['current_price'])
                    if prev_price > level_price:  # Coming from above
                        first_test_idx = i
                        break
            else:  # resistance
                # Resistance must be tested by price going UP  
                # Only count if this is part of an upward move (price was lower before)
                if i > 0 and current_price >= level_price:
                    prev_price = float(signal_list[i-1]['current_price'])
                    if prev_price < level_price:  # Coming from below
                        first_test_idx = i
                        break
        
        if first_test_idx is None or first_test_idx >= len(signal_list) - 12:
            continue
        
        # Check next 12 signals (2 minutes) to see momentum after break
        test_time = signal_list[first_test_idx]['time'].astimezone(IST)
        test_price = float(signal_list[first_test_idx]['current_price'])
        
        future_prices = [float(signal_list[j]['current_price']) 
                        for j in range(first_test_idx, min(first_test_idx + 13, len(signal_list)))]
        
        min_price_after = min(future_prices) if future_prices else test_price
        max_price_after = max(future_prices) if future_prices else test_price
        
        # Calculate break momentum
        if side == 'support':  # Support broken downward = bearish
            penetration = level_price - min(future_prices[1:]) if len(future_prices) > 1 else 0
            final_price = future_prices[-1] if future_prices else test_price
            momentum_strength = final_price - level_price  # Negative = stronger bearish
            signal_worked = final_price < level_price  # Price stayed below
        else:  # Resistance broken upward = bullish
            penetration = max(future_prices[1:]) - level_price if len(future_prices) > 1 else 0
            final_price = future_prices[-1] if future_prices else test_price
            momentum_strength = final_price - level_price  # Positive = stronger bullish
            signal_worked = final_price > level_price  # Price stayed above
        
        validated.append({
            **level_info,
            'test_time': test_time,
            'test_price': test_price,
            'price_min_after': min_price_after,
            'price_max_after': max_price_after,
            'penetration_points': penetration,
            'momentum_strength': momentum_strength,
            'momentum_confirmed': signal_worked
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
    print("\n" + "="*90)
    print("🎯 KEY LEVELS WITH PEAK/AVG QTY > 15k - MOMENTUM ANALYSIS")
    print("="*90)
    
    high_qty_levels = find_high_qty_levels(signals)
    
    if high_qty_levels:
        print(f"\nIdentified {len(high_qty_levels)} high-quantity levels")
        
        # DEBUG: Show the levels found
        print(f"\nDebug - High Qty Levels Found:")
        for (price, side), level_info in list(high_qty_levels.items())[:3]:
            print(f"  ₹{price} ({side}): peak_qty={level_info['peak_qty']}, avg_qty={level_info['avg_qty']}")
        
        print(f"\n\n" + "="*110)
        print("📋 LEVEL FORMATION ANALYSIS - When Did Each Level Appear & What Was The Intent?")
        print("="*110)
        
        # Analyze formation
        formation_data = analyze_level_formation(signals, high_qty_levels)
        
        if formation_data:
            # Sort by formation time
            formation_data.sort(key=lambda x: x['formation_time'])
            
            print(f"\n{'Formation Time':<16} {'Level':<10} {'Type':<18} {'Distance':<12} {'10Min Move':<12} {'Intent':<12}")
            print("-"*110)
            
            for level in formation_data:
                intent_icon = "✅" if level['intent_confirmed'] else "❌"
                move_icon = "📈" if level['price_move_after_10min'] > 0 else "📉"
                
                print(f"{level['formation_time'].strftime('%H:%M:%S'):<16} ₹{level['price']:<9.2f} {level['level_type']:<18} {level['distance_at_formation']:+7.2f}₹  {move_icon} {level['price_move_after_10min']:+8.2f}₹  {intent_icon} {level['aggression']:<11}")
            
            # Summary by aggression type
            print(f"\n📊 Formation Intent Analysis (10 Minute Horizon):")
            aggressive = [l for l in formation_data if abs(l['distance_at_formation']) <= 5]
            defensive = [l for l in formation_data if abs(l['distance_at_formation']) > 5]
            
            if aggressive:
                print(f"\n🔥 AGGRESSIVE BID/ASK (placed AT current price):")
                for level in aggressive:
                    status = "✅ Worked" if level['intent_confirmed'] else "❌ Failed"
                    print(f"   ₹{level['price']:.2f} ({level['side'].upper()}) - {level['peak_qty']:,} qty - {status}")
                    print(f"      → After 10 min: moved {level['price_move_after_10min']:+.2f}₹ (range ₹{level['price_low_10min']:.2f}-{level['price_high_10min']:.2f})")
            
            if defensive:
                print(f"\n🛡️ DEFENSIVE SUPPORT/RESISTANCE (placed away from price):")
                for level in defensive:
                    status = "✅ Worked" if level['intent_confirmed'] else "❌ Failed"
                    direction = "BELOW" if level['distance_at_formation'] < 0 else "ABOVE"
                    print(f"   ₹{level['price']:.2f} ({level['side'].upper()}) - {direction} market by {abs(level['distance_at_formation']):.2f}₹ - {status}")
                    print(f"      → After 10 min: moved {level['price_move_after_10min']:+.2f}₹ (range ₹{level['price_low_10min']:.2f}-{level['price_high_10min']:.2f})")
        
        print(f"\n\n" + "="*110)
        print("⚡ LEVEL MOMENTUM ANALYSIS - What Happened When Levels Were Tested?")
        print("="*110)
        
        print(f"\nAnalyzing momentum after break...\n")
        
        # Validate if they hold
        validated_levels = validate_level_holds(signals, high_qty_levels)
        
        if validated_levels:
            # Sort by penetration (strongest breaks first)
            validated_levels.sort(key=lambda x: abs(x['penetration_points']), reverse=True)
            
            print(f"{'Level':<12} {'Type':<12} {'Peak Qty':<12} {'Penetration':<14} {'Momentum':<12} {'Confirmed?':<12}")
            print("-"*95)
            
            for level in validated_levels:
                side_text = "SUPPORT↓" if level['side'] == 'support' else "RESIST↑"
                momentum_icon = "📈" if level['momentum_strength'] > 0 else "📉"
                confirmed = "✅ YES" if level['momentum_confirmed'] else "❌ NO"
                
                print(f"₹{level['price']:<11.2f} {side_text:<12} {level['peak_qty']:<11,.0f} {level['penetration_points']:<13.2f}₹ {momentum_icon} {level['momentum_strength']:+.2f}₹  {confirmed:<11}")
            
            # Summary statistics
            print("\n📊 Momentum Confirmation Statistics:")
            confirmed = [l for l in validated_levels if l['momentum_confirmed']]
            failed = [l for l in validated_levels if not l['momentum_confirmed']]
            
            if confirmed:
                confirm_rate = (len(confirmed) / len(validated_levels)) * 100
                print(f"   ✅ Momentum Confirmed: {len(confirmed)} ({confirm_rate:.0f}%)")
                avg_penetration = sum(l['penetration_points'] for l in confirmed) / len(confirmed)
                avg_momentum = sum(l['momentum_strength'] for l in confirmed) / len(confirmed)
                print(f"      Avg penetration: {avg_penetration:.2f}₹")
                print(f"      Avg momentum strength: {avg_momentum:+.2f}₹")
            
            if failed:
                fail_rate = (len(failed) / len(validated_levels)) * 100
                print(f"   ❌ Momentum Failed: {len(failed)} ({fail_rate:.0f}%)")
                avg_penetration = sum(l['penetration_points'] for l in failed) / len(failed)
                avg_momentum = sum(l['momentum_strength'] for l in failed) / len(failed)
                print(f"      Avg penetration: {avg_penetration:.2f}₹")
                print(f"      Avg momentum strength: {avg_momentum:+.2f}₹")
            
            # Strong momentum breaks (penetration > 20 points)
            strong_breaks = [l for l in validated_levels if l['penetration_points'] > 20]
            if strong_breaks:
                print(f"\n🚀 Strong Momentum Breaks (penetration > 20₹):")
                for level in strong_breaks:
                    status = "✅ Continued" if level['momentum_confirmed'] else "❌ Reversed"
                    print(f"   ₹{level['price']:.2f} {level['side'].upper()} - Penetration: {level['penetration_points']:.2f}₹ - {status}")
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
