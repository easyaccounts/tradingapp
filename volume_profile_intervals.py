"""
Volume Profile Analysis by 5-Minute Discrete Intervals
Shows volume and orderbook depth patterns in discrete time windows
Identifies breakthrough moments, contested levels, and walls
"""
import os
import psycopg2
from datetime import datetime, date, timedelta
from collections import defaultdict
from dotenv import load_dotenv
import json

load_dotenv()

# Token mapping: tick feed uses different IDs than depth feed
TICK_TO_DEPTH_MAPPING = {
    12602626: 49229,  # NIFTY JAN 2025 FUT
}

def get_instrument_tokens(tick_token):
    """Get both tick and depth tokens for an instrument"""
    depth_token = TICK_TO_DEPTH_MAPPING.get(tick_token, tick_token)
    return tick_token, depth_token


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 6432)),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD')
    )


def load_tick_data(instrument_token, start_date=None, end_date=None):
    """Load tick data with volume delta and aggressor side
    
    NOTE: Tick timestamps are stored as IST with +00:00 marker (bug in ingestion)
    They appear as 09:00:00+00:00 but actually mean 9:00 AM IST, not 9:00 AM UTC
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if not start_date:
        start_date = date.today()
    if not end_date:
        end_date = start_date
    
    query = """
        SELECT 
            time,
            last_price,
            volume_traded,
            volume_delta,
            aggressor_side
        FROM ticks
        WHERE instrument_token = %s
          AND DATE(time) BETWEEN %s AND %s
          AND last_price IS NOT NULL
          AND volume_delta IS NOT NULL
        ORDER BY time ASC
    """
    
    cursor.execute(query, (instrument_token, start_date, end_date))
    rows = cursor.fetchall()
    
    ticks = []
    for row in rows:
        ticks.append({
            'time': row[0],
            'price': float(row[1]),
            'volume': int(row[2]) if row[2] else 0,
            'volume_delta': int(row[3]) if row[3] else 0,
            'aggressor_side': row[4]
        })
    
    cursor.close()
    conn.close()
    
    return ticks


def calculate_volume_profile(ticks, tick_size=5):
    """Calculate volume profile with delta"""
    profile = defaultdict(lambda: {'buy_volume': 0, 'sell_volume': 0, 'total_volume': 0, 'delta': 0})
    
    for tick in ticks:
        price_level = round(tick['price'] / tick_size) * tick_size
        volume_delta = tick['volume_delta']
        
        if volume_delta == 0:
            continue
        
        if tick['aggressor_side'] == 'BUY':
            profile[price_level]['buy_volume'] += volume_delta
            profile[price_level]['delta'] += volume_delta
        elif tick['aggressor_side'] == 'SELL':
            profile[price_level]['sell_volume'] += volume_delta
            profile[price_level]['delta'] -= volume_delta
        else:  # NEUTRAL
            half_vol = volume_delta / 2
            profile[price_level]['buy_volume'] += half_vol
            profile[price_level]['sell_volume'] += half_vol
        
        profile[price_level]['total_volume'] += volume_delta
    
    return dict(profile)


def load_orderbook_depth(instrument_token, start_time, end_time, tick_size=5):
    """Load cumulative orderbook depth for the interval"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get raw depth data and round prices in Python for proper grouping
    query = """
        SELECT 
            price,
            side,
            quantity
        FROM depth_levels_200
        WHERE security_id = %s
          AND time >= %s
          AND time < %s
    """
    
    cursor.execute(query, (instrument_token, start_time, end_time))
    rows = cursor.fetchall()
    
    # Debug: Print first call info
    if not hasattr(load_orderbook_depth, '_debug_printed'):
        print(f"  [DEBUG] First depth query: security_id={instrument_token}, time={start_time} to {end_time}")
        print(f"  [DEBUG] Raw rows returned: {len(rows)}")
        if len(rows) > 0:
            sample_prices = [float(row[0]) for row in rows[:10]]
            print(f"  [DEBUG] Sample raw prices: {sample_prices}")
            print(f"  [DEBUG] Min/Max depth prices: ₹{min(float(r[0]) for r in rows):.2f} - ₹{max(float(r[0]) for r in rows):.2f}")
        load_orderbook_depth._debug_printed = True
    
    # Group by rounded price
    depth_aggregated = defaultdict(lambda: {'bid_qty': [], 'ask_qty': []})
    
    for row in rows:
        price = float(row[0])
        side = row[1]
        quantity = float(row[2])
        
        # Round to tick_size
        price_level = round(price / tick_size) * tick_size
        
        if side == 'BID':
            depth_aggregated[price_level]['bid_qty'].append(quantity)
        elif side == 'ASK':
            depth_aggregated[price_level]['ask_qty'].append(quantity)
    
    # Calculate averages
    depth = {}
    for price_level, data in depth_aggregated.items():
        bid_depth = sum(data['bid_qty']) / len(data['bid_qty']) if data['bid_qty'] else 0
        ask_depth = sum(data['ask_qty']) / len(data['ask_qty']) if data['ask_qty'] else 0
        depth[price_level] = {
            'bid_depth': bid_depth,
            'ask_depth': ask_depth,
            'snapshots': len(data['bid_qty']) + len(data['ask_qty'])
        }
    
    cursor.close()
    conn.close()
    
    return depth


def load_key_levels(instrument_token, start_time, end_time, threshold=0.3):
    """Load persistent key levels from depth_signals"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT DISTINCT
            time,
            key_levels,
            absorptions
        FROM depth_signals
        WHERE security_id = %s
          AND time >= %s
          AND time < %s
          AND (key_levels IS NOT NULL OR absorptions IS NOT NULL)
        ORDER BY time DESC
        LIMIT 50
    """
    
    cursor.execute(query, (instrument_token, start_time, end_time))
    rows = cursor.fetchall()
    
    # Aggregate key levels with strength and duration
    level_stats = defaultdict(lambda: {'strength': 0, 'count': 0, 'pressure': []})
    absorptions = []
    
    for row in rows:
        key_levels = row[1] if row[1] else []
        absorption_data = row[2] if row[2] else []
        
        # Process key levels
        for level_data in key_levels:
            if isinstance(level_data, dict):
                price = level_data.get('price')
                strength = level_data.get('strength', 1)
                pressure = level_data.get('pressure', 0)
                
                if price and abs(pressure) >= threshold:
                    level_stats[price]['strength'] += strength
                    level_stats[price]['count'] += 1
                    level_stats[price]['pressure'].append(pressure)
        
        # Process absorptions
        for absorption in absorption_data:
            if isinstance(absorption, dict):
                absorptions.append(absorption)
    
    # Calculate average stats
    key_levels = []
    for price, stats in level_stats.items():
        avg_strength = stats['strength'] / stats['count']
        avg_pressure = sum(stats['pressure']) / len(stats['pressure'])
        key_levels.append({
            'price': price,
            'strength': avg_strength,
            'pressure': avg_pressure,
            'occurrences': stats['count']
        })
    
    # Sort by strength
    key_levels.sort(key=lambda x: x['strength'], reverse=True)
    
    cursor.close()
    conn.close()
    
    return key_levels[:10], absorptions  # Top 10 levels


def print_interval_stats(interval_name, ticks, profile, orderbook_depth=None, key_levels=None, absorptions=None):
    """Print statistics for a time interval with orderbook analysis"""
    if not ticks:
        print(f"  No data for this interval")
        return
    
    print(f"\n{'='*80}")
    print(f"{interval_name}")
    print(f"{'='*80}")
    
    # Basic stats
    start_time = ticks[0]['time'].strftime('%H:%M')
    end_time = ticks[-1]['time'].strftime('%H:%M')
    start_price = ticks[0]['price']
    end_price = ticks[-1]['price']
    price_change = end_price - start_price
    price_change_pct = (price_change / start_price) * 100
    
    high_price = max(t['price'] for t in ticks)
    low_price = min(t['price'] for t in ticks)
    
    print(f"\nTime: {start_time} → {end_time}")
    print(f"Ticks: {len(ticks)}")
    print(f"Open: ₹{start_price:.2f} | Close: ₹{end_price:.2f} | Change: ₹{price_change:+.2f} ({price_change_pct:+.2f}%)")
    print(f"High: ₹{high_price:.2f} | Low: ₹{low_price:.2f} | Range: ₹{high_price - low_price:.2f}")
    
    if not profile:
        return
    
    # Volume stats
    prices = sorted(profile.keys())
    total_volumes = [profile[p]['total_volume'] for p in prices]
    buy_volumes = [profile[p]['buy_volume'] for p in prices]
    sell_volumes = [profile[p]['sell_volume'] for p in prices]
    deltas = [profile[p]['delta'] for p in prices]
    
    total_vol = sum(total_volumes)
    total_buy = sum(buy_volumes)
    total_sell = sum(sell_volumes)
    net_delta = sum(deltas)
    
    print(f"\nVolume: {total_vol:,.0f} | Buy: {total_buy:,.0f} | Sell: {total_sell:,.0f} | Net Delta: {net_delta:+,.0f}")
    
    # POC
    poc_idx = total_volumes.index(max(total_volumes))
    poc_price = prices[poc_idx]
    poc_volume = total_volumes[poc_idx]
    
    print(f"POC (Most Traded): ₹{poc_price:.2f} ({poc_volume:,.0f} volume)")
    
    # Top buying/selling levels
    positive_deltas = [(p, profile[p]['delta']) for p in prices if profile[p]['delta'] > 0]
    negative_deltas = [(p, profile[p]['delta']) for p in prices if profile[p]['delta'] < 0]
    
    if positive_deltas:
        positive_deltas.sort(key=lambda x: x[1], reverse=True)
        top_buy = positive_deltas[0]
        print(f"Strongest Buying: ₹{top_buy[0]:.2f} (+{top_buy[1]:,.0f} delta)")
    
    if negative_deltas:
        negative_deltas.sort(key=lambda x: x[1])
        top_sell = negative_deltas[0]
        print(f"Strongest Selling: ₹{top_sell[0]:.2f} ({top_sell[1]:,.0f} delta)")
    
    # Character of the interval
    if net_delta > total_vol * 0.1:
        character = "🟢 STRONG BULLISH"
    elif net_delta > total_vol * 0.03:
        character = "🟢 Bullish"
    elif net_delta < -total_vol * 0.1:
        character = "🔴 STRONG BEARISH"
    elif net_delta < -total_vol * 0.03:
        character = "🔴 Bearish"
    else:
        character = "⚪ Neutral/Balanced"
    
    print(f"\nCharacter: {character}")
    
    # === ORDERBOOK ANALYSIS ===
    if orderbook_depth:
        print(f"\n{'─'*80}")
        print("ORDERBOOK LIQUIDITY ANALYSIS")
        print(f"{'─'*80}")
        
        # Calculate liquidity consumption at key price levels
        consumption_data = []
        for price in prices:
            if price in orderbook_depth:
                traded_vol = profile[price]['total_volume']
                buy_vol = profile[price]['buy_volume']
                sell_vol = profile[price]['sell_volume']
                
                bid_depth = orderbook_depth[price]['bid_depth']
                ask_depth = orderbook_depth[price]['ask_depth']
                total_depth = bid_depth + ask_depth
                
                if total_depth > 0:
                    consumption_pct = (traded_vol / total_depth) * 100
                    
                    # Determine consumption type
                    if buy_vol > sell_vol * 1.5 and ask_depth > 0:
                        consumption_type = "BUY"
                        specific_consumption = (buy_vol / ask_depth) * 100 if ask_depth > 0 else 0
                    elif sell_vol > buy_vol * 1.5 and bid_depth > 0:
                        consumption_type = "SELL"
                        specific_consumption = (sell_vol / bid_depth) * 100 if bid_depth > 0 else 0
                    else:
                        consumption_type = "MIXED"
                        specific_consumption = consumption_pct
                    
                    consumption_data.append({
                        'price': price,
                        'consumption': specific_consumption,
                        'type': consumption_type,
                        'traded_vol': traded_vol,
                        'available_depth': total_depth,
                        'bid_depth': bid_depth,
                        'ask_depth': ask_depth
                    })
        
        # Show top consumption levels
        if consumption_data:
            consumption_data.sort(key=lambda x: x['consumption'], reverse=True)
            
            print("\nTop Liquidity Consumption Levels:")
            for i, data in enumerate(consumption_data[:5], 1):
                emoji = "🔴" if data['type'] == "SELL" else "🟢" if data['type'] == "BUY" else "⚪"
                print(f"  {i}. ₹{data['price']:.2f} - {emoji} {data['consumption']:.1f}% consumed "
                      f"({data['traded_vol']:,.0f} traded / {data['available_depth']:,.0f} available)")
            
            # Find walls (high depth, low consumption)
            walls = [d for d in consumption_data if d['consumption'] < 20 and d['available_depth'] > total_vol * 0.05]
            if walls:
                print("\nLiquidity Walls (High Depth, Low Consumption):")
                for wall in walls[:3]:
                    side = "BID" if wall['bid_depth'] > wall['ask_depth'] else "ASK"
                    depth_val = max(wall['bid_depth'], wall['ask_depth'])
                    emoji = "🟢" if side == "BID" else "🔴"
                    print(f"  {emoji} ₹{wall['price']:.2f} - {side} Wall: {depth_val:,.0f} contracts "
                          f"(only {wall['consumption']:.1f}% consumed)")
    
    # === KEY LEVELS FROM ORDERBOOK ===
    if key_levels:
        print(f"\n{'─'*80}")
        print("PERSISTENT KEY LEVELS (from orderbook)")
        print(f"{'─'*80}")
        
        # Match key levels with POC and price action
        for i, level in enumerate(key_levels[:5], 1):
            price_diff = abs(level['price'] - poc_price)
            current_price_diff = abs(level['price'] - end_price)
            
            # Determine level type
            pressure_type = "🔴 RESISTANCE" if level['pressure'] < 0 else "🟢 SUPPORT"
            
            # Check if near POC or current price
            tags = []
            if price_diff < 10:
                tags.append("MATCHES POC")
            if current_price_diff < 10:
                tags.append("NEAR CURRENT")
            
            tag_str = f" [{', '.join(tags)}]" if tags else ""
            
            print(f"  {i}. ₹{level['price']:.2f} - {pressure_type} "
                  f"(Strength: {level['strength']:.1f}x, Seen: {level['occurrences']}x){tag_str}")
    
    # === ABSORPTIONS ===
    if absorptions:
        print(f"\n{'─'*80}")
        print("ABSORPTION EVENTS")
        print(f"{'─'*80}")
        
        for i, absorption in enumerate(absorptions[:3], 1):
            if isinstance(absorption, dict):
                abs_price = absorption.get('price', 0)
                abs_type = absorption.get('type', 'unknown')
                reduction = absorption.get('reduction_pct', 0)
                
                print(f"  {i}. ₹{abs_price:.2f} - {abs_type} absorption "
                      f"({reduction:.1f}% depth reduction, price broke through)")


def split_by_intervals(ticks, interval_minutes=5):
    """Split ticks into discrete (non-cumulative) time intervals"""
    if not ticks:
        return []
    
    intervals = []
    current_interval = []
    
    # Start from first tick's time, rounded down to interval
    first_time = ticks[0]['time']
    interval_start = first_time.replace(minute=(first_time.minute // interval_minutes) * interval_minutes, second=0, microsecond=0)
    
    for tick in ticks:
        # Calculate which interval this tick belongs to
        tick_interval_start = tick['time'].replace(
            minute=(tick['time'].minute // interval_minutes) * interval_minutes,
            second=0,
            microsecond=0
        )
        
        if tick_interval_start != interval_start:
            # New interval started
            if current_interval:
                intervals.append((interval_start, current_interval))
            current_interval = [tick]
            interval_start = tick_interval_start
        else:
            current_interval.append(tick)
    
    # Don't forget the last interval
    if current_interval:
        intervals.append((interval_start, current_interval))
    
    return intervals


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Volume Profile by 5-minute discrete intervals')
    parser.add_argument('--instrument', type=int, help='Instrument token (default: from .env)')
    parser.add_argument('--date', type=str, help='Date (YYYY-MM-DD, default: today)')
    parser.add_argument('--tick-size', type=float, default=5, help='Price grouping interval (default: 5)')
    parser.add_argument('--symbol', type=str, help='Trading symbol for display')
    parser.add_argument('--interval', type=int, default=5, help='Interval in minutes (default: 5)')
    
    args = parser.parse_args()
    
    # Get instrument tokens (tick feed uses different ID than depth feed)
    if args.instrument:
        tick_token = args.instrument
        symbol = args.symbol or f"Token {tick_token}"
    else:
        # Default to NIFTY tick token
        tick_token = 12602626
        symbol = os.getenv('INSTRUMENT_NAME', 'NIFTY JAN 2025 FUT')
    
    # Get corresponding depth token
    tick_token, depth_token = get_instrument_tokens(tick_token)
    
    # Parse date
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        target_date = date.today()
    
    print("="*80)
    print(f"VOLUME PROFILE BY {args.interval}-MINUTE INTERVALS")
    print("="*80)
    print(f"\nSymbol: {symbol}")
    print(f"Date: {target_date}")
    print(f"Tick Size: {args.tick_size}")
    
    # Load data
    print("\nLoading tick data...")
    print(f"  Tick Token: {tick_token}, Depth Token: {depth_token}")
    ticks = load_tick_data(tick_token, target_date, target_date)
    
    if not ticks:
        print("✗ No tick data found for this date")
        return
    
    print(f"✓ Loaded {len(ticks)} ticks")
    
    # Split into intervals
    intervals = split_by_intervals(ticks, args.interval)
    print(f"✓ Split into {len(intervals)} intervals of {args.interval} minutes\n")
    
    print("Loading orderbook data for all intervals...")
    
    # Analyze each interval and collect data
    interval_data = []
    for interval_start, interval_ticks in intervals:
        if not interval_ticks:
            continue
            
        interval_end = interval_start + timedelta(minutes=args.interval)
        
        # Calculate metrics
        profile = calculate_volume_profile(interval_ticks, tick_size=args.tick_size)
        orderbook_depth = load_orderbook_depth(depth_token, interval_start, interval_end, tick_size=args.tick_size)
        
        # Debug for first interval
        if len(interval_data) == 0:
            depth_prices = sorted(orderbook_depth.keys())
            print(f"  [DEBUG] Profile prices: {sorted(profile.keys())[:10]}")
            print(f"  [DEBUG] Depth prices available ({len(depth_prices)} levels): {depth_prices[:10]} ... {depth_prices[-10:]}")
        
        # Get top volume price level
        if profile:
            prices = sorted(profile.keys())
            total_volumes = [profile[p]['total_volume'] for p in prices]
            poc_idx = total_volumes.index(max(total_volumes))
            poc_price = prices[poc_idx]
            poc_volume = total_volumes[poc_idx]
            net_delta = sum(profile[p]['delta'] for p in prices)
            total_volume = sum(total_volumes)
            
            # Debug POC lookup for first interval
            if len(interval_data) == 0:
                print(f"  [DEBUG] POC price: ₹{poc_price:.2f}")
                print(f"  [DEBUG] POC in depth data: {poc_price in orderbook_depth}")
            
            # Get avg depth at POC
            poc_depth = 0
            if poc_price in orderbook_depth:
                poc_depth = orderbook_depth[poc_price]['bid_depth'] + orderbook_depth[poc_price]['ask_depth']
            
            # Calculate price movement
            start_price = interval_ticks[0]['price']
            end_price = interval_ticks[-1]['price']
            price_change = end_price - start_price
            
            # Classify pattern
            if total_volume > 0 and poc_depth > 0:
                vol_to_depth_ratio = poc_volume / poc_depth
                
                if vol_to_depth_ratio > 5 and poc_depth < total_volume * 0.05:
                    pattern = "🔥 BREAKTHROUGH"
                elif vol_to_depth_ratio > 2 and poc_depth > total_volume * 0.1:
                    pattern = "⚔️  CONTESTED"
                elif vol_to_depth_ratio < 0.5 and poc_depth > total_volume * 0.1:
                    pattern = "🧱 WALL"
                elif poc_depth < total_volume * 0.02:
                    pattern = "💨 THIN"
                else:
                    pattern = "📊 NORMAL"
            else:
                pattern = "📊 NORMAL"
            
            interval_data.append({
                'time': interval_start.strftime('%H:%M'),
                'price_change': price_change,
                'volume': total_volume,
                'delta': net_delta,
                'poc_price': poc_price,
                'poc_volume': poc_volume,
                'poc_depth': poc_depth,
                'ratio': vol_to_depth_ratio if poc_depth > 0 else 0,
                'pattern': pattern
            })
    
    # Display table
    print(f"\n{'='*140}")
    print(f"INTERVAL ANALYSIS TABLE - {args.interval}-MINUTE PERIODS")
    print(f"{'='*140}")
    print(f"{'Time':<8} {'Price Δ':<10} {'Volume':<12} {'Delta':<12} {'POC':<10} {'POC Vol':<12} {'POC Depth':<12} {'Ratio':<8} {'Pattern':<15}")
    print(f"{'-'*140}")
    
    # Calculate outliers based on ratio
    ratios = [d['ratio'] for d in interval_data if d['ratio'] > 0]
    if ratios:
        avg_ratio = sum(ratios) / len(ratios)
        std_ratio = (sum((r - avg_ratio) ** 2 for r in ratios) / len(ratios)) ** 0.5
        outlier_threshold = avg_ratio + (2 * std_ratio)
    else:
        outlier_threshold = 999
    
    for data in interval_data:
        is_outlier = "⚠️ " if data['ratio'] > outlier_threshold else "  "
        print(f"{is_outlier}{data['time']:<6} {data['price_change']:>+8.2f}  {data['volume']:<10,.0f}  {data['delta']:>+10,.0f}  "
              f"₹{data['poc_price']:<8.2f} {data['poc_volume']:<10,.0f}  {data['poc_depth']:<10,.0f}  {data['ratio']:<6.1f}  {data['pattern']:<15}")
    
    print(f"{'-'*140}")
    print(f"\nPattern Legend:")
    print(f"  🔥 BREAKTHROUGH - High volume, low depth (aggressive consumption)")
    print(f"  ⚔️  CONTESTED - High volume, high depth (battle between bulls/bears)")
    print(f"  🧱 WALL - Low volume, high depth (strong barrier, not tested)")
    print(f"  💨 THIN - Very low depth (illiquid/volatile)")
    print(f"  📊 NORMAL - Balanced trading")
    print(f"\n⚠️  = Outlier (consumption ratio > {outlier_threshold:.1f}x)\n")
    
    # Overall summary
    print(f"\n{'='*80}")
    print("FULL DAY SUMMARY")
    print(f"{'='*80}")
    
    full_profile = calculate_volume_profile(ticks, tick_size=args.tick_size)
    
    prices = sorted(full_profile.keys())
    total_volumes = [full_profile[p]['total_volume'] for p in prices]
    buy_volumes = [full_profile[p]['buy_volume'] for p in prices]
    sell_volumes = [full_profile[p]['sell_volume'] for p in prices]
    deltas = [full_profile[p]['delta'] for p in prices]
    
    total_vol = sum(total_volumes)
    total_buy = sum(buy_volumes)
    total_sell = sum(sell_volumes)
    net_delta = sum(deltas)
    
    print(f"\nTotal Volume: {total_vol:,.0f}")
    print(f"Buy Volume: {total_buy:,.0f}")
    print(f"Sell Volume: {total_sell:,.0f}")
    print(f"Net Delta: {net_delta:+,.0f}")
    
    # Day POC
    poc_idx = total_volumes.index(max(total_volumes))
    poc_price = prices[poc_idx]
    print(f"\nDay POC: ₹{poc_price:.2f} ({full_profile[poc_price]['total_volume']:,.0f} volume)")
    
    print("\n" + "="*80)


if __name__ == '__main__':
    main()
