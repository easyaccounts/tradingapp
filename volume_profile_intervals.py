"""
Volume Profile Analysis by 15-Minute Cumulative Intervals
Shows cumulative volume and delta from market open through each interval
"""
import os
import psycopg2
from datetime import datetime, date, timedelta
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


def load_tick_data(instrument_token, start_date=None, end_date=None):
    """Load tick data with volume delta and aggressor side"""
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


def print_interval_stats(interval_name, ticks, profile):
    """Print statistics for a time interval"""
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


def split_by_intervals(ticks, interval_minutes=15):
    """Split ticks into cumulative time intervals from market start"""
    if not ticks:
        return []
    
    intervals = []
    
    # Start from first tick's time, rounded down to interval
    first_time = ticks[0]['time']
    market_open = first_time.replace(minute=(first_time.minute // interval_minutes) * interval_minutes, second=0, microsecond=0)
    
    # Find all interval boundaries
    interval_boundaries = []
    current_boundary = market_open
    last_tick_time = ticks[-1]['time']
    
    while current_boundary <= last_tick_time:
        current_boundary = current_boundary + timedelta(minutes=interval_minutes)
        interval_boundaries.append(current_boundary)
    
    # Create cumulative intervals - each includes ALL ticks from market open to that boundary
    for boundary in interval_boundaries:
        cumulative_ticks = [tick for tick in ticks if tick['time'] < boundary]
        if cumulative_ticks:
            intervals.append((boundary, cumulative_ticks))
    
    return intervals


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Volume Profile by 15-minute cumulative intervals')
    parser.add_argument('--instrument', type=int, help='Instrument token (default: from .env)')
    parser.add_argument('--date', type=str, help='Date (YYYY-MM-DD, default: today)')
    parser.add_argument('--tick-size', type=float, default=5, help='Price grouping interval (default: 5)')
    parser.add_argument('--symbol', type=str, help='Trading symbol for display')
    parser.add_argument('--interval', type=int, default=15, help='Interval in minutes (default: 15)')
    
    args = parser.parse_args()
    
    # Get instrument token
    if args.instrument:
        instrument_token = args.instrument
        symbol = args.symbol or f"Token {instrument_token}"
    else:
        instrument_token = int(os.getenv('SECURITY_ID', 0))
        symbol = os.getenv('INSTRUMENT_NAME', 'NIFTY FUT')
        if not instrument_token:
            print("Error: Please provide --instrument or set SECURITY_ID in .env")
            return
    
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
    ticks = load_tick_data(instrument_token, target_date, target_date)
    
    if not ticks:
        print("✗ No tick data found for this date")
        return
    
    print(f"✓ Loaded {len(ticks)} ticks")
    
    # Split into intervals
    intervals = split_by_intervals(ticks, args.interval)
    print(f"✓ Split into {len(intervals)} intervals of {args.interval} minutes")
    
    # Analyze each interval (cumulative from market open)
    market_open = ticks[0]['time']
    for interval_end, interval_ticks in intervals:
        interval_name = f"CUMULATIVE: {market_open.strftime('%H:%M')} → {interval_end.strftime('%H:%M')}"
        profile = calculate_volume_profile(interval_ticks, tick_size=args.tick_size)
        print_interval_stats(interval_name, interval_ticks, profile)
    
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
