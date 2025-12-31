"""
Volume Profile Chart with Delta from Tick Data
Shows volume distribution by price level with buy/sell delta
"""
import os
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime, date, timedelta
from collections import defaultdict
from dotenv import load_dotenv
import numpy as np

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
            aggressor_side,
            total_buy_quantity,
            total_sell_quantity,
            cvd_change
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
            'aggressor_side': row[4],
            'buy_qty': int(row[5]) if row[5] else 0,
            'sell_qty': int(row[6]) if row[6] else 0,
            'cvd_change': int(row[7]) if row[7] else 0
        })
    
    cursor.close()
    conn.close()
    
    return ticks


def calculate_volume_profile(ticks, tick_size=5):
    """
    Calculate volume profile with delta
    Groups ticks into price levels and aggregates buy/sell volume
    
    tick_size: Price grouping interval (e.g., 5 = group into 5-point buckets)
    """
    profile = defaultdict(lambda: {'buy_volume': 0, 'sell_volume': 0, 'total_volume': 0, 'delta': 0})
    
    for tick in ticks:
        # Round price to nearest tick_size
        price_level = round(tick['price'] / tick_size) * tick_size
        
        volume_delta = tick['volume_delta']
        
        # Skip if no volume change
        if volume_delta == 0:
            continue
        
        # Determine buy/sell based on aggressor side
        if tick['aggressor_side'] == 'BUY':
            profile[price_level]['buy_volume'] += volume_delta
            profile[price_level]['delta'] += volume_delta
        elif tick['aggressor_side'] == 'SELL':
            profile[price_level]['sell_volume'] += volume_delta
            profile[price_level]['delta'] -= volume_delta
        else:  # NEUTRAL - split 50/50
            half_vol = volume_delta / 2
            profile[price_level]['buy_volume'] += half_vol
            profile[price_level]['sell_volume'] += half_vol
        
        profile[price_level]['total_volume'] += volume_delta
    
    return dict(profile)


def plot_volume_profile(profile, title="Volume Profile with Delta"):
    """
    Create volume profile chart with delta visualization
    """
    if not profile:
        print("No data to plot")
        return
    
    # Sort by price
    prices = sorted(profile.keys())
    buy_volumes = [profile[p]['buy_volume'] for p in prices]
    sell_volumes = [profile[p]['sell_volume'] for p in prices]
    deltas = [profile[p]['delta'] for p in prices]
    total_volumes = [profile[p]['total_volume'] for p in prices]
    
    # Find POC (Point of Control) - price with highest volume
    poc_idx = total_volumes.index(max(total_volumes))
    poc_price = prices[poc_idx]
    
    # Calculate Value Area (70% of volume)
    total_vol = sum(total_volumes)
    target_vol = total_vol * 0.7
    
    # Find value area high and low
    sorted_by_vol = sorted(zip(prices, total_volumes), key=lambda x: x[1], reverse=True)
    cumulative = 0
    value_area_prices = []
    for price, vol in sorted_by_vol:
        value_area_prices.append(price)
        cumulative += vol
        if cumulative >= target_vol:
            break
    
    va_high = max(value_area_prices)
    va_low = min(value_area_prices)
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 10), sharey=True)
    
    # Left plot: Volume Profile (Buy/Sell split)
    ax1.barh(prices, buy_volumes, height=prices[1]-prices[0] if len(prices) > 1 else 1, 
             color='green', alpha=0.6, label='Buy Volume')
    ax1.barh(prices, [-v for v in sell_volumes], height=prices[1]-prices[0] if len(prices) > 1 else 1,
             color='red', alpha=0.6, label='Sell Volume')
    
    # Mark POC
    ax1.axhline(y=poc_price, color='yellow', linestyle='--', linewidth=2, label=f'POC: ₹{poc_price:.2f}')
    
    # Mark Value Area
    ax1.axhline(y=va_high, color='cyan', linestyle=':', linewidth=1.5, alpha=0.7, label=f'VA High: ₹{va_high:.2f}')
    ax1.axhline(y=va_low, color='cyan', linestyle=':', linewidth=1.5, alpha=0.7, label=f'VA Low: ₹{va_low:.2f}')
    
    ax1.set_xlabel('Volume (Buy = Right, Sell = Left)', fontsize=12)
    ax1.set_ylabel('Price (₹)', fontsize=12)
    ax1.set_title('Volume Profile (Buy vs Sell)', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    ax1.axvline(x=0, color='black', linewidth=0.5)
    
    # Right plot: Delta Profile
    colors = ['green' if d >= 0 else 'red' for d in deltas]
    ax2.barh(prices, deltas, height=prices[1]-prices[0] if len(prices) > 1 else 1,
             color=colors, alpha=0.7)
    
    # Mark POC
    ax2.axhline(y=poc_price, color='yellow', linestyle='--', linewidth=2, label=f'POC: ₹{poc_price:.2f}')
    
    # Mark Value Area
    ax2.axhline(y=va_high, color='cyan', linestyle=':', linewidth=1.5, alpha=0.7)
    ax2.axhline(y=va_low, color='cyan', linestyle=':', linewidth=1.5, alpha=0.7)
    
    ax2.set_xlabel('Delta (Buy Volume - Sell Volume)', fontsize=12)
    ax2.set_title('Volume Delta Profile', fontsize=14, fontweight='bold')
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)
    ax2.axvline(x=0, color='black', linewidth=1)
    
    plt.suptitle(title, fontsize=16, fontweight='bold', y=0.98)
    plt.tight_layout()
    
    # Print stats
    positive_delta_vol = sum(d for d in deltas if d > 0)
    negative_delta_vol = sum(abs(d) for d in deltas if d < 0)
    net_delta = sum(deltas)
    
    print("\n" + "="*80)
    print("VOLUME PROFILE STATISTICS")
    print("="*80)
    print(f"\nTotal Volume: {total_vol:,.0f}")
    print(f"Buy Volume: {sum(buy_volumes):,.0f}")
    print(f"Sell Volume: {sum(sell_volumes):,.0f}")
    print(f"Net Delta: {net_delta:+,.0f}")
    print(f"\nPoint of Control (POC): ₹{poc_price:.2f} ({profile[poc_price]['total_volume']:,.0f} volume)")
    print(f"Value Area High: ₹{va_high:.2f}")
    print(f"Value Area Low: ₹{va_low:.2f}")
    print(f"Value Area Range: ₹{va_high - va_low:.2f}")
    
    # Find highest delta levels
    positive_deltas = [(p, profile[p]['delta']) for p in prices if profile[p]['delta'] > 0]
    negative_deltas = [(p, profile[p]['delta']) for p in prices if profile[p]['delta'] < 0]
    
    if positive_deltas:
        positive_deltas.sort(key=lambda x: x[1], reverse=True)
        print(f"\nTop 3 Buying Levels:")
        for i, (p, d) in enumerate(positive_deltas[:3], 1):
            print(f"  {i}. ₹{p:.2f}: +{d:,.0f} delta")
    
    if negative_deltas:
        negative_deltas.sort(key=lambda x: x[1])
        print(f"\nTop 3 Selling Levels:")
        for i, (p, d) in enumerate(negative_deltas[:3], 1):
            print(f"  {i}. ₹{p:.2f}: {d:,.0f} delta")
    
    print("="*80)
    
    # Try to show plot, but save if no display available
    try:
        plt.show()
    except:
        # Headless environment - save to file
        filename = f"volume_profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"\n✓ Chart saved as: {filename}")
        plt.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate Volume Profile with Delta from tick data')
    parser.add_argument('--instrument', type=int, help='Instrument token (default: NIFTY FUT from .env)')
    parser.add_argument('--date', type=str, help='Date (YYYY-MM-DD, default: today)')
    parser.add_argument('--tick-size', type=float, default=5, help='Price grouping interval (default: 5)')
    parser.add_argument('--symbol', type=str, help='Trading symbol for title')
    
    args = parser.parse_args()
    
    # Get instrument token
    if args.instrument:
        instrument_token = args.instrument
        symbol = args.symbol or f"Token {instrument_token}"
    else:
        # Try to get from .env
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
    print("VOLUME PROFILE WITH DELTA")
    print("="*80)
    print(f"\nSymbol: {symbol}")
    print(f"Instrument Token: {instrument_token}")
    print(f"Date: {target_date}")
    print(f"Tick Size: {args.tick_size}")
    
    # Load data
    print("\nLoading tick data...")
    ticks = load_tick_data(instrument_token, target_date, target_date)
    
    if not ticks:
        print("✗ No tick data found for this date")
        return
    
    print(f"✓ Loaded {len(ticks)} ticks")
    print(f"  Time range: {ticks[0]['time'].strftime('%H:%M:%S')} to {ticks[-1]['time'].strftime('%H:%M:%S')}")
    print(f"  Price range: ₹{min(t['price'] for t in ticks):.2f} to ₹{max(t['price'] for t in ticks):.2f}")
    
    # Calculate volume profile
    print("\nCalculating volume profile...")
    profile = calculate_volume_profile(ticks, tick_size=args.tick_size)
    print(f"✓ Generated profile with {len(profile)} price levels")
    
    # Plot
    title = f"{symbol} - Volume Profile with Delta - {target_date.strftime('%Y-%m-%d')}"
    plot_volume_profile(profile, title)


if __name__ == '__main__':
    main()
