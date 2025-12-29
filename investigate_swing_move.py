"""
Investigate Swing Move: 14:19-14:27 (25934 → 25976, +42 points)
Analyze orderbook behavior before, during, and after the move
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tradingdb',
    'user': 'tradinguser',
    'password': '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
}

IST = pytz.timezone('Asia/Kolkata')

def fetch_data(start_time, end_time):
    """Fetch depth snapshots for the timeframe"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    query = f"""
    SELECT 
        timestamp AT TIME ZONE 'Asia/Kolkata' as time_ist,
        best_bid,
        best_ask,
        spread,
        total_bid_qty,
        total_ask_qty,
        total_bid_orders,
        total_ask_orders,
        imbalance_ratio,
        avg_bid_order_size,
        avg_ask_order_size,
        bid_vwap,
        ask_vwap,
        bid_50pct_level,
        ask_50pct_level
    FROM depth_200_snapshots
    WHERE timestamp AT TIME ZONE 'Asia/Kolkata' >= '{start_time}'
      AND timestamp AT TIME ZONE 'Asia/Kolkata' <= '{end_time}'
    ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def calculate_metrics(df):
    """Calculate additional metrics"""
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2
    df['total_liquidity'] = df['total_bid_qty'] + df['total_ask_qty']
    df['bid_vwap_dist'] = df['best_bid'] - df['bid_vwap']
    df['ask_vwap_dist'] = df['ask_vwap'] - df['best_ask']
    df['vwap_gap'] = df['ask_vwap'] - df['bid_vwap']
    
    # Changes
    df['price_change'] = df['mid_price'].diff()
    df['liquidity_change'] = df['total_liquidity'].diff()
    df['imbalance_change'] = df['imbalance_ratio'].diff()
    df['spread_change'] = df['spread'].diff()
    
    # Cumulative move
    df['cum_price_move'] = df['mid_price'] - df['mid_price'].iloc[0]
    
    return df

def analyze_phases(df):
    """Divide into before/during/after phases"""
    
    # Define phase boundaries
    move_start = pd.to_datetime('2025-12-29 14:19:00')
    move_end = pd.to_datetime('2025-12-29 14:27:00')
    
    before = df[df['time_ist'] < move_start].copy()
    during = df[(df['time_ist'] >= move_start) & (df['time_ist'] <= move_end)].copy()
    after = df[df['time_ist'] > move_end].copy()
    
    return before, during, after

def print_phase_summary(phase_name, df):
    """Print summary statistics for a phase"""
    if len(df) == 0:
        print(f"  {phase_name}: No data")
        return
    
    print(f"\n{phase_name}:")
    print(f"  {'='*90}")
    print(f"  Snapshots: {len(df)}")
    print(f"  Time range: {df['time_ist'].iloc[0].strftime('%H:%M:%S')} to {df['time_ist'].iloc[-1].strftime('%H:%M:%S')}")
    print()
    
    # Price
    price_start = df['mid_price'].iloc[0]
    price_end = df['mid_price'].iloc[-1]
    price_low = df['mid_price'].min()
    price_high = df['mid_price'].max()
    price_change = price_end - price_start
    
    print(f"  PRICE:")
    print(f"    Open: ₹{price_start:.2f}")
    print(f"    Close: ₹{price_end:.2f}")
    print(f"    High: ₹{price_high:.2f}")
    print(f"    Low: ₹{price_low:.2f}")
    print(f"    Move: {price_change:+.2f} ({(price_change/price_start)*100:+.2f}%)")
    print()
    
    # Liquidity
    liq_start = df['total_liquidity'].iloc[0]
    liq_end = df['total_liquidity'].iloc[-1]
    liq_avg = df['total_liquidity'].mean()
    liq_max = df['total_liquidity'].max()
    liq_change_pct = ((liq_end - liq_start) / liq_start) * 100
    
    print(f"  LIQUIDITY:")
    print(f"    Start: {liq_start:,}")
    print(f"    End: {liq_end:,}")
    print(f"    Average: {liq_avg:,.0f}")
    print(f"    Peak: {liq_max:,}")
    print(f"    Change: {liq_change_pct:+.1f}%")
    print()
    
    # Imbalance
    imb_start = df['imbalance_ratio'].iloc[0]
    imb_end = df['imbalance_ratio'].iloc[-1]
    imb_avg = df['imbalance_ratio'].mean()
    imb_max = df['imbalance_ratio'].max()
    imb_min = df['imbalance_ratio'].min()
    
    print(f"  IMBALANCE:")
    print(f"    Start: {imb_start:.3f}")
    print(f"    End: {imb_end:.3f}")
    print(f"    Average: {imb_avg:.3f}")
    print(f"    Range: {imb_min:.3f} to {imb_max:.3f}")
    
    if imb_avg > 1.15:
        print(f"    → STRONG BID-HEAVY (buying pressure)")
    elif imb_avg > 1.05:
        print(f"    → MODERATE BID-HEAVY")
    elif imb_avg < 0.85:
        print(f"    → STRONG ASK-HEAVY (selling pressure)")
    elif imb_avg < 0.95:
        print(f"    → MODERATE ASK-HEAVY")
    else:
        print(f"    → BALANCED")
    print()
    
    # Spread
    spread_start = df['spread'].iloc[0]
    spread_end = df['spread'].iloc[-1]
    spread_avg = df['spread'].mean()
    spread_max = df['spread'].max()
    
    print(f"  SPREAD:")
    print(f"    Start: ₹{spread_start:.2f}")
    print(f"    End: ₹{spread_end:.2f}")
    print(f"    Average: ₹{spread_avg:.2f}")
    print(f"    Peak: ₹{spread_max:.2f}")
    print()
    
    # VWAP Distance
    bid_vwap_dist_start = df['bid_vwap_dist'].iloc[0]
    bid_vwap_dist_end = df['bid_vwap_dist'].iloc[-1]
    bid_vwap_dist_avg = df['bid_vwap_dist'].mean()
    
    print(f"  VWAP DISTANCE:")
    print(f"    Bid VWAP distance start: ₹{bid_vwap_dist_start:.2f}")
    print(f"    Bid VWAP distance end: ₹{bid_vwap_dist_end:.2f}")
    print(f"    Bid VWAP distance avg: ₹{bid_vwap_dist_avg:.2f}")
    
    if bid_vwap_dist_avg < 27:
        print(f"    → Very close to VWAP (rubber band compressed)")
    elif bid_vwap_dist_avg > 33:
        print(f"    → Far from VWAP (rubber band stretched)")
    else:
        print(f"    → Normal distance from VWAP")
    print()
    
    # Order flow
    bid_orders_avg = df['total_bid_orders'].mean()
    ask_orders_avg = df['total_ask_orders'].mean()
    order_imbalance = bid_orders_avg / ask_orders_avg
    
    print(f"  ORDER FLOW:")
    print(f"    Avg bid orders: {bid_orders_avg:,.0f}")
    print(f"    Avg ask orders: {ask_orders_avg:,.0f}")
    print(f"    Order count imbalance: {order_imbalance:.3f}")
    print()

def find_key_events(df):
    """Find significant events in the data"""
    print("\n" + "="*100)
    print("KEY EVENTS DETECTED")
    print("="*100)
    
    events = []
    
    # Large liquidity spikes (>20% in one snapshot)
    liq_threshold = df['total_liquidity'].quantile(0.90)
    liq_spikes = df[df['total_liquidity'] > liq_threshold]
    
    if len(liq_spikes) > 0:
        print(f"\nLIQUIDITY SPIKES (>{liq_threshold:,.0f}):")
        for idx, row in liq_spikes.head(10).iterrows():
            print(f"  {row['time_ist'].strftime('%H:%M:%S')} - {row['total_liquidity']:,} " +
                  f"(Price: ₹{row['mid_price']:.2f}, Imb: {row['imbalance_ratio']:.3f})")
    
    # Extreme imbalance
    extreme_bid_heavy = df[df['imbalance_ratio'] > 1.15]
    extreme_ask_heavy = df[df['imbalance_ratio'] < 0.85]
    
    if len(extreme_bid_heavy) > 0:
        print(f"\nEXTREME BID-HEAVY (>1.15):")
        for idx, row in extreme_bid_heavy.head(10).iterrows():
            print(f"  {row['time_ist'].strftime('%H:%M:%S')} - Imb: {row['imbalance_ratio']:.3f} " +
                  f"(Price: ₹{row['mid_price']:.2f}, Liq: {row['total_liquidity']:,})")
    
    if len(extreme_ask_heavy) > 0:
        print(f"\nEXTREME ASK-HEAVY (<0.85):")
        for idx, row in extreme_ask_heavy.head(10).iterrows():
            print(f"  {row['time_ist'].strftime('%H:%M:%S')} - Imb: {row['imbalance_ratio']:.3f} " +
                  f"(Price: ₹{row['mid_price']:.2f}, Liq: {row['total_liquidity']:,})")
    
    # Large spread widening
    spread_threshold = df['spread'].quantile(0.90)
    wide_spreads = df[df['spread'] > spread_threshold]
    
    if len(wide_spreads) > 0:
        print(f"\nWIDE SPREADS (>₹{spread_threshold:.2f}):")
        for idx, row in wide_spreads.head(10).iterrows():
            print(f"  {row['time_ist'].strftime('%H:%M:%S')} - Spread: ₹{row['spread']:.2f} " +
                  f"(Price: ₹{row['mid_price']:.2f})")
    
    # Large single-tick price moves
    large_moves = df[df['price_change'].abs() > 2]
    
    if len(large_moves) > 0:
        print(f"\nLARGE PRICE MOVES (>₹2):")
        for idx, row in large_moves.head(10).iterrows():
            print(f"  {row['time_ist'].strftime('%H:%M:%S')} - Move: {row['price_change']:+.2f} " +
                  f"(Price: ₹{row['mid_price']:.2f}, Imb: {row['imbalance_ratio']:.3f})")
    
    print()

def minute_by_minute_analysis(df):
    """Analyze minute-by-minute progression"""
    print("\n" + "="*100)
    print("MINUTE-BY-MINUTE PROGRESSION")
    print("="*100)
    
    df['minute'] = df['time_ist'].dt.floor('min')
    
    minute_summary = df.groupby('minute').agg({
        'mid_price': ['first', 'last', 'min', 'max'],
        'total_liquidity': 'mean',
        'imbalance_ratio': 'mean',
        'spread': 'mean',
        'bid_vwap_dist': 'mean',
        'time_ist': 'count'
    }).round(2)
    
    print("\n{:<8} {:<8} {:<8} {:<8} {:<8} {:<10} {:<7} {:<7} {:<10} {:<8}".format(
        "Time", "Open", "Close", "Low", "High", "Liq", "Imb", "Spread", "VWAP Dist", "Snaps"
    ))
    print("-" * 100)
    
    for minute, row in minute_summary.iterrows():
        time_str = minute.strftime('%H:%M')
        open_price = row[('mid_price', 'first')]
        close_price = row[('mid_price', 'last')]
        low_price = row[('mid_price', 'min')]
        high_price = row[('mid_price', 'max')]
        avg_liq = row[('total_liquidity', 'mean')]
        avg_imb = row[('imbalance_ratio', 'mean')]
        avg_spread = row[('spread', 'mean')]
        avg_vwap_dist = row[('bid_vwap_dist', 'mean')]
        snap_count = int(row[('time_ist', 'count')])
        
        print(f"{time_str:<8} {open_price:<8.2f} {close_price:<8.2f} {low_price:<8.2f} {high_price:<8.2f} " +
              f"{avg_liq:<10,.0f} {avg_imb:<7.3f} {avg_spread:<7.2f} {avg_vwap_dist:<10.2f} {snap_count:<8}")
    
    print()

def detailed_timeline(df):
    """Show detailed snapshot-by-snapshot during the move"""
    print("\n" + "="*100)
    print("DETAILED TIMELINE: 14:19:00 - 14:27:00 (THE MOVE)")
    print("="*100)
    
    move_start = pd.to_datetime('2025-12-29 14:19:00')
    move_end = pd.to_datetime('2025-12-29 14:27:00')
    
    move_data = df[(df['time_ist'] >= move_start) & (df['time_ist'] <= move_end)].copy()
    
    if len(move_data) == 0:
        print("No data during the move period")
        return
    
    print("\n{:<12} {:<10} {:<10} {:<10} {:<10} {:<8} {:<8}".format(
        "Time", "Price", "Cum Move", "Liquidity", "Imb", "Spread", "VWAP Dst"
    ))
    print("-" * 100)
    
    # Show every 10th snapshot to avoid clutter
    for idx, row in move_data.iloc[::10].iterrows():
        time_str = row['time_ist'].strftime('%H:%M:%S')
        print(f"{time_str:<12} {row['mid_price']:<10.2f} {row['cum_price_move']:<+10.2f} " +
              f"{row['total_liquidity']:<10,.0f} {row['imbalance_ratio']:<8.3f} " +
              f"{row['spread']:<8.2f} {row['bid_vwap_dist']:<8.2f}")
    
    print()

def trading_insights(before, during, after):
    """Generate trading insights from the pattern"""
    print("\n" + "="*100)
    print("TRADING INSIGHTS: What Triggered the +42 Point Move?")
    print("="*100)
    print()
    
    if len(before) == 0 or len(during) == 0:
        print("Insufficient data for insights")
        return
    
    # Setup characteristics
    print("SETUP PHASE (Before 14:19):")
    print("-" * 100)
    
    before_imb = before['imbalance_ratio'].mean()
    before_liq = before['total_liquidity'].mean()
    before_spread = before['spread'].mean()
    before_vwap_dist = before['bid_vwap_dist'].mean()
    
    print(f"  Average imbalance: {before_imb:.3f}")
    print(f"  Average liquidity: {before_liq:,.0f}")
    print(f"  Average spread: ₹{before_spread:.2f}")
    print(f"  Average VWAP distance: ₹{before_vwap_dist:.2f}")
    print()
    
    # Trigger identification
    print("TRIGGER ANALYSIS:")
    print("-" * 100)
    
    # Compare first 5 snapshots of "during" vs last 5 of "before"
    trigger_point = during.head(10)
    setup_end = before.tail(10)
    
    imb_change = trigger_point['imbalance_ratio'].mean() - setup_end['imbalance_ratio'].mean()
    liq_change = trigger_point['total_liquidity'].mean() - setup_end['total_liquidity'].mean()
    spread_change = trigger_point['spread'].mean() - setup_end['spread'].mean()
    
    print(f"  Imbalance change: {imb_change:+.3f}")
    print(f"  Liquidity change: {liq_change:+,.0f} ({(liq_change/setup_end['total_liquidity'].mean())*100:+.1f}%)")
    print(f"  Spread change: {spread_change:+.2f}")
    print()
    
    # Determine trigger
    print("LIKELY TRIGGER:")
    print("-" * 100)
    
    if imb_change > 0.1:
        print(f"  ✓ IMBALANCE SURGE: {imb_change:+.3f} (bid-heavy pressure increased)")
    
    if liq_change < -10000:
        print(f"  ✓ LIQUIDITY WITHDRAWAL: {liq_change:+,.0f} (sellers stepped aside)")
    
    if spread_change > 1:
        print(f"  ✓ SPREAD WIDENING: {spread_change:+.2f} (volatility breakout)")
    
    if before_vwap_dist < 27:
        print(f"  ✓ COMPRESSED VWAP: ₹{before_vwap_dist:.2f} (rubber band release)")
    
    print()
    
    # Move characteristics
    print("MOVE CHARACTERISTICS (During 14:19-14:27):")
    print("-" * 100)
    
    during_imb = during['imbalance_ratio'].mean()
    during_liq = during['total_liquidity'].mean()
    during_spread = during['spread'].mean()
    
    print(f"  Sustained imbalance: {during_imb:.3f}")
    print(f"  Average liquidity: {during_liq:,.0f}")
    print(f"  Average spread: ₹{during_spread:.2f}")
    
    if during_imb > 1.1:
        print(f"  → Strong bid dominance throughout move")
    
    if during_liq < before_liq * 0.8:
        print(f"  → Low resistance (sellers absent)")
    
    print()
    
    # Tradeable pattern?
    print("TRADEABLE PATTERN:")
    print("-" * 100)
    
    if before_imb > 1.05 and imb_change > 0.05 and before_vwap_dist < 28:
        print(f"  ✓ YES - Accumulation + Compression + Imbalance Surge")
        print(f"  Entry: When imbalance spikes from {before_imb:.3f} to {trigger_point['imbalance_ratio'].mean():.3f}")
        print(f"  Confirmation: VWAP distance <₹28 + Imbalance >1.10")
        print(f"  Target: 20-40 points (actual move: +42 points)")
        print(f"  Stop: Below pre-move low (₹25934) = ~₹25930")
    else:
        print(f"  ? UNCLEAR - Pattern not clean enough for systematic trading")
    
    print()

def main():
    """Main analysis"""
    print()
    print("="*100)
    print("SWING MOVE INVESTIGATION: 14:19-14:27 (+42 POINTS)")
    print("="*100)
    print()
    
    # Fetch data (14:10 to 14:35 for context)
    start_time = '2025-12-29 14:10:00'
    end_time = '2025-12-29 14:35:00'
    
    print(f"Fetching data from {start_time} to {end_time}...")
    df = fetch_data(start_time, end_time)
    
    if len(df) == 0:
        print("No data found for this timeframe")
        return
    
    print(f"✓ Loaded {len(df):,} snapshots")
    print()
    
    # Calculate metrics
    df = calculate_metrics(df)
    
    # Divide into phases
    before, during, after = analyze_phases(df)
    
    # Phase summaries
    print("\n" + "="*100)
    print("PHASE ANALYSIS")
    print("="*100)
    
    print_phase_summary("BEFORE THE MOVE (14:10-14:19)", before)
    print_phase_summary("DURING THE MOVE (14:19-14:27)", during)
    print_phase_summary("AFTER THE MOVE (14:27-14:35)", after)
    
    # Key events
    find_key_events(df)
    
    # Minute-by-minute
    minute_by_minute_analysis(df)
    
    # Detailed timeline
    detailed_timeline(df)
    
    # Trading insights
    trading_insights(before, during, after)
    
    print("="*100)
    print("ANALYSIS COMPLETE")
    print("="*100)
    print()

if __name__ == "__main__":
    main()
