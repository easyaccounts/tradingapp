"""
Analyze Bid/Ask VWAP behavior during the 25934 breakdown (14:19-14:20)
"""

import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tradingdb',
    'user': 'tradinguser',
    'password': '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
}

def fetch_vwap_data():
    """Fetch detailed VWAP data for breakdown period"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    query = """
    SELECT 
        timestamp AT TIME ZONE 'Asia/Kolkata' as time_ist,
        best_bid,
        best_ask,
        (best_bid + best_ask) / 2 as mid_price,
        bid_vwap,
        ask_vwap,
        best_bid - bid_vwap as bid_vwap_dist,
        ask_vwap - best_ask as ask_vwap_dist,
        ask_vwap - bid_vwap as vwap_gap,
        total_bid_qty,
        total_ask_qty,
        imbalance_ratio,
        spread
    FROM depth_200_snapshots
    WHERE timestamp AT TIME ZONE 'Asia/Kolkata' >= '2025-12-29 14:18:00'
      AND timestamp AT TIME ZONE 'Asia/Kolkata' <= '2025-12-29 14:21:00'
    ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

def analyze_vwap_behavior(df):
    """Analyze VWAP changes during breakdown"""
    
    print()
    print("="*120)
    print("BID/ASK VWAP BEHAVIOR DURING 25934 BREAKDOWN")
    print("="*120)
    print()
    
    # Calculate changes
    df['price_change'] = df['mid_price'].diff()
    df['bid_vwap_change'] = df['bid_vwap'].diff()
    df['ask_vwap_change'] = df['ask_vwap'].diff()
    df['vwap_gap_change'] = df['vwap_gap'].diff()
    
    # Find the low point
    low_idx = df['mid_price'].idxmin()
    low_time = df.loc[low_idx, 'time_ist']
    low_price = df.loc[low_idx, 'mid_price']
    
    print(f"Lowest mid-price captured: ₹{low_price:.2f} at {low_time.strftime('%H:%M:%S')}")
    print()
    
    # Show detailed data around breakdown
    print("DETAILED VWAP TIMELINE (Every 3rd snapshot for readability):")
    print("-"*120)
    print(f"{'Time':<12} {'Price':<10} {'Chg':<8} {'Bid VWAP':<12} {'Ask VWAP':<12} " +
          f"{'VWAP Gap':<12} {'Bid Dist':<10} {'Ask Dist':<10} {'Imb':<8}")
    print("-"*120)
    
    for idx, row in df.iloc[::3].iterrows():
        time_str = row['time_ist'].strftime('%H:%M:%S')
        price = row['mid_price']
        price_chg = row['price_change']
        bid_vwap = row['bid_vwap']
        ask_vwap = row['ask_vwap']
        vwap_gap = row['vwap_gap']
        bid_dist = row['bid_vwap_dist']
        ask_dist = row['ask_vwap_dist']
        imb = row['imbalance_ratio']
        
        print(f"{time_str:<12} {price:<10.2f} {price_chg:<+8.2f} {bid_vwap:<12.2f} {ask_vwap:<12.2f} " +
              f"{vwap_gap:<12.2f} {bid_dist:<10.2f} {ask_dist:<10.2f} {imb:<8.3f}")
    
    print()
    
    # Analyze VWAP behavior during key phases
    print("="*120)
    print("PHASE ANALYSIS")
    print("="*120)
    print()
    
    # Before breakdown (14:18:00-14:19:00)
    before = df[df['time_ist'] < pd.to_datetime('2025-12-29 14:19:00')]
    print("BEFORE BREAKDOWN (14:18:00-14:19:00):")
    print(f"  Price range: ₹{before['mid_price'].min():.2f} - ₹{before['mid_price'].max():.2f}")
    print(f"  Bid VWAP avg: ₹{before['bid_vwap'].mean():.2f}")
    print(f"  Ask VWAP avg: ₹{before['ask_vwap'].mean():.2f}")
    print(f"  VWAP gap avg: ₹{before['vwap_gap'].mean():.2f}")
    print(f"  Bid VWAP distance avg: ₹{before['bid_vwap_dist'].mean():.2f}")
    print()
    
    # During breakdown (14:19:00-14:19:40)
    during = df[(df['time_ist'] >= pd.to_datetime('2025-12-29 14:19:00')) & 
                (df['time_ist'] <= pd.to_datetime('2025-12-29 14:19:40'))]
    print("DURING BREAKDOWN (14:19:00-14:19:40):")
    print(f"  Price range: ₹{during['mid_price'].min():.2f} - ₹{during['mid_price'].max():.2f}")
    print(f"  Price move: {during['mid_price'].iloc[-1] - during['mid_price'].iloc[0]:+.2f}")
    print(f"  Bid VWAP start: ₹{during['bid_vwap'].iloc[0]:.2f}")
    print(f"  Bid VWAP end: ₹{during['bid_vwap'].iloc[-1]:.2f}")
    print(f"  Bid VWAP move: {during['bid_vwap'].iloc[-1] - during['bid_vwap'].iloc[0]:+.2f}")
    print(f"  Ask VWAP start: ₹{during['ask_vwap'].iloc[0]:.2f}")
    print(f"  Ask VWAP end: ₹{during['ask_vwap'].iloc[-1]:.2f}")
    print(f"  Ask VWAP move: {during['ask_vwap'].iloc[-1] - during['ask_vwap'].iloc[0]:+.2f}")
    print()
    
    # After reversal (14:19:40-14:20:30)
    after = df[(df['time_ist'] > pd.to_datetime('2025-12-29 14:19:40')) & 
               (df['time_ist'] <= pd.to_datetime('2025-12-29 14:20:30'))]
    print("AFTER REVERSAL (14:19:40-14:20:30):")
    print(f"  Price range: ₹{after['mid_price'].min():.2f} - ₹{after['mid_price'].max():.2f}")
    print(f"  Price move: {after['mid_price'].iloc[-1] - after['mid_price'].iloc[0]:+.2f}")
    print(f"  Bid VWAP move: {after['bid_vwap'].iloc[-1] - after['bid_vwap'].iloc[0]:+.2f}")
    print(f"  Ask VWAP move: {after['ask_vwap'].iloc[-1] - after['ask_vwap'].iloc[0]:+.2f}")
    print()
    
    # Key observations
    print("="*120)
    print("KEY OBSERVATIONS")
    print("="*120)
    print()
    
    # VWAP elasticity during breakdown
    price_move_during = during['mid_price'].iloc[-1] - during['mid_price'].iloc[0]
    bid_vwap_move_during = during['bid_vwap'].iloc[-1] - during['bid_vwap'].iloc[0]
    ask_vwap_move_during = during['ask_vwap'].iloc[-1] - during['ask_vwap'].iloc[0]
    
    if price_move_during != 0:
        bid_elasticity = bid_vwap_move_during / price_move_during
        ask_elasticity = ask_vwap_move_during / price_move_during
        
        print(f"1. VWAP ELASTICITY DURING BREAKDOWN:")
        print(f"   Price moved: {price_move_during:+.2f}")
        print(f"   Bid VWAP moved: {bid_vwap_move_during:+.2f}")
        print(f"   Ask VWAP moved: {ask_vwap_move_during:+.2f}")
        print(f"   Bid VWAP elasticity: {bid_elasticity:.3f} (moved {bid_elasticity*100:.1f}% as much as price)")
        print(f"   Ask VWAP elasticity: {ask_elasticity:.3f} (moved {ask_elasticity*100:.1f}% as much as price)")
        print()
    
    # Distance changes
    bid_dist_start = during['bid_vwap_dist'].iloc[0]
    bid_dist_end = during['bid_vwap_dist'].iloc[-1]
    bid_dist_change = bid_dist_end - bid_dist_start
    
    print(f"2. VWAP DISTANCE BEHAVIOR:")
    print(f"   Bid VWAP distance at start: ₹{bid_dist_start:.2f}")
    print(f"   Bid VWAP distance at end: ₹{bid_dist_end:.2f}")
    print(f"   Change: {bid_dist_change:+.2f}")
    if bid_dist_change < -2:
        print(f"   → Price moved TOWARD bid VWAP (rubber band compressed)")
    elif bid_dist_change > 2:
        print(f"   → Price moved AWAY from bid VWAP (rubber band stretched)")
    else:
        print(f"   → Distance stayed stable (VWAPs tracking price)")
    print()
    
    # Gap behavior
    gap_start = during['vwap_gap'].iloc[0]
    gap_end = during['vwap_gap'].iloc[-1]
    gap_change = gap_end - gap_start
    
    print(f"3. VWAP GAP BEHAVIOR:")
    print(f"   Gap at start: ₹{gap_start:.2f}")
    print(f"   Gap at end: ₹{gap_end:.2f}")
    print(f"   Change: {gap_change:+.2f}")
    if gap_change < -2:
        print(f"   → VWAPs CONVERGED (gap contracted)")
    elif gap_change > 2:
        print(f"   → VWAPs DIVERGED (gap expanded)")
    else:
        print(f"   → Gap stayed stable")
    print()
    
    print("="*120)
    print()

def main():
    print("\nFetching VWAP data for breakdown period...")
    df = fetch_vwap_data()
    print(f"✓ Loaded {len(df):,} snapshots\n")
    
    analyze_vwap_behavior(df)

if __name__ == "__main__":
    main()
