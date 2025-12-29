"""
Analyze the specific order ratio shift event around 14:19-14:20
that preceded a 35+ point move
"""

import pandas as pd
import numpy as np
from datetime import datetime

# Load CSV
print("Loading depth snapshots...")
df = pd.read_csv('depth_snapshots_202512229.csv')

# Keep time as string for easier filtering
df['time_str'] = df['time_ist']
df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2

# Calculate order ratio (bid_orders / ask_orders)
df['order_ratio'] = df['total_bid_orders'] / df['total_ask_orders']
df['order_ratio_change'] = df['order_ratio'].diff()
df['order_ratio_pct_change'] = df['order_ratio'].pct_change(fill_method=None) * 100

# Calculate absolute shift magnitude
df['order_ratio_shift_magnitude'] = df['order_ratio_change'].abs()

# Price changes
df['price_change'] = df['mid_price'].diff()
df['price_change_5'] = df['mid_price'].diff(5)
df['price_change_10'] = df['mid_price'].diff(10)
df['price_change_20'] = df['mid_price'].diff(20)

# Filter for 14:19 to 14:28 timeframe for full move analysis
mask = (df['time_str'] >= '14:19:00') & (df['time_str'] <= '14:28:00')
event_df = df[mask].copy()

print(f"\n✓ Loaded {len(event_df)} snapshots from 14:19-14:28")
print()

# Find the largest order ratio shifts in this window
print("="*100)
print("LARGEST ORDER RATIO SHIFTS (14:18-14:30)")
print("="*100)
print()

# Get top 10 largest shifts
top_shifts = event_df.nlargest(10, 'order_ratio_shift_magnitude')[
    ['time_str', 'mid_price', 'order_ratio', 'order_ratio_change', 'order_ratio_pct_change', 
     'total_bid_orders', 'total_ask_orders', 'price_change', 'price_change_20']
]

print(top_shifts.to_string(index=False))
print()

# Detailed timeline around 14:19:00 - 14:20:00
print("="*100)
print("DETAILED TIMELINE: 14:19:00 - 14:20:30")
print("="*100)
print()

timeline_mask = (event_df['time_str'] >= '14:19:00') & (event_df['time_str'] <= '14:20:30')
timeline_df = event_df[timeline_mask].copy()

# Calculate future returns (20 snapshots ahead ≈ 4-8 seconds)
timeline_df['return_20'] = timeline_df['mid_price'].shift(-20) - timeline_df['mid_price']
timeline_df['return_50'] = timeline_df['mid_price'].shift(-50) - timeline_df['mid_price']
timeline_df['return_100'] = timeline_df['mid_price'].shift(-100) - timeline_df['mid_price']

print(f"{'Time':<12} {'Price':>8} {'Ratio':>7} {'Ratio Chg':>10} {'Pct Chg':>8} "
      f"{'Bid Ord':>8} {'Ask Ord':>8} {'Shift Mag':>10} {'Ret 20':>8} {'Ret 50':>8} {'Ret 100':>8}")
print("-"*115)

for idx, row in timeline_df.iterrows():
    time_str = row['time_str']
    price = row['mid_price']
    ratio = row['order_ratio']
    ratio_chg = row['order_ratio_change']
    pct_chg = row['order_ratio_pct_change']
    bid_orders = row['total_bid_orders']
    ask_orders = row['total_ask_orders']
    shift_mag = row['order_ratio_shift_magnitude']
    ret_20 = row['return_20'] if pd.notna(row['return_20']) else 0
    ret_50 = row['return_50'] if pd.notna(row['return_50']) else 0
    ret_100 = row['return_100'] if pd.notna(row['return_100']) else 0
    
    print(f"{time_str:<12} {price:>8.2f} {ratio:>7.4f} {ratio_chg:>+10.4f} {pct_chg:>+8.2f}% "
          f"{bid_orders:>8.0f} {ask_orders:>8.0f} {shift_mag:>10.4f} {ret_20:>+8.2f} {ret_50:>+8.2f} {ret_100:>+8.2f}")

print()

# Find significant order ratio surge moments
print("="*100)
print("SIGNIFICANT ORDER RATIO SURGE EVENTS")
print("="*100)
print()

# Define "significant" as >95th percentile of all shifts
threshold_95 = event_df['order_ratio_shift_magnitude'].quantile(0.95)
print(f"95th percentile threshold: {threshold_95:.4f}")
print()

surge_events = timeline_df[timeline_df['order_ratio_shift_magnitude'] > threshold_95].copy()

if len(surge_events) > 0:
    print(f"Found {len(surge_events)} surge events in 14:19-14:20 window:")
    print()
    
    for idx, row in surge_events.iterrows():
        time_str = row['time_str']
        print(f"Time: {time_str}")
        print(f"  Price: {row['mid_price']:.2f}")
        print(f"  Order Ratio: {row['order_ratio']:.4f}")
        print(f"  Ratio Change: {row['order_ratio_change']:+.4f} ({row['order_ratio_pct_change']:+.2f}%)")
        print(f"  Bid Orders: {row['total_bid_orders']:.0f}, Ask Orders: {row['total_ask_orders']:.0f}")
        print(f"  Future Returns: 20-snap: {row['return_20']:+.2f}, 50-snap: {row['return_50']:+.2f}, 100-snap: {row['return_100']:+.2f}")
        print()

# Correlation analysis
print("="*100)
print("CORRELATION: Order Ratio Shift → Future Returns")
print("="*100)
print()

corr_20 = timeline_df[['order_ratio_shift_magnitude', 'return_20']].corr().iloc[0, 1]
corr_50 = timeline_df[['order_ratio_shift_magnitude', 'return_50']].corr().iloc[0, 1]
corr_100 = timeline_df[['order_ratio_shift_magnitude', 'return_100']].corr().iloc[0, 1]

print(f"Correlation (Shift Magnitude → 20-snap return): {corr_20:+.4f}")
print(f"Correlation (Shift Magnitude → 50-snap return): {corr_50:+.4f}")
print(f"Correlation (Shift Magnitude → 100-snap return): {corr_100:+.4f}")
print()

# Directional analysis
print("="*100)
print("DIRECTIONAL ANALYSIS: Bid Surge vs Ask Surge")
print("="*100)
print()

bid_surge = timeline_df[timeline_df['order_ratio_change'] > 0.02]
ask_surge = timeline_df[timeline_df['order_ratio_change'] < -0.02]

print(f"Bid Ratio Surge (ratio increases >0.02):")
print(f"  Count: {len(bid_surge)}")
if len(bid_surge) > 0:
    print(f"  Avg 20-snap return: {bid_surge['return_20'].mean():+.2f}")
    print(f"  Avg 50-snap return: {bid_surge['return_50'].mean():+.2f}")
    print(f"  Avg 100-snap return: {bid_surge['return_100'].mean():+.2f}")
print()

print(f"Ask Ratio Surge (ratio decreases >0.02):")
print(f"  Count: {len(ask_surge)}")
if len(ask_surge) > 0:
    print(f"  Avg 20-snap return: {ask_surge['return_20'].mean():+.2f}")
    print(f"  Avg 50-snap return: {ask_surge['return_50'].mean():+.2f}")
    print(f"  Avg 100-snap return: {ask_surge['return_100'].mean():+.2f}")
print()

# Summary of the 35-point move
print("="*100)
print("SUMMARY: What Happened During 14:19-14:20")
print("="*100)
print()

price_low = timeline_df['mid_price'].min()
price_high = timeline_df['mid_price'].max()
price_move = price_high - price_low

low_idx = timeline_df['mid_price'].idxmin()
high_idx = timeline_df['mid_price'].idxmax()
low_time = timeline_df.loc[low_idx, 'time_str']
high_time = timeline_df.loc[high_idx, 'time_str']

print(f"Price Low: {price_low:.2f} at {low_time}")
print(f"Price High: {price_high:.2f} at {high_time}")
print(f"Total Move: {price_move:+.2f} points")
print()

# Order ratio at key moments
ratio_at_low = timeline_df.loc[low_idx, 'order_ratio']
ratio_at_high = timeline_df.loc[high_idx, 'order_ratio']

print(f"Order Ratio at Low: {ratio_at_low:.4f}")
print(f"Order Ratio at High: {ratio_at_high:.4f}")
print(f"Ratio Change: {ratio_at_high - ratio_at_low:+.4f}")
print()

print("="*100)
print("ANALYSIS COMPLETE")
print("="*100)
