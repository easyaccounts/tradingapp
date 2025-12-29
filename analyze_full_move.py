import pandas as pd
import numpy as np

print("Loading depth snapshots...")

# Load the CSV
df = pd.read_csv('depth_snapshots_202512229.csv')

# Keep time as string for comparison
df['time_str'] = df['time_ist']

# Calculate order metrics
df['order_ratio'] = df['total_bid_orders'] / df['total_ask_orders']
df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2

# Calculate changes
df['order_ratio_change'] = df['order_ratio'].diff()
df['order_ratio_pct_change'] = df['order_ratio'].pct_change(fill_method=None) * 100
df['order_ratio_shift_magnitude'] = df['order_ratio_change'].abs()
df['price_change'] = df['mid_price'].diff()

# Filter for 14:19 to 14:28 timeframe for full move analysis
mask = (df['time_str'] >= '14:19:00') & (df['time_str'] <= '14:28:00')
event_df = df[mask].copy()

print(f"\n✓ Loaded {len(event_df)} snapshots from 14:19-14:28")
print()

# Calculate future returns at multiple horizons
event_df['return_20'] = event_df['mid_price'].shift(-20) - event_df['mid_price']
event_df['return_50'] = event_df['mid_price'].shift(-50) - event_df['mid_price']
event_df['return_100'] = event_df['mid_price'].shift(-100) - event_df['mid_price']
event_df['return_500'] = event_df['mid_price'].shift(-500) - event_df['mid_price']  # ~2 min
event_df['return_1000'] = event_df['mid_price'].shift(-1000) - event_df['mid_price']  # ~4 min
event_df['return_2000'] = event_df['mid_price'].shift(-2000) - event_df['mid_price']  # ~8 min

# =============================================================================
# 1. LARGEST RATIO SHIFTS IN FULL PERIOD
# =============================================================================
print("="*100)
print("TOP 20 LARGEST ORDER RATIO SHIFTS (14:19-14:28)")
print("="*100)
print()

top_shifts = event_df.nlargest(20, 'order_ratio_shift_magnitude')[
    ['time_str', 'mid_price', 'order_ratio', 'order_ratio_change', 'order_ratio_pct_change', 
     'total_bid_orders', 'total_ask_orders', 'return_20', 'return_500', 'return_1000', 'return_2000']
]

print(top_shifts.to_string(index=False))
print()

# =============================================================================
# 2. KEY MOMENTS TIMELINE (Every 30 seconds)
# =============================================================================
print("="*100)
print("KEY MOMENTS TIMELINE (30-second intervals + major shifts)")
print("="*100)
print()

# Sample every ~150 snapshots (30 seconds) plus the largest shifts
sample_indices = range(0, len(event_df), 150)
sampled_df = event_df.iloc[list(sample_indices)].copy()

# Also add top 10 largest shifts
top_10_shifts = event_df.nlargest(10, 'order_ratio_shift_magnitude')
combined_df = pd.concat([sampled_df, top_10_shifts]).drop_duplicates().sort_values('time_str')

print(f"{'Time':<12} {'Price':>8} {'Ratio':>7} {'Chg':>7} {'Pct':>7} "
      f"{'Bid':>5} {'Ask':>5} {'R20':>6} {'R500':>7} {'R1k':>7} {'R2k':>7}")
print("-"*80)

for idx, row in combined_df.iterrows():
    print(f"{row['time_str']:12} {row['mid_price']:8.2f} {row['order_ratio']:7.4f} "
          f"{row['order_ratio_change']:+7.4f} {row['order_ratio_pct_change']:+7.2f}% "
          f"{int(row['total_bid_orders']):5d} {int(row['total_ask_orders']):5d} "
          f"{row['return_20']:+6.2f} {row['return_500']:+7.2f} {row['return_1000']:+7.2f} {row['return_2000']:+7.2f}")

print()

# =============================================================================
# 3. EARLY RATIO SHIFTS (14:19:00-14:20:00) vs LATER RETURNS
# =============================================================================
print("="*100)
print("EARLY RATIO SHIFTS (14:19-14:20) → LATER PRICE MOVES")
print("="*100)
print()

early_mask = (event_df['time_str'] >= '14:19:00') & (event_df['time_str'] <= '14:20:00')
early_df = event_df[early_mask].copy()

# Find significant shifts in early period
threshold_95 = early_df['order_ratio_shift_magnitude'].quantile(0.95)
early_surges = early_df[early_df['order_ratio_shift_magnitude'] >= threshold_95]

print(f"Found {len(early_surges)} significant ratio shifts in first minute (>95th percentile)")
print(f"Threshold: {threshold_95:.4f}\n")

print(f"{'Time':<12} {'Price':>8} {'Ratio':>7} {'Change':>8} {'Later Returns -->':>15} "
      f"{'2min':>7} {'4min':>7} {'8min':>7}")
print("-"*85)

for idx, row in early_surges.iterrows():
    print(f"{row['time_str']:12} {row['mid_price']:8.2f} {row['order_ratio']:7.4f} "
          f"{row['order_ratio_change']:+8.4f} {' ':15} "
          f"{row['return_500']:+7.2f} {row['return_1000']:+7.2f} {row['return_2000']:+7.2f}")

print()

# =============================================================================
# 4. CORRELATIONS
# =============================================================================
print("="*100)
print("CORRELATION: Order Ratio Shift → Future Returns")
print("="*100)
print()

corr_20 = event_df['order_ratio_shift_magnitude'].corr(event_df['return_20'])
corr_50 = event_df['order_ratio_shift_magnitude'].corr(event_df['return_50'])
corr_100 = event_df['order_ratio_shift_magnitude'].corr(event_df['return_100'])
corr_500 = event_df['order_ratio_shift_magnitude'].corr(event_df['return_500'])
corr_1000 = event_df['order_ratio_shift_magnitude'].corr(event_df['return_1000'])
corr_2000 = event_df['order_ratio_shift_magnitude'].corr(event_df['return_2000'])

print(f"Short-term:")
print(f"  20 snapshots (~4 sec):     {corr_20:+.4f}")
print(f"  50 snapshots (~10 sec):    {corr_50:+.4f}")
print(f"  100 snapshots (~20 sec):   {corr_100:+.4f}")
print()
print(f"Long-term:")
print(f"  500 snapshots (~2 min):    {corr_500:+.4f}")
print(f"  1000 snapshots (~4 min):   {corr_1000:+.4f}")
print(f"  2000 snapshots (~8 min):   {corr_2000:+.4f}")
print()

# =============================================================================
# 5. DIRECTIONAL ANALYSIS
# =============================================================================
print("="*100)
print("DIRECTIONAL ANALYSIS: Bid Surge vs Ask Surge")
print("="*100)
print()

bid_surge = event_df[event_df['order_ratio_change'] > 0.02]
ask_surge = event_df[event_df['order_ratio_change'] < -0.02]

print(f"Bid Ratio Surge (ratio increases >0.02):")
print(f"  Count: {len(bid_surge)}")
print(f"  Short-term: 20-snap: {bid_surge['return_20'].mean():+.2f}, 100-snap: {bid_surge['return_100'].mean():+.2f}")
print(f"  Long-term:  500-snap: {bid_surge['return_500'].mean():+.2f}, 1000-snap: {bid_surge['return_1000'].mean():+.2f}, 2000-snap: {bid_surge['return_2000'].mean():+.2f}")
print()

print(f"Ask Ratio Surge (ratio decreases >0.02):")
print(f"  Count: {len(ask_surge)}")
print(f"  Short-term: 20-snap: {ask_surge['return_20'].mean():+.2f}, 100-snap: {ask_surge['return_100'].mean():+.2f}")
print(f"  Long-term:  500-snap: {ask_surge['return_500'].mean():+.2f}, 1000-snap: {ask_surge['return_1000'].mean():+.2f}, 2000-snap: {ask_surge['return_2000'].mean():+.2f}")
print()

# =============================================================================
# 6. SUMMARY
# =============================================================================
print("="*100)
print("SUMMARY: The Complete 14:19-14:28 Move")
print("="*100)
print()

price_low_idx = event_df['mid_price'].idxmin()
price_high_idx = event_df['mid_price'].idxmax()

price_low = event_df.loc[price_low_idx, 'mid_price']
price_high = event_df.loc[price_high_idx, 'mid_price']
time_low = event_df.loc[price_low_idx, 'time_str']
time_high = event_df.loc[price_high_idx, 'time_str']

ratio_at_low = event_df.loc[price_low_idx, 'order_ratio']
ratio_at_high = event_df.loc[price_high_idx, 'order_ratio']

total_move = price_high - price_low

print(f"Price Low:  {price_low:.2f} at {time_low}")
print(f"Price High: {price_high:.2f} at {time_high}")
print(f"Total Move: {total_move:+.2f} points")
print()
print(f"Order Ratio at Low:  {ratio_at_low:.4f}")
print(f"Order Ratio at High: {ratio_at_high:.4f}")
print(f"Ratio Change:        {ratio_at_high - ratio_at_low:+.4f}")
print()

# Time analysis
start_price = event_df.iloc[0]['mid_price']
start_time = event_df.iloc[0]['time_str']
end_price = event_df.iloc[-1]['mid_price']
end_time = event_df.iloc[-1]['time_str']

print(f"Start ({start_time}): {start_price:.2f}")
print(f"End   ({end_time}): {end_price:.2f}")
print(f"Net Change:          {end_price - start_price:+.2f} points")
print()

print("="*100)
print("ANALYSIS COMPLETE")
print("="*100)
