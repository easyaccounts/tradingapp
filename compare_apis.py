import pandas as pd
import numpy as np
import pytz
from datetime import datetime

print("=" * 100)
print("KITECONNECT vs DHAN API COMPARISON")
print("=" * 100)

# Load data
dhan_df = pd.read_csv('dhan_nifty_futures_ticks.csv')
kite_df = pd.read_csv('kiteconnect_nifty_futures_ticks.csv')

# Parse timestamps
dhan_df['time'] = pd.to_datetime(dhan_df['time'])
kite_df['time'] = pd.to_datetime(kite_df['time'])

# Convert KiteConnect UTC to IST for comparison
ist = pytz.timezone('Asia/Kolkata')
if kite_df['time'].dt.tz is None:
    kite_df['time'] = kite_df['time'].dt.tz_localize('UTC').dt.tz_convert(ist)
else:
    kite_df['time'] = kite_df['time'].dt.tz_convert(ist)

if dhan_df['time'].dt.tz is None:
    dhan_df['time'] = dhan_df['time'].dt.tz_localize(ist)
else:
    dhan_df['time'] = dhan_df['time'].dt.tz_convert(ist)

print(f"\n📊 BASIC STATISTICS")
print("=" * 100)
print(f"{'Metric':<30} {'KiteConnect':<25} {'Dhan':<25} {'Difference':<20}")
print("-" * 100)

# Tick counts
kite_count = len(kite_df)
dhan_count = len(dhan_df)
print(f"{'Total Ticks':<30} {kite_count:<25,} {dhan_count:<25,} {dhan_count - kite_count:+,}")

# Time range
kite_start = kite_df['time'].min()
kite_end = kite_df['time'].max()
dhan_start = dhan_df['time'].min()
dhan_end = dhan_df['time'].max()

print(f"{'Start Time':<30} {kite_start.strftime('%H:%M:%S'):<25} {dhan_start.strftime('%H:%M:%S'):<25}")
print(f"{'End Time':<30} {kite_end.strftime('%H:%M:%S'):<25} {dhan_end.strftime('%H:%M:%S'):<25}")

# Duration
kite_duration = (kite_end - kite_start).total_seconds()
dhan_duration = (dhan_end - dhan_start).total_seconds()
print(f"{'Duration (seconds)':<30} {kite_duration:<25.1f} {dhan_duration:<25.1f} {dhan_duration - kite_duration:+.1f}")

# Tick rate
kite_rate = kite_count / kite_duration if kite_duration > 0 else 0
dhan_rate = dhan_count / dhan_duration if dhan_duration > 0 else 0
print(f"{'Average Tick Rate':<30} {kite_rate:<25.2f} {dhan_rate:<25.2f} {dhan_rate - kite_rate:+.2f}")

print(f"\n💰 PRICE DATA QUALITY")
print("=" * 100)

# Price comparison
kite_ltp = kite_df['last_price'].dropna()
dhan_ltp = dhan_df['ltp'].dropna()

print(f"{'Metric':<30} {'KiteConnect':<25} {'Dhan':<25}")
print("-" * 100)
print(f"{'Price Updates':<30} {len(kite_ltp):<25,} {len(dhan_ltp):<25,}")
print(f"{'Min Price':<30} ₹{kite_ltp.min():<24,.2f} ₹{dhan_ltp.min():<24,.2f}")
print(f"{'Max Price':<30} ₹{kite_ltp.max():<24,.2f} ₹{dhan_ltp.max():<24,.2f}")
print(f"{'Avg Price':<30} ₹{kite_ltp.mean():<24,.2f} ₹{dhan_ltp.mean():<24,.2f}")
print(f"{'Price Range':<30} ₹{kite_ltp.max() - kite_ltp.min():<24,.2f} ₹{dhan_ltp.max() - dhan_ltp.min():<24,.2f}")

# Unique prices
kite_unique = kite_ltp.nunique()
dhan_unique = dhan_ltp.nunique()
print(f"{'Unique Price Levels':<30} {kite_unique:<25,} {dhan_unique:<25,}")

print(f"\n📊 VOLUME & OI DATA")
print("=" * 100)
print(f"{'Metric':<30} {'KiteConnect':<25} {'Dhan':<25}")
print("-" * 100)

# Volume
kite_vol_start = kite_df['volume_traded'].iloc[0]
kite_vol_end = kite_df['volume_traded'].iloc[-1]
dhan_vol_start = dhan_df['volume'].iloc[0]
dhan_vol_end = dhan_df['volume'].iloc[-1]

print(f"{'Volume Start':<30} {kite_vol_start:<25,} {dhan_vol_start:<25,}")
print(f"{'Volume End':<30} {kite_vol_end:<25,} {dhan_vol_end:<25,}")
print(f"{'Volume Change':<30} {kite_vol_end - kite_vol_start:<25,} {dhan_vol_end - dhan_vol_start:<25,}")

# OI
kite_oi_start = kite_df['oi'].iloc[0]
kite_oi_end = kite_df['oi'].iloc[-1]
dhan_oi_start = dhan_df['oi'].iloc[0] if not pd.isna(dhan_df['oi'].iloc[0]) else dhan_df['oi'].dropna().iloc[0]
dhan_oi_end = dhan_df['oi'].iloc[-1] if not pd.isna(dhan_df['oi'].iloc[-1]) else dhan_df['oi'].dropna().iloc[-1]

print(f"{'OI Start':<30} {kite_oi_start:<25,} {dhan_oi_start:<25,.0f}")
print(f"{'OI End':<30} {kite_oi_end:<25,} {dhan_oi_end:<25,.0f}")
print(f"{'OI Change':<30} {kite_oi_end - kite_oi_start:<25,} {int(dhan_oi_end - dhan_oi_start):<25,}")

print(f"\n📈 MARKET DEPTH QUALITY")
print("=" * 100)
print(f"{'Metric':<30} {'KiteConnect':<25} {'Dhan':<25}")
print("-" * 100)

# Market depth availability
kite_has_depth = kite_df['bid_price_1'].notna().sum()
dhan_has_depth = dhan_df['bid_price_1'].notna().sum()
print(f"{'Ticks with Depth Data':<30} {kite_has_depth:<25,} {dhan_has_depth:<25,}")
print(f"{'Depth Coverage %':<30} {kite_has_depth/kite_count*100:<25.1f} {dhan_has_depth/dhan_count*100:<25.1f}")

# Bid-Ask spread analysis
kite_df['spread'] = kite_df['ask_price_1'] - kite_df['bid_price_1']
dhan_df['spread'] = dhan_df['ask_price_1'] - dhan_df['bid_price_1']

kite_spread = kite_df['spread'].dropna()
dhan_spread = dhan_df['spread'].dropna()

print(f"{'Avg Bid-Ask Spread':<30} ₹{kite_spread.mean():<24,.2f} ₹{dhan_spread.mean():<24,.2f}")
print(f"{'Min Spread':<30} ₹{kite_spread.min():<24,.2f} ₹{dhan_spread.min():<24,.2f}")
print(f"{'Max Spread':<30} ₹{kite_spread.max():<24,.2f} ₹{dhan_spread.max():<24,.2f}")

# Order counts
kite_has_orders = kite_df['bid_orders_1'].notna().sum()
dhan_has_orders = dhan_df['bid_orders_1'].notna().sum()
print(f"{'Ticks with Order Count':<30} {kite_has_orders:<25,} {dhan_has_orders:<25,}")

if kite_has_orders > 0:
    kite_avg_bid_orders = kite_df['bid_orders_1'].dropna().mean()
    kite_avg_ask_orders = kite_df['ask_orders_1'].dropna().mean()
    print(f"{'Avg Bid Orders (L1)':<30} {kite_avg_bid_orders:<25.1f}", end='')
else:
    print(f"{'Avg Bid Orders (L1)':<30} {'N/A':<25}", end='')

if dhan_has_orders > 0:
    dhan_avg_bid_orders = dhan_df['bid_orders_1'].dropna().mean()
    dhan_avg_ask_orders = dhan_df['ask_orders_1'].dropna().mean()
    print(f" {dhan_avg_bid_orders:<25.1f}")
    print(f"{'Avg Ask Orders (L1)':<30} {kite_avg_ask_orders:<25.1f} {dhan_avg_ask_orders:<25.1f}")
else:
    print(f" {'N/A':<25}")

# Total depth quantities
kite_df['total_bid_depth'] = kite_df[['bid_qty_1', 'bid_qty_2', 'bid_qty_3', 'bid_qty_4', 'bid_qty_5']].sum(axis=1)
kite_df['total_ask_depth'] = kite_df[['ask_qty_1', 'ask_qty_2', 'ask_qty_3', 'ask_qty_4', 'ask_qty_5']].sum(axis=1)
dhan_df['total_bid_depth'] = dhan_df[['bid_qty_1', 'bid_qty_2', 'bid_qty_3', 'bid_qty_4', 'bid_qty_5']].sum(axis=1)
dhan_df['total_ask_depth'] = dhan_df[['ask_qty_1', 'ask_qty_2', 'ask_qty_3', 'ask_qty_4', 'ask_qty_5']].sum(axis=1)

kite_avg_bid_depth = kite_df['total_bid_depth'].mean()
dhan_avg_bid_depth = dhan_df['total_bid_depth'].mean()
print(f"{'Avg Total Bid Depth (5L)':<30} {kite_avg_bid_depth:<25,.0f} {dhan_avg_bid_depth:<25,.0f}")

kite_avg_ask_depth = kite_df['total_ask_depth'].mean()
dhan_avg_ask_depth = dhan_df['total_ask_depth'].mean()
print(f"{'Avg Total Ask Depth (5L)':<30} {kite_avg_ask_depth:<25,.0f} {dhan_avg_ask_depth:<25,.0f}")

print(f"\n🔍 DATA COMPLETENESS")
print("=" * 100)
print(f"{'Field':<30} {'KiteConnect %':<25} {'Dhan %':<25}")
print("-" * 100)

fields_comparison = [
    ('last_price/ltp', 'last_price', 'ltp'),
    ('volume_traded/volume', 'volume_traded', 'volume'),
    ('oi', 'oi', 'oi'),
    ('bid_price_1', 'bid_price_1', 'bid_price_1'),
    ('ask_price_1', 'ask_price_1', 'ask_price_1'),
    ('bid_orders_1', 'bid_orders_1', 'bid_orders_1'),
]

for label, kite_col, dhan_col in fields_comparison:
    kite_pct = (kite_df[kite_col].notna().sum() / kite_count * 100)
    dhan_pct = (dhan_df[dhan_col].notna().sum() / dhan_count * 100)
    print(f"{label:<30} {kite_pct:<25.1f} {dhan_pct:<25.1f}")

print(f"\n⚡ UPDATE FREQUENCY ANALYSIS")
print("=" * 100)

# Calculate time between ticks
kite_df['time_diff'] = kite_df['time'].diff().dt.total_seconds()
dhan_df['time_diff'] = dhan_df['time'].diff().dt.total_seconds()

kite_time_diff = kite_df['time_diff'].dropna()
dhan_time_diff = dhan_df['time_diff'].dropna()

print(f"{'Metric':<30} {'KiteConnect':<25} {'Dhan':<25}")
print("-" * 100)
print(f"{'Avg Time Between Ticks':<30} {kite_time_diff.mean():<25.3f} {dhan_time_diff.mean():<25.3f}")
print(f"{'Median Time Between Ticks':<30} {kite_time_diff.median():<25.3f} {dhan_time_diff.median():<25.3f}")
print(f"{'Min Time Between Ticks':<30} {kite_time_diff.min():<25.3f} {dhan_time_diff.min():<25.3f}")
print(f"{'Max Time Between Ticks':<30} {kite_time_diff.max():<25.3f} {dhan_time_diff.max():<25.3f}")

# Tick distribution
print(f"\n📊 TICK DISTRIBUTION (seconds between ticks)")
print("-" * 100)
bins = [0, 0.1, 0.5, 1, 2, 5, 10, float('inf')]
labels = ['<0.1s', '0.1-0.5s', '0.5-1s', '1-2s', '2-5s', '5-10s', '>10s']

kite_dist = pd.cut(kite_time_diff, bins=bins, labels=labels).value_counts().sort_index()
dhan_dist = pd.cut(dhan_time_diff, bins=bins, labels=labels).value_counts().sort_index()

print(f"{'Interval':<15} {'KiteConnect':<20} {'Dhan':<20}")
print("-" * 55)
for label in labels:
    kite_val = kite_dist.get(label, 0)
    dhan_val = dhan_dist.get(label, 0)
    kite_pct = (kite_val / len(kite_time_diff) * 100) if len(kite_time_diff) > 0 else 0
    dhan_pct = (dhan_val / len(dhan_time_diff) * 100) if len(dhan_time_diff) > 0 else 0
    print(f"{label:<15} {kite_val:>6} ({kite_pct:>5.1f}%)   {dhan_val:>6} ({dhan_pct:>5.1f}%)")

print(f"\n✅ SUMMARY & VERDICT")
print("=" * 100)

# Calculate scores
kite_score = 0
dhan_score = 0

# More ticks = better
if dhan_count > kite_count:
    dhan_score += 1
    print(f"✓ Dhan has {dhan_count - kite_count:,} more ticks ({(dhan_count/kite_count - 1)*100:.1f}% more)")
else:
    kite_score += 1

# Order count data
if dhan_has_orders > kite_has_orders:
    dhan_score += 1
    print(f"✓ Dhan provides order count data on {dhan_has_orders:,} ticks vs {kite_has_orders:,}")
elif kite_has_orders > dhan_has_orders:
    kite_score += 1
    print(f"✓ KiteConnect provides order count data on {kite_has_orders:,} ticks vs {dhan_has_orders:,}")

# Spread quality
if dhan_spread.mean() <= kite_spread.mean():
    dhan_score += 1
    print(f"✓ Dhan has tighter average spread: ₹{dhan_spread.mean():.2f} vs ₹{kite_spread.mean():.2f}")
else:
    kite_score += 1
    print(f"✓ KiteConnect has tighter average spread: ₹{kite_spread.mean():.2f} vs ₹{dhan_spread.mean():.2f}")

# Update frequency
if dhan_rate > kite_rate:
    dhan_score += 1
    print(f"✓ Dhan has faster tick rate: {dhan_rate:.2f} vs {kite_rate:.2f} ticks/sec")
else:
    kite_score += 1
    print(f"✓ KiteConnect has faster tick rate: {kite_rate:.2f} vs {dhan_rate:.2f} ticks/sec")

print(f"\n🏆 FINAL SCORE: KiteConnect {kite_score} - {dhan_score} Dhan")

if dhan_score > kite_score:
    print("🥇 Winner: DHAN API provides better tick data quality")
elif kite_score > dhan_score:
    print("🥇 Winner: KITECONNECT provides better tick data quality")
else:
    print("🤝 TIE: Both APIs provide comparable data quality")

print("\n" + "=" * 100)
