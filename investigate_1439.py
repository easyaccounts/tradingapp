"""
Investigate what happened around 14:39
"""

import psycopg2
import pandas as pd
import pytz

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tradingdb',
    'user': 'tradinguser',
    'password': '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
}

IST = pytz.timezone('Asia/Kolkata')

print("Investigating 14:39 timeframe...")
print("=" * 100)
print()

conn = psycopg2.connect(**DB_CONFIG)

# Get data around 14:39 (±5 minutes)
query = """
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
    bid_vwap,
    ask_vwap,
    bid_50pct_level,
    ask_50pct_level
FROM depth_200_snapshots
WHERE timestamp AT TIME ZONE 'Asia/Kolkata' >= '2025-12-29 14:34:00'
  AND timestamp AT TIME ZONE 'Asia/Kolkata' <= '2025-12-29 14:44:00'
ORDER BY timestamp ASC
"""

df = pd.read_sql_query(query, conn)
conn.close()

print(f"Loaded {len(df)} snapshots from 14:34 to 14:44")
print()

# Calculate key metrics
df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2
df['total_liquidity'] = df['total_bid_qty'] + df['total_ask_qty']
df['total_orders'] = df['total_bid_orders'] + df['total_ask_orders']
df['bid_vwap_dist'] = df['best_bid'] - df['bid_vwap']

# Price movement
df['price_change'] = df['mid_price'].diff()
df['cumulative_change'] = df['mid_price'] - df['mid_price'].iloc[0]

# Find key events
print("KEY EVENTS DETECTED:")
print("-" * 100)

# Significant price moves
large_moves = df[abs(df['price_change']) > 2]
if len(large_moves) > 0:
    print("\n1. LARGE PRICE MOVES (>₹2):")
    for idx, row in large_moves.iterrows():
        print(f"   [{row['time_ist'].strftime('%H:%M:%S')}] "
              f"Price: ₹{row['mid_price']:.2f} ({row['price_change']:+.2f}), "
              f"Imbalance: {row['imbalance_ratio']:.2f}, Spread: ₹{row['spread']:.2f}")

# Liquidity spikes
liquidity_threshold = df['total_liquidity'].quantile(0.90)
liquidity_spikes = df[df['total_liquidity'] > liquidity_threshold]
if len(liquidity_spikes) > 0:
    print(f"\n2. LIQUIDITY SPIKES (>90th percentile = {liquidity_threshold:.0f}):")
    for idx, row in liquidity_spikes.head(10).iterrows():
        print(f"   [{row['time_ist'].strftime('%H:%M:%S')}] "
              f"Liquidity: {row['total_liquidity']:,.0f}, "
              f"Price: ₹{row['mid_price']:.2f}, "
              f"Imbalance: {row['imbalance_ratio']:.2f}")

# Imbalance extremes
extreme_imbalance = df[(df['imbalance_ratio'] < 0.85) | (df['imbalance_ratio'] > 1.15)]
if len(extreme_imbalance) > 0:
    print("\n3. EXTREME IMBALANCE EVENTS:")
    for idx, row in extreme_imbalance.head(10).iterrows():
        regime = "ASK-HEAVY" if row['imbalance_ratio'] < 0.85 else "BID-HEAVY"
        print(f"   [{row['time_ist'].strftime('%H:%M:%S')}] "
              f"{regime}: {row['imbalance_ratio']:.2f}, "
              f"Price: ₹{row['mid_price']:.2f}, "
              f"Spread: ₹{row['spread']:.2f}")

# Spread spikes
spread_threshold = df['spread'].quantile(0.90)
spread_spikes = df[df['spread'] > spread_threshold]
if len(spread_spikes) > 0:
    print(f"\n4. SPREAD SPIKES (>90th percentile = ₹{spread_threshold:.2f}):")
    for idx, row in spread_spikes.head(10).iterrows():
        print(f"   [{row['time_ist'].strftime('%H:%M:%S')}] "
              f"Spread: ₹{row['spread']:.2f}, "
              f"Price: ₹{row['mid_price']:.2f}, "
              f"Imbalance: {row['imbalance_ratio']:.2f}")

print()
print("=" * 100)
print("MINUTE-BY-MINUTE SUMMARY:")
print("=" * 100)
print()

# Group by minute and show summary
df['minute'] = df['time_ist'].dt.floor('T')
minute_summary = df.groupby('minute').agg({
    'mid_price': ['first', 'last', 'min', 'max'],
    'total_liquidity': ['mean', 'max'],
    'imbalance_ratio': ['mean', 'min', 'max'],
    'spread': ['mean', 'max'],
    'total_orders': 'mean',
    'time_ist': 'count'
}).round(2)

minute_summary.columns = ['Open', 'Close', 'Low', 'High', 'Avg_Liq', 'Max_Liq', 
                          'Avg_Imb', 'Min_Imb', 'Max_Imb', 'Avg_Spread', 'Max_Spread', 
                          'Avg_Orders', 'Snapshots']

# Calculate price change per minute
minute_summary['Change'] = minute_summary['Close'] - minute_summary['Open']

print(minute_summary.to_string())

print()
print("=" * 100)
print("AROUND 14:39 SPECIFICALLY:")
print("=" * 100)
print()

# Focus on 14:39
focus_time = df[
    (df['time_ist'] >= pd.Timestamp('2025-12-29 14:38:30', tz=IST)) &
    (df['time_ist'] <= pd.Timestamp('2025-12-29 14:39:30', tz=IST))
]

if len(focus_time) > 0:
    print(f"Before 14:39:")
    print(f"  Price: ₹{focus_time.iloc[0]['mid_price']:.2f}")
    print(f"  Liquidity: {focus_time.iloc[0]['total_liquidity']:,.0f}")
    print(f"  Imbalance: {focus_time.iloc[0]['imbalance_ratio']:.2f}")
    print(f"  Spread: ₹{focus_time.iloc[0]['spread']:.2f}")
    print()
    print(f"After 14:39:")
    print(f"  Price: ₹{focus_time.iloc[-1]['mid_price']:.2f} ({focus_time.iloc[-1]['mid_price'] - focus_time.iloc[0]['mid_price']:+.2f})")
    print(f"  Liquidity: {focus_time.iloc[-1]['total_liquidity']:,.0f} ({focus_time.iloc[-1]['total_liquidity'] - focus_time.iloc[0]['total_liquidity']:+,.0f})")
    print(f"  Imbalance: {focus_time.iloc[-1]['imbalance_ratio']:.2f} ({focus_time.iloc[-1]['imbalance_ratio'] - focus_time.iloc[0]['imbalance_ratio']:+.2f})")
    print(f"  Spread: ₹{focus_time.iloc[-1]['spread']:.2f} ({focus_time.iloc[-1]['spread'] - focus_time.iloc[0]['spread']:+.2f})")
    
    print()
    print("Snapshots during 14:39:")
    print("-" * 100)
    for idx, row in focus_time.iterrows():
        print(f"[{row['time_ist'].strftime('%H:%M:%S.%f')[:-3]}] "
              f"Price: ₹{row['mid_price']:.2f} ({row['price_change']:+.2f}), "
              f"Imb: {row['imbalance_ratio']:.2f}, "
              f"Spread: ₹{row['spread']:.2f}, "
              f"Liq: {row['total_liquidity']:,.0f}")
else:
    print("No data found for 14:39 exactly")

print()
print("=" * 100)
