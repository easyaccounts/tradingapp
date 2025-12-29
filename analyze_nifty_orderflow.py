import pandas as pd
import numpy as np
from datetime import datetime
import pytz

# Load the data
df = pd.read_csv('nifty_futures_today_all_ticks.csv')
df['time'] = pd.to_datetime(df['time'])

# Convert UTC to IST for display
ist = pytz.timezone('Asia/Kolkata')
df['time_ist'] = df['time'].dt.tz_convert(ist)
df['time_display'] = df['time_ist'].dt.strftime('%Y-%m-%d %H:%M:%S IST')

print("=" * 80)
print("NIFTY FUTURES ORDERFLOW ANALYSIS (Today's Session)")
print("=" * 80)
print(f"\nInstrument: {df['trading_symbol'].iloc[0]}")
print(f"Session: {df['time_ist'].min().strftime('%d-%b-%Y')} (IST)")
print(f"Time Range: {df['time_ist'].min().strftime('%H:%M:%S')} to {df['time_ist'].max().strftime('%H:%M:%S')} IST")
print(f"Total Ticks: {len(df)}")

# Price summary
print(f"\nPRICE SUMMARY:")
print(f"  Current Price: ₹{df['last_price'].iloc[0]:,.2f}")
print(f"  High: ₹{df['last_price'].max():,.2f}")
print(f"  Low: ₹{df['last_price'].min():,.2f}")
print(f"  Range: ₹{df['last_price'].max() - df['last_price'].min():,.2f}")
print(f"  Price Change: ₹{df['last_price'].iloc[0] - df['last_price'].iloc[-1]:,.2f}")

# Volume analysis
trade_ticks = df[df['volume_delta'] > 0]
print(f"\nVOLUME ANALYSIS:")
print(f"  Total Volume: {df['volume_delta'].sum():,} contracts")
print(f"  Trade Ticks: {len(trade_ticks)} ({len(trade_ticks)/len(df)*100:.1f}%)")
print(f"  Quote Ticks: {len(df) - len(trade_ticks)} ({(len(df)-len(trade_ticks))/len(df)*100:.1f}%)")
print(f"  Avg Trade Size: {trade_ticks['volume_delta'].mean():.0f} contracts")
print(f"  Max Trade Size: {trade_ticks['volume_delta'].max():.0f} contracts")

# Aggressor side analysis
buy_trades = df[df['aggressor_side'] == 'BUY']
sell_trades = df[df['aggressor_side'] == 'SELL']
neutral = df[df['aggressor_side'] == 'NEUTRAL']

buy_volume = buy_trades['volume_delta'].sum()
sell_volume = sell_trades['volume_delta'].sum()
total_traded = buy_volume + sell_volume

print(f"\nORDERFLOW BREAKDOWN:")
print(f"  Buy Trades: {len(buy_trades)} ticks | {buy_volume:,} contracts ({buy_volume/total_traded*100:.1f}%)")
print(f"  Sell Trades: {len(sell_trades)} ticks | {sell_volume:,} contracts ({sell_volume/total_traded*100:.1f}%)")
print(f"  Neutral Ticks: {len(neutral)} ticks")

# CVD analysis
cumulative_delta = df['cvd_change'].sum()
print(f"\nCUMULATIVE VOLUME DELTA (CVD):")
print(f"  Net CVD: {cumulative_delta:+,} contracts")
print(f"  Direction: {'🟢 BULLISH' if cumulative_delta > 0 else '🔴 BEARISH' if cumulative_delta < 0 else '⚪ NEUTRAL'}")
print(f"  Buy Pressure: {buy_volume:,} contracts")
print(f"  Sell Pressure: {sell_volume:,} contracts")
print(f"  Imbalance: {abs(buy_volume - sell_volume):,} contracts")

# OI analysis
oi_change = df['oi_delta'].sum()
current_oi = df['oi'].iloc[0]
print(f"\nOPEN INTEREST ANALYSIS:")
print(f"  Current OI: {current_oi:,} contracts")
print(f"  OI Change: {oi_change:+,} contracts")
print(f"  OI % Change: {(oi_change/current_oi)*100:+.2f}%")

# Price + OI interpretation
if cumulative_delta > 0 and oi_change > 0:
    interpretation = "🟢 LONG BUILD-UP (Price Up + OI Up = Bullish)"
elif cumulative_delta < 0 and oi_change > 0:
    interpretation = "🔴 SHORT BUILD-UP (Price Down + OI Up = Bearish)"
elif cumulative_delta > 0 and oi_change < 0:
    interpretation = "🟡 SHORT COVERING (Price Up + OI Down = Bearish Unwinding)"
elif cumulative_delta < 0 and oi_change < 0:
    interpretation = "🟡 LONG UNWINDING (Price Down + OI Down = Bullish Unwinding)"
else:
    interpretation = "⚪ NEUTRAL (No Clear Trend)"

print(f"  Interpretation: {interpretation}")

# Toxicity analysis
toxic_ticks = df[df['depth_toxicity_tick'] < 1.0]
high_toxicity = df[df['depth_toxicity_tick'] < 0.1]

print(f"\nDEPTH TOXICITY ANALYSIS:")
print(f"  Avg Toxicity: {df['depth_toxicity_tick'].mean():.4f}")
print(f"  Toxic Ticks (<1.0): {len(toxic_ticks)} ({len(toxic_ticks)/len(df)*100:.1f}%)")
print(f"  High Toxicity (<0.1): {len(high_toxicity)} ({len(high_toxicity)/len(df)*100:.1f}%)")
print(f"  Interpretation: {'🔴 High informed trading' if df['depth_toxicity_tick'].mean() < 0.5 else '🟢 Low informed trading'}")

# Kyle Lambda analysis
kyle_trades = df[df['kyle_lambda_tick'] > 0]
print(f"\nKYLE LAMBDA (Market Impact):")
print(f"  Avg Kyle Lambda: {kyle_trades['kyle_lambda_tick'].mean():.6f}")
print(f"  Max Kyle Lambda: {kyle_trades['kyle_lambda_tick'].max():.6f}")
print(f"  Interpretation: {'🟢 Low impact (liquid)' if kyle_trades['kyle_lambda_tick'].mean() < 0.0001 else '🔴 High impact (illiquid)'}")

# Time-based analysis (5-minute buckets)
df['minute_bucket'] = df['time'].dt.floor('5min')
time_analysis = df.groupby('minute_bucket').agg({
    'volume_delta': 'sum',
    'cvd_change': 'sum',
    'oi_delta': 'sum',
    'last_price': ['first', 'last', 'min', 'max']
}).round(2)

print(f"\n5-MINUTE INTERVAL ANALYSIS:")
print(time_analysis.tail(10))

# Most active price levels
price_levels = df[df['volume_delta'] > 0].groupby('last_price').agg({
    'volume_delta': 'sum',
    'aggressor_side': lambda x: (x == 'BUY').sum() - (x == 'SELL').sum()
}).sort_values('volume_delta', ascending=False).head(10)
price_levels.columns = ['Volume', 'Buy-Sell Imbalance']

print(f"\nTOP 10 MOST ACTIVE PRICE LEVELS:")
print(price_levels)

# Recent trend (last 100 ticks vs previous 100)
recent_100 = df.head(100)
previous_100 = df.iloc[100:200]

print(f"\nRECENT TREND (Last 100 vs Previous 100 ticks):")
print(f"  Recent CVD: {recent_100['cvd_change'].sum():+,} contracts")
print(f"  Previous CVD: {previous_100['cvd_change'].sum():+,} contracts")
print(f"  Recent Avg Price: ₹{recent_100['last_price'].mean():.2f}")
print(f"  Previous Avg Price: ₹{previous_100['last_price'].mean():.2f}")
print(f"  Momentum: {'🟢 ACCELERATING BULLISH' if recent_100['cvd_change'].sum() > previous_100['cvd_change'].sum() else '🔴 ACCELERATING BEARISH'}")

print("\n" + "=" * 80)
print("TRADING SIGNALS:")
print("=" * 80)

# Generate signals
signals = []
if cumulative_delta > 1000:
    signals.append("🟢 STRONG BUY PRESSURE - Net buyers dominating")
elif cumulative_delta < -1000:
    signals.append("🔴 STRONG SELL PRESSURE - Net sellers dominating")

if oi_change > 50000:
    signals.append("🟢 FRESH LONGS ENTERING - New positions building")
elif oi_change < -50000:
    signals.append("🔴 POSITION UNWINDING - Traders exiting")

if df['depth_toxicity_tick'].mean() < 0.3:
    signals.append("⚠️ HIGH TOXICITY - Informed traders active")

if kyle_trades['kyle_lambda_tick'].mean() > 0.0005:
    signals.append("⚠️ HIGH MARKET IMPACT - Low liquidity or large orders")

if len(signals) > 0:
    for signal in signals:
        print(f"  {signal}")
else:
    print("  ⚪ NO CLEAR SIGNALS - Market in consolidation")

print("\n" + "=" * 80)
