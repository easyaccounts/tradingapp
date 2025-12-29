"""
Real-time trading signals from 200-level depth data
"""
import pandas as pd
import numpy as np

# Read the depth data
df = pd.read_csv('dhan_200depth_nifty_futures.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2

print("="*80)
print("TRADING SIGNALS FROM 200-LEVEL DEPTH ANALYSIS")
print("="*80)
print(f"\nTotal Snapshots: {len(df)}")
print(f"Duration: {(df['timestamp'].max() - df['timestamp'].min()).total_seconds():.0f} seconds")
print(f"Time Range: {df['timestamp'].min().strftime('%H:%M:%S')} - {df['timestamp'].max().strftime('%H:%M:%S')}")

# Price movement
price_change = df['mid_price'].iloc[-1] - df['mid_price'].iloc[0]
price_pct = 100 * price_change / df['mid_price'].iloc[0]
print(f"\nPrice Movement: ₹{df['mid_price'].iloc[0]:.2f} → ₹{df['mid_price'].iloc[-1]:.2f}")
print(f"Change: ₹{price_change:.2f} ({price_pct:+.3f}%)")

# Calculate price changes
df['price_change'] = df['mid_price'].diff()
df['price_change_pct'] = df['price_change'] / df['mid_price'].shift(1) * 100

# Calculate rolling metrics
df['imbalance_ma5'] = df['imbalance_ratio'].rolling(5).mean()
df['imbalance_ma20'] = df['imbalance_ratio'].rolling(20).mean()
df['spread_ma5'] = df['spread'].rolling(5).mean()

print("\n" + "="*80)
print("SIGNAL 1: IMBALANCE DIVERGENCE (Price vs Order Flow)")
print("="*80)

# Find divergences where price went up but imbalance went down (absorption)
df['imbalance_change'] = df['imbalance_ratio'].diff(5)  # Change over 5 snapshots
df['price_change_5'] = df['mid_price'].diff(5)

# Strong absorption: price up, imbalance down
absorption_signals = df[
    (df['price_change_5'] > 0.5) &  # Price rising
    (df['imbalance_change'] < -0.05)  # Imbalance falling (more asks)
].copy()

print(f"\nAbsorption Events Found: {len(absorption_signals)}")
print("(Price rising while asks increase = Institutional buying into supply)\n")

if len(absorption_signals) > 0:
    print("Top 5 Absorption Signals (Bullish):")
    absorption_signals['strength'] = absorption_signals['price_change_5'] * abs(absorption_signals['imbalance_change'])
    top_absorption = absorption_signals.nlargest(5, 'strength')
    
    for idx, row in top_absorption.iterrows():
        print(f"\n  [{row['timestamp'].strftime('%H:%M:%S')}]")
        print(f"    Price: ₹{row['mid_price']:.2f} (up ₹{row['price_change_5']:.2f})")
        print(f"    Imbalance: {row['imbalance_ratio']:.2f} (down {abs(row['imbalance_change']):.3f})")
        print(f"    Spread: ₹{row['spread']:.2f}")
        print(f"    → SIGNAL: BUY (Institutions absorbing selling pressure)")

# Distribution: price down, imbalance up
distribution_signals = df[
    (df['price_change_5'] < -0.5) &  # Price falling
    (df['imbalance_change'] > 0.05)  # Imbalance rising (more bids)
].copy()

print(f"\n\nDistribution Events Found: {len(distribution_signals)}")
print("(Price falling while bids increase = Institutional selling into demand)\n")

if len(distribution_signals) > 0:
    print("Top 5 Distribution Signals (Bearish):")
    distribution_signals['strength'] = abs(distribution_signals['price_change_5']) * distribution_signals['imbalance_change']
    top_distribution = distribution_signals.nlargest(5, 'strength')
    
    for idx, row in top_distribution.iterrows():
        print(f"\n  [{row['timestamp'].strftime('%H:%M:%S')}]")
        print(f"    Price: ₹{row['mid_price']:.2f} (down ₹{abs(row['price_change_5']):.2f})")
        print(f"    Imbalance: {row['imbalance_ratio']:.2f} (up {row['imbalance_change']:.3f})")
        print(f"    Spread: ₹{row['spread']:.2f}")
        print(f"    → SIGNAL: SELL (Institutions selling into buying pressure)")

print("\n" + "="*80)
print("SIGNAL 2: EXTREME IMBALANCE (Institutional Positioning)")
print("="*80)

# Find extreme imbalances
very_bid_heavy = df[df['imbalance_ratio'] > 1.15]
very_ask_heavy = df[df['imbalance_ratio'] < 0.85]

print(f"\nVery Bid-Heavy (>1.15): {len(very_bid_heavy)} snapshots ({100*len(very_bid_heavy)/len(df):.1f}%)")
if len(very_bid_heavy) > 0:
    print(f"  Average price during: ₹{very_bid_heavy['mid_price'].mean():.2f}")
    print(f"  → SIGNAL: Strong institutional buying interest")

print(f"\nVery Ask-Heavy (<0.85): {len(very_ask_heavy)} snapshots ({100*len(very_ask_heavy)/len(df):.1f}%)")
if len(very_ask_heavy) > 0:
    print(f"  Average price during: ₹{very_ask_heavy['mid_price'].mean():.2f}")
    print(f"  → SIGNAL: Strong institutional selling interest")

# Track imbalance regime changes
df['imbalance_regime'] = 'neutral'
df.loc[df['imbalance_ratio'] > 1.10, 'imbalance_regime'] = 'bid_heavy'
df.loc[df['imbalance_ratio'] < 0.90, 'imbalance_regime'] = 'ask_heavy'

regime_changes = df[df['imbalance_regime'] != df['imbalance_regime'].shift(1)]
print(f"\nImbalance Regime Changes: {len(regime_changes)}")

print("\n" + "="*80)
print("SIGNAL 3: SPREAD WIDENING (Volatility Warning)")
print("="*80)

# Find spread spikes
spread_threshold = df['spread'].quantile(0.75)
wide_spreads = df[df['spread'] > spread_threshold]

print(f"\nWide Spread Events (>₹{spread_threshold:.2f}): {len(wide_spreads)} snapshots")
print(f"Average spread during normal: ₹{df[df['spread'] <= spread_threshold]['spread'].mean():.2f}")
print(f"Average spread during wide: ₹{wide_spreads['spread'].mean():.2f}")

# Check if price moved after spread widening
df['spread_spike'] = (df['spread'] > spread_threshold).astype(int)
df['future_price_change'] = df['mid_price'].shift(-10) - df['mid_price']  # Price change in next 10 snapshots

spread_spike_events = df[df['spread_spike'] == 1].copy()
if len(spread_spike_events) > 0:
    avg_move_after_spike = spread_spike_events['future_price_change'].mean()
    print(f"\nAverage price change after spread spike: ₹{avg_move_after_spike:.2f}")
    
    if abs(avg_move_after_spike) > 0.5:
        direction = "UP" if avg_move_after_spike > 0 else "DOWN"
        print(f"  → SIGNAL: Spread widening preceded {direction} move")
        print(f"  → STRATEGY: Wait for direction confirmation, then follow momentum")

print("\n" + "="*80)
print("SIGNAL 4: VWAP REVERSION (Mean Reversion Opportunities)")
print("="*80)

# Calculate distance from VWAPs
df['dist_from_bid_vwap'] = df['best_bid'] - df['bid_vwap']
df['dist_from_ask_vwap'] = df['best_ask'] - df['ask_vwap']

# Find when price was far from VWAP
far_from_bid_vwap = df[abs(df['dist_from_bid_vwap']) > 30]
far_from_ask_vwap = df[abs(df['dist_from_ask_vwap']) > 30]

print(f"\nBid far from Bid VWAP (>₹30): {len(far_from_bid_vwap)} times")
print(f"Ask far from Ask VWAP (>₹30): {len(far_from_ask_vwap)} times")

if len(far_from_bid_vwap) > 0:
    print(f"\nAverage Bid-VWAP gap: ₹{df['dist_from_bid_vwap'].mean():.2f}")
    print(f"Max gap: ₹{df['dist_from_bid_vwap'].max():.2f}")
    print(f"  → ENTRY ZONE: Around Bid VWAP ± ₹5 for better fills")

print("\n" + "="*80)
print("SIGNAL 5: LIQUIDITY CONCENTRATION SHIFT")
print("="*80)

# Track where 50% volume sits
df['bid_concentration'] = df['bid_50pct_level']
df['ask_concentration'] = df['ask_50pct_level']

print(f"\nBid 50% Volume Level:")
print(f"  Average: {df['bid_concentration'].mean():.0f}")
print(f"  Range: {df['bid_concentration'].min():.0f} - {df['bid_concentration'].max():.0f}")

# Deep liquidity (>100) vs shallow (<70)
deep_liquidity = df[df['bid_concentration'] > 100]
shallow_liquidity = df[df['bid_concentration'] < 70]

print(f"\nDeep Liquidity (>100): {len(deep_liquidity)} snapshots ({100*len(deep_liquidity)/len(df):.1f}%)")
if len(deep_liquidity) > 0:
    print(f"  → Market state: Institutional positioning, more stable")
    print(f"  → Strategy: Trend following, lower risk of flash moves")

print(f"\nShallow Liquidity (<70): {len(shallow_liquidity)} snapshots ({100*len(shallow_liquidity)/len(df):.1f}%)")
if len(shallow_liquidity) > 0:
    print(f"  → Market state: HFT-dominated, choppy")
    print(f"  → Strategy: Reduce size or avoid")

print("\n" + "="*80)
print("PRACTICAL TRADE SETUPS FROM THIS SESSION")
print("="*80)

# Find the best trade setups that occurred
print("\n1. BEST LONG ENTRY OPPORTUNITIES:")

# Long setup: Ask-heavy + tight spread + price near support
long_setups = df[
    (df['imbalance_ratio'] < 0.95) &  # More asks (oversold)
    (df['spread'] < 2.0) &  # Tight spread (low volatility)
    (df['best_bid'] < df['bid_vwap'] + 5)  # Near bid VWAP
].copy()

if len(long_setups) > 5:
    print(f"Found {len(long_setups)} potential long entries\n")
    
    # Show top 3
    long_setups['future_return'] = (df['mid_price'].shift(-20) - df['mid_price']) / df['mid_price'] * 100
    long_setups = long_setups.dropna(subset=['future_return'])
    
    if len(long_setups) > 0:
        best_longs = long_setups.nlargest(3, 'future_return')
        for idx, row in best_longs.iterrows():
            print(f"  [{row['timestamp'].strftime('%H:%M:%S')}]")
            print(f"    Entry: ₹{row['best_bid']:.2f} (bid)")
            print(f"    Imbalance: {row['imbalance_ratio']:.2f} (ask-heavy)")
            print(f"    Spread: ₹{row['spread']:.2f}")
            print(f"    Result: +₹{row['future_return'] * row['mid_price'] / 100:.2f} per contract")
            print()

print("2. BEST SHORT ENTRY OPPORTUNITIES:")

# Short setup: Bid-heavy + tight spread + price near resistance
short_setups = df[
    (df['imbalance_ratio'] > 1.05) &  # More bids (overbought)
    (df['spread'] < 2.0) &  # Tight spread
    (df['best_ask'] > df['ask_vwap'] - 5)  # Near ask VWAP
].copy()

if len(short_setups) > 5:
    print(f"Found {len(short_setups)} potential short entries\n")
    
    short_setups['future_return'] = (df['mid_price'] - df['mid_price'].shift(-20)) / df['mid_price'] * 100
    short_setups = short_setups.dropna(subset=['future_return'])
    
    if len(short_setups) > 0:
        best_shorts = short_setups.nlargest(3, 'future_return')
        for idx, row in best_shorts.iterrows():
            print(f"  [{row['timestamp'].strftime('%H:%M:%S')}]")
            print(f"    Entry: ₹{row['best_ask']:.2f} (ask)")
            print(f"    Imbalance: {row['imbalance_ratio']:.2f} (bid-heavy)")
            print(f"    Spread: ₹{row['spread']:.2f}")
            print(f"    Result: +₹{row['future_return'] * row['mid_price'] / 100:.2f} per contract")
            print()

print("="*80)
print("KEY TAKEAWAYS FOR LIVE TRADING")
print("="*80)

print(f"""
Based on this 7-minute session:

1. IMBALANCE REGIME:
   • Bid-heavy: {100*len(df[df['imbalance_ratio'] > 1.05])/len(df):.1f}% of time
   • Balanced: {100*len(df[(df['imbalance_ratio'] >= 0.95) & (df['imbalance_ratio'] <= 1.05)])/len(df):.1f}% of time
   • Ask-heavy: {100*len(df[df['imbalance_ratio'] < 0.95])/len(df):.1f}% of time
   
2. SPREAD CONDITIONS:
   • Tight (<₹2): {100*len(df[df['spread'] < 2])/len(df):.1f}% of time → Good for entry
   • Normal (₹2-4): {100*len(df[(df['spread'] >= 2) & (df['spread'] <= 4)])/len(df):.1f}% of time
   • Wide (>₹4): {100*len(df[df['spread'] > 4])/len(df):.1f}% of time → Volatility warning

3. BEST EXECUTION ZONES:
   • Bid VWAP: ₹{df['bid_vwap'].mean():.2f} ± ₹{df['bid_vwap'].std():.2f}
   • Ask VWAP: ₹{df['ask_vwap'].mean():.2f} ± ₹{df['ask_vwap'].std():.2f}
   • Current Bid: ₹{df['best_bid'].iloc[-1]:.2f}
   • Current Ask: ₹{df['best_ask'].iloc[-1]:.2f}

4. CURRENT MARKET STATE:
   • Imbalance: {df['imbalance_ratio'].iloc[-1]:.2f}
   • Spread: ₹{df['spread'].iloc[-1]:.2f}
   • 50% Volume: Bid L{df['bid_50pct_level'].iloc[-1]:.0f}, Ask L{df['ask_50pct_level'].iloc[-1]:.0f}
   
5. TRADING RECOMMENDATION:
""")

# Current state recommendation
current_imbalance = df['imbalance_ratio'].iloc[-1]
current_spread = df['spread'].iloc[-1]

if current_imbalance > 1.10:
    print("   → BULLISH BIAS: Excess bids suggest institutional accumulation")
    print(f"   → LONG on dips to ₹{df['bid_vwap'].iloc[-1]:.2f} (bid VWAP)")
    print(f"   → Stop: ₹{df['bid_vwap'].iloc[-1] - 5:.2f}")
elif current_imbalance < 0.90:
    print("   → BEARISH BIAS: Excess asks suggest institutional distribution")
    print(f"   → SHORT on rallies to ₹{df['ask_vwap'].iloc[-1]:.2f} (ask VWAP)")
    print(f"   → Stop: ₹{df['ask_vwap'].iloc[-1] + 5:.2f}")
else:
    print("   → NEUTRAL: Balanced order flow, range-bound likely")
    print(f"   → BUY: ₹{df['best_bid'].iloc[-1]:.2f}, SELL: ₹{df['best_ask'].iloc[-1]:.2f}")
    print("   → Mean reversion strategy recommended")

if current_spread > 3:
    print(f"   ⚠️  Wide spread (₹{current_spread:.2f}) suggests volatility coming")
    print("   → Reduce position size, use wider stops")

print("\n" + "="*80)
