"""
Identify key support/resistance levels from 200-level depth data
"""
import pandas as pd
import numpy as np
from collections import defaultdict

# Read the depth data
df = pd.read_csv('dhan_200depth_nifty_futures.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

print("="*80)
print("KEY PRICE LEVELS FROM 200-LEVEL DEPTH ANALYSIS")
print("="*80)
print(f"\nAnalyzing {len(df)} depth snapshots")
print(f"Time: {df['timestamp'].min().strftime('%H:%M:%S')} - {df['timestamp'].max().strftime('%H:%M:%S')}")

# Current market levels
latest = df.iloc[-1]
print(f"\n{'='*80}")
print("CURRENT MARKET STATE (Latest Snapshot)")
print("="*80)
print(f"Best Bid: ₹{latest['best_bid']:,.2f}")
print(f"Best Ask: ₹{latest['best_ask']:,.2f}")
print(f"Mid Price: ₹{(latest['best_bid'] + latest['best_ask'])/2:,.2f}")
print(f"Spread: ₹{latest['spread']:.2f}")
print(f"Imbalance: {latest['imbalance_ratio']:.2f} (Bid/Ask)")

# Key VWAP levels
print(f"\n{'='*80}")
print("VOLUME-WEIGHTED AVERAGE PRICES (VWAP)")
print("="*80)
avg_bid_vwap = df['bid_vwap'].mean()
avg_ask_vwap = df['ask_vwap'].mean()
print(f"\nBid Side VWAP: ₹{avg_bid_vwap:,.2f}")
print(f"  → ₹{latest['best_bid'] - avg_bid_vwap:.2f} below best bid")
print(f"  → For large BUY orders, expect ₹{latest['best_bid'] - avg_bid_vwap:.2f} slippage per contract")

print(f"\nAsk Side VWAP: ₹{avg_ask_vwap:,.2f}")
print(f"  → ₹{avg_ask_vwap - latest['best_ask']:.2f} above best ask")
print(f"  → For large SELL orders, expect ₹{avg_ask_vwap - latest['best_ask']:.2f} slippage per contract")

mid_vwap = (avg_bid_vwap + avg_ask_vwap) / 2
print(f"\nMid VWAP: ₹{mid_vwap:,.2f}")
print(f"  → Fair value for institutional orders")

# Support and resistance zones
print(f"\n{'='*80}")
print("KEY SUPPORT LEVELS (Strong Bid Liquidity)")
print("="*80)

# Identify support zones based on bid VWAP and best bid
support_levels = []

# Level 1: Best Bid (immediate support)
support_levels.append({
    'level': latest['best_bid'],
    'type': 'Immediate Support',
    'strength': 'Strong',
    'orders': latest['total_bid_orders'],
    'quantity': latest['total_bid_qty']
})

# Level 2: Bid VWAP (deep support)
support_levels.append({
    'level': avg_bid_vwap,
    'type': 'Deep Support (Bid VWAP)',
    'strength': 'Very Strong',
    'orders': 'Across 200 levels',
    'quantity': f"~{latest['total_bid_qty']:,.0f}"
})

# Level 3: 50% volume concentration
bid_50_price = latest['best_bid'] - (latest['bid_50pct_level'] * 0.05)  # Estimate ~₹0.05 per level
support_levels.append({
    'level': bid_50_price,
    'type': '50% Volume Zone',
    'strength': 'Strong',
    'orders': f"Level {int(latest['bid_50pct_level'])}",
    'quantity': f"~{latest['total_bid_qty']/2:,.0f}"
})

for i, level in enumerate(support_levels, 1):
    print(f"\n{i}. ₹{level['level']:,.2f} - {level['type']}")
    print(f"   Strength: {level['strength']}")
    print(f"   Orders: {level['orders']}")
    print(f"   Quantity: {level['quantity']}")

print(f"\n{'='*80}")
print("KEY RESISTANCE LEVELS (Strong Ask Liquidity)")
print("="*80)

# Identify resistance zones
resistance_levels = []

# Level 1: Best Ask (immediate resistance)
resistance_levels.append({
    'level': latest['best_ask'],
    'type': 'Immediate Resistance',
    'strength': 'Strong',
    'orders': latest['total_ask_orders'],
    'quantity': latest['total_ask_qty']
})

# Level 2: Ask VWAP (deep resistance)
resistance_levels.append({
    'level': avg_ask_vwap,
    'type': 'Deep Resistance (Ask VWAP)',
    'strength': 'Very Strong',
    'orders': 'Across 200 levels',
    'quantity': f"~{latest['total_ask_qty']:,.0f}"
})

# Level 3: 50% volume concentration
ask_50_price = latest['best_ask'] + (latest['ask_50pct_level'] * 0.05)  # Estimate ~₹0.05 per level
resistance_levels.append({
    'level': ask_50_price,
    'type': '50% Volume Zone',
    'strength': 'Strong',
    'orders': f"Level {int(latest['ask_50pct_level'])}",
    'quantity': f"~{latest['total_ask_qty']/2:,.0f}"
})

for i, level in enumerate(resistance_levels, 1):
    print(f"\n{i}. ₹{level['level']:,.2f} - {level['type']}")
    print(f"   Strength: {level['strength']}")
    print(f"   Orders: {level['orders']}")
    print(f"   Quantity: {level['quantity']}")

# Price ranges and zones
print(f"\n{'='*80}")
print("LIQUIDITY ZONES")
print("="*80)

bid_range = latest['best_bid'] - avg_bid_vwap
ask_range = avg_ask_vwap - latest['best_ask']

print(f"\nBid Liquidity Zone: ₹{avg_bid_vwap:,.2f} - ₹{latest['best_bid']:,.2f}")
print(f"  → Width: ₹{bid_range:.2f} (Support depth)")
print(f"  → Contains: ~{latest['total_bid_qty']:,.0f} contracts across 200 levels")
print(f"  → Average order size: {latest['avg_bid_order_size']:.0f} contracts")

print(f"\nAsk Liquidity Zone: ₹{latest['best_ask']:,.2f} - ₹{avg_ask_vwap:,.2f}")
print(f"  → Width: ₹{ask_range:.2f} (Resistance depth)")
print(f"  → Contains: ~{latest['total_ask_qty']:,.0f} contracts across 200 levels")
print(f"  → Average order size: {latest['avg_ask_order_size']:.0f} contracts")

print(f"\nNo-Trade Zone (Spread): ₹{latest['best_bid']:,.2f} - ₹{latest['best_ask']:,.2f}")
print(f"  → Width: ₹{latest['spread']:.2f}")
print(f"  → Bid here for BUY, Ask here for SELL (avoid market orders)")

# Imbalance analysis
print(f"\n{'='*80}")
print("MARKET PRESSURE ANALYSIS")
print("="*80)

imbalance = latest['imbalance_ratio']
bid_qty_excess = latest['total_bid_qty'] - latest['total_ask_qty']

print(f"\nCurrent Imbalance Ratio: {imbalance:.2f}")
print(f"Bid Quantity: {latest['total_bid_qty']:,.0f} contracts")
print(f"Ask Quantity: {latest['total_ask_qty']:,.0f} contracts")
print(f"Excess Bids: {bid_qty_excess:,.0f} contracts ({100*(imbalance-1):.1f}%)")

if imbalance > 1.15:
    pressure = "STRONG BULLISH"
    color = "🟢"
elif imbalance > 1.05:
    pressure = "BULLISH"
    color = "🔵"
elif imbalance < 0.95:
    pressure = "BEARISH"
    color = "🔴"
elif imbalance < 0.85:
    pressure = "STRONG BEARISH"
    color = "🔴🔴"
else:
    pressure = "NEUTRAL"
    color = "⚪"

print(f"\n{color} Market Pressure: {pressure}")

if imbalance > 1.10:
    print(f"  → {100*(imbalance-1):.1f}% more bid liquidity = institutional buying interest")
    print(f"  → Support at ₹{latest['best_bid']:,.2f} likely to hold")
    print(f"  → Breakout likely toward ₹{latest['best_ask'] + 2:.2f}+ if bid absorption continues")
elif imbalance < 0.90:
    print(f"  → {100*(1-imbalance):.1f}% more ask liquidity = institutional selling interest")
    print(f"  → Resistance at ₹{latest['best_ask']:,.2f} likely to hold")
    print(f"  → Breakdown likely toward ₹{latest['best_bid'] - 2:.2f}- if ask absorption continues")
else:
    print(f"  → Balanced market, range-bound between ₹{latest['best_bid']:,.2f}-₹{latest['best_ask']:,.2f}")

# Trading recommendations
print(f"\n{'='*80}")
print("TRADING RECOMMENDATIONS BASED ON KEY LEVELS")
print("="*80)

print(f"\n🎯 FOR LONG ENTRIES:")
print(f"  1. Best Entry: Bid at ₹{latest['best_bid']:,.2f} (immediate support)")
print(f"  2. Good Entry: Bid at ₹{(latest['best_bid'] + avg_bid_vwap)/2:,.2f} (mid-zone)")
print(f"  3. Strong Entry: Bid at ₹{avg_bid_vwap:,.2f} (deep support, VWAP)")
print(f"  → Stop Loss: ₹{avg_bid_vwap - 3:.2f} (below deep support)")
print(f"  → Target: ₹{latest['best_ask'] + latest['spread']:.2f}+ (above immediate resistance)")

print(f"\n🎯 FOR SHORT ENTRIES:")
print(f"  1. Best Entry: Ask at ₹{latest['best_ask']:,.2f} (immediate resistance)")
print(f"  2. Good Entry: Ask at ₹{(latest['best_ask'] + avg_ask_vwap)/2:,.2f} (mid-zone)")
print(f"  3. Strong Entry: Ask at ₹{avg_ask_vwap:,.2f} (deep resistance, VWAP)")
print(f"  → Stop Loss: ₹{avg_ask_vwap + 3:.2f} (above deep resistance)")
print(f"  → Target: ₹{latest['best_bid'] - latest['spread']:.2f}- (below immediate support)")

print(f"\n🎯 FOR LARGE ORDERS (>500 contracts):")
print(f"  BUY: Use limit orders between ₹{avg_bid_vwap:,.2f} - ₹{latest['best_bid']:,.2f}")
print(f"       → Avoid market orders (₹{latest['best_bid'] - avg_bid_vwap:.2f} slippage)")
print(f"       → Target fill at VWAP = 0.1% better execution")
print(f"  SELL: Use limit orders between ₹{latest['best_ask']:,.2f} - ₹{avg_ask_vwap:,.2f}")
print(f"        → Avoid market orders (₹{avg_ask_vwap - latest['best_ask']:.2f} slippage)")
print(f"        → Target fill at VWAP = 0.1% better execution")

# Historical context
print(f"\n{'='*80}")
print("DEPTH STABILITY OVER SESSION")
print("="*80)

bid_vwap_stability = df['bid_vwap'].std()
ask_vwap_stability = df['ask_vwap'].std()
imbalance_stability = df['imbalance_ratio'].std()

print(f"\nBid VWAP Range: ₹{df['bid_vwap'].min():,.2f} - ₹{df['bid_vwap'].max():,.2f}")
print(f"  Std Dev: ₹{bid_vwap_stability:.2f} ({'Stable' if bid_vwap_stability < 5 else 'Volatile'})")

print(f"\nAsk VWAP Range: ₹{df['ask_vwap'].min():,.2f} - ₹{df['ask_vwap'].max():,.2f}")
print(f"  Std Dev: ₹{ask_vwap_stability:.2f} ({'Stable' if ask_vwap_stability < 5 else 'Volatile'})")

print(f"\nImbalance Range: {df['imbalance_ratio'].min():.2f} - {df['imbalance_ratio'].max():.2f}")
print(f"  Std Dev: {imbalance_stability:.3f} ({'Consistent' if imbalance_stability < 0.1 else 'Shifting'})")

if imbalance_stability < 0.1:
    print(f"\n→ Consistent bid-heavy market = sustained institutional accumulation")
    print(f"→ Key support levels are reliable (low volatility)")
else:
    print(f"\n→ Shifting imbalance = institutional repositioning")
    print(f"→ Watch for breakout as imbalance stabilizes")

print(f"\n{'='*80}")
print("SUMMARY - KEY ACTIONABLE LEVELS")
print("="*80)
print(f"""
📊 CURRENT MARKET: ₹{(latest['best_bid'] + latest['best_ask'])/2:,.2f} ({pressure})

🔴 KEY RESISTANCE LEVELS:
   • ₹{avg_ask_vwap:,.2f} - Deep resistance (Ask VWAP, {latest['total_ask_qty']:,.0f} contracts)
   • ₹{latest['best_ask']:,.2f} - Immediate resistance (Best ask)

🟢 KEY SUPPORT LEVELS:
   • ₹{latest['best_bid']:,.2f} - Immediate support (Best bid)
   • ₹{avg_bid_vwap:,.2f} - Deep support (Bid VWAP, {latest['total_bid_qty']:,.0f} contracts)

⚡ EXECUTION STRATEGY:
   • Market orders: Expect ₹{(latest['best_bid'] - avg_bid_vwap + avg_ask_vwap - latest['best_ask'])/2:.2f} slippage
   • Limit orders at VWAP: Save 0.1% (₹{mid_vwap * 0.001:.2f} per contract)
   • Large orders (>500): MUST use limit orders near VWAP

💡 MARKET BIAS: {100*(imbalance-1):.1f}% excess bid liquidity
   → {pressure.capitalize()} pressure sustained across 200 levels
   → {'Favor longs, strong support below' if imbalance > 1.10 else 'Range-bound, mean reversion strategy'}
""")

print("="*80)
