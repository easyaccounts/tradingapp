import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

# Load the data
df = pd.read_csv('nifty_futures11_today_all_ticks.csv')
df['time'] = pd.to_datetime(df['time'])

# Convert UTC to IST for display
ist = pytz.timezone('Asia/Kolkata')
df['time_ist'] = df['time'].dt.tz_convert(ist)

print("=" * 100)
print("ORDERFLOW TRADING STRATEGY ANALYZER - NIFTY FUTURES")
print("=" * 100)

# Filter to trading ticks only (exclude quote updates)
df_trades = df[df['volume_delta'] > 0].copy()

print(f"\nSession Data: {df['time_ist'].min().strftime('%d-%b-%Y %H:%M:%S')} to {df['time_ist'].max().strftime('%H:%M:%S')} IST")
print(f"Total Ticks: {len(df):,} | Trade Ticks: {len(df_trades):,} ({len(df_trades)/len(df)*100:.1f}%)")

# ============================================================================
# STRATEGY 1: CVD MOMENTUM TRADING
# ============================================================================
print("\n" + "=" * 100)
print("STRATEGY 1: CVD MOMENTUM REVERSAL")
print("=" * 100)
print("Logic: Trade when cumulative volume delta shows strong divergence then reverses")

# Calculate rolling CVD over different windows
df['cvd_1min'] = df['cvd_change'].rolling(window=60, min_periods=1).sum()
df['cvd_5min'] = df['cvd_change'].rolling(window=300, min_periods=1).sum()
df['cvd_15min'] = df['cvd_change'].rolling(window=900, min_periods=1).sum()

# Price momentum
df['price_change_1min'] = df['last_price'].diff(60)
df['price_change_5min'] = df['last_price'].diff(300)

# Generate signals
signals_cvd = []
for i in range(300, len(df), 60):  # Check every minute after warmup
    row = df.iloc[i]
    
    # LONG SIGNAL: Strong selling exhaustion followed by buying
    if (row['cvd_5min'] < -10000 and  # Heavy selling in last 5 min
        row['cvd_1min'] > 5000 and     # But buying in last 1 min
        row['depth_toxicity_tick'] < 0.3):  # Informed traders active
        
        signals_cvd.append({
            'time': row['time_ist'],
            'type': 'LONG',
            'price': row['last_price'],
            'reason': f"Sell exhaustion reversal | 5min CVD: {row['cvd_5min']:,.0f} | 1min CVD: {row['cvd_1min']:,.0f}",
            'cvd_1min': row['cvd_1min'],
            'cvd_5min': row['cvd_5min']
        })
    
    # SHORT SIGNAL: Strong buying exhaustion followed by selling
    elif (row['cvd_5min'] > 10000 and  # Heavy buying in last 5 min
          row['cvd_1min'] < -5000 and   # But selling in last 1 min
          row['depth_toxicity_tick'] < 0.3):  # Informed traders active
        
        signals_cvd.append({
            'time': row['time_ist'],
            'type': 'SHORT',
            'price': row['last_price'],
            'reason': f"Buy exhaustion reversal | 5min CVD: {row['cvd_5min']:,.0f} | 1min CVD: {row['cvd_1min']:,.0f}",
            'cvd_1min': row['cvd_1min'],
            'cvd_5min': row['cvd_5min']
        })

print(f"\nCVD Momentum Signals Generated: {len(signals_cvd)}")
if signals_cvd:
    print("\nTop 5 CVD Signals:")
    for sig in signals_cvd[:5]:
        print(f"  {sig['time'].strftime('%H:%M:%S')} | {sig['type']:5} @ ₹{sig['price']:,.2f} | {sig['reason']}")

# ============================================================================
# STRATEGY 2: TOXIC FLOW FOLLOWING
# ============================================================================
print("\n" + "=" * 100)
print("STRATEGY 2: TOXIC FLOW (Informed Trader Following)")
print("=" * 100)
print("Logic: Follow large toxic orders from informed traders")

# Identify toxic flows (high volume + low toxicity + high kyle lambda)
df['is_toxic'] = (
    (df['depth_toxicity_tick'] < 0.2) &  # High toxicity
    (df['kyle_lambda_tick'] > 0.0005) &   # High market impact
    (df['volume_delta'] > 1000)            # Significant size
)

toxic_trades = df[df['is_toxic']].copy()

signals_toxic = []
for i, row in toxic_trades.iterrows():
    signal_type = row['aggressor_side']
    if signal_type in ['BUY', 'SELL']:
        signals_toxic.append({
            'time': row['time_ist'],
            'type': signal_type,
            'price': row['last_price'],
            'volume': row['volume_delta'],
            'toxicity': row['depth_toxicity_tick'],
            'kyle': row['kyle_lambda_tick'],
            'reason': f"Toxic {signal_type} | Vol: {row['volume_delta']:,} | Tox: {row['depth_toxicity_tick']:.3f} | Kyle: {row['kyle_lambda_tick']:.6f}"
        })

print(f"\nToxic Flow Signals: {len(signals_toxic)}")
if signals_toxic:
    print("\nTop 10 Largest Toxic Orders:")
    toxic_df = pd.DataFrame(signals_toxic).sort_values('volume', ascending=False).head(10)
    for _, sig in toxic_df.iterrows():
        print(f"  {sig['time'].strftime('%H:%M:%S')} | {sig['type']:5} @ ₹{sig['price']:,.2f} | {sig['reason']}")

# ============================================================================
# STRATEGY 3: OI + PRICE MOMENTUM
# ============================================================================
print("\n" + "=" * 100)
print("STRATEGY 3: OPEN INTEREST MOMENTUM")
print("=" * 100)
print("Logic: Trade in direction of OI build-up with price confirmation")

# Calculate OI rolling changes
df['oi_change_5min'] = df['oi_delta'].rolling(window=300, min_periods=1).sum()
df['price_momentum'] = df['last_price'].diff(300)  # 5-min price change

signals_oi = []
for i in range(300, len(df), 120):  # Check every 2 minutes
    row = df.iloc[i]
    
    # LONG SIGNAL: Fresh long build-up (OI up + Price up)
    if (row['oi_change_5min'] > 50000 and 
        row['price_momentum'] > 10 and
        row['cvd_1min'] > 2000):
        
        signals_oi.append({
            'time': row['time_ist'],
            'type': 'LONG',
            'price': row['last_price'],
            'reason': f"Fresh LONG build | OI: +{row['oi_change_5min']:,.0f} | Price: +₹{row['price_momentum']:.1f}",
            'oi_change': row['oi_change_5min']
        })
    
    # SHORT SIGNAL: Fresh short build-up (OI up + Price down)
    elif (row['oi_change_5min'] > 50000 and 
          row['price_momentum'] < -10 and
          row['cvd_1min'] < -2000):
        
        signals_oi.append({
            'time': row['time_ist'],
            'type': 'SHORT',
            'price': row['last_price'],
            'reason': f"Fresh SHORT build | OI: +{row['oi_change_5min']:,.0f} | Price: {row['price_momentum']:.1f}",
            'oi_change': row['oi_change_5min']
        })

print(f"\nOI Momentum Signals: {len(signals_oi)}")
if signals_oi:
    print("\nTop 5 OI Signals:")
    for sig in signals_oi[:5]:
        print(f"  {sig['time'].strftime('%H:%M:%S')} | {sig['type']:5} @ ₹{sig['price']:,.2f} | {sig['reason']}")

# ============================================================================
# STRATEGY 4: ABSORPTION PATTERNS
# ============================================================================
print("\n" + "=" * 100)
print("STRATEGY 4: ORDER ABSORPTION (Institutional Accumulation)")
print("=" * 100)
print("Logic: Detect when large orders are absorbed without price movement")

# Find absorption: large volume delta but small price movement
df['price_change_10'] = df['last_price'].diff(10).abs()
df['volume_10tick'] = df['volume_delta'].rolling(window=10).sum()

absorption_threshold = 5000  # Large volume
price_stability = 2.0  # Price doesn't move much

absorptions = df[
    (df['volume_10tick'] > absorption_threshold) & 
    (df['price_change_10'] < price_stability) &
    (df['volume_delta'] > 500)
].copy()

signals_absorption = []
for i, row in absorptions.iterrows():
    # Check net flow direction
    idx = df.index.get_loc(i)
    if idx < 20:
        continue
    
    recent_cvd = df.iloc[idx-20:idx]['cvd_change'].sum()
    
    if abs(recent_cvd) > 3000:  # Strong directional flow
        signal_type = 'LONG' if recent_cvd > 0 else 'SHORT'
        signals_absorption.append({
            'time': row['time_ist'],
            'type': signal_type,
            'price': row['last_price'],
            'volume': row['volume_10tick'],
            'cvd': recent_cvd,
            'reason': f"Absorption | Vol: {row['volume_10tick']:,.0f} contracts | Price stable | CVD: {recent_cvd:+,.0f}"
        })

print(f"\nAbsorption Signals: {len(signals_absorption)}")
if signals_absorption:
    print("\nTop 5 Absorption Patterns:")
    absorption_df = pd.DataFrame(signals_absorption).sort_values('volume', ascending=False).head(5)
    for _, sig in absorption_df.iterrows():
        print(f"  {sig['time'].strftime('%H:%M:%S')} | {sig['type']:5} @ ₹{sig['price']:,.2f} | {sig['reason']}")

# ============================================================================
# COMBINED SIGNAL SCORING
# ============================================================================
print("\n" + "=" * 100)
print("MULTI-STRATEGY CONFLUENCE ZONES (High Probability Setups)")
print("=" * 100)

# Create timeline with all signals
all_signals = []

for sig in signals_cvd:
    all_signals.append({**sig, 'strategy': 'CVD_MOMENTUM', 'score': 2})

for sig in signals_toxic:
    all_signals.append({**sig, 'strategy': 'TOXIC_FLOW', 'score': 3})

for sig in signals_oi:
    all_signals.append({**sig, 'strategy': 'OI_MOMENTUM', 'score': 2})

for sig in signals_absorption:
    all_signals.append({**sig, 'strategy': 'ABSORPTION', 'score': 1})

if all_signals:
    signals_df = pd.DataFrame(all_signals)
    signals_df['time_bucket'] = signals_df['time'].dt.floor('5min')
    
    # Find confluence: multiple strategies agreeing
    confluence = signals_df.groupby(['time_bucket', 'type']).agg({
        'score': 'sum',
        'strategy': lambda x: ', '.join(x.unique()),
        'price': 'mean'
    }).reset_index()
    
    confluence = confluence[confluence['score'] >= 4].sort_values('score', ascending=False)
    
    print(f"\nHigh-Confidence Confluence Zones: {len(confluence)}")
    if len(confluence) > 0:
        print("\nTop Confluence Setups:")
        for _, row in confluence.head(10).iterrows():
            print(f"  {row['time_bucket'].strftime('%H:%M')} | {row['type']:5} @ ₹{row['price']:,.2f} | Score: {row['score']} | Strategies: {row['strategy']}")

# ============================================================================
# RISK MANAGEMENT RULES
# ============================================================================
print("\n" + "=" * 100)
print("RISK MANAGEMENT FRAMEWORK")
print("=" * 100)

print("""
POSITION SIZING:
  • Base risk: 1% of capital per trade
  • Max position: 50 lots NIFTY futures (₹3.75 lakh margin)
  • Scale into high-confidence setups (confluence ≥ 4)

STOP LOSS:
  • CVD Momentum: ₹15-20 from entry (aggressive reversal)
  • Toxic Flow: ₹10-15 from entry (follow the whale)
  • OI Momentum: ₹20-30 from entry (trend following)
  • Absorption: ₹8-12 from entry (tight stop, quick scalp)

PROFIT TARGETS:
  • Target 1: 2R (2x stop loss)
  • Target 2: 3R (3x stop loss)
  • Trail stop: Lock 1R profit after 2R hit

ENTRY TIMING:
  • Wait for confluence of 2+ strategies
  • Enter on pullback after signal (1-3 ticks)
  • Avoid first 15 minutes (9:15-9:30) - choppy
  • Best hours: 10:00-11:30, 14:00-15:15

EXIT RULES:
  • Exit 50% at Target 1, move stop to breakeven
  • Trail remaining 50% with ₹5 trailing stop
  • Close all by 15:25 (avoid closing volatility)
  • Hard exit if opposing toxic flow detected

AVOID TRADING:
  • During news events (RBI, Budget, major data)
  • If market gapped >1% overnight
  • Low toxicity everywhere (no informed flow)
""")

# ============================================================================
# BACKTEST SUMMARY
# ============================================================================
print("\n" + "=" * 100)
print("TODAY'S SIGNAL SUMMARY")
print("=" * 100)

total_signals = len(signals_cvd) + len(signals_toxic) + len(signals_oi) + len(signals_absorption)
print(f"\nTotal Signals Generated: {total_signals}")
print(f"  • CVD Momentum: {len(signals_cvd)}")
print(f"  • Toxic Flow: {len(signals_toxic)}")
print(f"  • OI Momentum: {len(signals_oi)}")
print(f"  • Absorption: {len(signals_absorption)}")

if all_signals:
    signals_df = pd.DataFrame(all_signals)
    
    long_signals = signals_df[signals_df['type'] == 'LONG']
    short_signals = signals_df[signals_df['type'] == 'SHORT']
    
    print(f"\nDirection Bias:")
    print(f"  • LONG: {len(long_signals)} signals")
    print(f"  • SHORT: {len(short_signals)} signals")
    print(f"  • Net Bias: {'🟢 BULLISH' if len(long_signals) > len(short_signals) else '🔴 BEARISH' if len(short_signals) > len(long_signals) else '⚪ NEUTRAL'}")

print("\n" + "=" * 100)
print("LIVE TRADING RECOMMENDATION")
print("=" * 100)

# Get current market state (last 100 ticks)
recent = df.tail(100)
current_cvd = recent['cvd_change'].sum()
current_price = df['last_price'].iloc[-1]
current_toxic_pct = (recent['depth_toxicity_tick'] < 0.3).sum() / len(recent) * 100

print(f"\nCurrent Market State:")
print(f"  Price: ₹{current_price:,.2f}")
print(f"  Recent CVD (100 ticks): {current_cvd:+,.0f}")
print(f"  Toxic Flow: {current_toxic_pct:.1f}% of ticks")
print(f"  Trend: {'🟢 BUYING' if current_cvd > 1000 else '🔴 SELLING' if current_cvd < -1000 else '⚪ NEUTRAL'}")

if current_cvd > 5000:
    print(f"\n🟢 BIAS: LONG | Strong buying pressure detected")
    print(f"  • Wait for pullback to ₹{current_price - 10:.2f}-{current_price - 15:.2f}")
    print(f"  • Target: ₹{current_price + 20:.2f} (2R) and ₹{current_price + 30:.2f} (3R)")
    print(f"  • Stop Loss: ₹{current_price - 18:.2f}")
elif current_cvd < -5000:
    print(f"\n🔴 BIAS: SHORT | Strong selling pressure detected")
    print(f"  • Wait for bounce to ₹{current_price + 10:.2f}-{current_price + 15:.2f}")
    print(f"  • Target: ₹{current_price - 20:.2f} (2R) and ₹{current_price - 30:.2f} (3R)")
    print(f"  • Stop Loss: ₹{current_price + 18:.2f}")
else:
    print(f"\n⚪ BIAS: NEUTRAL | Wait for clearer signal or trade range")
    print(f"  • Support: ₹{recent['last_price'].min():,.2f}")
    print(f"  • Resistance: ₹{recent['last_price'].max():,.2f}")

print("\n" + "=" * 100)
