"""
Analyze 200-Level Depth Snapshots vs Price Movements
Correlates orderbook metrics with price changes to identify patterns
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import pytz

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tradingdb',
    'user': 'tradinguser',
    'password': '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
}

IST = pytz.timezone('Asia/Kolkata')

def fetch_depth_data():
    """Fetch all depth snapshots from database"""
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    
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
        avg_bid_order_size,
        avg_ask_order_size,
        bid_vwap,
        ask_vwap,
        bid_50pct_level,
        ask_50pct_level
    FROM depth_200_snapshots
    ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"✓ Loaded {len(df)} snapshots from database")
    print(f"  Time range: {df['time_ist'].min()} to {df['time_ist'].max()}")
    print(f"  Duration: {(df['time_ist'].max() - df['time_ist'].min()).total_seconds():.1f} seconds")
    print()
    
    return df

def calculate_derived_metrics(df):
    """Calculate price changes and derived metrics"""
    print("Calculating derived metrics...")
    
    # Mid price
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2
    
    # Price changes (forward looking)
    df['price_change_1'] = df['mid_price'].diff(1)  # Next snapshot
    df['price_change_5'] = df['mid_price'].diff(5)  # 5 snapshots ahead
    df['price_change_20'] = df['mid_price'].diff(20)  # 20 snapshots ahead
    df['price_change_100'] = df['mid_price'].diff(100)  # 100 snapshots ahead
    
    # Reverse shift for forward-looking (what happened after this snapshot)
    df['future_return_1'] = df['price_change_1'].shift(-1)
    df['future_return_5'] = df['price_change_5'].shift(-5)
    df['future_return_20'] = df['price_change_20'].shift(-20)
    df['future_return_100'] = df['price_change_100'].shift(-100)
    
    # Imbalance changes
    df['imbalance_change'] = df['imbalance_ratio'].diff(1)
    df['imbalance_change_5'] = df['imbalance_ratio'].diff(5)
    
    # Spread changes
    df['spread_change'] = df['spread'].diff(1)
    df['spread_pct'] = (df['spread'] / df['mid_price']) * 100
    
    # VWAP distances
    df['bid_vwap_distance'] = df['best_bid'] - df['bid_vwap']
    df['ask_vwap_distance'] = df['ask_vwap'] - df['best_ask']
    
    # Liquidity concentration
    df['avg_50pct_level'] = (df['bid_50pct_level'] + df['ask_50pct_level']) / 2
    
    # Total orderbook liquidity
    df['total_liquidity'] = df['total_bid_qty'] + df['total_ask_qty']
    df['liquidity_change'] = df['total_liquidity'].pct_change()
    
    print(f"✓ Calculated derived metrics")
    print()
    
    return df

def analyze_imbalance_vs_price(df):
    """Analyze correlation between imbalance and price movements"""
    print("=" * 80)
    print("ANALYSIS 1: IMBALANCE vs PRICE MOVEMENTS")
    print("=" * 80)
    print()
    
    # Categorize imbalance regimes
    df['imbalance_regime'] = pd.cut(
        df['imbalance_ratio'],
        bins=[0, 0.85, 0.95, 1.05, 1.15, 999],
        labels=['Very Ask-Heavy (<0.85)', 'Ask-Heavy (0.85-0.95)', 
                'Balanced (0.95-1.05)', 'Bid-Heavy (1.05-1.15)', 
                'Very Bid-Heavy (>1.15)']
    )
    
    # Average future returns by regime
    print("Average Price Change After Imbalance Regime:")
    print("-" * 80)
    
    regime_analysis = df.groupby('imbalance_regime').agg({
        'future_return_1': 'mean',
        'future_return_5': 'mean',
        'future_return_20': 'mean',
        'future_return_100': 'mean',
        'imbalance_ratio': 'count'
    }).round(3)
    
    regime_analysis.columns = ['Next Tick', '5 Ticks', '20 Ticks', '100 Ticks', 'Count']
    print(regime_analysis)
    print()
    
    # Absorption patterns (imbalance drops but price rises)
    absorption = df[
        (df['imbalance_change'] < -0.05) &  # Imbalance dropped significantly
        (df['future_return_5'] > 0)  # But price went up
    ]
    
    if len(absorption) > 0:
        print(f"Absorption Events Found: {len(absorption)}")
        print(f"  Average imbalance drop: {absorption['imbalance_change'].mean():.3f}")
        print(f"  Average price rise after: ₹{absorption['future_return_5'].mean():.2f}")
        print(f"  Best absorption: {absorption['time_ist'].iloc[absorption['future_return_5'].argmax()]}")
        print(f"    Imbalance: {absorption['imbalance_ratio'].iloc[absorption['future_return_5'].argmax()]:.2f}")
        print(f"    Price gain: ₹{absorption['future_return_5'].max():.2f}")
    else:
        print("No clear absorption events detected")
    print()
    
    # Distribution patterns (imbalance rises but price falls)
    distribution = df[
        (df['imbalance_change'] > 0.05) &  # Imbalance increased significantly
        (df['future_return_5'] < 0)  # But price went down
    ]
    
    if len(distribution) > 0:
        print(f"Distribution Events Found: {len(distribution)}")
        print(f"  Average imbalance rise: {distribution['imbalance_change'].mean():.3f}")
        print(f"  Average price drop after: ₹{distribution['future_return_5'].mean():.2f}")
        print(f"  Worst distribution: {distribution['time_ist'].iloc[distribution['future_return_5'].argmin()]}")
        print(f"    Imbalance: {distribution['imbalance_ratio'].iloc[distribution['future_return_5'].argmin()]:.2f}")
        print(f"    Price loss: ₹{distribution['future_return_5'].min():.2f}")
    else:
        print("No clear distribution events detected")
    print()

def analyze_spread_dynamics(df):
    """Analyze spread widening and its impact on price"""
    print("=" * 80)
    print("ANALYSIS 2: SPREAD DYNAMICS")
    print("=" * 80)
    print()
    
    # Spread percentiles
    spread_75th = df['spread'].quantile(0.75)
    spread_90th = df['spread'].quantile(0.90)
    
    print(f"Spread Statistics:")
    print(f"  Median: ₹{df['spread'].median():.2f}")
    print(f"  75th percentile: ₹{spread_75th:.2f}")
    print(f"  90th percentile: ₹{spread_90th:.2f}")
    print(f"  Max: ₹{df['spread'].max():.2f}")
    print()
    
    # Categorize spreads
    df['spread_regime'] = pd.cut(
        df['spread'],
        bins=[0, 2, 3, 5, 999],
        labels=['Tight (<₹2)', 'Normal (₹2-3)', 'Wide (₹3-5)', 'Very Wide (>₹5)']
    )
    
    print("Price Volatility After Spread Regime:")
    print("-" * 80)
    
    spread_analysis = df.groupby('spread_regime').agg({
        'future_return_5': ['mean', 'std'],
        'future_return_20': ['mean', 'std'],
        'spread': 'count'
    }).round(3)
    
    print(spread_analysis)
    print()
    
    # Spread spikes before big moves
    spread_spikes = df[df['spread'] > spread_90th]
    
    if len(spread_spikes) > 0:
        print(f"Spread Spike Events (>90th percentile): {len(spread_spikes)}")
        print(f"  Average spread during spike: ₹{spread_spikes['spread'].mean():.2f}")
        print(f"  Average price move after 5 ticks: ₹{spread_spikes['future_return_5'].mean():.2f}")
        print(f"  Average price move after 20 ticks: ₹{spread_spikes['future_return_20'].mean():.2f}")
        print(f"  Volatility (std) after spike: ₹{spread_spikes['future_return_20'].std():.2f}")
        
        # Direction after spike
        up_moves = len(spread_spikes[spread_spikes['future_return_20'] > 0])
        down_moves = len(spread_spikes[spread_spikes['future_return_20'] < 0])
        print(f"  Direction after spike: {up_moves} up, {down_moves} down")
    print()

def analyze_vwap_reversion(df):
    """Analyze VWAP distance and mean reversion"""
    print("=" * 80)
    print("ANALYSIS 3: VWAP MEAN REVERSION")
    print("=" * 80)
    print()
    
    print(f"VWAP Distance Statistics:")
    print(f"  Avg bid-VWAP gap: ₹{df['bid_vwap_distance'].mean():.2f} (±{df['bid_vwap_distance'].std():.2f})")
    print(f"  Avg ask-VWAP gap: ₹{df['ask_vwap_distance'].mean():.2f} (±{df['ask_vwap_distance'].std():.2f})")
    print()
    
    # Extreme VWAP distances
    bid_vwap_75th = df['bid_vwap_distance'].quantile(0.75)
    ask_vwap_75th = df['ask_vwap_distance'].quantile(0.75)
    
    # Far from bid VWAP (oversold?)
    far_from_bid_vwap = df[df['bid_vwap_distance'] > bid_vwap_75th]
    
    if len(far_from_bid_vwap) > 0:
        print(f"Price Far Above Bid VWAP (>75th percentile): {len(far_from_bid_vwap)} times")
        print(f"  Average gap: ₹{far_from_bid_vwap['bid_vwap_distance'].mean():.2f}")
        print(f"  Average reversion (20 ticks): ₹{far_from_bid_vwap['future_return_20'].mean():.2f}")
        if far_from_bid_vwap['future_return_20'].mean() < 0:
            print(f"  → Mean reversion CONFIRMED (price came down)")
        else:
            print(f"  → No mean reversion (price continued up)")
    print()
    
    # Far from ask VWAP (overbought?)
    far_from_ask_vwap = df[df['ask_vwap_distance'] > ask_vwap_75th]
    
    if len(far_from_ask_vwap) > 0:
        print(f"Price Far Below Ask VWAP (>75th percentile): {len(far_from_ask_vwap)} times")
        print(f"  Average gap: ₹{far_from_ask_vwap['ask_vwap_distance'].mean():.2f}")
        print(f"  Average reversion (20 ticks): ₹{far_from_ask_vwap['future_return_20'].mean():.2f}")
        if far_from_ask_vwap['future_return_20'].mean() > 0:
            print(f"  → Mean reversion CONFIRMED (price came up)")
        else:
            print(f"  → No mean reversion (price continued down)")
    print()

def analyze_liquidity_concentration(df):
    """Analyze liquidity concentration shifts"""
    print("=" * 80)
    print("ANALYSIS 4: LIQUIDITY CONCENTRATION")
    print("=" * 80)
    print()
    
    print(f"50% Volume Level Statistics:")
    print(f"  Bid 50% level: {df['bid_50pct_level'].mean():.1f} (±{df['bid_50pct_level'].std():.1f})")
    print(f"  Ask 50% level: {df['ask_50pct_level'].mean():.1f} (±{df['ask_50pct_level'].std():.1f})")
    print(f"  Average: {df['avg_50pct_level'].mean():.1f}")
    print()
    
    # Deep liquidity (institutional)
    deep_liquidity = df[df['avg_50pct_level'] > 100]
    shallow_liquidity = df[df['avg_50pct_level'] < 80]
    
    if len(deep_liquidity) > 0:
        print(f"Deep Liquidity (50% at >100 levels): {len(deep_liquidity)} snapshots ({len(deep_liquidity)/len(df)*100:.1f}%)")
        print(f"  Average spread: ₹{deep_liquidity['spread'].mean():.2f}")
        print(f"  Average imbalance: {deep_liquidity['imbalance_ratio'].mean():.2f}")
        print(f"  Price stability (20-tick std): ₹{deep_liquidity['future_return_20'].std():.2f}")
    
    if len(shallow_liquidity) > 0:
        print(f"Shallow Liquidity (50% at <80 levels): {len(shallow_liquidity)} snapshots ({len(shallow_liquidity)/len(df)*100:.1f}%)")
        print(f"  Average spread: ₹{shallow_liquidity['spread'].mean():.2f}")
        print(f"  Average imbalance: {shallow_liquidity['imbalance_ratio'].mean():.2f}")
        print(f"  Price stability (20-tick std): ₹{shallow_liquidity['future_return_20'].std():.2f}")
    print()
    
    if len(deep_liquidity) > 0 and len(shallow_liquidity) > 0:
        print("Deep vs Shallow Liquidity Comparison:")
        print(f"  Spread difference: ₹{deep_liquidity['spread'].mean() - shallow_liquidity['spread'].mean():.2f}")
        print(f"  Volatility difference: ₹{shallow_liquidity['future_return_20'].std() - deep_liquidity['future_return_20'].std():.2f}")
    print()

def find_best_trading_setups(df):
    """Find actual high-quality trading setups from the data"""
    print("=" * 80)
    print("ANALYSIS 5: BEST TRADING SETUPS FROM ACTUAL DATA")
    print("=" * 80)
    print()
    
    # Setup 1: Absorption with tight spread
    absorption_setups = df[
        (df['imbalance_ratio'] < 0.90) &  # Ask-heavy
        (df['spread'] < 2.5) &  # Tight spread
        (df['future_return_20'].notna())
    ].copy()
    
    if len(absorption_setups) > 0:
        absorption_setups['score'] = -absorption_setups['imbalance_ratio'] * absorption_setups['future_return_20']
        top_absorptions = absorption_setups.nlargest(5, 'score')
        
        print("TOP 5 ABSORPTION SETUPS (Ask-Heavy + Tight Spread):")
        print("-" * 80)
        for idx, row in top_absorptions.iterrows():
            print(f"  {row['time_ist']}")
            print(f"    Imbalance: {row['imbalance_ratio']:.2f}, Spread: ₹{row['spread']:.2f}")
            print(f"    Entry: ₹{row['mid_price']:.2f} → Result: {row['future_return_20']:+.2f} (20 ticks)")
        print()
    
    # Setup 2: Distribution with tight spread
    distribution_setups = df[
        (df['imbalance_ratio'] > 1.10) &  # Bid-heavy
        (df['spread'] < 2.5) &  # Tight spread
        (df['future_return_20'].notna())
    ].copy()
    
    if len(distribution_setups) > 0:
        distribution_setups['score'] = distribution_setups['imbalance_ratio'] * -distribution_setups['future_return_20']
        top_distributions = distribution_setups.nlargest(5, 'score')
        
        print("TOP 5 DISTRIBUTION SETUPS (Bid-Heavy + Tight Spread):")
        print("-" * 80)
        for idx, row in top_distributions.iterrows():
            print(f"  {row['time_ist']}")
            print(f"    Imbalance: {row['imbalance_ratio']:.2f}, Spread: ₹{row['spread']:.2f}")
            print(f"    Entry: ₹{row['mid_price']:.2f} → Result: {row['future_return_20']:+.2f} (20 ticks)")
        print()
    
    # Setup 3: Sustained imbalance
    df['imbalance_sustained'] = (
        (df['imbalance_ratio'].rolling(10).mean() < 0.90) |
        (df['imbalance_ratio'].rolling(10).mean() > 1.10)
    )
    
    sustained_setups = df[
        (df['imbalance_sustained'] == True) &
        (df['spread'] < 3) &
        (df['future_return_100'].notna())
    ]
    
    if len(sustained_setups) > 0:
        print(f"SUSTAINED IMBALANCE SETUPS: {len(sustained_setups)} events")
        print(f"  Average 100-tick return: ₹{sustained_setups['future_return_100'].mean():.2f}")
        print(f"  Win rate: {len(sustained_setups[sustained_setups['future_return_100'] > 0])/len(sustained_setups)*100:.1f}%")
        print(f"  Best return: ₹{sustained_setups['future_return_100'].max():.2f}")
    print()

def generate_summary(df):
    """Generate overall summary and key insights"""
    print("=" * 80)
    print("KEY INSIGHTS SUMMARY")
    print("=" * 80)
    print()
    
    total_snapshots = len(df)
    duration_minutes = (df['time_ist'].max() - df['time_ist'].min()).total_seconds() / 60
    price_range = df['mid_price'].max() - df['mid_price'].min()
    
    print(f"Dataset Overview:")
    print(f"  Total snapshots: {total_snapshots:,}")
    print(f"  Duration: {duration_minutes:.1f} minutes")
    print(f"  Price range: ₹{price_range:.2f}")
    print(f"  Average spread: ₹{df['spread'].mean():.2f}")
    print(f"  Average imbalance: {df['imbalance_ratio'].mean():.2f}")
    print()
    
    # Correlation analysis
    print("Correlation Between Metrics:")
    print("-" * 80)
    
    correlations = {
        'Imbalance → Next Price': df[['imbalance_ratio', 'future_return_1']].corr().iloc[0, 1],
        'Imbalance → 5-Tick Price': df[['imbalance_ratio', 'future_return_5']].corr().iloc[0, 1],
        'Imbalance → 20-Tick Price': df[['imbalance_ratio', 'future_return_20']].corr().iloc[0, 1],
        'Spread → Future Volatility': df['spread'].corr(df['future_return_20'].abs()),
        'Total Liquidity → Price Stability': df['total_liquidity'].corr(df['future_return_20'].abs()),
    }
    
    for metric, corr in correlations.items():
        if pd.notna(corr):
            strength = "Strong" if abs(corr) > 0.3 else "Moderate" if abs(corr) > 0.1 else "Weak"
            direction = "positive" if corr > 0 else "negative"
            print(f"  {metric}: {corr:+.3f} ({strength} {direction})")
    print()
    
    # Trading recommendations
    print("Trading Recommendations Based on Data:")
    print("-" * 80)
    
    # Best imbalance threshold
    extreme_ask_heavy = df[df['imbalance_ratio'] < 0.85]
    extreme_bid_heavy = df[df['imbalance_ratio'] > 1.15]
    
    if len(extreme_ask_heavy) > 0:
        avg_return_ask = extreme_ask_heavy['future_return_20'].mean()
        print(f"1. Imbalance <0.85 (Ask-Heavy): Avg 20-tick return = ₹{avg_return_ask:+.2f}")
        if avg_return_ask > 0:
            print(f"   → BUY signal confirmed ({len(extreme_ask_heavy)} occurrences)")
    
    if len(extreme_bid_heavy) > 0:
        avg_return_bid = extreme_bid_heavy['future_return_20'].mean()
        print(f"2. Imbalance >1.15 (Bid-Heavy): Avg 20-tick return = ₹{avg_return_bid:+.2f}")
        if avg_return_bid < 0:
            print(f"   → SELL signal confirmed ({len(extreme_bid_heavy)} occurrences)")
    
    # Spread threshold
    tight_spread = df[df['spread'] < 2]
    if len(tight_spread) > 0:
        print(f"3. Tight Spread (<₹2): Best for entries ({len(tight_spread)} snapshots, {len(tight_spread)/len(df)*100:.1f}%)")
    
    wide_spread = df[df['spread'] > 4]
    if len(wide_spread) > 0:
        print(f"4. Wide Spread (>₹4): Volatility warning ({len(wide_spread)} snapshots, {len(wide_spread)/len(df)*100:.1f}%)")
    
    print()

def main():
    """Main analysis pipeline"""
    print()
    print("=" * 80)
    print("200-LEVEL DEPTH ANALYSIS: PRICE vs ORDERBOOK MOVEMENTS")
    print("=" * 80)
    print()
    
    # Fetch data
    df = fetch_depth_data()
    
    if len(df) < 50:
        print("⚠️  Not enough data for meaningful analysis (need at least 50 snapshots)")
        print("   Let the depth collector run for at least 2-3 minutes")
        return
    
    # Calculate metrics
    df = calculate_derived_metrics(df)
    
    # Run analyses
    analyze_imbalance_vs_price(df)
    analyze_spread_dynamics(df)
    analyze_vwap_reversion(df)
    analyze_liquidity_concentration(df)
    find_best_trading_setups(df)
    generate_summary(df)
    
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print()

if __name__ == "__main__":
    main()
