"""
Analyze Bid/Ask VWAP Behavior During Price Movements
Understanding how 200-level orderbook VWAPs respond to price changes
"""

import psycopg2
import pandas as pd
import numpy as np
from scipy import stats
import pytz

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tradingdb',
    'user': 'tradinguser',
    'password': '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
}

IST = pytz.timezone('Asia/Kolkata')

def fetch_data():
    """Fetch depth data with VWAPs"""
    print("Fetching VWAP and price data...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    query = """
    SELECT 
        timestamp AT TIME ZONE 'Asia/Kolkata' as time_ist,
        best_bid,
        best_ask,
        spread,
        bid_vwap,
        ask_vwap,
        total_bid_qty,
        total_ask_qty,
        imbalance_ratio
    FROM depth_200_snapshots
    ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"✓ Loaded {len(df):,} snapshots")
    print()
    return df

def calculate_metrics(df):
    """Calculate price and VWAP movement metrics"""
    print("Calculating movement metrics...")
    
    # Price metrics
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2
    df['price_change'] = df['mid_price'].diff()
    df['price_change_5'] = df['mid_price'].diff(5)
    df['price_change_20'] = df['mid_price'].diff(20)
    df['price_move'] = df['price_change'].abs()
    
    # VWAP distances
    df['bid_vwap_dist'] = df['best_bid'] - df['bid_vwap']
    df['ask_vwap_dist'] = df['ask_vwap'] - df['best_ask']
    df['vwap_gap'] = df['ask_vwap'] - df['bid_vwap']
    
    # VWAP changes
    df['bid_vwap_change'] = df['bid_vwap'].diff()
    df['ask_vwap_change'] = df['ask_vwap'].diff()
    df['vwap_gap_change'] = df['vwap_gap'].diff()
    
    # VWAP velocity
    df['bid_vwap_velocity'] = df['bid_vwap'].diff(5)
    df['ask_vwap_velocity'] = df['ask_vwap'].diff(5)
    
    # Distance changes
    df['bid_vwap_dist_change'] = df['bid_vwap_dist'].diff()
    df['ask_vwap_dist_change'] = df['ask_vwap_dist'].diff()
    
    # VWAP elasticity (how much VWAP moves per unit price move)
    df['bid_vwap_elasticity'] = df['bid_vwap_change'] / df['price_change'].replace(0, np.nan)
    df['ask_vwap_elasticity'] = df['ask_vwap_change'] / df['price_change'].replace(0, np.nan)
    
    # Price direction
    df['price_up'] = df['price_change'] > 0
    df['price_down'] = df['price_change'] < 0
    
    print(f"✓ Calculated metrics")
    print()
    return df

def analyze_vwap_price_correlation(df):
    """Analyze correlation between VWAP and price movements"""
    print("=" * 100)
    print("ANALYSIS 1: VWAP-PRICE MOVEMENT CORRELATION")
    print("=" * 100)
    print()
    
    # Overall correlations
    correlations = {
        'Price ↔ Bid VWAP Change': df[['price_change', 'bid_vwap_change']].corr().iloc[0, 1],
        'Price ↔ Ask VWAP Change': df[['price_change', 'ask_vwap_change']].corr().iloc[0, 1],
        'Price ↔ Bid Distance Change': df[['price_change', 'bid_vwap_dist_change']].corr().iloc[0, 1],
        'Price ↔ Ask Distance Change': df[['price_change', 'ask_vwap_dist_change']].corr().iloc[0, 1],
        'Price ↔ VWAP Gap Change': df[['price_change', 'vwap_gap_change']].corr().iloc[0, 1],
    }
    
    print("Correlations:")
    print("-" * 100)
    for metric, corr in correlations.items():
        if pd.notna(corr):
            strength = "Strong" if abs(corr) > 0.5 else "Moderate" if abs(corr) > 0.3 else "Weak"
            print(f"  {metric:<40} {corr:+.4f} ({strength})")
    print()

def analyze_vwap_elasticity(df):
    """Analyze how much VWAP moves per unit price change"""
    print("=" * 100)
    print("ANALYSIS 2: VWAP ELASTICITY (Movement Ratio)")
    print("=" * 100)
    print()
    
    # Filter valid data
    valid_data = df[(df['price_change'].abs() > 0.1) & (df['bid_vwap_elasticity'].notna())].copy()
    
    print("VWAP Elasticity Statistics:")
    print("-" * 100)
    print(f"  Bid VWAP Elasticity:")
    print(f"    Mean: {valid_data['bid_vwap_elasticity'].mean():.4f}")
    print(f"    Median: {valid_data['bid_vwap_elasticity'].median():.4f}")
    print(f"    → When price moves ₹1, bid VWAP moves ~₹{valid_data['bid_vwap_elasticity'].median():.2f}")
    print()
    print(f"  Ask VWAP Elasticity:")
    print(f"    Mean: {valid_data['ask_vwap_elasticity'].mean():.4f}")
    print(f"    Median: {valid_data['ask_vwap_elasticity'].median():.4f}")
    print(f"    → When price moves ₹1, ask VWAP moves ~₹{valid_data['ask_vwap_elasticity'].median():.2f}")
    print()
    
    # Compare up vs down moves
    print("Elasticity by Direction:")
    print("-" * 100)
    
    up_moves = valid_data[valid_data['price_up'] == True]
    down_moves = valid_data[valid_data['price_down'] == True]
    
    if len(up_moves) > 0:
        print(f"  Price UP moves:")
        print(f"    Bid VWAP elasticity: {up_moves['bid_vwap_elasticity'].median():.4f}")
        print(f"    Ask VWAP elasticity: {up_moves['ask_vwap_elasticity'].median():.4f}")
    
    if len(down_moves) > 0:
        print(f"  Price DOWN moves:")
        print(f"    Bid VWAP elasticity: {down_moves['bid_vwap_elasticity'].median():.4f}")
        print(f"    Ask VWAP elasticity: {down_moves['ask_vwap_elasticity'].median():.4f}")
    
    print()

def analyze_vwap_distance_behavior(df):
    """Analyze how VWAP distance changes during price moves"""
    print("=" * 100)
    print("ANALYSIS 3: VWAP DISTANCE BEHAVIOR")
    print("=" * 100)
    print()
    
    # Categorize by price move size
    df['price_move_cat'] = pd.cut(
        df['price_move'],
        bins=[0, 0.5, 1.5, 3.0, 999],
        labels=['Small (<₹0.5)', 'Medium (₹0.5-1.5)', 'Large (₹1.5-3)', 'Huge (>₹3)']
    )
    
    print("Bid VWAP Distance Change by Price Move Size:")
    print("-" * 100)
    
    distance_analysis = df.groupby('price_move_cat', observed=True).agg({
        'bid_vwap_dist_change': ['mean', 'std'],
        'ask_vwap_dist_change': ['mean', 'std'],
        'vwap_gap_change': ['mean', 'std'],
        'mid_price': 'count'
    }).round(4)
    
    print(distance_analysis)
    print()
    
    print("Interpretation:")
    print("-" * 100)
    bid_dist_changes = df.groupby('price_move_cat', observed=True)['bid_vwap_dist_change'].mean()
    
    for cat in bid_dist_changes.index:
        change = bid_dist_changes[cat]
        if pd.notna(change):
            if change > 0.1:
                print(f"  {cat}: Distance INCREASES (+{change:.2f}) → Price moving AWAY from VWAP (stretching)")
            elif change < -0.1:
                print(f"  {cat}: Distance DECREASES ({change:.2f}) → Price moving TOWARD VWAP (compression)")
            else:
                print(f"  {cat}: Distance STABLE (~{change:.2f}) → VWAP tracking price")
    print()

def analyze_vwap_gap_dynamics(df):
    """Analyze bid-ask VWAP gap behavior"""
    print("=" * 100)
    print("ANALYSIS 4: BID-ASK VWAP GAP DYNAMICS")
    print("=" * 100)
    print()
    
    print(f"VWAP Gap Statistics:")
    print(f"  Average gap: ₹{df['vwap_gap'].mean():.2f}")
    print(f"  Median gap: ₹{df['vwap_gap'].median():.2f}")
    print(f"  Range: ₹{df['vwap_gap'].min():.2f} to ₹{df['vwap_gap'].max():.2f}")
    print()
    
    # Gap expansion/contraction during moves
    print("Gap Behavior During Price Moves:")
    print("-" * 100)
    
    # Up moves
    up_moves = df[df['price_change'] > 1].copy()
    if len(up_moves) > 0:
        print(f"  During UP moves (>₹1):")
        print(f"    Gap change: {up_moves['vwap_gap_change'].mean():+.2f}")
        if up_moves['vwap_gap_change'].mean() > 0:
            print(f"    → Gap EXPANDS (VWAPs diverge)")
        else:
            print(f"    → Gap CONTRACTS (VWAPs converge)")
    
    # Down moves
    down_moves = df[df['price_change'] < -1].copy()
    if len(down_moves) > 0:
        print(f"  During DOWN moves (<-₹1):")
        print(f"    Gap change: {down_moves['vwap_gap_change'].mean():+.2f}")
        if down_moves['vwap_gap_change'].mean() > 0:
            print(f"    → Gap EXPANDS (VWAPs diverge)")
        else:
            print(f"    → Gap CONTRACTS (VWAPs converge)")
    
    print()

def analyze_vwap_lead_lag(df):
    """Determine if VWAP leads or lags price"""
    print("=" * 100)
    print("ANALYSIS 5: VWAP LEAD/LAG ANALYSIS")
    print("=" * 100)
    print()
    
    # Calculate correlations at different lags
    print("Cross-Correlation Analysis:")
    print("-" * 100)
    
    max_lag = 20
    bid_correlations = []
    ask_correlations = []
    
    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            # Price leads VWAP
            corr_bid = df['price_change'].corr(df['bid_vwap_change'].shift(-lag))
            corr_ask = df['price_change'].corr(df['ask_vwap_change'].shift(-lag))
        else:
            # VWAP leads price
            corr_bid = df['price_change'].shift(-lag).corr(df['bid_vwap_change'])
            corr_ask = df['price_change'].shift(-lag).corr(df['ask_vwap_change'])
        
        bid_correlations.append((lag, corr_bid))
        ask_correlations.append((lag, corr_ask))
    
    # Find peak correlations
    bid_peak = max(bid_correlations, key=lambda x: abs(x[1]) if pd.notna(x[1]) else 0)
    ask_peak = max(ask_correlations, key=lambda x: abs(x[1]) if pd.notna(x[1]) else 0)
    
    print(f"  Bid VWAP Peak Correlation:")
    print(f"    Lag: {bid_peak[0]} ticks")
    print(f"    Correlation: {bid_peak[1]:.4f}")
    if bid_peak[0] < 0:
        print(f"    → Price LEADS bid VWAP by {abs(bid_peak[0])} ticks")
    elif bid_peak[0] > 0:
        print(f"    → Bid VWAP LEADS price by {bid_peak[0]} ticks")
    else:
        print(f"    → Synchronous movement")
    print()
    
    print(f"  Ask VWAP Peak Correlation:")
    print(f"    Lag: {ask_peak[0]} ticks")
    print(f"    Correlation: {ask_peak[1]:.4f}")
    if ask_peak[0] < 0:
        print(f"    → Price LEADS ask VWAP by {abs(ask_peak[0])} ticks")
    elif ask_peak[0] > 0:
        print(f"    → Ask VWAP LEADS price by {ask_peak[0]} ticks")
    else:
        print(f"    → Synchronous movement")
    print()

def analyze_vwap_mean_reversion_strength(df):
    """Analyze mean reversion toward VWAPs"""
    print("=" * 100)
    print("ANALYSIS 6: VWAP MEAN REVERSION STRENGTH")
    print("=" * 100)
    print()
    
    # Calculate future returns based on current VWAP distance
    for horizon in [5, 20, 50]:
        df[f'future_return_{horizon}'] = df['mid_price'].diff(horizon).shift(-horizon)
    
    # Categorize by distance from VWAP
    df['bid_vwap_dist_cat'] = pd.qcut(
        df['bid_vwap_dist'],
        q=5,
        labels=['Very Close', 'Close', 'Mid', 'Far', 'Very Far'],
        duplicates='drop'
    )
    
    print("Future Returns by Current Distance from Bid VWAP:")
    print("-" * 100)
    
    reversion_analysis = df.groupby('bid_vwap_dist_cat', observed=True).agg({
        'future_return_5': 'mean',
        'future_return_20': 'mean',
        'future_return_50': 'mean',
        'bid_vwap_dist': 'mean',
        'mid_price': 'count'
    }).round(3)
    
    reversion_analysis.columns = ['5-Tick', '20-Tick', '50-Tick', 'Avg Distance', 'Count']
    print(reversion_analysis)
    print()
    
    print("Mean Reversion Observations:")
    print("-" * 100)
    
    very_far = reversion_analysis.loc['Very Far', '20-Tick'] if 'Very Far' in reversion_analysis.index else None
    very_close = reversion_analysis.loc['Very Close', '20-Tick'] if 'Very Close' in reversion_analysis.index else None
    
    if very_far is not None and very_close is not None:
        reversion_strength = very_close - very_far
        print(f"  When price is Very Far from VWAP: {very_far:+.3f} (20-tick return)")
        print(f"  When price is Very Close to VWAP: {very_close:+.3f} (20-tick return)")
        print(f"  Reversion strength: {reversion_strength:+.3f}")
        print()
        if reversion_strength > 0.5:
            print(f"  → STRONG mean reversion detected")
            print(f"  → Trade TOWARD VWAP when far, AWAY when close")
        elif reversion_strength > 0.2:
            print(f"  → MODERATE mean reversion")
        else:
            print(f"  → WEAK mean reversion")
    print()

def trading_insights(df):
    """Generate actionable trading insights"""
    print("=" * 100)
    print("TRADING INSIGHTS FROM VWAP ANALYSIS")
    print("=" * 100)
    print()
    
    # Calculate key metrics
    bid_vwap_avg_dist = df['bid_vwap_dist'].mean()
    ask_vwap_avg_dist = df['ask_vwap_dist'].mean()
    vwap_gap_avg = df['vwap_gap'].mean()
    
    # Elasticity
    valid_elasticity = df[(df['price_change'].abs() > 0.1) & (df['bid_vwap_elasticity'].notna())]
    bid_elasticity = valid_elasticity['bid_vwap_elasticity'].median()
    ask_elasticity = valid_elasticity['ask_vwap_elasticity'].median()
    
    print("1. VWAP AS HIDDEN SLIPPAGE INDICATOR:")
    print("-" * 100)
    print(f"   Average bid VWAP distance: ₹{bid_vwap_avg_dist:.2f}")
    print(f"   Average ask VWAP distance: ₹{ask_vwap_avg_dist:.2f}")
    print(f"   → For 100-lot order (7,500 qty), hidden slippage:")
    print(f"     BUY: ~₹{ask_vwap_avg_dist * 7500:,.0f}")
    print(f"     SELL: ~₹{bid_vwap_avg_dist * 7500:,.0f}")
    print()
    
    print("2. VWAP MOVEMENT TRACKING:")
    print("-" * 100)
    print(f"   Bid VWAP elasticity: {bid_elasticity:.4f}")
    print(f"   Ask VWAP elasticity: {ask_elasticity:.4f}")
    if abs(bid_elasticity) < 0.5:
        print(f"   → VWAP moves SLOWER than price (elastic = {bid_elasticity:.2f})")
        print(f"   → When price spikes, VWAP lags = opportunity for VWAP reversion trades")
    else:
        print(f"   → VWAP tracks price closely")
    print()
    
    print("3. MEAN REVERSION TRADE SETUP:")
    print("-" * 100)
    
    # Find optimal distance thresholds
    far_threshold = df['bid_vwap_dist'].quantile(0.75)
    close_threshold = df['bid_vwap_dist'].quantile(0.25)
    
    far_from_vwap = df[df['bid_vwap_dist'] > far_threshold]
    close_to_vwap = df[df['bid_vwap_dist'] < close_threshold]
    
    if len(far_from_vwap) > 0:
        far_return = (far_from_vwap['mid_price'].shift(-20) - far_from_vwap['mid_price']).mean()
        print(f"   When price >₹{far_threshold:.2f} above bid VWAP:")
        print(f"     Average 20-tick return: {far_return:+.3f}")
        if far_return < -0.2:
            print(f"     → SHORT setup (mean reversion down)")
    
    if len(close_to_vwap) > 0:
        close_return = (close_to_vwap['mid_price'].shift(-20) - close_to_vwap['mid_price']).mean()
        print(f"   When price <₹{close_threshold:.2f} above bid VWAP:")
        print(f"     Average 20-tick return: {close_return:+.3f}")
        if close_return > 0.2:
            print(f"     → LONG setup (mean reversion up)")
    print()
    
    print("4. VWAP GAP AS VOLATILITY INDICATOR:")
    print("-" * 100)
    print(f"   Average VWAP gap: ₹{vwap_gap_avg:.2f}")
    
    wide_gap = df[df['vwap_gap'] > df['vwap_gap'].quantile(0.75)]
    narrow_gap = df[df['vwap_gap'] < df['vwap_gap'].quantile(0.25)]
    
    if len(wide_gap) > 0:
        wide_volatility = wide_gap['price_change'].abs().mean()
        print(f"   Wide gap (>75th pct): Average price volatility = ₹{wide_volatility:.3f}")
    
    if len(narrow_gap) > 0:
        narrow_volatility = narrow_gap['price_change'].abs().mean()
        print(f"   Narrow gap (<25th pct): Average price volatility = ₹{narrow_volatility:.3f}")
    
    if len(wide_gap) > 0 and len(narrow_gap) > 0:
        if wide_volatility > narrow_volatility * 1.5:
            print(f"   → Wide VWAP gap = HIGH volatility ahead")
            print(f"   → Use wider stops and larger targets")
    print()

def main():
    """Main analysis pipeline"""
    print()
    print("=" * 100)
    print("BID/ASK VWAP BEHAVIOR ANALYSIS")
    print("=" * 100)
    print()
    
    # Fetch data
    df = fetch_data()
    
    if len(df) < 200:
        print("⚠️  Need at least 200 snapshots for analysis")
        return
    
    # Calculate metrics
    df = calculate_metrics(df)
    
    # Run analyses
    analyze_vwap_price_correlation(df)
    analyze_vwap_elasticity(df)
    analyze_vwap_distance_behavior(df)
    analyze_vwap_gap_dynamics(df)
    analyze_vwap_lead_lag(df)
    analyze_vwap_mean_reversion_strength(df)
    trading_insights(df)
    
    print("=" * 100)
    print("ANALYSIS COMPLETE")
    print("=" * 100)
    print()

if __name__ == "__main__":
    main()
