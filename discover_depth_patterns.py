"""
Comprehensive Pattern Discovery: Depth Metrics vs Price Movements
Statistical analysis to find ALL correlations and patterns in orderbook data
"""

import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from scipy import stats
from itertools import combinations

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
    print("Fetching data from database...")
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
    
    print(f"✓ Loaded {len(df):,} snapshots")
    print(f"  Time: {df['time_ist'].min()} to {df['time_ist'].max()}")
    print(f"  Duration: {(df['time_ist'].max() - df['time_ist'].min()).total_seconds():.0f}s")
    print()
    
    return df

def calculate_all_metrics(df):
    """Calculate comprehensive set of derived metrics"""
    print("Calculating all derived metrics...")
    
    # Price metrics
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2
    df['price_velocity'] = df['mid_price'].diff(1)
    df['price_acceleration'] = df['price_velocity'].diff(1)
    
    # Future returns (what we're trying to predict)
    for horizon in [1, 3, 5, 10, 20, 50, 100]:
        df[f'return_{horizon}'] = df['mid_price'].diff(horizon).shift(-horizon)
        df[f'abs_return_{horizon}'] = df[f'return_{horizon}'].abs()
    
    # Imbalance metrics
    df['imbalance_change'] = df['imbalance_ratio'].diff(1)
    df['imbalance_velocity'] = df['imbalance_change'].diff(1)
    df['imbalance_extreme'] = (df['imbalance_ratio'] - 1.0).abs()
    df['imbalance_direction'] = np.where(df['imbalance_ratio'] > 1.0, 1, -1)
    
    # Rolling imbalance features
    for window in [5, 10, 20, 50]:
        df[f'imbalance_ma{window}'] = df['imbalance_ratio'].rolling(window).mean()
        df[f'imbalance_std{window}'] = df['imbalance_ratio'].rolling(window).std()
        df[f'imbalance_trend{window}'] = df['imbalance_ratio'] - df[f'imbalance_ma{window}']
    
    # Spread metrics
    df['spread_pct'] = (df['spread'] / df['mid_price']) * 100
    df['spread_change'] = df['spread'].diff(1)
    df['spread_velocity'] = df['spread_change'].diff(1)
    df['spread_z_score'] = (df['spread'] - df['spread'].mean()) / df['spread'].std()
    
    for window in [10, 20, 50]:
        df[f'spread_ma{window}'] = df['spread'].rolling(window).mean()
        df[f'spread_expanding'] = (df['spread'] > df[f'spread_ma{window}']).astype(int)
    
    # VWAP metrics
    df['bid_vwap_dist'] = df['best_bid'] - df['bid_vwap']
    df['ask_vwap_dist'] = df['ask_vwap'] - df['best_ask']
    df['vwap_gap'] = df['ask_vwap'] - df['bid_vwap']
    df['bid_vwap_dist_pct'] = (df['bid_vwap_dist'] / df['bid_vwap']) * 100
    df['ask_vwap_dist_pct'] = (df['ask_vwap_dist'] / df['ask_vwap']) * 100
    
    # Liquidity metrics
    df['total_liquidity'] = df['total_bid_qty'] + df['total_ask_qty']
    df['liquidity_imbalance'] = df['total_bid_qty'] - df['total_ask_qty']
    df['liquidity_change'] = df['total_liquidity'].pct_change()
    df['bid_liquidity_change'] = df['total_bid_qty'].pct_change()
    df['ask_liquidity_change'] = df['total_ask_qty'].pct_change()
    
    # Order size metrics
    df['avg_order_size'] = (df['avg_bid_order_size'] + df['avg_ask_order_size']) / 2
    df['order_size_imbalance'] = df['avg_bid_order_size'] - df['avg_ask_order_size']
    df['order_size_ratio'] = df['avg_bid_order_size'] / df['avg_ask_order_size']
    
    # Order count metrics
    df['total_orders'] = df['total_bid_orders'] + df['total_ask_orders']
    df['order_count_imbalance'] = df['total_bid_orders'] - df['total_ask_orders']
    df['orders_per_unit_liquidity'] = df['total_orders'] / df['total_liquidity']
    
    # Concentration metrics
    df['avg_50pct_level'] = (df['bid_50pct_level'] + df['ask_50pct_level']) / 2
    df['concentration_imbalance'] = df['bid_50pct_level'] - df['ask_50pct_level']
    df['concentration_depth'] = np.where(df['avg_50pct_level'] > 100, 1, 0)  # Deep vs shallow
    
    # Composite indicators
    df['momentum_score'] = df['imbalance_ratio'] * df['price_velocity']
    df['pressure_score'] = df['imbalance_extreme'] * df['spread']
    df['liquidity_stress'] = df['spread'] / df['total_liquidity'] * 1000000
    
    # Regime classification
    df['price_regime'] = pd.qcut(df['mid_price'], q=5, labels=['Very Low', 'Low', 'Mid', 'High', 'Very High'], duplicates='drop')
    df['volatility_regime'] = pd.qcut(df['spread'], q=4, labels=['Calm', 'Normal', 'Active', 'Volatile'], duplicates='drop')
    
    print(f"✓ Created {len(df.columns)} total features")
    print()
    
    return df

def correlation_analysis(df):
    """Comprehensive correlation analysis with all future returns"""
    print("=" * 100)
    print("CORRELATION ANALYSIS: All Metrics vs Future Returns")
    print("=" * 100)
    print()
    
    # Select predictor variables (exclude future returns and metadata)
    predictors = [col for col in df.columns if not col.startswith('return_') 
                  and not col.startswith('abs_return_')
                  and col not in ['time_ist', 'best_bid', 'best_ask', 'mid_price', 
                                  'price_regime', 'volatility_regime']]
    
    # Target variables (future returns at different horizons)
    targets = [col for col in df.columns if col.startswith('return_') and not col.startswith('return_abs')]
    
    # Calculate correlations
    correlations = {}
    for target in targets:
        horizon = target.split('_')[1]
        corrs = []
        
        for predictor in predictors:
            if df[predictor].notna().sum() > 100:  # Need enough data
                corr, pval = stats.pearsonr(df[predictor].dropna(), 
                                           df[target][df[predictor].notna()].dropna())
                if not np.isnan(corr) and abs(corr) > 0.01:  # Filter noise
                    corrs.append({
                        'metric': predictor,
                        'correlation': corr,
                        'p_value': pval,
                        'significant': pval < 0.01
                    })
        
        correlations[horizon] = pd.DataFrame(corrs).sort_values('correlation', 
                                                                  key=abs, 
                                                                  ascending=False)
    
    # Print top correlations for each horizon
    for horizon in ['1', '5', '20', '100']:
        if horizon in correlations:
            print(f"Top Correlations → {horizon}-Tick Future Return:")
            print("-" * 100)
            top_corrs = correlations[horizon].head(15)
            for _, row in top_corrs.iterrows():
                sig = "***" if row['p_value'] < 0.001 else "**" if row['p_value'] < 0.01 else "*"
                strength = "STRONG" if abs(row['correlation']) > 0.3 else "MODERATE" if abs(row['correlation']) > 0.15 else "WEAK"
                print(f"  {row['metric']:<35} {row['correlation']:+.4f} {sig:<3} ({strength})")
            print()
    
    return correlations

def non_linear_patterns(df):
    """Detect non-linear relationships using binning"""
    print("=" * 100)
    print("NON-LINEAR PATTERN DETECTION")
    print("=" * 100)
    print()
    
    # Test key metrics with quintile binning
    metrics_to_test = [
        ('imbalance_ratio', 'Imbalance'),
        ('spread', 'Spread'),
        ('bid_vwap_dist', 'Bid VWAP Distance'),
        ('total_liquidity', 'Total Liquidity'),
        ('avg_50pct_level', 'Liquidity Concentration'),
        ('imbalance_change', 'Imbalance Change'),
        ('momentum_score', 'Momentum Score')
    ]
    
    for metric, name in metrics_to_test:
        if metric in df.columns and df[metric].notna().sum() > 100:
            # Create quintiles
            try:
                df[f'{metric}_quintile'] = pd.qcut(df[metric], q=5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'], duplicates='drop')
                
                # Analyze returns by quintile
                results = df.groupby(f'{metric}_quintile', observed=True).agg({
                    'return_5': 'mean',
                    'return_20': 'mean',
                    'return_100': 'mean',
                    'mid_price': 'count'
                }).round(3)
                
                results.columns = ['5-Tick', '20-Tick', '100-Tick', 'Count']
                
                # Check for monotonic relationship
                is_monotonic = all(results['20-Tick'].diff().dropna() > 0) or all(results['20-Tick'].diff().dropna() < 0)
                
                # Check for U-shape or inverse-U
                middle_extreme = abs(results['20-Tick'].iloc[2]) < abs(results['20-Tick'].iloc[[0, -1]].mean())
                
                print(f"{name}:")
                print(results)
                if is_monotonic:
                    print("  → Linear relationship detected")
                elif middle_extreme:
                    print("  → U-shaped or inverse-U relationship detected")
                else:
                    print("  → Non-monotonic pattern")
                print()
                
            except Exception as e:
                continue

def interaction_effects(df):
    """Find powerful combinations of metrics"""
    print("=" * 100)
    print("INTERACTION EFFECTS: Metric Combinations")
    print("=" * 100)
    print()
    
    # Define metric pairs to test
    pairs = [
        ('imbalance_ratio', 'spread', 'Imbalance × Spread'),
        ('imbalance_ratio', 'bid_vwap_dist', 'Imbalance × VWAP Distance'),
        ('spread', 'total_liquidity', 'Spread × Liquidity'),
        ('imbalance_change', 'spread_change', 'Imbalance Change × Spread Change'),
        ('imbalance_extreme', 'spread', 'Extreme Imbalance × Spread'),
        ('bid_vwap_dist', 'spread', 'VWAP Distance × Spread'),
    ]
    
    for metric1, metric2, name in pairs:
        if metric1 in df.columns and metric2 in df.columns:
            # Create combination categories
            try:
                df[f'{metric1}_cat'] = pd.qcut(df[metric1], q=3, labels=['Low', 'Mid', 'High'], duplicates='drop')
                df[f'{metric2}_cat'] = pd.qcut(df[metric2], q=3, labels=['Low', 'Mid', 'High'], duplicates='drop')
                
                # Cross-tabulation
                results = df.pivot_table(
                    values='return_20',
                    index=f'{metric1}_cat',
                    columns=f'{metric2}_cat',
                    aggfunc='mean'
                ).round(3)
                
                print(f"{name}:")
                print(results)
                
                # Find best combination
                best_val = results.max().max()
                worst_val = results.min().min()
                best_combo = results.stack().idxmax()
                worst_combo = results.stack().idxmin()
                
                print(f"  Best combo: {best_combo[0]} {metric1.split('_')[0]} + {best_combo[1]} {metric2.split('_')[0]} = {best_val:+.3f}")
                print(f"  Worst combo: {worst_combo[0]} {metric1.split('_')[0]} + {worst_combo[1]} {metric2.split('_')[0]} = {worst_val:+.3f}")
                print()
                
            except Exception as e:
                continue

def regime_based_analysis(df):
    """Analyze how patterns change in different market regimes"""
    print("=" * 100)
    print("REGIME-BASED ANALYSIS")
    print("=" * 100)
    print()
    
    # Price trend regime (based on recent price movement)
    df['price_trend'] = df['mid_price'].rolling(50).apply(lambda x: 1 if x.iloc[-1] > x.iloc[0] else -1)
    df['trending'] = df['price_trend'].notna()
    
    # Volatility regime
    df['recent_volatility'] = df['return_5'].rolling(20).std()
    df['high_vol'] = df['recent_volatility'] > df['recent_volatility'].quantile(0.75)
    
    print("Imbalance Signal by Price Trend:")
    print("-" * 100)
    
    for trend, trend_name in [(1, 'UPTREND'), (-1, 'DOWNTREND')]:
        trend_data = df[df['price_trend'] == trend]
        if len(trend_data) > 100:
            print(f"\n{trend_name}:")
            
            # Test imbalance in this regime
            results = trend_data.groupby(pd.qcut(trend_data['imbalance_ratio'], q=5, 
                                                  labels=['Very Ask-Heavy', 'Ask-Heavy', 'Balanced', 
                                                         'Bid-Heavy', 'Very Bid-Heavy'],
                                                  duplicates='drop'), 
                                        observed=True).agg({
                'return_20': 'mean',
                'return_100': 'mean',
                'mid_price': 'count'
            }).round(3)
            
            results.columns = ['20-Tick', '100-Tick', 'Count']
            print(results)
    
    print("\n\nImbalance Signal by Volatility Regime:")
    print("-" * 100)
    
    for vol_regime, vol_name in [(True, 'HIGH VOLATILITY'), (False, 'LOW VOLATILITY')]:
        vol_data = df[df['high_vol'] == vol_regime]
        if len(vol_data) > 100:
            print(f"\n{vol_name}:")
            
            results = vol_data.groupby(pd.qcut(vol_data['imbalance_ratio'], q=5,
                                               labels=['Very Ask-Heavy', 'Ask-Heavy', 'Balanced',
                                                      'Bid-Heavy', 'Very Bid-Heavy'],
                                               duplicates='drop'),
                                      observed=True).agg({
                'return_20': 'mean',
                'return_100': 'mean',
                'mid_price': 'count'
            }).round(3)
            
            results.columns = ['20-Tick', '100-Tick', 'Count']
            print(results)
    
    print()

def discovery_insights(correlations, df):
    """Generate insights from discovered patterns"""
    print("=" * 100)
    print("PATTERN DISCOVERY INSIGHTS")
    print("=" * 100)
    print()
    
    # Find strongest predictors for each timeframe
    print("Strongest Predictors by Timeframe:")
    print("-" * 100)
    
    for horizon in ['1', '5', '20', '100']:
        if horizon in correlations and len(correlations[horizon]) > 0:
            top_metric = correlations[horizon].iloc[0]
            print(f"\n{horizon}-Tick Horizon:")
            print(f"  Best predictor: {top_metric['metric']}")
            print(f"  Correlation: {top_metric['correlation']:+.4f}")
            print(f"  Significance: p={top_metric['p_value']:.6f}")
            
            # Show directional impact
            metric_name = top_metric['metric']
            if metric_name in df.columns:
                high_val = df[df[metric_name] > df[metric_name].quantile(0.75)][f'return_{horizon}'].mean()
                low_val = df[df[metric_name] < df[metric_name].quantile(0.25)][f'return_{horizon}'].mean()
                
                print(f"  When {metric_name} is HIGH: {high_val:+.3f} average return")
                print(f"  When {metric_name} is LOW: {low_val:+.3f} average return")
                print(f"  Difference: {high_val - low_val:+.3f}")
    
    print("\n\nUnexpected Findings:")
    print("-" * 100)
    
    # Find counter-intuitive patterns
    interesting = []
    
    for horizon in ['20', '100']:
        if horizon in correlations:
            corr_df = correlations[horizon]
            
            # Metrics that have opposite correlation at different timeframes
            if '5' in correlations:
                short_term = correlations['5'].set_index('metric')['correlation']
                long_term = corr_df.set_index('metric')['correlation']
                
                for metric in short_term.index:
                    if metric in long_term.index:
                        if short_term[metric] * long_term[metric] < 0 and abs(short_term[metric]) > 0.1:
                            interesting.append(f"  {metric}: reverses from {short_term[metric]:+.3f} (5-tick) to {long_term[metric]:+.3f} ({horizon}-tick)")
    
    if interesting:
        for finding in interesting[:5]:
            print(finding)
    else:
        print("  No major reversals found - patterns are consistent across timeframes")
    
    print()

def main():
    """Main analysis pipeline"""
    print()
    print("=" * 100)
    print("COMPREHENSIVE PATTERN DISCOVERY: Depth Metrics vs Price Movements")
    print("=" * 100)
    print()
    
    # Load data
    df = fetch_depth_data()
    
    if len(df) < 200:
        print("⚠️  Need at least 200 snapshots for statistical significance")
        return
    
    # Calculate all metrics
    df = calculate_all_metrics(df)
    
    # Remove rows with insufficient future data
    df = df[df['return_100'].notna()].copy()
    
    print(f"Analysis sample: {len(df):,} snapshots with complete data")
    print()
    
    # Run analyses
    correlations = correlation_analysis(df)
    non_linear_patterns(df)
    interaction_effects(df)
    regime_based_analysis(df)
    discovery_insights(correlations, df)
    
    print("=" * 100)
    print("ANALYSIS COMPLETE")
    print("=" * 100)
    print()

if __name__ == "__main__":
    main()
