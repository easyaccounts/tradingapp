"""
Analyze Bid vs Ask Order Counts and Price Movement
Focus: Order count as retail participation indicator
"""

import psycopg2
import pandas as pd
import numpy as np
from scipy import stats

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tradingdb',
    'user': 'tradinguser',
    'password': '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
}

def fetch_data():
    """Fetch order count and price data"""
    print("Fetching order count data...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    query = """
    SELECT 
        timestamp AT TIME ZONE 'Asia/Kolkata' as time_ist,
        best_bid,
        best_ask,
        total_bid_qty,
        total_ask_qty,
        total_bid_orders,
        total_ask_orders,
        imbalance_ratio,
        avg_bid_order_size,
        avg_ask_order_size
    FROM depth_200_snapshots
    ORDER BY timestamp ASC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"✓ Loaded {len(df):,} snapshots")
    print()
    return df

def calculate_metrics(df):
    """Calculate order-based metrics"""
    print("Calculating order metrics...")
    
    # Price
    df['mid_price'] = (df['best_bid'] + df['best_ask']) / 2
    
    # Order count metrics
    df['order_count_imbalance'] = df['total_bid_orders'] / df['total_ask_orders']
    df['total_orders'] = df['total_bid_orders'] + df['total_ask_orders']
    df['order_count_diff'] = df['total_bid_orders'] - df['total_ask_orders']
    
    # Quantity vs Order divergence
    df['qty_imbalance'] = df['imbalance_ratio']  # Already calculated (bid_qty / ask_qty)
    df['imbalance_divergence'] = df['order_count_imbalance'] - df['qty_imbalance']
    
    # Order size analysis
    df['bid_order_size'] = df['total_bid_qty'] / df['total_bid_orders']
    df['ask_order_size'] = df['total_ask_qty'] / df['total_ask_orders']
    df['order_size_ratio'] = df['bid_order_size'] / df['ask_order_size']
    
    # Changes
    df['order_count_imb_change'] = df['order_count_imbalance'].diff()
    df['bid_orders_change'] = df['total_bid_orders'].diff()
    df['ask_orders_change'] = df['total_ask_orders'].diff()
    
    # Price movements (future returns)
    for horizon in [1, 5, 10, 20, 50]:
        df[f'return_{horizon}'] = df['mid_price'].diff(horizon).shift(-horizon)
    
    print(f"✓ Calculated metrics")
    print()
    return df

def analyze_order_count_correlation(df):
    """Correlate order counts with price movement"""
    print("="*100)
    print("ANALYSIS 1: ORDER COUNT vs PRICE MOVEMENT CORRELATION")
    print("="*100)
    print()
    
    metrics = [
        'order_count_imbalance',
        'order_count_diff',
        'total_bid_orders',
        'total_ask_orders',
        'total_orders',
        'imbalance_divergence',
        'order_size_ratio'
    ]
    
    horizons = [1, 5, 10, 20, 50]
    
    print(f"{'Metric':<30} {'1-Tick':<10} {'5-Tick':<10} {'10-Tick':<10} {'20-Tick':<10} {'50-Tick':<10}")
    print("-"*100)
    
    for metric in metrics:
        correlations = []
        for h in horizons:
            corr = df[[metric, f'return_{h}']].corr().iloc[0, 1]
            correlations.append(f"{corr:+.4f}" if pd.notna(corr) else "N/A")
        
        print(f"{metric:<30} {correlations[0]:<10} {correlations[1]:<10} {correlations[2]:<10} {correlations[3]:<10} {correlations[4]:<10}")
    
    print()

def analyze_order_imbalance_quintiles(df):
    """Analyze returns by order count imbalance quintiles"""
    print("="*100)
    print("ANALYSIS 2: RETURNS BY ORDER COUNT IMBALANCE")
    print("="*100)
    print()
    
    # Create quintiles
    df['order_imb_quintile'] = pd.qcut(
        df['order_count_imbalance'],
        q=5,
        labels=['Very Ask-Heavy', 'Ask-Heavy', 'Balanced', 'Bid-Heavy', 'Very Bid-Heavy'],
        duplicates='drop'
    )
    
    print("Future Returns by Order Count Imbalance Quintile:")
    print("-"*100)
    
    quintile_analysis = df.groupby('order_imb_quintile', observed=True).agg({
        'return_1': 'mean',
        'return_5': 'mean',
        'return_10': 'mean',
        'return_20': 'mean',
        'return_50': 'mean',
        'order_count_imbalance': ['mean', 'min', 'max'],
        'mid_price': 'count'
    }).round(4)
    
    print(quintile_analysis)
    print()
    
    # Check if pattern is linear or non-linear
    very_ask = quintile_analysis.loc['Very Ask-Heavy', ('return_20', 'mean')] if 'Very Ask-Heavy' in quintile_analysis.index else None
    very_bid = quintile_analysis.loc['Very Bid-Heavy', ('return_20', 'mean')] if 'Very Bid-Heavy' in quintile_analysis.index else None
    
    if very_ask is not None and very_bid is not None:
        edge = very_bid - very_ask
        print(f"Edge: Very Bid-Heavy vs Very Ask-Heavy (20-tick): {edge:+.4f}")
        if abs(edge) > 0.5:
            print(f"→ STRONG predictive power")
        elif abs(edge) > 0.2:
            print(f"→ MODERATE predictive power")
        else:
            print(f"→ WEAK predictive power")
    print()

def analyze_order_surges(df):
    """Detect sudden order count changes"""
    print("="*100)
    print("ANALYSIS 3: ORDER SURGE EVENTS")
    print("="*100)
    print()
    
    # Define surge thresholds
    bid_order_threshold = df['bid_orders_change'].quantile(0.95)
    ask_order_threshold = df['ask_orders_change'].quantile(0.95)
    
    # Find surges
    bid_surges = df[df['bid_orders_change'] > bid_order_threshold].copy()
    ask_surges = df[df['ask_orders_change'] > ask_order_threshold].copy()
    
    print(f"Bid Order Surge Threshold: +{bid_order_threshold:.0f} orders")
    print(f"Found {len(bid_surges)} bid order surge events")
    print()
    
    if len(bid_surges) > 0:
        print("Bid Order Surges - Future Returns:")
        print(f"  1-tick:  {bid_surges['return_1'].mean():+.4f}")
        print(f"  5-tick:  {bid_surges['return_5'].mean():+.4f}")
        print(f"  10-tick: {bid_surges['return_10'].mean():+.4f}")
        print(f"  20-tick: {bid_surges['return_20'].mean():+.4f}")
        print(f"  50-tick: {bid_surges['return_50'].mean():+.4f}")
        print()
    
    print(f"Ask Order Surge Threshold: +{ask_order_threshold:.0f} orders")
    print(f"Found {len(ask_surges)} ask order surge events")
    print()
    
    if len(ask_surges) > 0:
        print("Ask Order Surges - Future Returns:")
        print(f"  1-tick:  {ask_surges['return_1'].mean():+.4f}")
        print(f"  5-tick:  {ask_surges['return_5'].mean():+.4f}")
        print(f"  10-tick: {ask_surges['return_10'].mean():+.4f}")
        print(f"  20-tick: {ask_surges['return_20'].mean():+.4f}")
        print(f"  50-tick: {ask_surges['return_50'].mean():+.4f}")
        print()

def analyze_order_qty_divergence(df):
    """Analyze when order count and quantity imbalance diverge"""
    print("="*100)
    print("ANALYSIS 4: ORDER COUNT vs QUANTITY IMBALANCE DIVERGENCE")
    print("="*100)
    print()
    
    print("What this shows:")
    print("  Positive divergence: More BID orders than quantities suggest (retail buying)")
    print("  Negative divergence: More ASK orders than quantities suggest (retail selling)")
    print()
    
    # Create divergence categories
    df['divergence_category'] = pd.cut(
        df['imbalance_divergence'],
        bins=[-999, -0.1, -0.05, 0.05, 0.1, 999],
        labels=['Strong Retail Sell', 'Retail Sell', 'Aligned', 'Retail Buy', 'Strong Retail Buy']
    )
    
    divergence_analysis = df.groupby('divergence_category', observed=True).agg({
        'return_1': 'mean',
        'return_5': 'mean',
        'return_10': 'mean',
        'return_20': 'mean',
        'return_50': 'mean',
        'imbalance_divergence': 'mean',
        'mid_price': 'count'
    }).round(4)
    
    print("Future Returns by Order-Quantity Divergence:")
    print("-"*100)
    print(divergence_analysis)
    print()
    
    # Interpretation
    if 'Strong Retail Buy' in divergence_analysis.index and 'Strong Retail Sell' in divergence_analysis.index:
        retail_buy = divergence_analysis.loc['Strong Retail Buy', ('return_20', 'mean')]
        retail_sell = divergence_analysis.loc['Strong Retail Sell', ('return_20', 'mean')]
        
        print("Interpretation (20-tick horizon):")
        print(f"  Strong Retail Buy (many small bid orders): {retail_buy:+.4f}")
        print(f"  Strong Retail Sell (many small ask orders): {retail_sell:+.4f}")
        
        if retail_buy < -0.2:
            print(f"  → Retail buying = BEARISH (contrarian signal works!)")
        if retail_sell > 0.2:
            print(f"  → Retail selling = BULLISH (contrarian signal works!)")
    print()

def analyze_extreme_order_counts(df):
    """Analyze extreme order count levels"""
    print("="*100)
    print("ANALYSIS 5: EXTREME ORDER COUNT LEVELS")
    print("="*100)
    print()
    
    # Find extremes
    bid_orders_high = df['total_bid_orders'].quantile(0.90)
    bid_orders_low = df['total_bid_orders'].quantile(0.10)
    ask_orders_high = df['total_ask_orders'].quantile(0.90)
    ask_orders_low = df['total_ask_orders'].quantile(0.10)
    
    # Create categories
    high_bid_orders = df[df['total_bid_orders'] > bid_orders_high]
    low_bid_orders = df[df['total_bid_orders'] < bid_orders_low]
    high_ask_orders = df[df['total_ask_orders'] > ask_orders_high]
    low_ask_orders = df[df['total_ask_orders'] < ask_orders_low]
    
    print(f"High Bid Orders (>{bid_orders_high:.0f}):")
    print(f"  Count: {len(high_bid_orders)}")
    print(f"  20-tick return: {high_bid_orders['return_20'].mean():+.4f}")
    print()
    
    print(f"Low Bid Orders (<{bid_orders_low:.0f}):")
    print(f"  Count: {len(low_bid_orders)}")
    print(f"  20-tick return: {low_bid_orders['return_20'].mean():+.4f}")
    print()
    
    print(f"High Ask Orders (>{ask_orders_high:.0f}):")
    print(f"  Count: {len(high_ask_orders)}")
    print(f"  20-tick return: {high_ask_orders['return_20'].mean():+.4f}")
    print()
    
    print(f"Low Ask Orders (<{ask_orders_low:.0f}):")
    print(f"  Count: {len(low_ask_orders)}")
    print(f"  20-tick return: {low_ask_orders['return_20'].mean():+.4f}")
    print()

def find_tradeable_setups(df):
    """Identify specific tradeable patterns based on order counts"""
    print("="*100)
    print("TRADEABLE SETUPS FROM ORDER COUNT ANALYSIS")
    print("="*100)
    print()
    
    # Pattern 1: Extreme order count imbalance
    extreme_bid_heavy = df[df['order_count_imbalance'] > df['order_count_imbalance'].quantile(0.95)]
    extreme_ask_heavy = df[df['order_count_imbalance'] < df['order_count_imbalance'].quantile(0.05)]
    
    print("Pattern 1: EXTREME ORDER IMBALANCE")
    print("-"*100)
    print(f"Extreme Bid-Heavy Orders (>95th percentile):")
    print(f"  Threshold: {df['order_count_imbalance'].quantile(0.95):.3f}")
    print(f"  Occurrences: {len(extreme_bid_heavy)}")
    print(f"  Avg 20-tick return: {extreme_bid_heavy['return_20'].mean():+.4f}")
    print(f"  Win rate (>0): {(extreme_bid_heavy['return_20'] > 0).sum() / len(extreme_bid_heavy) * 100:.1f}%")
    print()
    
    print(f"Extreme Ask-Heavy Orders (<5th percentile):")
    print(f"  Threshold: {df['order_count_imbalance'].quantile(0.05):.3f}")
    print(f"  Occurrences: {len(extreme_ask_heavy)}")
    print(f"  Avg 20-tick return: {extreme_ask_heavy['return_20'].mean():+.4f}")
    print(f"  Win rate (>0): {(extreme_ask_heavy['return_20'] > 0).sum() / len(extreme_ask_heavy) * 100:.1f}%")
    print()
    
    # Pattern 2: Order surge + divergence
    bid_surge_thresh = df['bid_orders_change'].quantile(0.90)
    order_surges_with_divergence = df[
        (df['bid_orders_change'] > bid_surge_thresh) & 
        (df['imbalance_divergence'] > 0.05)
    ]
    
    print("Pattern 2: BID ORDER SURGE + RETAIL BUYING")
    print("-"*100)
    print(f"Conditions: Bid orders surge >{bid_surge_thresh:.0f} + Many small orders")
    print(f"Occurrences: {len(order_surges_with_divergence)}")
    if len(order_surges_with_divergence) > 0:
        print(f"Avg 20-tick return: {order_surges_with_divergence['return_20'].mean():+.4f}")
        print(f"Interpretation: Retail FOMO buying - contrarian SHORT?")
    print()
    
    # Pattern 3: Order exhaustion (very low orders)
    low_total_orders = df[df['total_orders'] < df['total_orders'].quantile(0.10)]
    
    print("Pattern 3: ORDER EXHAUSTION (Low Participation)")
    print("-"*100)
    print(f"Threshold: <{df['total_orders'].quantile(0.10):.0f} total orders")
    print(f"Occurrences: {len(low_total_orders)}")
    print(f"Avg 20-tick return: {low_total_orders['return_20'].mean():+.4f}")
    print(f"Interpretation: Low participation = range-bound or reversal zone")
    print()
    
    print("="*100)
    print()

def main():
    """Main analysis"""
    print()
    print("="*100)
    print("ORDER COUNT ANALYSIS")
    print("="*100)
    print()
    
    # Fetch and prepare data
    df = fetch_data()
    df = calculate_metrics(df)
    
    # Run analyses
    analyze_order_count_correlation(df)
    analyze_order_imbalance_quintiles(df)
    analyze_order_surges(df)
    analyze_order_qty_divergence(df)
    analyze_extreme_order_counts(df)
    find_tradeable_setups(df)
    
    print("="*100)
    print("ANALYSIS COMPLETE")
    print("="*100)
    print()

if __name__ == "__main__":
    main()
