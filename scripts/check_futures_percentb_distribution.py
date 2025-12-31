"""
Analyze the distribution of Bollinger %B values on NIFTY futures volume
to understand why we're getting so few signals
"""
import sys
import os
import psycopg2
import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import analysis functions
from scripts.bollinger_b_analysis import bollinger_percent_b


def get_db_connection():
    """Connect to PostgreSQL database"""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    db_host = 'localhost'
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'tradingdb')
    db_user = os.getenv('DB_USER', 'tradinguser')
    db_password = os.getenv('DB_PASSWORD')
    
    return psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )


def load_nifty_futures_data():
    """Load NIFTY futures continuous data from database"""
    conn = get_db_connection()
    
    try:
        query = """
            SELECT time as date, open, high, low, close, volume, oi
            FROM historical_data
            WHERE trading_symbol = %s AND timeframe = %s
            ORDER BY time ASC
        """
        
        df = pd.read_sql(query, conn, params=('NIFTY_FUT', 'day'))
        
        if df.empty:
            return None
        
        df['date'] = pd.to_datetime(df['date'])
        return df
        
    finally:
        conn.close()


def main():
    print("=" * 100)
    print(" " * 25 + "NIFTY FUTURES BOLLINGER %B DISTRIBUTION ANALYSIS")
    print("=" * 100)
    
    # Load data
    print("\nLoading NIFTY Futures data...")
    df = load_nifty_futures_data()
    
    if df is None or len(df) < 50:
        print("✗ No data found")
        return
    
    print(f"✓ Loaded {len(df)} candles from {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    
    # Calculate Bollinger %B on volume
    print("\nCalculating Bollinger %B on volume...")
    df['percent_b'], _, _, _ = bollinger_percent_b(df['volume'], 20, 2, ma_type='sma')
    
    # Drop NaN values (first 20 candles don't have %B)
    valid_df = df.dropna(subset=['percent_b'])
    print(f"✓ Calculated %B for {len(valid_df)} candles (skipped first 20 for MA warmup)")
    
    # Distribution statistics
    print("\n" + "=" * 100)
    print("BOLLINGER %B DISTRIBUTION ON VOLUME")
    print("=" * 100)
    
    print(f"\nBasic Statistics:")
    print(f"  Mean: {valid_df['percent_b'].mean():.2f}")
    print(f"  Median: {valid_df['percent_b'].median():.2f}")
    print(f"  Std Dev: {valid_df['percent_b'].std():.2f}")
    print(f"  Min: {valid_df['percent_b'].min():.2f}")
    print(f"  Max: {valid_df['percent_b'].max():.2f}")
    
    # Percentile breakdown
    print(f"\nPercentile Breakdown:")
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    for p in percentiles:
        val = np.percentile(valid_df['percent_b'], p)
        print(f"  {p:2d}th percentile: {val:7.2f}")
    
    # Count extreme values
    print(f"\n" + "=" * 100)
    print("EXTREME VALUE COUNTS (Current Strategy Thresholds)")
    print("=" * 100)
    
    extreme_high = len(valid_df[valid_df['percent_b'] > 105])
    extreme_low = len(valid_df[valid_df['percent_b'] < -5])
    
    print(f"\nCurrent Thresholds (Equities Strategy):")
    print(f"  %B > 105 (Bullish alerts): {extreme_high} candles ({extreme_high/len(valid_df)*100:.2f}%)")
    print(f"  %B < -5  (Bearish alerts): {extreme_low} candles ({extreme_low/len(valid_df)*100:.2f}%)")
    print(f"  Total extreme signals: {extreme_high + extreme_low}")
    
    # Test alternative thresholds
    print(f"\n" + "=" * 100)
    print("SIGNAL COUNT WITH ALTERNATIVE THRESHOLDS")
    print("=" * 100)
    
    threshold_tests = [
        (100, 0),
        (90, 10),
        (80, 20),
        (70, 30),
        (60, 40),
    ]
    
    for bull_thresh, bear_thresh in threshold_tests:
        bull_count = len(valid_df[valid_df['percent_b'] > bull_thresh])
        bear_count = len(valid_df[valid_df['percent_b'] < bear_thresh])
        total = bull_count + bear_count
        pct = total / len(valid_df) * 100
        print(f"  Threshold >={bull_thresh:3d} / <={bear_thresh:2d}: "
              f"Bull={bull_count:4d}, Bear={bear_count:4d}, Total={total:4d} ({pct:5.2f}%)")
    
    # Show actual extreme dates
    print(f"\n" + "=" * 100)
    print(f"ACTUAL EXTREME SIGNALS (Current Threshold: >105 / <-5)")
    print("=" * 100)
    
    high_signals = valid_df[valid_df['percent_b'] > 105].sort_values(by='date')
    low_signals = valid_df[valid_df['percent_b'] < -5].sort_values(by='date')
    
    if len(high_signals) > 0:
        print(f"\nBullish Alerts (%B > 105): {len(high_signals)}")
        for idx, row in high_signals.iterrows():
            print(f"  {row['date'].strftime('%Y-%m-%d')}: %B = {row['percent_b']:7.2f}, Volume = {row['volume']:,}")
    else:
        print(f"\nNo bullish alerts (%B > 105) found in entire dataset!")
    
    if len(low_signals) > 0:
        print(f"\nBearish Alerts (%B < -5): {len(low_signals)}")
        for idx, row in low_signals.iterrows():
            print(f"  {row['date'].strftime('%Y-%m-%d')}: %B = {row['percent_b']:7.2f}, Volume = {row['volume']:,}")
    else:
        print(f"\nNo bearish alerts (%B < -5) found in entire dataset!")
    
    print("\n" + "=" * 100)


if __name__ == '__main__':
    main()
