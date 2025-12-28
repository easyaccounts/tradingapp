"""
Run Bollinger %B analysis on NIFTY Futures continuous data from database
Tests the extreme continuation strategy on daily futures timeframe
"""

import sys
import os
import psycopg2
import pandas as pd
import numpy as np

# Add parent directory to path for importing bollinger_b_analysis functions
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import analysis functions
exec(open('scripts/bollinger_b_analysis.py').read().split('if __name__')[0])


def get_db_connection():
    """Connect to PostgreSQL database - try configured host, fallback to localhost"""
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'tradingdb')
    db_user = os.getenv('DB_USER', 'tradinguser')
    db_password = os.getenv('DB_PASSWORD', '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=')
    
    # Try configured host first
    try:
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
    except psycopg2.OperationalError as e:
        # If pgbouncer or other Docker hostname fails, try localhost
        if db_host != 'localhost':
            print(f"Configured host '{db_host}' failed, trying localhost...")
            return psycopg2.connect(
                host='localhost',
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
        else:
            raise e


def load_nifty_futures_data(timeframe='day'):
    """Load NIFTY futures continuous data from database"""
    conn = get_db_connection()
    
    try:
        query = """
            SELECT time as date, open, high, low, close, volume, oi
            FROM historical_data
            WHERE trading_symbol = %s AND timeframe = %s
            ORDER BY time ASC
        """
        
        df = pd.read_sql(query, conn, params=('NIFTY_FUT', timeframe))
        
        if df.empty:
            return None
        
        df['date'] = pd.to_datetime(df['date'])
        return df
        
    finally:
        conn.close()


def main():
    print("=" * 100)
    print(" " * 30 + "NIFTY FUTURES BOLLINGER %B ANALYSIS")
    print("=" * 100)
    
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    print("\n1. Loading NIFTY Futures continuous data from database...")
    df = load_nifty_futures_data(timeframe='day')
    
    if df is None or len(df) < 50:
        print("✗ No NIFTY futures data found or insufficient data")
        print("Please run: python scripts/download_nifty_futures.py first")
        return
    
    print(f"✓ Loaded {len(df)} candles")
    print(f"  Date range: {df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    
    # Calculate Bollinger %B
    print("\n2. Calculating Bollinger %B on Volume...")
    df['percent_b'], _, _, _ = bollinger_percent_b(df['volume'], 20, 2, ma_type='sma')
    
    # Identify signals
    print("3. Identifying extreme signals (volume > avg)...")
    alerts = identify_signals(df, df['percent_b'])
    print(f"✓ Found {len(alerts)} alert signals")
    
    if len(alerts) == 0:
        print("\n✗ No signals found. Strategy may not work on this data.")
        return
    
    # Analyze performance
    print("\n4. Analyzing signal performance (with transaction costs & slippage)...")
    results = analyze_signal_performance(df, alerts, confirmation_window=10)
    
    if len(results) == 0:
        print("\n✗ No confirmed trades found within the confirmation window.")
        return
    
    print(f"✓ Analyzed {len(results)} confirmed trades")
    
    # Calculate stats
    print("\n" + "=" * 100)
    print("NIFTY FUTURES TRADING RESULTS")
    print("=" * 100)
    
    winning_trades = sum(1 for r in results if r['pnl_net'] > 0)
    total_trades = len(results)
    win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
    total_pnl_gross = sum(r['pnl_gross'] for r in results)
    total_pnl_net = sum(r['pnl_net'] for r in results)
    avg_pnl_gross = total_pnl_gross / total_trades if total_trades > 0 else 0
    avg_pnl_net = total_pnl_net / total_trades if total_trades > 0 else 0
    avg_cost = np.mean([r['transaction_cost'] for r in results])
    total_cost = sum(r['transaction_cost'] for r in results)
    
    print(f"\nTotal Confirmed Trades: {total_trades}")
    print(f"Win Rate: {winning_trades}/{total_trades} ({win_rate:.1f}%)")
    print(f"Total P&L (Gross): {total_pnl_gross:+.2f}%")
    print(f"Total P&L (Net): {total_pnl_net:+.2f}%")
    print(f"Total Transaction Costs: {total_cost:.2f}%")
    print(f"Total Slippage: {len(results) * 0.05:.2f}%")
    print(f"Average P&L per Trade (Gross): {avg_pnl_gross:+.2f}%")
    print(f"Average P&L per Trade (Net): {avg_pnl_net:+.2f}%")
    print(f"Average Transaction Cost per Trade: {avg_cost:.3f}%")
    print(f"Average Slippage per Trade: 0.050%")
    
    # Breakdown by signal type
    bullish_signals = [r for r in results if r['signal_type'] == 'BULLISH_ALERT']
    bearish_signals = [r for r in results if r['signal_type'] == 'BEARISH_ALERT']
    
    if bullish_signals:
        bull_win = sum(1 for r in bullish_signals if r['pnl_net'] > 0)
        bull_pnl = sum(r['pnl_net'] for r in bullish_signals)
        print(f"\n🟢 Bullish Trades: {len(bullish_signals)} | Win: {bull_win} ({bull_win/len(bullish_signals)*100:.1f}%) | Net P&L: {bull_pnl:+.2f}%")
    
    if bearish_signals:
        bear_win = sum(1 for r in bearish_signals if r['pnl_net'] > 0)
        bear_pnl = sum(r['pnl_net'] for r in bearish_signals)
        print(f"🔴 Bearish Trades: {len(bearish_signals)} | Win: {bear_win} ({bear_win/len(bearish_signals)*100:.1f}%) | Net P&L: {bear_pnl:+.2f}%")
    
    # Latest 5 trades
    print(f"\n📊 Latest 5 Trades:")
    for r in results[-5:]:
        win_loss = "WIN" if r['pnl_net'] > 0 else "LOSS"
        signal_type = "🟢" if r['signal_type'] == 'BULLISH_ALERT' else "🔴"
        print(f"   {signal_type} {r['alert_date'].strftime('%Y-%m-%d')} → {r['exit_date'].strftime('%Y-%m-%d')} | "
              f"Entry: ₹{r['entry_price']:.2f} | Exit: ₹{r['exit_price']:.2f} | "
              f"Net P&L: {r['pnl_net']:+.2f}% | {win_loss} | {r['exit_reason']}")
    
    print("\n" + "=" * 100)
    print("Analysis Complete!")
    print("=" * 100)


if __name__ == '__main__':
    main()
