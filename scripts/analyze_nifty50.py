"""
Run Bollinger %B analysis on all Nifty 50 symbols from database
Tests the extreme continuation strategy on daily timeframe
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

# Nifty 50 symbols
NIFTY50_SYMBOLS = [
    'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK',
    'HINDUNILVR', 'ITC', 'SBIN', 'BHARTIARTL', 'KOTAKBANK',
    'LT', 'AXISBANK', 'ASIANPAINT', 'MARUTI', 'HCLTECH',
    'BAJFINANCE', 'WIPRO', 'ULTRACEMCO', 'TITAN', 'NESTLEIND',
    'SUNPHARMA', 'TATAMOTORS', 'ONGC', 'NTPC', 'POWERGRID',
    'ADANIPORTS', 'COALINDIA', 'TATASTEEL', 'JSWSTEEL', 'M&M',
    'BAJAJFINSV', 'TECHM', 'INDUSINDBK', 'HINDALCO', 'DRREDDY',
    'CIPLA', 'EICHERMOT', 'BRITANNIA', 'APOLLOHOSP', 'GRASIM',
    'HEROMOTOCO', 'DIVISLAB', 'ADANIENT', 'UPL', 'TATACONSUM',
    'BAJAJ-AUTO', 'SHRIRAMFIN', 'LTIM', 'BPCL', 'SBILIFE',
]


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD', '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=')
    )


def load_symbol_data(symbol, timeframe='day'):
    """Load historical data for a symbol from database"""
    conn = get_db_connection()
    
    try:
        query = """
            SELECT time as date, open, high, low, close, volume, oi
            FROM historical_data
            WHERE trading_symbol = %s AND timeframe = %s
            ORDER BY time ASC
        """
        
        df = pd.read_sql(query, conn, params=(symbol, timeframe))
        
        if df.empty:
            return None
        
        df['date'] = pd.to_datetime(df['date'])
        return df
        
    finally:
        conn.close()


def analyze_symbol(symbol):
    """Run Bollinger %B analysis on a single symbol"""
    try:
        # Load data
        df = load_symbol_data(symbol)
        
        if df is None or len(df) < 50:
            return None
        
        # Calculate Bollinger %B
        df['percent_b'], _, _, _ = bollinger_percent_b(df['volume'], 20, 2, ma_type='sma')
        
        # Identify signals
        alerts = identify_signals(df, df['percent_b'])
        
        if len(alerts) == 0:
            return {
                'symbol': symbol,
                'total_trades': 0,
                'win_rate': 0,
                'total_pnl': 0,
                'avg_pnl': 0,
                'candles': len(df)
            }
        
        # Analyze performance
        results = analyze_signal_performance(df, alerts, confirmation_window=10)
        
        if len(results) == 0:
            return {
                'symbol': symbol,
                'total_trades': 0,
                'win_rate': 0,
                'total_pnl': 0,
                'avg_pnl': 0,
                'candles': len(df)
            }
        
        # Calculate stats
        winning_trades = sum(1 for r in results if r['pnl'] > 0)
        total_trades = len(results)
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
        total_pnl = sum(r['pnl'] for r in results)
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
        
        return {
            'symbol': symbol,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'candles': len(df),
            'alerts': len(alerts)
        }
        
    except Exception as e:
        print(f"✗ Error analyzing {symbol}: {e}")
        return None


def main():
    print("=" * 100)
    print(" " * 30 + "NIFTY 50 BOLLINGER %B ANALYSIS")
    print(" " * 35 + "Daily Timeframe")
    print("=" * 100)
    print()
    
    results = []
    
    for i, symbol in enumerate(NIFTY50_SYMBOLS, 1):
        print(f"[{i:2d}/50] Analyzing {symbol:15s}...", end=" ")
        
        result = analyze_symbol(symbol)
        
        if result is None:
            print("✗ No data or error")
            continue
        
        if result['total_trades'] == 0:
            print(f"⚪ No trades")
        else:
            status = "✅" if result['total_pnl'] > 0 else "❌"
            print(f"{status} {result['total_trades']:3d} trades | {result['win_rate']:5.1f}% win | {result['total_pnl']:+7.2f}% P&L")
        
        results.append(result)
    
    # Summary table
    print()
    print("=" * 100)
    print(" " * 40 + "SUMMARY RESULTS")
    print("=" * 100)
    print()
    
    # Sort by total P&L
    results_sorted = sorted([r for r in results if r['total_trades'] > 0], key=lambda x: x['total_pnl'], reverse=True)
    
    print(f"{'Rank':<6} {'Symbol':<15} {'Trades':<8} {'Win %':<8} {'Total P&L':<12} {'Avg P&L':<10} {'Alerts':<8}")
    print("-" * 100)
    
    for i, r in enumerate(results_sorted, 1):
        status = "✅" if r['total_pnl'] > 0 else "❌"
        print(f"{i:<6} {r['symbol']:<15} {r['total_trades']:<8} {r['win_rate']:<7.1f}% {r['total_pnl']:+11.2f}% {r['avg_pnl']:+9.2f}% {r['alerts']:<8} {status}")
    
    # Overall statistics
    profitable_symbols = sum(1 for r in results_sorted if r['total_pnl'] > 0)
    total_symbols = len(results_sorted)
    total_trades = sum(r['total_trades'] for r in results_sorted)
    total_wins = sum(r['winning_trades'] for r in results_sorted)
    overall_winrate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    overall_pnl = sum(r['total_pnl'] for r in results_sorted)
    
    print()
    print("=" * 100)
    print(f"Profitable Symbols: {profitable_symbols}/{total_symbols} ({profitable_symbols/total_symbols*100:.1f}%)")
    print(f"Total Trades: {total_trades}")
    print(f"Overall Win Rate: {overall_winrate:.1f}%")
    print(f"Overall P&L: {overall_pnl:+.2f}%")
    print(f"Average P&L per Symbol: {overall_pnl/total_symbols:+.2f}%")
    print("=" * 100)
    
    # Top 10 performers
    print()
    print("🏆 TOP 10 PERFORMERS:")
    for i, r in enumerate(results_sorted[:10], 1):
        print(f"  {i:2d}. {r['symbol']:<12} {r['total_pnl']:+7.2f}% ({r['total_trades']} trades, {r['win_rate']:.1f}% win)")
    
    # Bottom 10 performers
    if len(results_sorted) >= 10:
        print()
        print("📉 BOTTOM 10 PERFORMERS:")
        for i, r in enumerate(results_sorted[-10:], 1):
            print(f"  {i:2d}. {r['symbol']:<12} {r['total_pnl']:+7.2f}% ({r['total_trades']} trades, {r['win_rate']:.1f}% win)")
    
    print()
    print("=" * 100)


if __name__ == '__main__':
    main()
