"""
Analyze Bollinger %B strategy on all Nifty 500 stocks
Reads data from TimescaleDB and generates performance report
"""

import sys
import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime

# Add parent directory to path to import from bollinger_b_analysis
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import only the functions we need, not the main execution
from scripts.bollinger_b_analysis import (
    calculate_transaction_costs,
    triangular_ma,
    bollinger_percent_b,
    identify_signals,
    analyze_signal_performance
)


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host='localhost',
        port='5432',
        database='tradingdb',
        user='tradinguser',
        password='5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
    )


def get_all_symbols(min_candles=1000):
    """Get all symbols from database with sufficient data"""
    try:
        conn = get_db_connection()
        
        query = """
            SELECT trading_symbol, COUNT(*) as candle_count
            FROM historical_data
            WHERE timeframe = 'day'
            GROUP BY trading_symbol
            HAVING COUNT(*) >= %s
            ORDER BY trading_symbol
        """
        
        df = pd.read_sql(query, conn, params=(min_candles,))
        conn.close()
        
        return df['trading_symbol'].tolist()
        
    except Exception as e:
        print(f"Error getting symbols: {e}")
        return []


def load_symbol_data(symbol, timeframe='day'):
    """Load historical data for a symbol"""
    try:
        conn = get_db_connection()
        
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
                'total_pnl_gross': 0,
                'total_pnl_net': 0,
                'avg_pnl_gross': 0,
                'avg_pnl_net': 0,
                'candles': len(df)
            }
        
        # Analyze performance
        results = analyze_signal_performance(df, alerts, confirmation_window=10)
        
        if len(results) == 0:
            return {
                'symbol': symbol,
                'total_trades': 0,
                'win_rate': 0,
                'total_pnl_gross': 0,
                'total_pnl_net': 0,
                'avg_pnl_gross': 0,
                'avg_pnl_net': 0,
                'candles': len(df)
            }
        
        # Calculate stats using NET P&L
        winning_trades = sum(1 for r in results if r['pnl_net'] > 0)
        total_trades = len(results)
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
        total_pnl_gross = sum(r['pnl_gross'] for r in results)
        total_pnl_net = sum(r['pnl_net'] for r in results)
        avg_pnl_gross = total_pnl_gross / total_trades if total_trades > 0 else 0
        avg_pnl_net = total_pnl_net / total_trades if total_trades > 0 else 0
        
        return {
            'symbol': symbol,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'win_rate': win_rate,
            'total_pnl_gross': total_pnl_gross,
            'total_pnl_net': total_pnl_net,
            'avg_pnl_gross': avg_pnl_gross,
            'avg_pnl_net': avg_pnl_net,
            'candles': len(df),
            'alerts': len(alerts)
        }
        
    except Exception as e:
        print(f"✗ Error analyzing {symbol}: {e}")
        return None


def main():
    print("=" * 100)
    print(" " * 30 + "NIFTY 500 BOLLINGER %B ANALYSIS")
    print(" " * 35 + "Daily Timeframe")
    print("=" * 100)
    print()
    
    # Get all symbols with sufficient data
    symbols = get_all_symbols(min_candles=1000)
    
    if not symbols:
        print("No symbols found in database!")
        sys.exit(1)
    
    print(f"Found {len(symbols)} symbols with >1000 candles")
    print()
    
    # Analyze each symbol
    results = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i:3d}/{len(symbols)}] Analyzing {symbol:15s}...", end=" ")
        
        result = analyze_symbol(symbol)
        
        if result is None:
            print("✗ No data or error")
            continue
        
        if result['total_trades'] == 0:
            print(f"⚪ No trades")
        else:
            status = "✅" if result['total_pnl_net'] > 0 else "❌"
            print(f"{status} {result['total_trades']:3d} trades | {result['win_rate']:5.1f}% win | Gross: {result['total_pnl_gross']:+7.2f}% | Net: {result['total_pnl_net']:+7.2f}%")
        
        results.append(result)
    
    # Summary table
    print()
    print("=" * 100)
    print(" " * 30 + "SUMMARY RESULTS (WITH TRANSACTION COSTS & SLIPPAGE)")
    print("=" * 100)
    print()
    
    # Sort by net P&L
    results_sorted = sorted([r for r in results if r['total_trades'] > 0], key=lambda x: x['total_pnl_net'], reverse=True)
    
    print(f"{'Rank':<6} {'Symbol':<15} {'Trades':<8} {'Win %':<8} {'Gross P&L':<12} {'Net P&L':<12} {'Avg Net':<10} {'Status':<8}")
    print("-" * 100)
    
    for i, r in enumerate(results_sorted[:50], 1):  # Show top 50
        status = "✅" if r['total_pnl_net'] > 0 else "❌"
        print(f"{i:<6} {r['symbol']:<15} {r['total_trades']:<8} {r['win_rate']:<7.1f}% {r['total_pnl_gross']:+11.2f}% {r['total_pnl_net']:+11.2f}% {r['avg_pnl_net']:+9.2f}% {status}")
    
    # Overall statistics
    profitable_symbols = sum(1 for r in results_sorted if r['total_pnl_net'] > 0)
    total_symbols = len(results_sorted)
    total_trades = sum(r['total_trades'] for r in results_sorted)
    total_wins = sum(r['winning_trades'] for r in results_sorted)
    overall_winrate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    overall_pnl_gross = sum(r['total_pnl_gross'] for r in results_sorted)
    overall_pnl_net = sum(r['total_pnl_net'] for r in results_sorted)
    total_cost = overall_pnl_gross - overall_pnl_net
    
    print()
    print("=" * 100)
    print(f"Profitable Symbols (Net): {profitable_symbols}/{total_symbols} ({profitable_symbols/total_symbols*100:.1f}%)")
    print(f"Total Trades: {total_trades}")
    print(f"Overall Win Rate: {overall_winrate:.1f}%")
    print(f"Overall P&L (Gross): {overall_pnl_gross:+.2f}%")
    print(f"Overall P&L (Net): {overall_pnl_net:+.2f}%")
    print(f"Total Transaction Costs + Slippage: {total_cost:.2f}%")
    print(f"Average P&L per Symbol (Net): {overall_pnl_net/total_symbols:+.2f}%")
    print("=" * 100)
    
    # Top 10 performers
    print()
    print("🏆 TOP 20 PERFORMERS (Net P&L):")
    for i, r in enumerate(results_sorted[:20], 1):
        print(f"  {i:2d}. {r['symbol']:<12} Net: {r['total_pnl_net']:+7.2f}% (Gross: {r['total_pnl_gross']:+7.2f}%) | {r['total_trades']} trades, {r['win_rate']:.1f}% win")
    
    # Bottom 10 performers
    if len(results_sorted) >= 20:
        print()
        print("📉 BOTTOM 20 PERFORMERS (Net P&L):")
        for i, r in enumerate(results_sorted[-20:], 1):
            print(f"  {i:2d}. {r['symbol']:<12} Net: {r['total_pnl_net']:+7.2f}% (Gross: {r['total_pnl_gross']:+7.2f}%) | {r['total_trades']} trades, {r['win_rate']:.1f}% win")
    
    print()
    print("=" * 100)


if __name__ == '__main__':
    main()
