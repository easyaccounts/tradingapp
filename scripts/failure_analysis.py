"""
Failure Analysis for Bollinger %B Strategy
Analyzes why bottom performers fail and identifies improvement opportunities
"""

import sys
import pandas as pd
import numpy as np
import psycopg2
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scripts.bollinger_b_analysis import (
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


def load_symbol_data(symbol, timeframe='day'):
    """Load historical data for a symbol"""
    try:
        conn = get_db_connection()
        
        query = """
            SELECT time as date, open, high, low, close, volume
            FROM historical_data
            WHERE trading_symbol = %s AND timeframe = %s
            ORDER BY time
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol, timeframe))
        conn.close()
        
        if len(df) < 100:
            return None
            
        return df
        
    except Exception as e:
        print(f"Error loading {symbol}: {e}")
        return None


def analyze_symbol_detailed(symbol):
    """Run strategy and return detailed results"""
    df = load_symbol_data(symbol)
    
    if df is None or len(df) < 1000:
        return None
    
    # Calculate Bollinger %B on volume
    percent_b, middle, upper, lower = bollinger_percent_b(df['volume'], period=20, std_dev=2, ma_type='sma')
    
    # Identify signals
    alerts = identify_signals(df, percent_b)
    
    if len(alerts) == 0:
        return None
    
    # Analyze performance with detailed metrics
    results = analyze_signal_performance(df, alerts, confirmation_window=10)
    
    if len(results) == 0:
        return None
    
    # Return symbol name with results
    for r in results:
        r['symbol'] = symbol
    
    return results


def main():
    print("=" * 100)
    print(" " * 30 + "FAILURE ANALYSIS")
    print(" " * 25 + "Identifying Why Bottom Performers Fail")
    print("=" * 100)
    
    # Define top and bottom performers (from your Nifty 50 results)
    top_performers = ['BAJAJFINSV', 'SBIN', 'ADANIENT', 'BPCL', 'TATACONSUM', 
                      'ASIANPAINT', 'GRASIM', 'TECHM', 'HINDALCO', 'ICICIBANK']
    
    bottom_performers = ['TITAN', 'DIVISLAB', 'POWERGRID', 'BAJAJ-AUTO', 'CIPLA',
                         'ONGC', 'AXISBANK', 'HEROMOTOCO', 'LT', 'UPL']
    
    print(f"\n📊 Analyzing {len(top_performers)} top performers and {len(bottom_performers)} bottom performers...")
    
    # Collect all trades
    all_top_trades = []
    all_bottom_trades = []
    
    print("\n🔍 Processing top performers...")
    for symbol in top_performers:
        print(f"   {symbol}...", end='', flush=True)
        results = analyze_symbol_detailed(symbol)
        if results:
            all_top_trades.extend(results)
            print(f" ✓ {len(results)} trades")
        else:
            print(" ✗ No data")
    
    print("\n🔍 Processing bottom performers...")
    for symbol in bottom_performers:
        print(f"   {symbol}...", end='', flush=True)
        results = analyze_symbol_detailed(symbol)
        if results:
            all_bottom_trades.extend(results)
            print(f" ✓ {len(results)} trades")
        else:
            print(" ✗ No data")
    
    # Convert to DataFrames
    df_top = pd.DataFrame(all_top_trades)
    df_bottom = pd.DataFrame(all_bottom_trades)
    
    # Export to CSV
    df_top.to_csv('/opt/tradingapp/top_performers_trades.csv', index=False)
    df_bottom.to_csv('/opt/tradingapp/bottom_performers_trades.csv', index=False)
    
    print(f"\n✅ Exported trade logs:")
    print(f"   Top: {len(df_top)} trades → top_performers_trades.csv")
    print(f"   Bottom: {len(df_bottom)} trades → bottom_performers_trades.csv")
    
    # Statistical Analysis
    print("\n" + "=" * 100)
    print(" " * 35 + "COMPARATIVE ANALYSIS")
    print("=" * 100)
    
    # Overall metrics
    print(f"\n📊 OVERALL METRICS:")
    print(f"{'Metric':<30} {'Top Performers':<20} {'Bottom Performers':<20} {'Difference':<15}")
    print("-" * 100)
    
    metrics = [
        ('Total Trades', 'count', ''),
        ('Win Rate %', 'win', lambda x: x.mean() * 100),
        ('Avg Net P&L %', 'pnl_net', 'mean'),
        ('Days to Confirm', 'days_to_confirm', 'mean'),
        ('Alert Vol Ratio', 'alert_volume_ratio', 'mean'),
        ('Confirm Vol Ratio', 'confirm_volume_ratio', 'mean'),
        ('Breakout Strength %', 'breakout_strength', 'mean'),
        ('Pullback Depth %', 'pullback_depth', 'mean'),
        ('Bars Held (Win)', None, None),
        ('Bars Held (Loss)', None, None),
        ('Max Favorable %', 'max_favorable', 'mean'),
        ('Max Adverse %', 'max_adverse', 'mean'),
    ]
    
    for metric_name, col, agg in metrics:
        if col == 'count':
            top_val = len(df_top)
            bottom_val = len(df_bottom)
            diff = top_val - bottom_val
            print(f"{metric_name:<30} {top_val:<20} {bottom_val:<20} {diff:>+15}")
        elif col == 'win':
            top_val = df_top['win'].mean() * 100
            bottom_val = df_bottom['win'].mean() * 100
            diff = top_val - bottom_val
            print(f"{metric_name:<30} {top_val:<20.1f} {bottom_val:<20.1f} {diff:>+14.1f}pp")
        elif metric_name.startswith('Bars Held'):
            if 'Win' in metric_name:
                top_val = df_top[df_top['win']]['bars_held'].mean()
                bottom_val = df_bottom[df_bottom['win']]['bars_held'].mean()
            else:
                top_val = df_top[~df_top['win']]['bars_held'].mean()
                bottom_val = df_bottom[~df_bottom['win']]['bars_held'].mean()
            diff = top_val - bottom_val
            print(f"{metric_name:<30} {top_val:<20.1f} {bottom_val:<20.1f} {diff:>+14.1f}")
        else:
            top_val = df_top[col].mean()
            bottom_val = df_bottom[col].mean()
            diff = top_val - bottom_val
            print(f"{metric_name:<30} {top_val:<20.2f} {bottom_val:<20.2f} {diff:>+14.2f}")
    
    # Failure Pattern Analysis
    print("\n" + "=" * 100)
    print(" " * 30 + "FAILURE PATTERN IDENTIFICATION")
    print("=" * 100)
    
    # Pattern A: Slow Confirmation
    print(f"\n🔍 PATTERN A: Slow Confirmation (Days 7-10)")
    top_slow = df_top[df_top['days_to_confirm'] >= 7]
    bottom_slow = df_bottom[df_bottom['days_to_confirm'] >= 7]
    print(f"   Top: {len(top_slow)}/{len(df_top)} ({len(top_slow)/len(df_top)*100:.1f}%) - Win rate: {top_slow['win'].mean()*100:.1f}%")
    print(f"   Bottom: {len(bottom_slow)}/{len(df_bottom)} ({len(bottom_slow)/len(df_bottom)*100:.1f}%) - Win rate: {bottom_slow['win'].mean()*100:.1f}%")
    print(f"   → Potential improvement: Filter confirmations after Day 6")
    
    # Pattern B: Weak Breakout
    print(f"\n🔍 PATTERN B: Weak Breakout (<1% above alert)")
    top_weak = df_top[df_top['breakout_strength'] < 1.0]
    bottom_weak = df_bottom[df_bottom['breakout_strength'] < 1.0]
    print(f"   Top: {len(top_weak)}/{len(df_top)} ({len(top_weak)/len(df_top)*100:.1f}%) - Win rate: {top_weak['win'].mean()*100:.1f}%")
    print(f"   Bottom: {len(bottom_weak)}/{len(df_bottom)} ({len(bottom_weak)/len(df_bottom)*100:.1f}%) - Win rate: {bottom_weak['win'].mean()*100:.1f}%")
    print(f"   → Potential improvement: Require 1%+ breakout strength")
    
    # Pattern C: Deep Pullback
    print(f"\n🔍 PATTERN C: Deep Pullback (>8% retracement)")
    top_deep = df_top[df_top['pullback_depth'] > 8.0]
    bottom_deep = df_bottom[df_bottom['pullback_depth'] > 8.0]
    print(f"   Top: {len(top_deep)}/{len(df_top)} ({len(top_deep)/len(df_top)*100:.1f}%) - Win rate: {top_deep['win'].mean()*100:.1f}%")
    print(f"   Bottom: {len(bottom_deep)}/{len(df_bottom)} ({len(bottom_deep)/len(df_bottom)*100:.1f}%) - Win rate: {bottom_deep['win'].mean()*100:.1f}%")
    print(f"   → Potential improvement: Skip if pullback > 8%")
    
    # Pattern D: Immediate Stop-Out
    print(f"\n🔍 PATTERN D: Immediate Stop-Out (≤3 bars, never ahead)")
    top_immediate = df_top[(df_top['bars_held'] <= 3) & (df_top['max_favorable'] < 0.5) & (~df_top['win'])]
    bottom_immediate = df_bottom[(df_bottom['bars_held'] <= 3) & (df_bottom['max_favorable'] < 0.5) & (~df_bottom['win'])]
    print(f"   Top: {len(top_immediate)}/{len(df_top[~df_top['win']])} losers ({len(top_immediate)/len(df_top[~df_top['win']])*100:.1f}%)")
    print(f"   Bottom: {len(bottom_immediate)}/{len(df_bottom[~df_bottom['win']])} losers ({len(bottom_immediate)/len(df_bottom[~df_bottom['win']])*100:.1f}%)")
    print(f"   → Potential improvement: Wider initial stop or confirm with second candle")
    
    # Exit Reason Analysis
    print(f"\n🔍 EXIT REASON BREAKDOWN:")
    print(f"\n   Top Performers:")
    print(df_top['exit_reason'].value_counts())
    print(f"\n   Bottom Performers:")
    print(df_bottom['exit_reason'].value_counts())
    
    # Combined Filter Impact Estimate
    print("\n" + "=" * 100)
    print(" " * 30 + "FILTER IMPACT ESTIMATION")
    print("=" * 100)
    
    print(f"\nIf we apply ALL identified filters to bottom performers:")
    
    # Simulate filters
    df_bottom_filtered = df_bottom[
        (df_bottom['days_to_confirm'] <= 6) &
        (df_bottom['breakout_strength'] >= 1.0) &
        (df_bottom['pullback_depth'] <= 8.0)
    ]
    
    print(f"   Trades remaining: {len(df_bottom_filtered)}/{len(df_bottom)} ({len(df_bottom_filtered)/len(df_bottom)*100:.1f}%)")
    print(f"   Win rate: {df_bottom['win'].mean()*100:.1f}% → {df_bottom_filtered['win'].mean()*100:.1f}% ({df_bottom_filtered['win'].mean()*100 - df_bottom['win'].mean()*100:+.1f}pp)")
    print(f"   Avg P&L: {df_bottom['pnl_net'].mean():.2f}% → {df_bottom_filtered['pnl_net'].mean():.2f}% ({df_bottom_filtered['pnl_net'].mean() - df_bottom['pnl_net'].mean():+.2f}%)")
    
    print(f"\n✅ Analysis complete! Review the CSVs for detailed trade-by-trade data.")
    print("=" * 100)


if __name__ == '__main__':
    main()
