#!/usr/bin/env python3
"""
Analyze cumulative options positioning for NIFTY 50 immediate expiry
Shows OI activity and premium behavior for calls vs puts from market open to current time
"""

import os
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
import pytz
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '6432')),
    'database': os.getenv('DB_NAME', 'tradingdb'),
    'user': os.getenv('DB_USER', 'tradinguser'),
    'password': os.getenv('DB_PASSWORD')
}

IST = pytz.timezone('Asia/Kolkata')


def get_nifty_options_expiries():
    """Get all available NIFTY option expiries from instruments table"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT DISTINCT expiry
    FROM instruments
    WHERE trading_symbol LIKE 'NIFTY%'
    AND instrument_type IN ('CE', 'PE')
    AND exchange = 'NFO'
    AND expiry IS NOT NULL
    AND expiry >= CURRENT_DATE
    ORDER BY expiry
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        expiries = [row['expiry'] for row in results]
        return expiries
    finally:
        cursor.close()
        conn.close()


def get_immediate_expiry():
    """Get the immediate (nearest) NIFTY expiry"""
    expiries = get_nifty_options_expiries()
    
    if not expiries:
        print("❌ No NIFTY options found in database")
        return None
    
    print(f"Found {len(expiries)} future expiries")
    
    # Return the first one (already sorted by date, future only)
    immediate_expiry = expiries[0]
    print(f"Immediate expiry selected: {immediate_expiry}")
    
    return immediate_expiry


def analyze_options_positioning(expiry, cutoff_time=None):
    """Analyze cumulative options positioning for a specific expiry"""
    
    if cutoff_time is None:
        cutoff_time = datetime.now(IST)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"\n{'='*100}")
    print(f"📊 OPTIONS POSITIONING ANALYSIS - NIFTY {expiry.strftime('%Y-%m-%d')}")
    print(f"Analysis Time: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"{'='*100}\n")
    
    # Get all NIFTY options tokens for this expiry
    tokens_query = """
    SELECT DISTINCT 
        instrument_token,
        trading_symbol,
        strike,
        instrument_type,
        segment
    FROM instruments
    WHERE segment = %s
    AND name LIKE %s
    AND expiry = %s
    ORDER BY strike, instrument_type
    """
    
    try:
        cursor.execute(tokens_query, ('NFO-OPT', 'NIFTY%', expiry))
        token_data = cursor.fetchall()
        
        if not token_data:
            print(f"❌ No options data found for expiry {expiry}")
            cursor.close()
            conn.close()
            return
        
        # Separate calls and puts by instrument_token
        call_tokens = {}  # {strike: instrument_token}
        put_tokens = {}
        
        for row in token_data:
            token = row['instrument_token']
            strike = row['strike']
            symbol = row['trading_symbol']
            
            if row['instrument_type'] == 'CE':
                call_tokens[strike] = (token, symbol)
            elif row['instrument_type'] == 'PE':
                put_tokens[strike] = (token, symbol)
        
        print(f"Found {len(call_tokens)} call strikes and {len(put_tokens)} put strikes\n")
        
        # Build token lists
        call_token_list = [t[0] for t in call_tokens.values()]
        put_token_list = [t[0] for t in put_tokens.values()]
        
        print(f"Call tokens: {call_token_list[:5]}... (first 5 of {len(call_token_list)})")
        print(f"Put tokens: {put_token_list[:5]}... (first 5 of {len(put_token_list)})\n")
        
        # Check if any tick data exists for these specific tokens today
        check_query = """
        SELECT COUNT(*) as count, COUNT(DISTINCT instrument_token) as distinct_tokens
        FROM ticks
        WHERE time >= DATE_TRUNC('day', %s AT TIME ZONE 'Asia/Kolkata')
        AND time <= %s
        AND instrument_token = ANY(%s)
        """
        
        # Combine all tokens
        all_tokens = call_token_list + put_token_list
        
        cursor.execute(check_query, (cutoff_time, cutoff_time, all_tokens))
        data_check = cursor.fetchone()
        
        if not data_check or data_check['count'] == 0:
            print("⚠️  No tick data found for these option tokens TODAY")
            print("   Market may not be open yet or data collection hasn't started")
            print(f"   Current time: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n")
            cursor.close()
            conn.close()
            return
        else:
            print(f"✓ Found {data_check['count']} tick records for {data_check['distinct_tokens']} distinct tokens\n")
        
        # Only proceed with analysis if we have data
        if data_check and data_check['count'] > 0:
            # Analyze each option type
            print(f"{'='*100}")
            print("🔵 CALLS ANALYSIS (CE)")
            print(f"{'='*100}\n")
            
            analyze_option_type(conn, 'CE', call_tokens, cutoff_time)
            
            print(f"\n{'='*100}")
            print("🔴 PUTS ANALYSIS (PE)")
            print(f"{'='*100}\n")
            
            analyze_option_type(conn, 'PE', put_tokens, cutoff_time)
            
            # Skip comparative analysis for now (causes PostgreSQL memory issues with large token sets)
            # print(f"\n{'='*100}")
            # print("📈 COMPARATIVE ANALYSIS")
            # print(f"{'='*100}\n")
            # 
            # compare_call_put_positioning(conn, call_tokens, put_tokens, cutoff_time)
        
    finally:
        cursor.close()
        conn.close()


def analyze_option_type(conn, option_type, tokens_dict, cutoff_time):
    """Analyze OI and premium behavior for a specific option type in 5-minute intervals
    
    tokens_dict: {strike: (instrument_token, trading_symbol)}
    """
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get the start of the day
    market_start = cutoff_time.replace(hour=9, minute=15, second=0, microsecond=0)
    
    # Extract all tokens
    all_tokens = [t[0] for t in tokens_dict.values()]
    
    # Query aggregated data in 5-minute intervals
    interval_query = """
    SELECT 
        DATE_TRUNC('minute', time) - 
        (EXTRACT(MINUTE FROM time)::int % 5) * INTERVAL '1 minute' as interval_start,
        SUM(oi) as total_oi,
        AVG(last_price) as avg_premium,
        COUNT(DISTINCT instrument_token) as token_count
    FROM ticks
    WHERE instrument_token = ANY(%s)
    AND time >= %s
    AND time <= %s
    GROUP BY interval_start
    ORDER BY interval_start
    """
    
    cursor.execute(interval_query, (all_tokens, market_start, cutoff_time))
    intervals = cursor.fetchall()
    
    if not intervals:
        print("No data found for this option type\n")
        cursor.close()
        return
    
    print(f"{'Time':<20} {'Total OI':<15} {'OI Change':<15} {'Avg Premium':<15} {'Premium Δ':<15}")
    print("-" * 80)
    
    prev_oi = None
    prev_premium = None
    
    for row in intervals:
        interval_time = row['interval_start']
        total_oi = row['total_oi'] or 0
        avg_premium = float(row['avg_premium']) if row['avg_premium'] else 0
        
        # Calculate changes
        oi_change = (total_oi - prev_oi) if prev_oi is not None else 0
        oi_change_pct = ((oi_change / prev_oi) * 100) if prev_oi and prev_oi > 0 else 0
        premium_change = (avg_premium - prev_premium) if prev_premium is not None else 0
        
        time_str = interval_time.strftime('%H:%M')
        print(f"{time_str:<20} {total_oi:<14,} {oi_change:>+13,} ({oi_change_pct:>5.1f}%) {avg_premium:>13.2f}₹ {premium_change:>+13.2f}₹")
        
        prev_oi = total_oi
        prev_premium = avg_premium
    
    # Print summary
    if len(intervals) > 0:
        first = intervals[0]
        last = intervals[-1]
        total_oi_change = (last['total_oi'] or 0) - (first['total_oi'] or 0)
        total_oi_change_pct = ((total_oi_change / first['total_oi']) * 100) if first['total_oi'] and first['total_oi'] > 0 else 0
        avg_premium_change = (float(last['avg_premium']) if last['avg_premium'] else 0) - (float(first['avg_premium']) if first['avg_premium'] else 0)
        
        print("-" * 80)
        print(f"\n📊 {option_type} Summary:")
        print(f"   Opening OI: {first['total_oi'] or 0:,}")
        print(f"   Current OI: {last['total_oi'] or 0:,}")
        print(f"   Total OI Change: {total_oi_change:+,} ({total_oi_change_pct:+.1f}%)")
        print(f"   Opening Avg Premium: ₹{float(first['avg_premium']) if first['avg_premium'] else 0:.2f}")
        print(f"   Current Avg Premium: ₹{float(last['avg_premium']) if last['avg_premium'] else 0:.2f}")
        print(f"   Avg Premium Change: ₹{avg_premium_change:+.2f}")
    
    cursor.close()


def compare_call_put_positioning(conn, call_tokens, put_tokens, cutoff_time):
    """Compare calls vs puts positioning
    
    call_tokens, put_tokens: {strike: (instrument_token, symbol)}
    """
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    market_open = cutoff_time.replace(hour=9, minute=15, second=0, microsecond=0)
    
    # Extract token lists for efficient querying
    call_token_list = [t[0] for t in call_tokens.values()]
    put_token_list = [t[0] for t in put_tokens.values()]
    
    if not call_token_list or not put_token_list:
        return
    
    # Call OI at market open
    call_open_query = """
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE instrument_token = ANY(%s)
    AND time >= %s AND time < %s
    """
    
    cursor.execute(call_open_query, (call_token_list, market_open, market_open + timedelta(minutes=5)))
    call_open = cursor.fetchone()
    
    # Call OI current
    call_current_query = """
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE instrument_token = ANY(%s)
    AND time <= %s
    """
    
    cursor.execute(call_current_query, (call_token_list, cutoff_time))
    call_current = cursor.fetchone()
    
    # Put OI at market open
    put_open_query = """
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE instrument_token = ANY(%s)
    AND time >= %s AND time < %s
    """
    
    cursor.execute(put_open_query, (put_token_list, market_open, market_open + timedelta(minutes=5)))
    put_open = cursor.fetchone()
    
    # Put OI current
    put_current_query = """
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE instrument_token = ANY(%s)
    AND time <= %s
    """
    
    cursor.execute(put_current_query, (put_token_list, cutoff_time))
    put_current = cursor.fetchone()
    
    cursor.close()
    
    # Display comparison
    call_oi_open = call_open['total_oi'] or 0 if call_open else 0
    call_oi_current = call_current['total_oi'] or 0 if call_current else 0
    put_oi_open = put_open['total_oi'] or 0 if put_open else 0
    put_oi_current = put_current['total_oi'] or 0 if put_current else 0
    
    call_premium_open = call_open['avg_premium'] or 0 if call_open else 0
    call_premium_current = call_current['avg_premium'] or 0 if call_current else 0
    put_premium_open = put_open['avg_premium'] or 0 if put_open else 0
    put_premium_current = put_current['avg_premium'] or 0 if put_current else 0
    
    print(f"{'Metric':<30} {'Calls Open':<15} {'Calls Current':<15} {'Puts Open':<15} {'Puts Current':<15}")
    print("-"*90)
    print(f"{'Total OI':<30} {call_oi_open:<14,} {call_oi_current:<14,} {put_oi_open:<14,} {put_oi_current:<14,}")
    print(f"{'Avg Premium':<30} ₹{call_premium_open:<13.2f} ₹{call_premium_current:<13.2f} ₹{put_premium_open:<13.2f} ₹{put_premium_current:<13.2f}")
    
    # Calculate ratios
    call_put_oi_ratio_open = call_oi_open / put_oi_open if put_oi_open > 0 else 0
    call_put_oi_ratio_current = call_oi_current / put_oi_current if put_oi_current > 0 else 0
    
    print(f"\nCall/Put OI Ratio:")
    print(f"  Open: {call_put_oi_ratio_open:.2f}x")
    print(f"  Current: {call_put_oi_ratio_current:.2f}x")
    print(f"  Change: {call_put_oi_ratio_current - call_put_oi_ratio_open:+.2f}x")
    
    # Interpretation
    print(f"\n📈 Interpretation:")
    if call_put_oi_ratio_current > call_put_oi_ratio_open:
        print(f"  → More bullish: Call OI gaining vs Put OI")
    elif call_put_oi_ratio_current < call_put_oi_ratio_open:
        print(f"  → More bearish: Put OI gaining vs Call OI")
    else:
        print(f"  → Balanced: Call/Put ratio unchanged")


def main():
    import sys
    
    print("Fetching NIFTY option data...\n")
    
    # Get immediate expiry
    expiry = get_immediate_expiry()
    
    if not expiry:
        return
    
    # Allow optional date argument for testing (format: YYYY-MM-DD)
    analysis_time = None
    if len(sys.argv) > 1:
        try:
            from datetime import datetime
            analysis_time = datetime.strptime(sys.argv[1], '%Y-%m-%d').replace(hour=15, minute=30)
            analysis_time = IST.localize(analysis_time)
            print(f"Analysis date override: {analysis_time.strftime('%Y-%m-%d %H:%M:%S IST')}\n")
        except:
            print("Invalid date format. Use: python script.py YYYY-MM-DD")
            return
    
    # Analyze positioning
    analyze_options_positioning(expiry, analysis_time)


if __name__ == '__main__':
    main()
