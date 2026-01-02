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
    SELECT DISTINCT trading_symbol
    FROM instruments
    WHERE trading_symbol LIKE 'NIFTY%'
    AND instrument_type IN ('CE', 'PE')
    AND exchange = 'NFO'
    ORDER BY trading_symbol
    """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        expiries = set()
        
        for row in results:
            symbol = row['trading_symbol']
            # Extract expiry from symbol (e.g., NIFTY26JAN23750CE -> 26JAN)
            if symbol:
                # Remove 'NIFTY' prefix and 'CE'/'PE' suffix, then extract YYMMMSTRIKE
                import re
                match = re.match(r'NIFTY(\d{2}\w{3})\d+[CP]E', symbol)
                if match:
                    expiries.add(match.group(1))
        
        return sorted(expiries)
    finally:
        cursor.close()
        conn.close()


def parse_expiry_date(expiry_str):
    """Parse expiry string into sortable date. Format: YYMMMSTRIKE (e.g., 26JAN23750)"""
    import re
    from datetime import datetime
    
    # Extract date pattern: YYMMMSTRIKE
    match = re.match(r'(\d{2})(\w{3})', expiry_str)
    if not match:
        return None
    
    year_str, month_str = match.groups()
    
    try:
        # Map month abbreviations
        months = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                  'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
        month = months[month_str]
        year = 2000 + int(year_str)  # 26 -> 2026
        
        # Expiry is typically last Thursday of the month, use day 25 as approximation
        date = datetime(year, month, 25)
        return date
    except (ValueError, KeyError):
        return None


def get_immediate_expiry():
    """Get the immediate (nearest) NIFTY expiry"""
    expiries = get_nifty_options_expiries()
    
    if not expiries:
        print("❌ No NIFTY options found in database")
        return None
    
    print(f"Found {len(expiries)} expiries. Parsing dates...")
    
    # Parse and sort expiries by date
    parsed = []
    today = datetime.now(IST).date()
    
    for exp in expiries:
        parsed_date = parse_expiry_date(exp)
        if parsed_date and parsed_date.date() >= today:  # Only future expiries
            parsed.append((parsed_date, exp))
    
    if not parsed:
        print("❌ No future expiry dates found")
        return None
    
    # Sort by date and return the immediate (nearest future) one
    parsed.sort(key=lambda x: x[0])
    immediate_expiry = parsed[0][1]
    print(f"Immediate expiry selected: {immediate_expiry} ({parsed[0][0].strftime('%Y-%m-%d')})")
    
    return immediate_expiry


def analyze_options_positioning(expiry, cutoff_time=None):
    """Analyze cumulative options positioning for a specific expiry"""
    
    if cutoff_time is None:
        cutoff_time = datetime.now(IST)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"\n{'='*100}")
    print(f"📊 OPTIONS POSITIONING ANALYSIS - NIFTY {expiry}")
    print(f"Analysis Time: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"{'='*100}\n")
    
    # Get all strikes for this expiry from instruments table
    nifty_pattern = f'NIFTY{expiry}%'
    
    strikes_query = """
    SELECT DISTINCT 
        trading_symbol,
        instrument_token,
        strike
    FROM instruments
    WHERE trading_symbol LIKE %s
    AND instrument_type IN ('CE', 'PE')
    AND exchange = 'NFO'
    ORDER BY strike
    """
    
    try:
        cursor.execute(strikes_query, (nifty_pattern,))
        strike_data = cursor.fetchall()
        
        if not strike_data:
            print(f"❌ No options data found for expiry {expiry}")
            return
        
        # Group into calls and puts
        call_data = {}
        put_data = {}
        
        for row in strike_data:
            strike = row['strike']
            symbol = row['trading_symbol']
            
            if symbol.endswith('CE'):
                call_data[strike] = symbol
            elif symbol.endswith('PE'):
                put_data[strike] = symbol
        
        print(f"Found {len(call_data)} call strikes and {len(put_data)} put strikes\n")
        
        # Analyze each option type
        print(f"{'='*100}")
        print("🔵 CALLS ANALYSIS (CE)")
        print(f"{'='*100}\n")
        
        analyze_option_type(conn, 'CE', call_data, cutoff_time)
        
        print(f"\n{'='*100}")
        print("🔴 PUTS ANALYSIS (PE)")
        print(f"{'='*100}\n")
        
        analyze_option_type(conn, 'PE', put_data, cutoff_time)
        
        print(f"\n{'='*100}")
        print("📈 COMPARATIVE ANALYSIS")
        print(f"{'='*100}\n")
        
        compare_call_put_positioning(conn, call_data, put_data, cutoff_time)
        
    finally:
        cursor.close()
        conn.close()


def analyze_option_type(conn, option_type, strikes_dict, cutoff_time):
    """Analyze OI and premium behavior for a specific option type"""
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get market open time
    market_open = cutoff_time.replace(hour=9, minute=15, second=0, microsecond=0)
    
    total_oi_open = 0
    total_oi_current = 0
    total_premium_open = 0
    total_premium_current = 0
    premium_status = []
    oi_status = []
    
    print(f"{'Strike':<10} {'OI Open':<12} {'OI Current':<12} {'OI Change':<14} {'Premium Open':<14} {'Premium Now':<14} {'Premium Δ':<12}")
    print("-"*100)
    
    for strike in sorted(strikes_dict.keys()):
        symbol = strikes_dict[strike]
        
        # Get opening tick (first tick after market open)
        open_query = f"""
        SELECT last_price, oi
        FROM ticks
        WHERE trading_symbol = %s
        AND time >= %s
        AND time < %s
        ORDER BY time ASC
        LIMIT 1
        """
        
        cursor.execute(open_query, (symbol, market_open, market_open + timedelta(minutes=5)))
        open_data = cursor.fetchone()
        
        if not open_data:
            continue
        
        open_oi = open_data['oi'] or 0
        open_premium = float(open_data['last_price']) if open_data['last_price'] else 0
        
        # Get current tick (latest tick up to cutoff time)
        current_query = f"""
        SELECT last_price, oi
        FROM ticks
        WHERE trading_symbol = %s
        AND time <= %s
        ORDER BY time DESC
        LIMIT 1
        """
        
        cursor.execute(current_query, (symbol, cutoff_time))
        current_data = cursor.fetchone()
        
        if not current_data:
            continue
        
        current_oi = current_data['oi'] or 0
        current_premium = float(current_data['last_price']) if current_data['last_price'] else 0
        
        # Calculate changes
        oi_change = current_oi - open_oi
        oi_change_pct = (oi_change / open_oi * 100) if open_oi > 0 else 0
        premium_change = current_premium - open_premium
        
        # Accumulate totals
        total_oi_open += open_oi
        total_oi_current += current_oi
        total_premium_open += open_premium
        total_premium_current += current_premium
        
        # Store status
        oi_icon = "📈" if oi_change > 0 else "📉" if oi_change < 0 else "→"
        premium_icon = "📈" if premium_change > 0 else "📉" if premium_change < 0 else "→"
        
        print(f"{strike:<10} {open_oi:<11,} {current_oi:<11,} {oi_change:>+10,} ({oi_change_pct:>5.1f}%) {open_premium:>13.2f}₹ {current_premium:>13.2f}₹ {premium_change:>+10.2f}₹")
        
        oi_status.append((strike, oi_change > 0))
        premium_status.append((strike, premium_change > 0))
    
    # Summary
    total_oi_change = total_oi_current - total_oi_open
    total_oi_change_pct = (total_oi_change / total_oi_open * 100) if total_oi_open > 0 else 0
    total_premium_change = total_premium_current - total_premium_open
    
    print("-"*100)
    print(f"{'TOTAL':<10} {total_oi_open:<11,} {total_oi_current:<11,} {total_oi_change:>+10,} ({total_oi_change_pct:>5.1f}%) {total_premium_open:>13.2f}₹ {total_premium_current:>13.2f}₹ {total_premium_change:>+10.2f}₹")
    
    # Analysis summary
    print(f"\n📊 {option_type} Summary:")
    increasing_oi = sum(1 for _, inc in oi_status if inc)
    decreasing_oi = len(oi_status) - increasing_oi
    
    increasing_premium = sum(1 for _, inc in premium_status if inc)
    decreasing_premium = len(premium_status) - increasing_oi
    
    print(f"   OI Increasing: {increasing_oi}/{len(oi_status)} strikes")
    print(f"   OI Decreasing: {decreasing_oi}/{len(oi_status)} strikes")
    print(f"   Overall OI Change: {total_oi_change:+,} ({total_oi_change_pct:+.1f}%)")
    
    print(f"   Premium Appreciating: {increasing_premium}/{len(premium_status)} strikes")
    print(f"   Premium Decaying: {decreasing_premium}/{len(premium_status)} strikes")
    print(f"   Overall Premium Change: ₹{total_premium_change:+.2f}")


def compare_call_put_positioning(conn, call_strikes, put_strikes, cutoff_time):
    """Compare calls vs puts positioning"""
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    market_open = cutoff_time.replace(hour=9, minute=15, second=0, microsecond=0)
    
    # Get aggregate OI for calls and puts
    call_symbols = ','.join([f"'{s}'" for s in call_strikes.values()])
    put_symbols = ','.join([f"'{s}'" for s in put_strikes.values()])
    
    # Call OI
    call_open_query = f"""
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE trading_symbol IN ({call_symbols})
    AND time >= %s AND time < %s
    ORDER BY time ASC LIMIT 1
    """
    
    cursor.execute(call_open_query, (market_open, market_open + timedelta(minutes=5)))
    call_open = cursor.fetchone()
    
    call_current_query = f"""
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE trading_symbol IN ({call_symbols})
    AND time <= %s
    ORDER BY time DESC LIMIT 1
    """
    
    cursor.execute(call_current_query, (cutoff_time,))
    call_current = cursor.fetchone()
    
    # Put OI
    put_open_query = f"""
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE trading_symbol IN ({put_symbols})
    AND time >= %s AND time < %s
    ORDER BY time ASC LIMIT 1
    """
    
    cursor.execute(put_open_query, (market_open, market_open + timedelta(minutes=5)))
    put_open = cursor.fetchone()
    
    put_current_query = f"""
    SELECT SUM(oi) as total_oi, AVG(last_price) as avg_premium
    FROM ticks
    WHERE trading_symbol IN ({put_symbols})
    AND time <= %s
    ORDER BY time DESC LIMIT 1
    """
    
    cursor.execute(put_current_query, (cutoff_time,))
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
    print("Fetching NIFTY option data...\n")
    
    # Get immediate expiry
    expiry = get_immediate_expiry()
    
    if not expiry:
        return
    
    # Analyze positioning
    analyze_options_positioning(expiry)


if __name__ == '__main__':
    main()
