"""
Fetch and store historical data for Nifty 50 symbols
Downloads daily historical data and stores in PostgreSQL database
"""

import sys
import os
import redis
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
import pandas as pd
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.ingestion.kite_auth import get_access_token

# Nifty 50 trading symbols
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


def get_nifty50_instruments_from_csv():
    """Load Nifty 50 instruments from instruments.csv"""
    csv_path = 'instruments.csv'
    
    if not os.path.exists(csv_path):
        print(f"✗ instruments.csv not found at {csv_path}")
        return {}
    
    df = pd.read_csv(csv_path)
    
    # Filter for NSE equity instruments in Nifty 50
    nifty50_df = df[
        (df['exchange'] == 'NSE') &
        (df['instrument_type'] == 'EQ') &
        (df['tradingsymbol'].isin(NIFTY50_SYMBOLS))
    ]
    
    instruments = {}
    for _, row in nifty50_df.iterrows():
        symbol = row['tradingsymbol']
        instruments[symbol] = int(row['instrument_token'])
    
    return instruments


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD', '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=')
    )


def create_historical_table(conn):
    """Create historical_data table if not exists"""
    with open('database/historical_data.sql', 'r') as f:
        sql = f.read()
    
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("✓ Historical data table ready")


def fetch_and_store_data(kite, instrument_token, symbol, timeframe, from_date, to_date, conn):
    """Fetch historical data from Kite and store in database"""
    try:
        print(f"Fetching {symbol} ({instrument_token}) - {timeframe}...")
        
        # Fetch data from Kite
        data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=timeframe,
            continuous=False,
            oi=True
        )
        
        if not data:
            print(f"  ⚠ No data returned for {symbol}")
            return 0
        
        # Prepare data for insertion
        records = []
        for candle in data:
            records.append((
                candle['date'],
                instrument_token,
                symbol,
                timeframe,
                float(candle['open']),
                float(candle['high']),
                float(candle['low']),
                float(candle['close']),
                int(candle['volume']),
                int(candle['oi']) if candle.get('oi') else None
            ))
        
        # Insert into database (upsert - update if exists)
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO historical_data 
                (time, instrument_token, trading_symbol, timeframe, open, high, low, close, volume, oi)
                VALUES %s
                ON CONFLICT (time, instrument_token, timeframe) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    oi = EXCLUDED.oi,
                    created_at = NOW()
                """,
                records
            )
        conn.commit()
        
        print(f"  ✓ Stored {len(records)} candles for {symbol}")
        return len(records)
        
    except Exception as e:
        print(f"  ✗ Error fetching {symbol}: {e}")
        return 0


def main():
    # Configuration
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    timeframe = 'day'  # day, 60minute, 30minute, 15minute, etc.
    days_back = 2000  # Maximum for daily data
    
    print("=" * 80)
    print("NIFTY 50 HISTORICAL DATA DOWNLOADER")
    print("=" * 80)
    print(f"Timeframe: {timeframe}")
    print(f"Period: Last {days_back} days")
    print("=" * 80)
    
    # Load Nifty 50 instruments from CSV
    print("Loading instruments from instruments.csv...")
    nifty50_instruments = get_nifty50_instruments_from_csv()
    
    if not nifty50_instruments:
        print("✗ No Nifty 50 instruments found in instruments.csv")
        return
    
    print(f"✓ Found {len(nifty50_instruments)} Nifty 50 instruments")
    
    # Show missing symbols
    missing = set(NIFTY50_SYMBOLS) - set(nifty50_instruments.keys())
    if missing:
        print(f"⚠ Missing symbols: {', '.join(sorted(missing))}")
    
    print("=" * 80)
    
    # Get Kite access token
    try:
        access_token = get_access_token(redis_url)
        print(f"✓ Access token retrieved")
    except Exception as e:
        print(f"✗ Failed to get access token: {e}")
        return
    
    # Initialize KiteConnect
    api_key = os.getenv('KITE_API_KEY')
    if not api_key:
        # Try Redis
        r = redis.from_url(redis_url, decode_responses=True)
        api_key = r.get('kite_api_key')
    
    if not api_key:
        print("✗ KITE_API_KEY not found")
        return
    
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    print(f"✓ KiteConnect initialized")
    
    # Connect to database
    try:
        conn = get_db_connection()
        print(f"✓ Database connected")
        
        # Create table if not exists
        create_historical_table(conn)
        
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return
    
    # Calculate date range
    to_date = datetime.now()
    from_date = to_date - timedelta(days=days_back)
    
    print(f"\nDate range: {from_date.date()} to {to_date.date()}")
    print("=" * 80)
    
    # Fetch data for each symbol
    total_candles = 0
    success_count = 0
    
    for symbol, instrument_token in nifty50_instruments.items():
        candles = fetch_and_store_data(
            kite, instrument_token, symbol, timeframe, 
            from_date, to_date, conn
        )
        
        if candles > 0:
            success_count += 1
            total_candles += candles
        
        # Rate limiting - Kite allows 3 requests/second
        time.sleep(0.35)
    
    conn.close()
    
    print("=" * 80)
    print(f"✓ Download complete!")
    print(f"  Success: {success_count}/{len(nifty50_instruments)} symbols")
    print(f"  Total candles: {total_candles:,}")
    print("=" * 80)


if __name__ == '__main__':
    main()
