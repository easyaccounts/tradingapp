"""
Download historical daily data for Nifty 500 stocks
Stores in TimescaleDB historical_data table
"""

import os
import sys
import time
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """Create database connection"""
    try:
        return psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'tradingdb'),
            user=os.getenv('DB_USER', 'tradinguser'),
            password=os.getenv('DB_PASSWORD', '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=')
        )
    except Exception as e:
        print(f"Database connection error: {e}")
        sys.exit(1)


def get_nifty500_instruments_from_csv():
    """Read Nifty 500 instruments from instruments.csv"""
    try:
        # Read instruments.csv
        df = pd.read_csv('instruments.csv')
        
        # Filter for NSE equity instruments
        nse_eq = df[
            (df['exchange'] == 'NSE') & 
            (df['instrument_type'] == 'EQ')
        ].copy()
        
        # Get unique trading symbols
        all_symbols = set(nse_eq['tradingsymbol'].unique())
        
        # Read Nifty 500 list from CSV if available, otherwise return all NSE EQ
        try:
            nifty500_df = pd.read_csv('nifty500_list.csv')
            nifty500_symbols = set(nifty500_df['Symbol'].str.strip().unique())
            
            # Filter instruments to only Nifty 500
            nifty500_instruments = nse_eq[nse_eq['tradingsymbol'].isin(nifty500_symbols)]
            
            print(f"✓ Found {len(nifty500_instruments)} Nifty 500 instruments from nifty500_list.csv")
            
        except FileNotFoundError:
            print("⚠ nifty500_list.csv not found, will download all NSE EQ stocks")
            nifty500_instruments = nse_eq
            print(f"✓ Found {len(nifty500_instruments)} NSE EQ instruments")
        
        return nifty500_instruments[['instrument_token', 'tradingsymbol', 'name']].to_dict('records')
        
    except Exception as e:
        print(f"Error reading instruments: {e}")
        sys.exit(1)


def fetch_and_store_data(kite, instrument_token, trading_symbol, days=2000):
    """Fetch historical data and store in database"""
    try:
        # Calculate date range
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days)
        
        # Fetch data from Kite
        data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval='day'
        )
        
        if not data:
            return 0
        
        # Connect to database
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Insert data with upsert logic
        insert_query = """
            INSERT INTO historical_data 
            (time, instrument_token, trading_symbol, timeframe, open, high, low, close, volume, oi)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, instrument_token, timeframe) 
            DO UPDATE SET 
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                oi = EXCLUDED.oi,
                created_at = NOW()
        """
        
        count = 0
        for candle in data:
            cur.execute(insert_query, (
                candle['date'],
                instrument_token,
                trading_symbol,
                'day',
                candle['open'],
                candle['high'],
                candle['low'],
                candle['close'],
                candle['volume'],
                candle.get('oi', 0)
            ))
            count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        return count
        
    except Exception as e:
        print(f"Error fetching {trading_symbol}: {e}")
        return 0


def main():
    print("=" * 100)
    print(" " * 30 + "NIFTY 500 HISTORICAL DATA DOWNLOAD")
    print("=" * 100)
    
    # Get API credentials
    api_key = os.getenv('KITE_API_KEY')
    if not api_key:
        print("Error: KITE_API_KEY not found in environment")
        sys.exit(1)
    
    # Try to get access token from Redis or environment
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        access_token = r.get('kite_access_token')
    except:
        access_token = os.getenv('KITE_ACCESS_TOKEN')
    
    if not access_token:
        print("Error: KITE_ACCESS_TOKEN not found")
        sys.exit(1)
    
    # Initialize Kite Connect
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    # Get Nifty 500 instruments
    instruments = get_nifty500_instruments_from_csv()
    total_instruments = len(instruments)
    
    if total_instruments == 0:
        print("No instruments found!")
        sys.exit(1)
    
    print(f"\n⏳ Starting download for {total_instruments} instruments...")
    print(f"   Expected time: ~{total_instruments * 0.35 / 60:.1f} minutes\n")
    
    # Download data
    total_candles = 0
    success_count = 0
    failed_symbols = []
    
    for i, instrument in enumerate(instruments, 1):
        symbol = instrument['tradingsymbol']
        token = instrument['instrument_token']
        
        print(f"[{i:3d}/{total_instruments}] Downloading {symbol:15s}...", end=" ", flush=True)
        
        candle_count = fetch_and_store_data(kite, token, symbol)
        
        if candle_count > 0:
            total_candles += candle_count
            success_count += 1
            print(f"✓ {candle_count:4d} candles")
        else:
            failed_symbols.append(symbol)
            print(f"✗ Failed")
        
        # Rate limiting - 3 requests per second
        time.sleep(0.35)
    
    # Summary
    print()
    print("=" * 100)
    print(f"✓ Download complete!")
    print(f"   Success: {success_count}/{total_instruments} symbols")
    print(f"   Total candles: {total_candles:,}")
    
    if failed_symbols:
        print(f"\n⚠ Failed symbols ({len(failed_symbols)}):")
        for symbol in failed_symbols[:20]:  # Show first 20
            print(f"   - {symbol}")
        if len(failed_symbols) > 20:
            print(f"   ... and {len(failed_symbols) - 20} more")
    
    print("=" * 100)


if __name__ == '__main__':
    main()
