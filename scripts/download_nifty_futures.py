"""
Download NIFTY Futures continuous historical data to database
Fetches daily continuous futures data with continuous=1 parameter
"""

import os
import sys
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
from kiteconnect import KiteConnect
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_kite_client():
    """Initialize KiteConnect client"""
    api_key = os.getenv('KITE_API_KEY')
    access_token = os.getenv('KITE_ACCESS_TOKEN')
    
    if not api_key or not access_token:
        print("Error: KITE_API_KEY and KITE_ACCESS_TOKEN must be set in .env")
        sys.exit(1)
    
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD', '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ=')
    )


def fetch_and_store_futures_data(kite, instrument_token, symbol, from_date, to_date, conn):
    """Fetch continuous futures data from Kite and store in database"""
    try:
        print(f"\nFetching {symbol} continuous futures...")
        print(f"  Period: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
        print(f"  Instrument Token: {instrument_token}")
        
        # Fetch continuous data from Kite
        data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval='day',
            continuous=True,  # This is the key parameter for continuous futures
            oi=True
        )
        
        if not data:
            print(f"  ⚠ No data returned for {symbol}")
            return 0
        
        print(f"  ✓ Fetched {len(data)} candles")
        
        # Prepare data for insertion
        records = []
        for candle in data:
            records.append((
                candle['date'],
                instrument_token,
                symbol,
                'day',
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
        
        print(f"  ✓ Stored {len(records)} candles in database")
        print(f"  Date range: {records[0][0].strftime('%Y-%m-%d')} to {records[-1][0].strftime('%Y-%m-%d')}")
        
        return len(records)
        
    except Exception as e:
        print(f"  ✗ Error fetching {symbol}: {e}")
        import traceback
        traceback.print_exc()
        return 0


def main():
    print("=" * 80)
    print(" " * 20 + "NIFTY FUTURES CONTINUOUS DATA DOWNLOADER")
    print("=" * 80)
    
    # Initialize Kite client
    print("\n1. Initializing Kite client...")
    kite = get_kite_client()
    print("   ✓ Kite client ready")
    
    # Connect to database
    print("\n2. Connecting to database...")
    conn = get_db_connection()
    print("   ✓ Database connected")
    
    # NIFTY futures instrument token (current month contract)
    # We use continuous=True, so it will stitch contracts automatically
    NIFTY_FUTURES_TOKEN = 12683010  # NIFTY25DECFUT
    
    # Calculate date range (2000 days back from today)
    to_date = datetime.now()
    from_date = to_date - timedelta(days=2000)
    
    print(f"\n3. Downloading NIFTY continuous futures data...")
    print(f"   Requesting ~2000 days of data")
    
    # Fetch and store data
    total_candles = fetch_and_store_futures_data(
        kite=kite,
        instrument_token=NIFTY_FUTURES_TOKEN,
        symbol='NIFTY_FUT',
        from_date=from_date,
        to_date=to_date,
        conn=conn
    )
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 80)
    print("DOWNLOAD COMPLETE!")
    print("=" * 80)
    print(f"Total candles stored: {total_candles}")
    print(f"Symbol: NIFTY_FUT (continuous)")
    print(f"Timeframe: day")
    print(f"Table: historical_data")
    print("\nTo query the data:")
    print("  SELECT * FROM historical_data WHERE trading_symbol = 'NIFTY_FUT' ORDER BY time;")
    print("=" * 80)


if __name__ == '__main__':
    main()
