#!/usr/bin/env python3
"""
Update Instruments Script
Downloads instrument master data from KiteConnect and loads into Redis & PostgreSQL

Usage:
    python update_instruments.py

Environment Variables Required:
    KITE_API_KEY - KiteConnect API key
    REDIS_URL - Redis connection URL
    DATABASE_URL - PostgreSQL connection URL
"""

import os
import sys
import redis
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from kiteconnect import KiteConnect

# Load environment variables
load_dotenv()

# Configuration
KITE_API_KEY = os.getenv("KITE_API_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL")

# Exchanges to download (add more as needed)
EXCHANGES = ["NFO", "NSE", "BSE", "BFO", "MCX"]


def get_access_token() -> str:
    """
    Retrieve access token from Redis
    
    Returns:
        str: Access token
    """
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        token = redis_client.get("kite_access_token")
        redis_client.close()
        
        if not token:
            print("ERROR: Access token not found in Redis")
            print("Please authenticate via the web interface first: http://localhost/")
            sys.exit(1)
        
        return token
    except Exception as e:
        print(f"ERROR: Failed to retrieve access token: {e}")
        sys.exit(1)


def download_instruments(kite: KiteConnect, exchange: str) -> list:
    """
    Download instruments for a specific exchange
    
    Args:
        kite: KiteConnect instance
        exchange: Exchange code (NSE, NFO, BSE, etc.)
    
    Returns:
        list: List of instrument dictionaries
    """
    try:
        print(f"Downloading instruments for {exchange}...")
        instruments = kite.instruments(exchange)
        print(f"  ✓ Downloaded {len(instruments)} instruments from {exchange}")
        return instruments
    except Exception as e:
        print(f"  ✗ Failed to download from {exchange}: {e}")
        return []


def store_in_redis(instruments: list) -> int:
    """
    Store instruments in Redis as hashes
    
    Format: instrument:{token} -> hash with fields
    
    Args:
        instruments: List of instrument dictionaries
    
    Returns:
        int: Number of instruments stored
    """
    try:
        print("\nStoring instruments in Redis...")
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        
        stored_count = 0
        
        for instrument in instruments:
            key = f"instrument:{instrument['instrument_token']}"
            
            # Prepare data
            data = {
                'tradingsymbol': instrument.get('tradingsymbol', ''),
                'exchange': instrument.get('exchange', ''),
                'instrument_type': instrument.get('instrument_type', ''),
                'segment': instrument.get('segment', ''),
                'name': instrument.get('name', ''),
                'expiry': str(instrument.get('expiry', '')),
                'strike': str(instrument.get('strike', 0)),
                'tick_size': str(instrument.get('tick_size', 0)),
                'lot_size': str(instrument.get('lot_size', 0)),
                'exchange_token': str(instrument.get('exchange_token', 0))
            }
            
            # Store in Redis
            redis_client.hset(key, mapping=data)
            stored_count += 1
        
        redis_client.close()
        
        print(f"  ✓ Stored {stored_count} instruments in Redis")
        return stored_count
    
    except Exception as e:
        print(f"  ✗ Failed to store in Redis: {e}")
        return 0


def store_in_database(instruments: list) -> int:
    """
    Store instruments in PostgreSQL database
    Uses INSERT ... ON CONFLICT to update existing records
    
    Args:
        instruments: List of instrument dictionaries
    
    Returns:
        int: Number of instruments stored
    """
    try:
        print("\nStoring instruments in PostgreSQL...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        stored_count = 0
        
        insert_query = """
            INSERT INTO instruments (
                instrument_token, exchange_token, trading_symbol, name,
                exchange, segment, instrument_type, expiry, strike,
                tick_size, lot_size, last_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (instrument_token)
            DO UPDATE SET
                exchange_token = EXCLUDED.exchange_token,
                trading_symbol = EXCLUDED.trading_symbol,
                name = EXCLUDED.name,
                exchange = EXCLUDED.exchange,
                segment = EXCLUDED.segment,
                instrument_type = EXCLUDED.instrument_type,
                expiry = EXCLUDED.expiry,
                strike = EXCLUDED.strike,
                tick_size = EXCLUDED.tick_size,
                lot_size = EXCLUDED.lot_size,
                last_updated = EXCLUDED.last_updated;
        """
        
        for instrument in instruments:
            try:
                cursor.execute(insert_query, (
                    instrument.get('instrument_token'),
                    instrument.get('exchange_token'),
                    instrument.get('tradingsymbol', ''),
                    instrument.get('name', ''),
                    instrument.get('exchange', ''),
                    instrument.get('segment', ''),
                    instrument.get('instrument_type', ''),
                    instrument.get('expiry'),
                    instrument.get('strike'),
                    instrument.get('tick_size'),
                    instrument.get('lot_size'),
                    datetime.now()
                ))
                stored_count += 1
            except Exception as e:
                print(f"  ! Failed to insert {instrument.get('tradingsymbol')}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"  ✓ Stored {stored_count} instruments in PostgreSQL")
        return stored_count
    
    except Exception as e:
        print(f"  ✗ Failed to store in database: {e}")
        return 0


def print_summary(instruments_by_exchange: dict):
    """
    Print summary of downloaded instruments
    
    Args:
        instruments_by_exchange: Dictionary mapping exchange to instrument list
    """
    print("\n" + "="*60)
    print("INSTRUMENTS UPDATE SUMMARY")
    print("="*60)
    
    total_instruments = sum(len(instruments) for instruments in instruments_by_exchange.values())
    
    print(f"\nTotal Exchanges: {len(instruments_by_exchange)}")
    print(f"Total Instruments: {total_instruments}")
    print("\nBreakdown by Exchange:")
    
    for exchange, instruments in sorted(instruments_by_exchange.items()):
        print(f"  {exchange:10s}: {len(instruments):6d} instruments")
    
    print("\n" + "="*60)


def main():
    """Main entry point"""
    print("="*60)
    print("Trading Platform - Instruments Update Script")
    print("="*60)
    
    # Validate environment variables
    if not KITE_API_KEY:
        print("ERROR: KITE_API_KEY environment variable not set")
        sys.exit(1)
    
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL environment variable not set")
        sys.exit(1)
    
    # Get access token
    access_token = get_access_token()
    
    # Initialize KiteConnect
    print(f"\nInitializing KiteConnect (API Key: {KITE_API_KEY[:10]}...)")
    kite = KiteConnect(api_key=KITE_API_KEY)
    kite.set_access_token(access_token)
    
    # Download instruments from all exchanges
    all_instruments = []
    instruments_by_exchange = {}
    
    for exchange in EXCHANGES:
        instruments = download_instruments(kite, exchange)
        if instruments:
            all_instruments.extend(instruments)
            instruments_by_exchange[exchange] = instruments
    
    if not all_instruments:
        print("\nERROR: No instruments downloaded")
        sys.exit(1)
    
    # Store in Redis
    redis_count = store_in_redis(all_instruments)
    
    # Store in PostgreSQL
    db_count = store_in_database(all_instruments)
    
    # Print summary
    print_summary(instruments_by_exchange)
    
    print(f"\n✓ Update complete!")
    print(f"  - Redis: {redis_count} instruments")
    print(f"  - PostgreSQL: {db_count} instruments")
    print(f"\nInstruments are now available for ingestion service.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
