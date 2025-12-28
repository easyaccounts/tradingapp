"""
Get Nifty 50 instrument tokens from instruments table
"""

import psycopg2
import os

def get_db_connection():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'tradingapp'),
        user=os.getenv('DB_USER', 'tradingapp'),
        password=os.getenv('DB_PASSWORD', 'tradingapp123')
    )


# Nifty 50 trading symbols (as of Dec 2025)
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


def get_nifty50_instruments():
    """Fetch Nifty 50 instrument tokens from database"""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            # Query instruments table for NSE equity instruments matching Nifty 50 symbols
            cur.execute("""
                SELECT 
                    trading_symbol,
                    instrument_token,
                    name,
                    lot_size,
                    tick_size
                FROM instruments
                WHERE exchange = 'NSE'
                  AND instrument_type = 'EQ'
                  AND trading_symbol = ANY(%s)
                ORDER BY trading_symbol
            """, (NIFTY50_SYMBOLS,))
            
            results = cur.fetchall()
            
            instruments = {}
            for row in results:
                symbol = row[0]
                instruments[symbol] = {
                    'instrument_token': row[1],
                    'name': row[2],
                    'lot_size': row[3],
                    'tick_size': row[4]
                }
            
            return instruments
            
    finally:
        conn.close()


if __name__ == '__main__':
    print("Fetching Nifty 50 instruments from database...")
    instruments = get_nifty50_instruments()
    
    print(f"\nFound {len(instruments)} instruments:\n")
    print(f"{'Symbol':<15} {'Token':<12} {'Name':<40}")
    print("=" * 70)
    
    for symbol, data in sorted(instruments.items()):
        print(f"{symbol:<15} {data['instrument_token']:<12} {data['name']:<40}")
    
    print(f"\n✓ Total: {len(instruments)}/{len(NIFTY50_SYMBOLS)} symbols found")
    
    # Show missing symbols
    missing = set(NIFTY50_SYMBOLS) - set(instruments.keys())
    if missing:
        print(f"\n⚠ Missing symbols: {', '.join(sorted(missing))}")
