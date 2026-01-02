"""
Dhan Instruments Sync Script
Downloads api-scrip-master.csv from Dhan and syncs to instruments table

Usage:
    python sync_dhan_instruments.py [--db-url DB_URL] [--csv-path CSV_PATH]
    
Example:
    python sync_dhan_instruments.py --db-url postgresql://tradinguser:password@localhost:6432/tradingdb
    python sync_dhan_instruments.py --csv-path ./api-scrip-master.csv
"""

import csv
import psycopg2
import structlog
import sys
import argparse
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import requests

logger = structlog.get_logger()

# Dhan instrument master CSV URL
DHAN_INSTRUMENTS_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Exchange segment mapping (from Dhan docs)
EXCHANGE_SEGMENT_MAP = {
    0: "NSE_EQ",
    1: "NSE_FNO",
    2: "NSE_CURRENCY",
    3: "BSE_EQ",
    4: "BSE_FNO",
    5: "BSE_CURRENCY",
    6: "MCX_COMM"
}

# Reverse mapping for string-based exchange IDs (newer CSV format)
EXCHANGE_STRING_TO_CODE = {
    'NSE': 0,
    'BSE': 3,
    'MCX': 6,
    'NSE_EQ': 0,
    'NSE_FNO': 1,
    'NSE_CURRENCY': 2,
    'BSE_EQ': 3,
    'BSE_FNO': 4,
    'BSE_CURRENCY': 5,
    'MCX_COMM': 6
}


def download_instruments_csv(url: str, output_path: str) -> bool:
    """
    Download Dhan instrument master CSV
    
    Args:
        url: URL to download from
        output_path: Local path to save CSV
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info("downloading_instruments", url=url)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
        
        logger.info("download_complete", path=output_path, size_bytes=len(response.content))
        return True
    
    except requests.RequestException as e:
        logger.error("download_failed", error=str(e))
        return False


def parse_dhan_csv(csv_path: str) -> List[Dict]:
    """
    Parse Dhan instrument CSV
    
    CSV Columns:
        SEM_SMST_SECURITY_ID, SEM_EXM_EXCH_ID, SEM_SEGMENT, SEM_TRADING_SYMBOL,
        SEM_INSTRUMENT_NAME, SEM_EXPIRY_DATE, SEM_STRIKE_PRICE, SEM_OPTION_TYPE,
        SEM_TICK_SIZE, SEM_LOT_UNITS, SM_SYMBOL_NAME
    
    Args:
        csv_path: Path to CSV file
    
    Returns:
        List of dicts with parsed instrument data
    """
    instruments = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    # Parse exchange segment (handle both numeric and string formats)
                    exch_id = row['SEM_EXM_EXCH_ID'].strip()
                    
                    # Try parsing as integer first (old format)
                    try:
                        exchange_segment_code = int(exch_id)
                    except ValueError:
                        # New format: string like 'NSE', 'BSE', 'MCX'
                        exchange_segment_code = EXCHANGE_STRING_TO_CODE.get(exch_id)
                        if exchange_segment_code is None:
                            logger.warning("unknown_exchange", exchange=exch_id)
                            continue
                    
                    exchange_segment = EXCHANGE_SEGMENT_MAP.get(exchange_segment_code, 'UNKNOWN')
                    
                    # Determine exchange (NSE/BSE/MCX)
                    exchange = exchange_segment.split('_')[0]
                    
                    # Determine segment (EQ/FNO/CURRENCY/COMM)
                    segment = exchange_segment.split('_')[1] if '_' in exchange_segment else None
                    
                    # Parse option type
                    option_type = row['SEM_OPTION_TYPE'].strip()
                    if option_type not in ['CE', 'PE']:
                        option_type = None
                    
                    # Determine instrument type
                    instrument_type = None
                    if option_type:
                        instrument_type = 'CE' if option_type == 'CE' else 'PE'
                    elif row['SEM_INSTRUMENT_NAME'].strip() in ['FUTIDX', 'FUTSTK', 'FUTCOM']:
                        instrument_type = 'FUT'
                    elif row['SEM_INSTRUMENT_NAME'].strip() in ['INDEX', 'EQ']:
                        instrument_type = 'EQ'
                    
                    # Parse expiry date
                    expiry = None
                    expiry_str = row['SEM_EXPIRY_DATE'].strip()
                    if expiry_str:
                        try:
                            # Format: DD-MMM-YYYY (e.g., "28-NOV-2024")
                            expiry = datetime.strptime(expiry_str, '%d-%b-%Y').date()
                        except ValueError:
                            pass
                    
                    # Parse strike price
                    strike = None
                    strike_str = row['SEM_STRIKE_PRICE'].strip()
                    if strike_str:
                        try:
                            strike = float(strike_str)
                        except ValueError:
                            pass
                    
                    # Parse tick size
                    tick_size = None
                    tick_str = row['SEM_TICK_SIZE'].strip()
                    if tick_str:
                        try:
                            tick_size = float(tick_str)
                        except ValueError:
                            pass
                    
                    # Parse lot size
                    lot_size = None
                    lot_str = row['SEM_LOT_UNITS'].strip()
                    if lot_str:
                        try:
                            lot_size = int(lot_str)
                        except ValueError:
                            pass
                    
                    instrument = {
                        'security_id': row['SEM_SMST_SECURITY_ID'].strip(),
                        'trading_symbol': row['SEM_TRADING_SYMBOL'].strip(),
                        'name': row['SEM_INSTRUMENT_NAME'].strip(),
                        'exchange': exchange,
                        'segment': segment,
                        'exchange_segment_code': exchange_segment_code,
                        'instrument_type': instrument_type,
                        'expiry': expiry,
                        'strike': strike,
                        'option_type': option_type,
                        'tick_size': tick_size,
                        'lot_size': lot_size,
                        'symbol_name': row.get('SM_SYMBOL_NAME', '').strip()
                    }
                    
                    instruments.append(instrument)
                
                except Exception as e:
                    logger.error("row_parse_failed", row=row, error=str(e))
                    continue
        
        logger.info("csv_parsed", total_instruments=len(instruments))
        return instruments
    
    except FileNotFoundError:
        logger.error("csv_not_found", path=csv_path)
        return []
    except Exception as e:
        logger.error("csv_parse_failed", error=str(e))
        return []


def generate_instrument_token(security_id: str, exchange_segment_code: int) -> int:
    """
    Generate unique instrument_token from security_id and exchange
    
    Strategy: Use hash of security_id + exchange to create unique integer
    This ensures backward compatibility with existing instrument_token system
    
    Args:
        security_id: Dhan security ID (string)
        exchange_segment_code: Exchange segment code (0-6)
    
    Returns:
        Unique instrument token (integer)
    """
    # Create composite key
    composite = f"{security_id}:{exchange_segment_code}"
    
    # Generate hash and take lower 31 bits (positive int)
    token = hash(composite) & 0x7FFFFFFF
    
    return token


def sync_to_database(instruments: List[Dict], db_url: str, batch_size: int = 1000) -> Tuple[int, int]:
    """
    Sync Dhan instruments to database
    
    Uses INSERT ... ON CONFLICT to update existing instruments
    Marks all as source='dhan' and is_active=TRUE
    
    Args:
        instruments: List of parsed instruments
        db_url: PostgreSQL connection URL
        batch_size: Number of instruments per batch
    
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    inserted = 0
    updated = 0
    
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        # Insert/update in batches
        for i in range(0, len(instruments), batch_size):
            batch = instruments[i:i+batch_size]
            
            # Generate instrument tokens
            values = []
            for inst in batch:
                token = generate_instrument_token(
                    inst['security_id'],
                    inst['exchange_segment_code']
                )
                
                values.append((
                    token,
                    inst['trading_symbol'],
                    inst['name'],
                    inst['exchange'],
                    inst['segment'],
                    inst['instrument_type'],
                    inst['expiry'],
                    inst['strike'],
                    inst['tick_size'],
                    inst['lot_size'],
                    inst['security_id'],
                    'dhan',
                    True  # is_active
                ))
            
            # Upsert query
            insert_query = """
                INSERT INTO instruments (
                    instrument_token, trading_symbol, name, exchange, segment,
                    instrument_type, expiry, strike, tick_size,
                    lot_size, security_id, source, is_active
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (instrument_token) DO UPDATE SET
                    trading_symbol = EXCLUDED.trading_symbol,
                    name = EXCLUDED.name,
                    exchange = EXCLUDED.exchange,
                    segment = EXCLUDED.segment,
                    instrument_type = EXCLUDED.instrument_type,
                    expiry = EXCLUDED.expiry,
                    strike = EXCLUDED.strike,
                    tick_size = EXCLUDED.tick_size,
                    lot_size = EXCLUDED.lot_size,
                    security_id = EXCLUDED.security_id,
                    source = EXCLUDED.source,
                    is_active = EXCLUDED.is_active
            """
            
            cursor.executemany(insert_query, values)
            
            # Track inserts vs updates (approximate)
            inserted += cursor.rowcount
            
            logger.info("batch_synced", batch_num=i//batch_size + 1, rows=len(batch))
        
        conn.commit()
        
        logger.info(
            "sync_complete",
            total_instruments=len(instruments),
            rows_affected=inserted
        )
        
        cursor.close()
        conn.close()
        
        return (inserted, 0)  # Can't distinguish inserts from updates with executemany
    
    except psycopg2.Error as e:
        logger.error("database_sync_failed", error=str(e))
        if 'conn' in locals():
            conn.rollback()
        return (0, 0)


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Sync Dhan instruments to database')
    parser.add_argument(
        '--db-url',
        default=os.getenv('DATABASE_URL'),
        help='PostgreSQL connection URL (default: $DATABASE_URL)'
    )
    parser.add_argument(
        '--csv-path',
        default='./api-scrip-master.csv',
        help='Path to Dhan CSV file (default: ./api-scrip-master.csv)'
    )
    parser.add_argument(
        '--download',
        action='store_true',
        help='Download CSV from Dhan before syncing'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of instruments per batch (default: 1000)'
    )
    
    args = parser.parse_args()
    
    # Validate database URL
    if not args.db_url:
        logger.error("database_url_missing", hint="Use --db-url or set $DATABASE_URL")
        sys.exit(1)
    
    # Download CSV if requested
    if args.download:
        if not download_instruments_csv(DHAN_INSTRUMENTS_URL, args.csv_path):
            logger.error("download_failed", url=DHAN_INSTRUMENTS_URL)
            sys.exit(1)
    
    # Check CSV exists
    if not os.path.exists(args.csv_path):
        logger.error("csv_not_found", path=args.csv_path)
        sys.exit(1)
    
    # Parse CSV
    instruments = parse_dhan_csv(args.csv_path)
    if not instruments:
        logger.error("no_instruments_parsed")
        sys.exit(1)
    
    # Sync to database
    inserted, updated = sync_to_database(instruments, args.db_url, args.batch_size)
    
    logger.info("sync_summary", inserted=inserted, updated=updated)
    print(f"\n✅ Sync complete: {inserted} rows affected")


if __name__ == "__main__":
    main()
