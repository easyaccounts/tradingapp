#!/usr/bin/env python3
"""
Unified Instruments Sync Script
Downloads instruments from Kite API, stores in Postgres & Redis, updates .env

Features:
- Single HTTP call (no auth required)
- Stores in Postgres + Redis
- Updates .env INSTRUMENTS line (backward compatible)
- Adds is_active flag for future DB-driven subscription

Usage:
    python sync_instruments.py [--filter nifty|banknifty|all]
    
Examples:
    python sync_instruments.py --filter nifty     # Update NIFTY options in .env
    python sync_instruments.py --filter all       # Store all, don't update .env
"""

import os
import sys
import csv
import redis
import psycopg2
import requests
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Configuration
INSTRUMENTS_URL = "https://api.kite.trade/instruments"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL")
ENV_FILE = Path(__file__).parent.parent / ".env"
BACKUP_DIR = Path(__file__).parent.parent / "backups"


def download_instruments() -> str:
    """Download instruments CSV from Kite API (public endpoint, no auth needed)"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Downloading from {INSTRUMENTS_URL}...")
    try:
        response = requests.get(INSTRUMENTS_URL, timeout=30)
        response.raise_for_status()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Downloaded {len(response.content):,} bytes")
        return response.text
    except Exception as e:
        print(f"✗ ERROR: Failed to download: {e}")
        sys.exit(1)


def parse_instruments(csv_content: str) -> List[Dict]:
    """Parse CSV into list of instrument dictionaries"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Parsing instruments...")
    
    instruments = []
    csv_reader = csv.DictReader(csv_content.splitlines())
    
    for row in csv_reader:
        instrument = {
            'instrument_token': int(row.get('instrument_token', 0)),
            'exchange_token': int(row.get('exchange_token', 0)),
            'trading_symbol': row.get('tradingsymbol', ''),
            'name': row.get('name', ''),
            'exchange': row.get('exchange', ''),
            'segment': row.get('segment', ''),
            'instrument_type': row.get('instrument_type', ''),
            'expiry': row.get('expiry') or None,
            'strike': float(row.get('strike', 0)) if row.get('strike') else None,
            'tick_size': float(row.get('tick_size', 0)) if row.get('tick_size') else None,
            'lot_size': int(row.get('lot_size', 0)) if row.get('lot_size') else None
        }
        instruments.append(instrument)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Parsed {len(instruments):,} instruments")
    return instruments


def filter_instruments(instruments: List[Dict], filter_type: str) -> List[int]:
    """Filter instruments and return list of tokens"""
    if filter_type == 'all':
        return []  # Don't filter, don't update .env
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Filtering for {filter_type.upper()}...")
    
    tokens = []
    for inst in instruments:
        name = inst['name']
        segment = inst['segment']
        
        if filter_type == 'nifty':
            # NIFTY options + futures from NFO
            if name == 'NIFTY' and segment in ['NFO-OPT', 'NFO-FUT']:
                tokens.append(inst['instrument_token'])
        
        elif filter_type == 'banknifty':
            # BANKNIFTY options + futures from NFO
            if name == 'BANKNIFTY' and segment in ['NFO-OPT', 'NFO-FUT']:
                tokens.append(inst['instrument_token'])
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Filtered {len(tokens):,} tokens")
    return tokens


def store_in_postgres(instruments: List[Dict]) -> int:
    """Store instruments in PostgreSQL with ON CONFLICT update"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Storing in PostgreSQL...")
    
    if not DATABASE_URL:
        print("  ⚠ DATABASE_URL not set, skipping Postgres")
        return 0
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # First, check if is_active column exists, if not add it
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'instruments' AND column_name = 'is_active'
        """)
        
        if not cursor.fetchone():
            print("  + Adding is_active column...")
            cursor.execute("ALTER TABLE instruments ADD COLUMN is_active BOOLEAN DEFAULT FALSE")
            conn.commit()
        
        # Bulk insert with ON CONFLICT
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
        
        inserted = 0
        for inst in instruments:
            try:
                cursor.execute(insert_query, (
                    inst['instrument_token'],
                    inst['exchange_token'],
                    inst['trading_symbol'],
                    inst['name'],
                    inst['exchange'],
                    inst['segment'],
                    inst['instrument_type'],
                    inst['expiry'],
                    inst['strike'],
                    inst['tick_size'],
                    inst['lot_size'],
                    datetime.now()
                ))
                inserted += 1
            except Exception as e:
                print(f"  ! Failed {inst['trading_symbol']}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Stored {inserted:,} in PostgreSQL")
        return inserted
    
    except Exception as e:
        print(f"  ✗ PostgreSQL error: {e}")
        return 0


def store_in_redis(instruments: List[Dict]) -> int:
    """Store instruments in Redis as hashes (for enricher cache)"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Storing in Redis...")
    
    try:
        redis_client: redis.Redis = redis.from_url(REDIS_URL, decode_responses=True)  # type: ignore
        
        stored = 0
        for inst in instruments:
            key = f"instrument:{inst['instrument_token']}"
            data = {
                'tradingsymbol': inst['trading_symbol'],
                'exchange': inst['exchange'],
                'instrument_type': inst['instrument_type'],
                'segment': inst['segment'],
                'name': inst['name'],
                'expiry': str(inst['expiry']) if inst['expiry'] else '',
                'strike': str(inst['strike']) if inst['strike'] else '0',
                'tick_size': str(inst['tick_size']) if inst['tick_size'] else '0',
                'lot_size': str(inst['lot_size']) if inst['lot_size'] else '0',
                'exchange_token': str(inst['exchange_token'])
            }
            redis_client.hset(key, mapping=data)
            stored += 1
        
        redis_client.close()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Stored {stored:,} in Redis")
        return stored
    
    except Exception as e:
        print(f"  ✗ Redis error: {e}")
        return 0


def backup_env_file():
    """Backup .env file before modification"""
    if not ENV_FILE.exists():
        return
    
    BACKUP_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = BACKUP_DIR / f".env.backup.{timestamp}"
    
    with open(ENV_FILE, 'r') as src, open(backup_file, 'w') as dst:
        dst.write(src.read())
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Backed up .env")


def update_env_file(tokens: List[int]):
    """Update INSTRUMENTS line in .env (backward compatibility)"""
    if not tokens:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ⊗ Skipping .env update (no filter applied)")
        return
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Updating .env file...")
    
    if not ENV_FILE.exists():
        print(f"  ✗ {ENV_FILE} not found")
        return
    
    backup_env_file()
    
    with open(ENV_FILE, 'r') as f:
        lines = f.readlines()
    
    instruments_line = f"INSTRUMENTS={','.join(map(str, tokens))}\n"
    updated = False
    
    for i, line in enumerate(lines):
        if line.startswith('INSTRUMENTS='):
            lines[i] = instruments_line
            updated = True
            break
    
    if not updated:
        lines.append(f"\n# Auto-updated by sync_instruments.py\n")
        lines.append(instruments_line)
    
    with open(ENV_FILE, 'w') as f:
        f.writelines(lines)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Updated .env with {len(tokens):,} tokens")


def mark_instruments_active(tokens: List[int]) -> int:
    """Mark filtered instruments as active in database"""
    if not tokens or not DATABASE_URL:
        return 0
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Marking {len(tokens):,} instruments as active in DB...")
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # First, mark all as inactive
        cursor.execute("UPDATE instruments SET is_active = FALSE")
        
        # Then mark filtered ones as active
        if tokens:
            placeholders = ','.join(['%s'] * len(tokens))
            cursor.execute(f"""
                UPDATE instruments 
                SET is_active = TRUE 
                WHERE instrument_token IN ({placeholders})
            """, tokens)
        
        conn.commit()
        affected = cursor.rowcount
        cursor.close()
        conn.close()
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✓ Marked {affected:,} instruments as active")
        return affected
    
    except Exception as e:
        print(f"  ✗ Failed to mark active: {e}")
        return 0


def print_summary(instruments: List[Dict], filter_type: str, tokens: List[int]):
    """Print execution summary"""
    print("\n" + "="*70)
    print("SYNC SUMMARY")
    print("="*70)
    
    # Count by exchange
    exchanges = {}
    for inst in instruments:
        ex = inst['exchange']
        exchanges[ex] = exchanges.get(ex, 0) + 1
    
    print(f"\nTotal Instruments: {len(instruments):,}")
    print(f"\nBy Exchange:")
    for ex, count in sorted(exchanges.items()):
        print(f"  {ex:10s}: {count:6,}")
    
    if tokens:
        print(f"\nFiltered ({filter_type.upper()}): {len(tokens):,} tokens written to .env")
    else:
        print(f"\nNo .env update (filter={filter_type})")
    
    print("\n" + "="*70)


def main():
    parser = argparse.ArgumentParser(description='Sync instruments from Kite API')
    parser.add_argument(
        '--filter',
        choices=['nifty', 'banknifty', 'all'],
        default='nifty',
        help='Filter type for .env update (default: nifty)'
    )
    args = parser.parse_args()
    
    print("="*70)
    print("UNIFIED INSTRUMENTS SYNC")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Filter: {args.filter}")
    print()
    
    # Download
    csv_content = download_instruments()
    
    # Parse
    instruments = parse_instruments(csv_content)
    
    # Store in databases
    pg_count = store_in_postgres(instruments)
    redis_count = store_in_redis(instruments)
    
    # Filter and update .env (if filter specified)
    tokens = filter_instruments(instruments, args.filter)
    
    # Mark as active in database
    active_count = mark_instruments_active(tokens)
    
    # Update .env for backward compatibility
    update_env_file(tokens)
    
    # Summary
    print_summary(instruments, args.filter, tokens)
    
    print(f"\n✓ Sync completed at {datetime.now().strftime('%H:%M:%S')}")
    
    if tokens:
        print("\n⚠ IMPORTANT: Restart ingestion service to apply changes:")
        print("   docker compose restart ingestion\n")
    
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n✗ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
