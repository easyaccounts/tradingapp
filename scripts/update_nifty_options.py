#!/usr/bin/env python3
"""
Update NIFTY Options and Futures Tokens in .env file
Downloads instruments from Kite API and filters NFO-OPT and NFO-FUT NIFTY instruments
Should be run weekly (every Wednesday) to refresh tokens
"""

import os
import sys
import csv
import requests
from datetime import datetime
from pathlib import Path

# Configuration
INSTRUMENTS_URL = "https://api.kite.trade/instruments"
ENV_FILE = Path(__file__).parent.parent / ".env"
BACKUP_DIR = Path(__file__).parent.parent / "backups"

def download_instruments():
    """Download instruments CSV from Kite API"""
    print(f"[{datetime.now()}] Downloading instruments from {INSTRUMENTS_URL}...")
    try:
        response = requests.get(INSTRUMENTS_URL, timeout=30)
        response.raise_for_status()
        print(f"[{datetime.now()}] Downloaded {len(response.content)} bytes")
        return response.text
    except Exception as e:
        print(f"ERROR: Failed to download instruments: {e}")
        sys.exit(1)

def parse_and_filter_instruments(csv_content):
    """Parse CSV and filter NIFTY options and futures from NFO-OPT and NFO-FUT segments"""
    print(f"[{datetime.now()}] Parsing and filtering instruments...")
    
    tokens = []
    options_count = 0
    futures_count = 0
    csv_reader = csv.DictReader(csv_content.splitlines())
    
    for row in csv_reader:
        segment = row.get('segment')
        name = row.get('name')
        
        # Filter: (segment = NFO-OPT OR NFO-FUT) and name = NIFTY
        if name == 'NIFTY' and segment in ['NFO-OPT', 'NFO-FUT']:
            token = row.get('instrument_token')
            if token:
                tokens.append(token)
                if segment == 'NFO-OPT':
                    options_count += 1
                else:
                    futures_count += 1
    
    print(f"[{datetime.now()}] Found {len(tokens)} NIFTY tokens:")
    print(f"  - Options (NFO-OPT): {options_count}")
    print(f"  - Futures (NFO-FUT): {futures_count}")
    return tokens

def backup_env_file():
    """Create a backup of the current .env file"""
    if not ENV_FILE.exists():
        print(f"WARNING: {ENV_FILE} does not exist")
        return
    
    BACKUP_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = BACKUP_DIR / f".env.backup.{timestamp}"
    
    with open(ENV_FILE, 'r') as src, open(backup_file, 'w') as dst:
        dst.write(src.read())
    
    print(f"[{datetime.now()}] Backed up .env to {backup_file}")

def update_env_file(tokens):
    """Update INSTRUMENTS line in .env file"""
    if not ENV_FILE.exists():
        print(f"ERROR: {ENV_FILE} not found")
        sys.exit(1)
    
    # Create backup first
    backup_env_file()
    
    # Read current .env content
    with open(ENV_FILE, 'r') as f:
        lines = f.readlines()
    
    # Find and update INSTRUMENTS line
    instruments_line = f"INSTRUMENTS={','.join(tokens)}\n"
    updated = False
    
    for i, line in enumerate(lines):
        if line.startswith('INSTRUMENTS='):
            lines[i] = instruments_line
            updated = True
            print(f"[{datetime.now()}] Updated INSTRUMENTS line")
            break
    
    # If INSTRUMENTS line doesn't exist, append it
    if not updated:
        lines.append(f"\n# NIFTY Options tokens (auto-updated)\n")
        lines.append(instruments_line)
        print(f"[{datetime.now()}] Added INSTRUMENTS line")
    
    # Write back to file
    with open(ENV_FILE, 'w') as f:
        f.writelines(lines)
    
    print(f"[{datetime.now()}] Successfully updated {ENV_FILE}")
    print(f"[{datetime.now()}] Total tokens: {len(tokens)}")

def main():
    """Main execution"""
    print("=" * 70)
    print("NIFTY Options & Futures Tokens Update Script")
    print("=" * 70)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Download instruments
    csv_content = download_instruments()
    
    # Parse and filter
    tokens = parse_and_filter_instruments(csv_content)
    
    if not tokens:
        print("ERROR: No NIFTY options tokens found!")
        sys.exit(1)
    
    # Show sample tokens
    print(f"\nSample tokens (first 10):")
    for token in tokens[:10]:
        print(f"  - {token}")
    print()
    
    # Update .env file
    update_env_file(tokens)
    
    print()
    print("=" * 70)
    print("Update completed successfully!")
    print(f"Finished at: {datetime.now()}")
    print("=" * 70)
    print()
    print("IMPORTANT: Restart the ingestion service to apply changes:")
    print("  docker-compose restart ingestion")
    print()

if __name__ == "__main__":
    main()
