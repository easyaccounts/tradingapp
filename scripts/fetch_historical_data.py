#!/usr/bin/env python3
"""
Fetch Historical Data from KiteConnect API

This script fetches historical OHLCV candle data for instruments from Zerodha's KiteConnect API.
Supports various intervals and optional OI (Open Interest) data.

Usage:
    python fetch_historical_data.py --instrument_token 256265 --interval day --from "2024-01-01 00:00:00" --to "2024-12-31 23:59:59"
    python fetch_historical_data.py --instrument_token 256265 --interval minute --from "2024-12-20 09:15:00" --to "2024-12-20 15:30:00" --oi 1
"""

import os
import sys
import argparse
import json
import redis
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

# Add parent directory to path to import kiteconnect if installed locally
try:
    from kiteconnect import KiteConnect
except ImportError:
    print("ERROR: kiteconnect library not found. Install it using: pip install kiteconnect")
    sys.exit(1)


def read_token_from_file():
    """Read access token from persistent file"""
    token_file_path = "/app/data/access_token.txt"
    try:
        if os.path.exists(token_file_path):
            with open(token_file_path, 'r') as f:
                token = f.read().strip()
            return token if token else None
        return None
    except Exception as e:
        return None


def get_access_token(redis_url_override=None):
    """
    Retrieve Kite access token from file (primary) or Redis (fallback)
    Same pattern as ingestion service
    
    Args:
        redis_url_override: Optional Redis URL to use instead of environment variable
    """
    # Try reading from file first (primary source)
    token = read_token_from_file()
    
    if token:
        print(f"✓ Access token retrieved from file")
        return token
    
    # Fallback to Redis
    redis_url = redis_url_override or os.getenv('REDIS_URL')
    if redis_url:
        try:
            redis_client = redis.from_url(redis_url, decode_responses=True)
            if redis_client:
                token = redis_client.get("kite_access_token")
                
                if token:
                    print(f"✓ Access token retrieved from Redis ({redis_url})")
                    return token
        except Exception as e:
            print(f"Warning: Could not connect to Redis ({redis_url}): {e}")
    
    # If neither works, raise error
    domain = os.getenv("DOMAIN", "localhost")
    protocol = "https" if os.getenv("ENVIRONMENT") == "production" else "http"
    raise Exception(
        f"Kite access token not found in file or Redis. "
        f"Please authenticate via the web interface first: {protocol}://{domain}/"
    )


def load_credentials(redis_url=None):
    """Load API credentials from environment variables
    
    Args:
        redis_url: Optional Redis URL to override environment variable
    """
    api_key = os.getenv('KITE_API_KEY')
    
    if not api_key:
        print("ERROR: KITE_API_KEY must be set in environment or .env file")
        sys.exit(1)
    
    try:
        access_token = get_access_token(redis_url_override=redis_url)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)
    
    return api_key, access_token


def validate_interval(interval):
    """Validate the interval parameter"""
    valid_intervals = ['minute', 'day', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute']
    if interval not in valid_intervals:
        raise ValueError(f"Invalid interval. Must be one of: {', '.join(valid_intervals)}")
    return interval


def validate_date(date_string):
    """Validate and parse date string"""
    try:
        # Try parsing with time
        return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            # Try parsing without time
            return datetime.strptime(date_string, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Invalid date format: {date_string}. Use 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'")


def fetch_historical_data(instrument_token, from_date, to_date, interval, continuous=False, oi=False, redis_url=None):
    """
    Fetch historical data from KiteConnect API
    
    Args:
        instrument_token (int): Instrument identifier
        from_date (str/datetime): Start date
        to_date (str/datetime): End date
        interval (str): Candle interval (minute, day, 3minute, etc.)
        continuous (bool): Get continuous data for F&O contracts
        oi (bool): Include Open Interest data
        redis_url (str): Optional Redis URL to override environment variable
    
    Returns:
        list: List of candle records
    """
    # Load credentials
    api_key, access_token = load_credentials(redis_url=redis_url)
    
    # Initialize KiteConnect
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    # Validate inputs
    interval = validate_interval(interval)
    
    # Parse dates if strings
    if isinstance(from_date, str):
        from_date = validate_date(from_date)
    if isinstance(to_date, str):
        to_date = validate_date(to_date)
    
    # Fetch historical data
    print(f"Fetching historical data for instrument {instrument_token}...")
    print(f"Interval: {interval}")
    print(f"From: {from_date}")
    print(f"To: {to_date}")
    print(f"Continuous: {continuous}")
    print(f"OI: {oi}")
    print()
    
    try:
        data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
            continuous=continuous,
            oi=oi
        )
        
        print(f"✓ Successfully fetched {len(data)} candles")
        return data
        
    except Exception as e:
        print(f"✗ Error fetching data: {str(e)}")
        sys.exit(1)


def format_output(data, output_format='json', output_file=None):
    """
    Format and optionally save the output
    
    Args:
        data (list): Historical data records
        output_format (str): Output format ('json', 'csv', 'print')
        output_file (str): Optional output file path
    """
    if output_format == 'json':
        # Convert datetime objects to strings for JSON serialization
        json_data = []
        for record in data:
            json_record = record.copy()
            if 'date' in json_record and isinstance(json_record['date'], datetime):
                json_record['date'] = json_record['date'].isoformat()
            json_data.append(json_record)
        
        output = json.dumps(json_data, indent=2)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(output)
            print(f"\n✓ Data saved to {output_file}")
        else:
            print("\n" + output)
    
    elif output_format == 'csv':
        import csv
        
        if not data:
            print("No data to output")
            return
        
        # Get field names from first record
        fieldnames = list(data[0].keys())
        
        if output_file:
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for record in data:
                    # Convert datetime to string
                    row = record.copy()
                    if 'date' in row and isinstance(row['date'], datetime):
                        row['date'] = row['date'].isoformat()
                    writer.writerow(row)
            print(f"\n✓ Data saved to {output_file}")
        else:
            # Print to stdout
            import io
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for record in data:
                row = record.copy()
                if 'date' in row and isinstance(row['date'], datetime):
                    row['date'] = row['date'].isoformat()
                writer.writerow(row)
            print("\n" + output.getvalue())
    
    elif output_format == 'print':
        print("\n" + "="*80)
        print("HISTORICAL DATA")
        print("="*80)
        for i, record in enumerate(data):
            print(f"\nCandle {i+1}:")
            for key, value in record.items():
                print(f"  {key}: {value}")
        print("="*80)


def main():
    """Main function to handle command-line execution"""
    parser = argparse.ArgumentParser(
        description='Fetch historical OHLCV data from KiteConnect API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch daily data for a year
  python fetch_historical_data.py --instrument_token 256265 --interval day --from "2024-01-01" --to "2024-12-31"
  
  # Fetch 1-minute data for a day with OI
  python fetch_historical_data.py --instrument_token 256265 --interval minute --from "2024-12-20 09:15:00" --to "2024-12-20 15:30:00" --oi 1
  
  # Fetch continuous F&O data and save to CSV
  python fetch_historical_data.py --instrument_token 12345678 --interval day --from "2024-01-01" --to "2024-12-31" --continuous 1 --format csv --output data.csv
        """
    )
    
    # Required arguments
    parser.add_argument('--instrument_token', type=int, required=True,
                        help='Instrument token (get from instruments API)')
    parser.add_argument('--interval', type=str, required=True,
                        choices=['minute', 'day', '3minute', '5minute', '10minute', '15minute', '30minute', '60minute'],
                        help='Candle interval')
    parser.add_argument('--from', dest='from_date', type=str, required=True,
                        help='Start date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--to', dest='to_date', type=str, required=True,
                        help='End date (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)')
    
    # Optional arguments
    parser.add_argument('--continuous', type=int, choices=[0, 1], default=0,
                        help='Get continuous data for F&O contracts (0 or 1, default: 0)')
    parser.add_argument('--oi', type=int, choices=[0, 1], default=0,
                        help='Include Open Interest data (0 or 1, default: 0)')
    parser.add_argument('--redis-url', type=str, default=None,
                        help='Redis URL (e.g., redis://localhost:6379) - overrides environment variable')
    
    # Output options
    parser.add_argument('--format', type=str, choices=['json', 'csv', 'print'], default='json',
                        help='Output format (default: json)')
    parser.add_argument('--output', type=str, default=None,
                        help='Output file path (default: print to stdout)')
    
    args = parser.parse_args()
    
    # Fetch data
    data = fetch_historical_data(
        instrument_token=args.instrument_token,
        from_date=args.from_date,
        to_date=args.to_date,
        interval=args.interval,
        continuous=bool(args.continuous),
        oi=bool(args.oi),
        redis_url=args.redis_url
    )
    
    # Format and output
    format_output(data, output_format=args.format, output_file=args.output)


if __name__ == '__main__':
    main()
