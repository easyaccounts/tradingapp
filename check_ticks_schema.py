#!/usr/bin/env python3
"""Check ticks table schema for options data"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '6432')),
    'database': os.getenv('DB_NAME', 'tradingdb'),
    'user': os.getenv('DB_USER', 'tradinguser'),
    'password': os.getenv('DB_PASSWORD')
}

def check_schema():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get ticks table columns
    query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'ticks'
    ORDER BY ordinal_position
    """
    
    cursor.execute(query)
    columns = cursor.fetchall()
    
    print("TICKS TABLE SCHEMA:")
    print("="*80)
    for col in columns:
        print(f"{col['column_name']:<30} {col['data_type']:<20} Nullable: {col['is_nullable']}")
    
    # Get sample data
    print("\n" + "="*80)
    print("SAMPLE TICKS DATA:")
    print("="*80)
    
    sample_query = """
    SELECT * FROM ticks
    LIMIT 5
    """
    
    cursor.execute(sample_query)
    samples = cursor.fetchall()
    
    if samples:
        print("\nFirst row keys:")
        for key in samples[0].keys():
            print(f"  - {key}")
        
        print(f"\nFirst row values:")
        for key, value in samples[0].items():
            print(f"  {key}: {value}")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    check_schema()
