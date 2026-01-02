#!/usr/bin/env python3
"""Debug script to inspect the structure of key_levels data"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import pytz
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '6432')),
    'database': os.getenv('DB_NAME', 'tradingdb'),
    'user': os.getenv('DB_USER', 'tradinguser'),
    'password': os.getenv('DB_PASSWORD')
}

IST = pytz.timezone('Asia/Kolkata')

def debug_level_structure():
    """Fetch and display the structure of key_levels"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT 
        time,
        current_price,
        key_levels
    FROM depth_signals
    WHERE time >= DATE_TRUNC('day', NOW() AT TIME ZONE 'Asia/Kolkata')
    ORDER BY time ASC
    LIMIT 5
    """
    
    cursor.execute(query)
    data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    if not data:
        print("❌ No signals found")
        return
    
    print("Sample Key Levels Structure:\n")
    
    for i, record in enumerate(data):
        print(f"Signal #{i+1} at {record['time'].astimezone(IST).strftime('%H:%M:%S')}")
        print(f"  Current Price: ₹{record['current_price']}")
        
        levels = record['key_levels'] if record['key_levels'] else []
        if levels:
            print(f"  Number of levels: {len(levels)}")
            print(f"  Sample level: {json.dumps(levels[0], indent=4, default=str)}")
            
            # Show all unique keys
            if levels:
                keys = set()
                for level in levels:
                    keys.update(level.keys())
                print(f"  All available keys: {sorted(keys)}")
        else:
            print(f"  No levels")
        print()

if __name__ == '__main__':
    debug_level_structure()
