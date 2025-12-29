"""
Export depth snapshot data to CSV
"""

import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'tradingdb',
    'user': 'tradinguser',
    'password': '5Ke1Ne9TLlI1TNv1F6JEfefNvDvy0jZv66Sh0vqQJKQ='
}

def export_to_csv():
    """Export depth snapshots to CSV"""
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    query = """
    SELECT 
        timestamp AT TIME ZONE 'Asia/Kolkata' as time_ist,
        security_id,
        instrument_name,
        best_bid,
        best_ask,
        spread,
        total_bid_qty,
        total_ask_qty,
        total_bid_orders,
        total_ask_orders,
        imbalance_ratio,
        avg_bid_order_size,
        avg_ask_order_size,
        bid_vwap,
        ask_vwap,
        bid_50pct_level,
        ask_50pct_level
    FROM depth_200_snapshots
    WHERE timestamp AT TIME ZONE 'Asia/Kolkata' >= '2025-12-29 14:10:00'
      AND timestamp AT TIME ZONE 'Asia/Kolkata' <= '2025-12-29 15:30:00'
    ORDER BY timestamp ASC
    """
    
    print("Fetching data...")
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"✓ Loaded {len(df):,} snapshots")
    
    # Save to CSV
    output_file = '/opt/tradingapp/depth_snapshots_20251229.csv'
    df.to_csv(output_file, index=False)
    
    print(f"✓ Exported to {output_file}")
    print(f"  File size: {len(df)} rows")
    print()
    print("To download to your local machine, run:")
    print(f"  scp root@82.180.144.255:{output_file} C:\\tradingapp\\")

if __name__ == "__main__":
    export_to_csv()
