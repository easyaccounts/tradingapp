#!/usr/bin/env python3
"""
Signal Generator Replay
Replays signal generation metrics on historical depth data from a specific date
"""

import os
import psycopg2
from datetime import datetime, timedelta
from collections import deque, defaultdict
import json
from dotenv import load_dotenv
import sys

# Import signal generator modules
from tracking import LevelTracker
from metrics import identify_key_levels, detect_absorptions, calculate_pressure

load_dotenv()

def get_db_connection():
    """Connect to TimescaleDB"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 6432)),
        database=os.getenv('DB_NAME', 'tradingdb'),
        user=os.getenv('DB_USER', 'tradinguser'),
        password=os.getenv('DB_PASSWORD')
    )

def get_snapshots_for_date(conn, target_date_str):
    """Get all unique timestamps (snapshots) for a specific date"""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT time
        FROM depth_levels_200
        WHERE time::date = %s::date
        ORDER BY time
    """, (target_date_str,))
    
    snapshots = [row[0] for row in cur.fetchall()]
    cur.close()
    return snapshots

def reconstruct_snapshot(conn, timestamp):
    """Reconstruct a depth snapshot from raw depth_levels_200 rows"""
    cur = conn.cursor()
    cur.execute("""
        SELECT level_num, side, price, quantity, orders
        FROM depth_levels_200
        WHERE time >= %s AND time < %s + INTERVAL '1 second'
          AND price > 0
        ORDER BY side DESC, level_num
    """, (timestamp, timestamp))
    
    rows = cur.fetchall()
    cur.close()
    
    bids = []
    asks = []
    
    for row in rows:
        level_data = {
            'level': row[0],
            'price': float(row[2]),
            'quantity': int(row[3]),
            'orders': int(row[4])
        }
        
        if row[1].lower() == 'bid':
            bids.append(level_data)
        else:
            asks.append(level_data)
    
    # Return as snapshot dict matching Redis format
    return {
        'timestamp': timestamp.isoformat(),
        'current_price': (bids[0]['price'] + asks[0]['price']) / 2 if bids and asks else 0,
        'bids': bids,
        'asks': asks
    }

def replay_signals(target_date_str, output_file=None):
    """Replay signal generation for a full day"""
    conn = get_db_connection()
    
    try:
        # Get all snapshots for the target date
        snapshots = get_snapshots_for_date(conn, target_date_str)
        
        if not snapshots:
            print(f"No data found for {target_date_str}")
            return
        
        print(f"\n{'='*100}")
        print(f"SIGNAL GENERATOR REPLAY - {target_date_str}")
        print(f"{'='*100}")
        print(f"Total snapshots: {len(snapshots)}")
        print(f"Time range: {snapshots[0].strftime('%Y-%m-%d %H:%M:%S UTC')} to {snapshots[-1].strftime('%H:%M:%S UTC')}")
        print(f"{'='*100}\n")
        
        # Initialize state
        level_tracker = LevelTracker()
        snapshot_buffer = deque(maxlen=600)
        calculation_interval = 10  # seconds
        last_calculation_time = None
        signals_output = []
        
        print("Processing snapshots (calculating every 10 seconds)...")
        
        # Process each snapshot
        for i, ts in enumerate(snapshots):
            # Reconstruct snapshot from DB
            snapshot = reconstruct_snapshot(conn, ts)
            snapshot_buffer.append(snapshot)
            
            # Calculate every 10 seconds (approximately)
            if last_calculation_time is None or (ts - last_calculation_time).total_seconds() >= calculation_interval:
                current_price = snapshot['current_price']
                
                # Run metrics
                key_levels = identify_key_levels(snapshot, level_tracker, current_price)
                absorptions = detect_absorptions(level_tracker, snapshot_buffer, current_price)
                pressure = calculate_pressure(snapshot_buffer, current_price)
                
                # Save signals to output (no console logging)
                signal_log = {
                    'timestamp': ts.isoformat(),
                    'current_price': current_price,
                    'key_levels_count': len(key_levels),
                    'key_levels': key_levels,
                    'absorptions_count': len(absorptions),
                    'absorptions': absorptions,
                    'pressure': pressure
                }
                signals_output.append(signal_log)
                
                last_calculation_time = ts
            
            # Progress indicator every 100 snapshots
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{len(snapshots)} snapshots processed...")
        
        # Save output
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(signals_output, f, indent=2, default=str)
            print(f"\n✓ Signals saved to {output_file}")
        
        # Summary statistics
        print(f"\n{'='*100}")
        print(f"REPLAY SUMMARY")
        print(f"{'='*100}")
        print(f"Total calculation cycles: {len(signals_output)}")
        print(f"Total key levels detected: {sum(s['key_levels_count'] for s in signals_output)}")
        print(f"Total absorptions detected: {sum(s['absorptions_count'] for s in signals_output)}")
        
        # Count pressure states
        pressure_states = defaultdict(int)
        for sig in signals_output:
            pressure_states[sig['pressure']['state']] += 1
        print(f"\nPressure distribution:")
        for state, count in sorted(pressure_states.items()):
            print(f"  {state}: {count} cycles")
        
        print(f"{'='*100}\n")
        
    finally:
        conn.close()

def main():
    """Entry point"""
    if len(sys.argv) < 2:
        # Default to today
        target_date = datetime.now().strftime('%Y-%m-%d')
    else:
        target_date = sys.argv[1]
    
    output_file = f'/opt/tradingapp/logs/signal-replay-{target_date}.json'
    
    print(f"Replaying signals for {target_date}...")
    replay_signals(target_date, output_file)

if __name__ == '__main__':
    main()
