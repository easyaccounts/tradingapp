#!/usr/bin/env python3
"""
Market Hours Control - Start/Stop PM2 services based on market hours
Run via PM2 cron: 0 3,10 * * 1-5 (9:00 AM and 4:00 PM IST)
"""

import subprocess
import sys
from datetime import datetime

# Services that should only run during market hours
MARKET_HOUR_SERVICES = [
    'ingestion',
    'worker-1', 
    'worker-2',
    'worker-3',
    'depth-collector',
    'signal-generator'
]

def run_pm2_command(action: str, services: list) -> bool:
    """Run pm2 start/stop for given services"""
    try:
        for service in services:
            result = subprocess.run(
                ['pm2', action, service],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                print(f"✓ {action.upper()} {service}")
            else:
                print(f"✗ Failed to {action} {service}: {result.stderr}")
        
        # Save PM2 state
        subprocess.run(['pm2', 'save'], capture_output=True)
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    now = datetime.now()
    hour = now.hour  # VPS is UTC+1, so 9 AM IST = 4:30 UTC = 5:30 CET (approx)
    
    # Determine action based on current time
    # 9:00 AM IST = 03:30 UTC = 04:30 CET (winter) / 05:30 CEST (summer)
    # 4:00 PM IST = 10:30 UTC = 11:30 CET (winter) / 12:30 CEST (summer)
    
    # IST is UTC+5:30, VPS is CET (UTC+1)
    # So 9:00 IST = 3:30 UTC = 4:30 CET
    # And 16:00 IST = 10:30 UTC = 11:30 CET
    
    # This script should be called with argument: start or stop
    if len(sys.argv) < 2:
        print("Usage: market_hours_control.py [start|stop]")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    
    if action == 'start':
        print(f"\n{'='*60}")
        print(f"STARTING MARKET HOUR SERVICES - {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        run_pm2_command('start', MARKET_HOUR_SERVICES)
        print(f"\n✓ All market hour services started\n")
        
    elif action == 'stop':
        print(f"\n{'='*60}")
        print(f"STOPPING MARKET HOUR SERVICES - {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")
        run_pm2_command('stop', MARKET_HOUR_SERVICES)
        print(f"\n✓ All market hour services stopped\n")
        
    else:
        print(f"Invalid action: {action}. Use 'start' or 'stop'")
        sys.exit(1)

if __name__ == "__main__":
    main()
