#!/usr/bin/env python3
"""
Wrapper for analyze_depth_insights.py to send KEY INSIGHTS to Slack
Runs every 5 minutes during market hours via PM2 cron
"""

import subprocess
import requests
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL')
SCRIPT_PATH = '/opt/tradingapp/analyze_depth_insights.py'

def extract_key_insights(output: str) -> str:
    """Extract strongest levels (⭐ ₹) + full KEY INSIGHTS section"""
    lines = output.split('\n')
    
    # Step 1: Find KEY INSIGHTS boundaries
    key_insights_start = None
    key_insights_end = None
    for i, line in enumerate(lines):
        if 'KEY INSIGHTS' in line and '=' in line:
            key_insights_start = i
            for j in range(i + 1, len(lines)):
                if '='*50 in lines[j]:
                    key_insights_end = j
                    break
            break
    
    if key_insights_start is None:
        print(f"DEBUG: KEY INSIGHTS not found")
        return None
    
    print(f"DEBUG: KEY INSIGHTS at lines {key_insights_start} to {key_insights_end}")
    
    print(f"DEBUG: KEY INSIGHTS at lines {key_insights_start} to {key_insights_end}")
    
    # Step 2: Find separator before KEY INSIGHTS (working backwards)
    separator_idx = None
    for i in range(key_insights_start - 1, -1, -1):
        if lines[i].startswith('----') and len(lines[i]) > 50:
            separator_idx = i
            break
    
    if separator_idx is None:
        print(f"DEBUG: No separator found, returning just KEY INSIGHTS")
        return '\n'.join(lines[key_insights_start:key_insights_end]).strip()
    
    print(f"DEBUG: Separator at line {separator_idx}, content: '{lines[separator_idx][:50]}...'")
    print(f"DEBUG: Next line after separator: '{lines[separator_idx + 1][:80]}...'")
    
    # Step 3: Collect ONLY starred levels (⭐ ₹)
    # Start after separator, skip legend/separator lines, stop at first non-starred level
    strongest_levels = []
    i = separator_idx + 1
    
    while i < key_insights_start:
        line = lines[i]
        
        # STOP at first non-starred level
        if line.startswith('   ₹'):
            break
        
        # Collect starred level
        if line.startswith('⭐ ₹'):
            level_block = [line]
            i += 1
            # Collect detail lines (indented, not starting new level)
            while i < key_insights_start:
                next_line = lines[i]
                # Stop at any new level
                if next_line.startswith('⭐ ₹') or next_line.startswith('   ₹'):
                    break
                level_block.append(next_line)
                i += 1
            strongest_levels.extend(level_block)
            strongest_levels.append("")  # blank line between levels
        else:
            # Skip legend, separator, empty lines
            i += 1
    
    # Step 4: Build final result
    result = []
    if strongest_levels:
        result.append("🌟 STRONGEST PERSISTENT LEVELS")
        result.append("=" * 100)
        result.extend(strongest_levels)
        print(f"DEBUG: Collected {len(strongest_levels)} lines of starred levels")
    else:
        print(f"DEBUG: No starred levels collected!")
    
    # Add KEY INSIGHTS
    result.extend(lines[key_insights_start:key_insights_end])
    
    final = '\n'.join(result).strip()
    print(f"DEBUG: Final result length: {len(final)} chars")
    return final

def send_to_slack(message: str) -> bool:
    """Send insights to Slack"""
    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK_URL not set in .env")
        return False
    
    # Format for Slack - simple text payload (most compatible)
    ist_now = datetime.now(ZoneInfo("Asia/Kolkata")).strftime('%H:%M:%S IST')
    
    # Truncate if too long (Slack has 3000 char limit for text blocks)
    if len(message) > 2900:
        message = message[:2900] + "\n... (truncated)"
    
    payload = {
        "text": f"📊 *Market Depth Insights - {ist_now}*\n\n{message}"
    }
    
    try:
        response = requests.post(
            SLACK_WEBHOOK,
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"✓ Sent insights to Slack at {ist_now}")
            return True
        else:
            print(f"ERROR: Slack webhook returned {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"ERROR sending to Slack: {e}")
        return False

def main():
    """Run analysis and send KEY INSIGHTS to Slack"""
    
    # Check if during market hours (9:15 AM - 3:30 PM IST)
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    hour, minute = now.hour, now.minute
    
    # Market hours: 9:15 to 15:30 (3:30 PM)
    if not ((hour == 9 and minute >= 15) or (10 <= hour < 15) or (hour == 15 and minute <= 30)):
        print(f"Outside market hours ({hour:02d}:{minute:02d}), skipping")
        return
    
    print(f"Running depth insights analysis at {now.strftime('%H:%M:%S')}...")
    
    # Run the analysis script
    try:
        result = subprocess.run(
            ['/opt/tradingapp/venv/bin/python', SCRIPT_PATH],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode != 0:
            print(f"ERROR: Script failed with code {result.returncode}")
            print(f"STDERR: {result.stderr}")
            return
        
        # Extract KEY INSIGHTS section
        insights = extract_key_insights(result.stdout)
        
        if not insights:
            print("WARNING: No KEY INSIGHTS section found in output")
            print("Output:", result.stdout[:500])  # First 500 chars for debug
            return
        
        # Send to Slack
        if send_to_slack(insights):
            print("✓ Successfully sent insights to Slack")
        else:
            print("✗ Failed to send to Slack")
            
    except subprocess.TimeoutExpired:
        print("ERROR: Script timed out after 60 seconds")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main()
