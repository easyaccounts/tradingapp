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
from dotenv import load_dotenv

load_dotenv()

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL')
SCRIPT_PATH = '/opt/tradingapp/analyze_depth_insights.py'

def extract_key_insights(output: str) -> str:
    """Extract strongest levels (⭐ ₹) + full KEY INSIGHTS section"""
    lines = output.split('\n')
    
    # Find KEY INSIGHTS section first
    key_insights_start = None
    key_insights_end = None
    for i, line in enumerate(lines):
        if 'KEY INSIGHTS' in line and '=' in line:
            key_insights_start = i
            # Find end of KEY INSIGHTS
            for j in range(i + 1, len(lines)):
                if '='*50 in lines[j]:
                    key_insights_end = j
                    break
            break
    
    if key_insights_start is None:
        return None
    
    # Find last occurrence of separator before KEY INSIGHTS
    # This separator comes right before the actual strongest levels list
    separator_idx = None
    for i in range(key_insights_start - 1, -1, -1):
        if lines[i].startswith('----') and len(lines[i]) > 50:
            separator_idx = i
            break
    
    if separator_idx is None:
        # Fallback: just return KEY INSIGHTS
        return '\n'.join(lines[key_insights_start:key_insights_end]).strip()
    
    # Extract ONLY starred levels AFTER separator and BEFORE first non-starred level
    strongest_levels = []
    i = separator_idx + 1
    
    while i < key_insights_start:
        line = lines[i]
        
        # Stop at first non-starred level (starts with spaces + ₹)
        if line.startswith('   ₹'):
            break
        
        # Found a strongest level (⭐ ₹)
        if line.startswith('⭐ ₹'):
            level_lines = [line]
            i += 1
            # Capture detail lines (lines that don't start a new level)
            while i < key_insights_start:
                next_line = lines[i]
                # Stop at: any line starting with ⭐ or ₹ or blank line followed by ⭐/₹
                if (next_line.startswith('⭐') or 
                    next_line.startswith('   ₹')):
                    break
                level_lines.append(next_line)
                i += 1
            strongest_levels.extend(level_lines)
            strongest_levels.append("")  # Add spacing
        else:
            i += 1
    
    # Build result
    result = []
    if strongest_levels:
        result.append("🌟 STRONGEST PERSISTENT LEVELS")
        result.append("=" * 100)
        result.extend(strongest_levels)
        result.append("")
    
    result.extend(lines[key_insights_start:key_insights_end])
    
    return '\n'.join(result).strip()

def send_to_slack(message: str) -> bool:
    """Send insights to Slack"""
    if not SLACK_WEBHOOK:
        print("ERROR: SLACK_WEBHOOK_URL not set in .env")
        return False
    
    # Format for Slack - simple text payload (most compatible)
    ist_now = datetime.now().strftime('%H:%M:%S IST')
    
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
    now = datetime.now()
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
