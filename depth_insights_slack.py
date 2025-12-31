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
    
    # Extract strongest levels (lines starting with ⭐ ₹)
    strongest_levels = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Found a strongest level (must have both ⭐ and ₹ on same line)
        if line.startswith('⭐ ₹'):
            # Capture this level and all its details until next level
            level_lines = [line]
            i += 1
            while i < len(lines):
                next_line = lines[i]
                # Stop at: next starred level, non-starred level (starts with spaces+₹), separator, or KEY INSIGHTS
                if (next_line.startswith('⭐ ₹') or 
                    next_line.startswith('   ₹') or
                    next_line.startswith('⭐ =') or
                    next_line.startswith('---') or
                    'KEY INSIGHTS' in next_line):
                    break
                level_lines.append(next_line)
                i += 1
            strongest_levels.extend(level_lines)
            strongest_levels.append("")  # Add spacing between levels
        else:
            i += 1
    
    # Find KEY INSIGHTS section
    key_insights_start = None
    for i, line in enumerate(lines):
        if 'KEY INSIGHTS' in line and '=' in line:
            key_insights_start = i
            break
    
    if key_insights_start is None:
        return None
    
    # Find end of KEY INSIGHTS
    key_insights_end = len(lines)
    for i in range(key_insights_start + 1, len(lines)):
        if ('='*50 in lines[i] and 'Analysis complete' in lines[i+1] if i+1 < len(lines) else False):
            key_insights_end = i
            break
    
    # Combine: strongest levels + KEY INSIGHTS
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
