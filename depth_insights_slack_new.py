#!/usr/bin/env python3
"""
Wrapper for analyze_depth_insights.py to send KEY INSIGHTS to Slack
Runs every 5 minutes during market hours via PM2 cron
"""

import sys
import os
sys.path.insert(0, '/opt/tradingapp')

from analyze_depth_insights import get_analysis_data, get_volume_at_price, get_db_connection
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK_URL')
NIFTY_JAN_FUT_TOKEN = 12602626  # Kite instrument token

def format_level_for_slack(level, current_price):
    """Format a single level with all details"""
    side_label = 'SUPPORT' if level['side'] == 'bid' else 'RESISTANCE'
    
    # Calculate signal
    history = level.get('history', [])
    if len(history) >= 3:
        early_avg = sum(h['orders'] for h in history[:len(history)//3]) / (len(history)//3)
        late_avg = sum(h['orders'] for h in history[-len(history)//3:]) / (len(history)//3)
        change_pct = ((late_avg - early_avg) / early_avg * 100) if early_avg > 0 else 0
        
        if change_pct < -40:
            signal = "⚠️  ABSORPTION (weakening)"
        elif change_pct > 40:
            signal = "📈 ACCUMULATION (strengthening)"
        else:
            signal = "━  STABLE"
    else:
        signal = "━  STABLE"
    
    first_seen = history[0]['time'].strftime('%H:%M:%S') if history else ''
    last_seen = history[-1]['time'].strftime('%H:%M:%S') if history else ''
    
    lines = []
    lines.append(f"⭐ ₹{level['price']:>10.2f} {side_label:>10} | Peak: {level['max_orders']:>2} orders | "
                 f"Avg: {level['avg_orders']:>4.1f} | Seen: {level['appearances']:>3}x | "
                 f"{first_seen} → {last_seen}")
    lines.append(f"  Qty: Peak {level['max_quantity']:,} | Avg {level['avg_quantity']:,.0f}")
    lines.append(f"  {signal}")
    
    # Get volume data
    conn = get_db_connection()
    try:
        vol_data = get_volume_at_price(conn, level['price'], NIFTY_JAN_FUT_TOKEN)
        if vol_data['tested']:
            if vol_data['total_volume'] >= 1000:
                delta_direction = "🟢" if vol_data['net_delta'] > 0 else "🔴" if vol_data['net_delta'] < 0 else "⚪"
                validation = "✅ TESTED" if vol_data['total_volume'] >= 5000 else "⚠️  Lightly tested"
                lines.append(f"  📊 TRADED: {vol_data['trades']} trades | {vol_data['total_volume']:,} contracts")
                lines.append(f"     {delta_direction} Buy: {vol_data['buy_volume']:,} ({vol_data['buy_pct']:.0f}%) | "
                           f"Sell: {vol_data['sell_volume']:,} ({vol_data['sell_pct']:.0f}%) | "
                           f"Delta: {vol_data['net_delta']:+,} {validation}")
    finally:
        conn.close()
    
    # Distance from current price
    if current_price:
        distance = level['price'] - current_price
        if abs(distance) < 50:
            position = "ABOVE" if distance > 0 else "BELOW"
            lines.append(f"  📍 {abs(distance):.2f} points {position} current price")
    
    # Timeline
    if len(history) > 0:
        timeline_points = history[::max(1, len(history)//12)][:12]  # Max 12 points
        timeline = " ".join([f"{h['time'].strftime('%H:%M')}({h['orders']})" for h in timeline_points])
        lines.append(f"  Timeline: {timeline}")
    
    return '\n'.join(lines)

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
    
    try:
        # Get analysis data directly (no subprocess)
        analysis = get_analysis_data()
        
        if not analysis:
            print("WARNING: No analysis data available")
            return
        
        # Build Slack message
        message_parts = []
        
        # Header with strongest levels
        message_parts.append("🌟 STRONGEST PERSISTENT LEVELS")
        message_parts.append("=" * 100)
        
        for level in analysis['strongest_levels']:
            formatted = format_level_for_slack(level, analysis['current_price'])
            message_parts.append(formatted)
            message_parts.append("")  # blank line
        
        # Add KEY INSIGHTS section
        message_parts.append(analysis['key_insights_text'])
        
        message = '\n'.join(message_parts)
        
        # Send to Slack
        if send_to_slack(message):
            print("✓ Successfully sent insights to Slack")
        else:
            print("✗ Failed to send to Slack")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
