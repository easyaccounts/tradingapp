"""
Slack Alerting Module
Formats and sends trading signals to Slack
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List


class SlackAlerter:
    """Handles Slack webhook alerts with deduplication"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.last_alerts = {}  # {signal_type: timestamp}
        self.cooldown_minutes = 5
    
    def should_send_alert(self, signal_type: str, data: dict) -> bool:
        """
        Determine if alert should be sent based on:
        - Cooldown period (5 minutes between similar alerts)
        - Signal confidence/strength
        """
        # Check cooldown
        if signal_type in self.last_alerts:
            last_time = self.last_alerts[signal_type]
            if datetime.now() - last_time < timedelta(minutes=self.cooldown_minutes):
                return False
        
        # Apply confidence filters
        if signal_type == 'key_level':
            # Only alert on strong levels (3x+ average)
            if data.get('strength', 0) < 3.0:
                return False
            # Level must be at least 10 seconds old (proven)
            if data.get('age_seconds', 0) < 10:
                return False
        
        elif signal_type == 'absorption':
            # Only alert on significant absorptions (70%+ reduction)
            if data.get('reduction_pct', 0) < 70:
                return False
            # Must have price action confirmation
            if not data.get('price_broke', False):
                return False
        
        elif signal_type == 'pressure_change':
            # Only alert on strong pressure (0.4+ imbalance)
            primary = data.get('pressure_60s', data.get('60s', 0))
            if abs(primary) < 0.4:
                return False
        
        return True
    
    def send_alert(self, signal_type: str, data: dict) -> bool:
        """
        Send alert to Slack if conditions are met
        Returns True if sent, False if filtered
        """
        if not self.should_send_alert(signal_type, data):
            return False
        
        message = self.format_message(signal_type, data)
        
        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                timeout=5
            )
            
            if response.status_code == 200:
                self.last_alerts[signal_type] = datetime.now()
                return True
            else:
                print(f"Slack webhook failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"Error sending Slack alert: {e}")
            return False
    
    def format_message(self, signal_type: str, data: dict) -> dict:
        """Format signal data into Slack message blocks"""
        
        if signal_type == 'key_level':
            return self._format_key_level(data)
        elif signal_type == 'absorption':
            return self._format_absorption(data)
        elif signal_type == 'pressure_change':
            return self._format_pressure_change(data)
        else:
            return {"text": f"Unknown signal: {signal_type}"}
    
    def _format_key_level(self, data: dict) -> dict:
        """Format key level discovery alert"""
        side = data['side']
        emoji = "🟢" if side == 'support' else "🔴"
        
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} Strong {side.upper()} Detected"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Price:*\n₹{data['price']:.2f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Orders:*\n{data['orders']:,}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Strength:*\n{data['strength']:.1f}x avg"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Age:*\n{data['age_display']}"
                        }
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Distance from price: {data['distance']:.0f} points | Tests: {data['tests']}"
                        }
                    ]
                }
            ]
        }
    
    def _format_absorption(self, data: dict) -> dict:
        """Format absorption/breakout alert"""
        side = data['side']
        emoji = "⚡" if data['price_broke'] else "⚠️"
        status = "BREAKING THROUGH" if data['price_broke'] else "WEAKENING"
        
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} {side.upper()} {status}"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Level:*\n₹{data['price']:.2f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Current:*\n₹{data['current_price']:.2f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Reduction:*\n{data['reduction_pct']}%"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Before:*\n{data['orders_before']:,} → {data['orders_now']:,}"
                        }
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Status: {data['status'].upper()} | Started: {data['started_at']}"
                        }
                    ]
                }
            ]
        }
    
    def _format_pressure_change(self, data: dict) -> dict:
        """Format pressure shift alert"""
        state = data['state']
        
        if state == 'bullish':
            emoji = "🚀"
            color = "#28a745"
        elif state == 'bearish':
            emoji = "📉"
            color = "#dc3545"
        else:
            emoji = "⚖️"
            color = "#6c757d"
        
        return {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} Market Pressure: {state.upper()}"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*30s:*\n{data.get('pressure_30s', data.get('30s', 0)):+.3f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*60s:*\n{data.get('pressure_60s', data.get('60s', 0)):+.3f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*120s:*\n{data.get('pressure_120s', data.get('120s', 0)):+.3f}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Price:*\n₹{data['current_price']:.2f}"
                        }
                    ]
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "Positive = Buy pressure | Negative = Sell pressure"
                        }
                    ]
                }
            ]
        }
    
    def send_startup_message(self) -> bool:
        """Send startup notification"""
        message = {
            "text": "🟢 Signal Generator Started",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🟢 Signal Generator Online"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "Monitoring NIFTY 200-level depth for trading signals"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}"
                        }
                    ]
                }
            ]
        }
        
        try:
            response = requests.post(self.webhook_url, json=message, timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"Error sending startup message: {e}")
            return False
    
    def send_shutdown_message(self) -> bool:
        """Send shutdown notification"""
        message = {
            "text": "🔴 Signal Generator Stopped",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🔴 Signal Generator Offline"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Stopped at {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}"
                        }
                    ]
                }
            ]
        }
        
        try:
            response = requests.post(self.webhook_url, json=message, timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"Error sending shutdown message: {e}")
            return False
