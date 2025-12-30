"""
Signal Generator Service
Subscribes to depth snapshots, calculates trading signals, publishes to Slack + Redis + DB
"""

import os
import json
import signal
import sys
from collections import deque
from datetime import datetime
import time
import redis
import psycopg2
from psycopg2.extras import execute_values
import pytz

from tracking import LevelTracker
from metrics import identify_key_levels, detect_absorptions, calculate_pressure
from slack_alerts import SlackAlerter


class SignalGenerator:
    """Main service class"""
    
    def __init__(self):
        # Configuration
        self.redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
        self.db_config = {
            'host': os.getenv('DB_HOST', 'timescaledb'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'tradingdb'),
            'user': os.getenv('DB_USER', 'tradinguser'),
            'password': os.getenv('DB_PASSWORD', 'tradingpass')
        }
        self.security_id = int(os.getenv('SECURITY_ID', 49543))
        self.slack_webhook = os.getenv('SLACK_WEBHOOK_URL')
        
        # State
        self.snapshot_buffer = deque(maxlen=600)  # 60 seconds at 5 snapshots/sec
        self.level_tracker = LevelTracker()
        self.last_calculation_time = time.time()
        self.calculation_interval = 10  # Calculate every 10 seconds
        self.running = False
        self.last_market_state = 'neutral'
        
        # Connections
        self.redis_client = None
        self.db_conn = None
        self.db_cursor = None
        self.pubsub = None
        self.slack = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.running = False
    
    def connect(self):
        """Establish connections to Redis and Database"""
        print("Connecting to Redis...")
        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        
        print("Connecting to TimescaleDB...")
        self.db_conn = psycopg2.connect(**self.db_config)
        self.db_cursor = self.db_conn.cursor()
        
        print("Initializing Slack alerter...")
        if self.slack_webhook:
            self.slack = SlackAlerter(self.slack_webhook)
            self.slack.send_startup_message()
        else:
            print("WARNING: SLACK_WEBHOOK_URL not set, alerts disabled")
            self.slack = None
        
        print("✓ All connections established")
    
    def subscribe(self):
        """Subscribe to depth snapshots channel"""
        channel = 'depth_snapshots:NIFTY'
        print(f"Subscribing to {channel}...")
        self.pubsub.subscribe(channel)
        print(f"✓ Subscribed to {channel}")
    
    def run(self):
        """Main service loop"""
        self.running = True
        print("\n🚀 Signal Generator running...\n")
        
        try:
            for message in self.pubsub.listen():
                if not self.running:
                    break
                
                if message['type'] != 'message':
                    continue
                
                try:
                    # Parse snapshot and ensure numeric types
                    snapshot = json.loads(message['data'])
                    
                    # Convert string numbers to proper types (Redis sometimes returns strings)
                    snapshot['current_price'] = float(snapshot['current_price'])
                    for bid in snapshot['bids']:
                        bid['price'] = float(bid['price'])
                        bid['quantity'] = int(bid['quantity'])
                        bid['orders'] = int(bid['orders'])
                    for ask in snapshot['asks']:
                        ask['price'] = float(ask['price'])
                        ask['quantity'] = int(ask['quantity'])
                        ask['orders'] = int(ask['orders'])
                    
                    self.process_snapshot(snapshot)
                    
                    # Calculate metrics every 10 seconds
                    current_time = time.time()
                    if current_time - self.last_calculation_time >= self.calculation_interval:
                        self.calculate_and_publish()
                        self.last_calculation_time = current_time
                        
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue
        
        except KeyboardInterrupt:
            print("\nInterrupted by user")
        finally:
            self.shutdown()
    
    def process_snapshot(self, snapshot: dict):
        """Add snapshot to rolling buffer"""
        self.snapshot_buffer.append(snapshot)
    
    def calculate_and_publish(self):
        """Calculate all metrics and publish results"""
        if len(self.snapshot_buffer) < 30:  # Need at least 6 seconds of data
            print("Insufficient data, waiting...")
            return
        
        # Get current state
        current_snapshot = self.snapshot_buffer[-1]
        current_price = current_snapshot['current_price']
        timestamp = datetime.fromisoformat(current_snapshot['timestamp'])
        
        print(f"\n[{timestamp.strftime('%H:%M:%S')}] Calculating metrics at ₹{current_price:.2f}...")
        
        # Calculate 3 metrics
        key_levels = identify_key_levels(current_snapshot, self.level_tracker, current_price)
        absorptions = detect_absorptions(self.level_tracker, self.snapshot_buffer, current_price)
        pressure = calculate_pressure(self.snapshot_buffer, current_price)
        
        print(f"  Key levels: {len(key_levels)} | Absorptions: {len(absorptions)} | Pressure: {pressure['state']}")
        
        # Publish to Redis (real-time state)
        self.publish_to_redis(current_price, key_levels, absorptions, pressure)
        
        # Save to Database (historical)
        self.save_to_database(timestamp, current_price, key_levels, absorptions, pressure)
        
        # Send Slack alerts (filtered)
        self.check_and_send_alerts(current_price, key_levels, absorptions, pressure)
    
    def publish_to_redis(self, current_price: float, key_levels: list, absorptions: list, pressure: dict):
        """Publish current state to Redis for real-time access"""
        state = {
            'timestamp': datetime.now(pytz.UTC).isoformat(),
            'current_price': current_price,
            'key_levels': key_levels,
            'absorptions': absorptions,
            'pressure': pressure,
            'market_state': pressure['state']
        }
        
        # Store in Redis with 60 second expiry
        key = f'signal_state:NIFTY:{self.security_id}'
        self.redis_client.setex(key, 60, json.dumps(state, default=str))
    
    def save_to_database(self, timestamp: datetime, current_price: float, 
                        key_levels: list, absorptions: list, pressure: dict):
        """Save calculated signals to TimescaleDB"""
        try:
            query = """
            INSERT INTO depth_signals 
            (time, security_id, current_price, key_levels, absorptions, 
             pressure_30s, pressure_60s, pressure_120s, market_state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            self.db_cursor.execute(query, (
                timestamp,
                self.security_id,
                current_price,
                json.dumps(key_levels),
                json.dumps(absorptions),
                pressure['30s'],
                pressure['60s'],
                pressure['120s'],
                pressure['state']
            ))
            
            self.db_conn.commit()
            
        except Exception as e:
            print(f"Error saving to database: {e}")
            self.db_conn.rollback()
    
    def check_and_send_alerts(self, current_price: float, key_levels: list, 
                              absorptions: list, pressure: dict):
        """Check conditions and send Slack alerts if warranted"""
        if not self.slack:
            return
        
        # Alert on new strong key levels
        for level in key_levels:
            if level['strength'] >= 3.0 and level['age_seconds'] >= 10:
                data = {**level, 'current_price': current_price}
                sent = self.slack.send_alert('key_level', data)
                if sent:
                    print(f"  📢 Slack: Strong {level['side']} at ₹{level['price']:.2f}")
        
        # Alert on absorptions/breakouts
        for absorption in absorptions:
            if absorption['reduction_pct'] >= 70 and absorption['price_broke']:
                sent = self.slack.send_alert('absorption', absorption)
                if sent:
                    print(f"  📢 Slack: {absorption['side'].upper()} breaking at ₹{absorption['price']:.2f}")
        
        # Alert on significant pressure changes
        if pressure['state'] != self.last_market_state:
            if abs(pressure['60s']) >= 0.4:  # Strong pressure
                data = {**pressure, 'current_price': current_price}
                sent = self.slack.send_alert('pressure_change', data)
                if sent:
                    print(f"  📢 Slack: Pressure shift to {pressure['state'].upper()}")
            
            self.last_market_state = pressure['state']
    
    def shutdown(self):
        """Clean shutdown"""
        print("\n🛑 Shutting down Signal Generator...")
        
        if self.slack:
            self.slack.send_shutdown_message()
        
        if self.pubsub:
            self.pubsub.unsubscribe()
            self.pubsub.close()
        
        if self.redis_client:
            self.redis_client.close()
        
        if self.db_cursor:
            self.db_cursor.close()
        
        if self.db_conn:
            self.db_conn.close()
        
        print("✓ Shutdown complete")


def main():
    """Entry point"""
    print("=" * 60)
    print("NIFTY Signal Generator Service")
    print("=" * 60)
    
    generator = SignalGenerator()
    
    try:
        generator.connect()
        generator.subscribe()
        generator.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
