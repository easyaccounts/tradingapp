"""
KiteConnect WebSocket Handler
Manages WebSocket connection to Kite and processes incoming ticks
"""

import sys
import time
import os
import structlog
import requests
from typing import List, Dict
from kiteconnect import KiteTicker
from models import KiteTick, MarketDepth, MarketDepthItem
from validator import validate_tick
from enricher import enrich_tick, InstrumentInfo
from publisher import RabbitMQPublisher

logger = structlog.get_logger()


def send_slack_notification(webhook_url: str, message: str, emoji: str = "🚨") -> bool:
    """Send simple text notification to Slack"""
    if not webhook_url:
        return False
    
    try:
        payload = {
            "text": f"{emoji} {message}",
            "username": "Trading App Ingestion"
        }
        response = requests.post(webhook_url, json=payload, timeout=5)
        return response.status_code == 200
    except Exception as e:
        logger.warning("slack_notification_failed", error=str(e))
        return False


class KiteWebSocketHandler:
    """
    Handles KiteConnect WebSocket connection and tick processing
    """
    
    MODE_LTP = "ltp"
    MODE_QUOTE = "quote"
    MODE_FULL = "full"
    
    # Batch publishing configuration from environment
    BATCH_SIZE = int(os.getenv("INGESTION_BATCH_SIZE", 500))  # Number of ticks per batch
    BATCH_TIMEOUT = float(os.getenv("INGESTION_BATCH_TIMEOUT", 0.5))  # Seconds before forcing flush
    
    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments: List[int],
        publisher: RabbitMQPublisher,
        instruments_cache: Dict[int, InstrumentInfo],
        slack_webhook_url: str = ""
    ):
        """
        Initialize WebSocket handler
        
        Args:
            api_key: Kite API key
            access_token: Access token from Redis
            instruments: List of instrument tokens to subscribe
            publisher: RabbitMQ publisher instance
            instruments_cache: Dictionary of instrument metadata
            slack_webhook_url: Optional Slack webhook for error notifications
        """
        self.api_key = api_key
        self.access_token = access_token
        self.instruments = instruments
        self.publisher = publisher
        self.instruments_cache = instruments_cache
        self.slack_webhook_url = slack_webhook_url
        
        # Statistics
        self.tick_count = 0
        self.valid_tick_count = 0
        self.invalid_tick_count = 0
        self.published_count = 0
        self.start_time = time.time()
        
        # Batch publishing
        self.tick_buffer = []
        self.last_publish_time = time.time()
        self.batch_publish_count = 0
        
        # Reconnection settings
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # Initial delay in seconds
        self.max_reconnect_delay = 60
        
        # Initialize KiteTicker
        self.kws = None
        self._init_ticker()
        
        logger.info(
            "websocket_handler_initialized",
            instruments=len(self.instruments),
            tokens=self.instruments
        )
    
    def _init_ticker(self):
        """Initialize KiteTicker with callbacks"""
        self.kws = KiteTicker(self.api_key, self.access_token)
        
        # Register callbacks
        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error
        self.kws.on_reconnect = self.on_reconnect
        self.kws.on_noreconnect = self.on_noreconnect
        
        logger.info("kite_ticker_initialized")
    
    def on_connect(self, ws, response):
        """
        Callback when WebSocket connection is established
        Subscribe to instruments in FULL mode for market depth
        """
        logger.info(
            "websocket_connected",
            response=response,
            instruments_count=len(self.instruments)
        )
        
        try:
            # Subscribe to instruments in FULL mode to get market depth
            ws.subscribe(self.instruments)
            ws.set_mode(ws.MODE_FULL, self.instruments)
            
            logger.info(
                "instruments_subscribed",
                mode="FULL",
                instruments=self.instruments
            )
            
            # Reset reconnection counter on successful connection
            self.reconnect_attempts = 0
        
        except Exception as e:
            logger.error("subscription_failed", error=str(e))
    
    def on_ticks(self, ws, ticks):
        """
        Callback when ticks are received
        
        Process each tick:
        1. Parse into KiteTick model
        2. Validate data quality
        3. Enrich with metadata and derived metrics
        4. Publish to RabbitMQ
        """
        self.tick_count += len(ticks)
        
        for raw_tick_data in ticks:
            try:
                # Parse depth data if present
                depth = None
                if 'depth' in raw_tick_data and raw_tick_data['depth']:
                    depth_data = raw_tick_data['depth']
                    
                    # Parse buy side
                    buy_depth = [
                        MarketDepthItem(
                            quantity=item.get('quantity', 0),
                            price=item.get('price', 0.0),
                            orders=item.get('orders', 0)
                        )
                        for item in depth_data.get('buy', [])
                    ]
                    
                    # Parse sell side
                    sell_depth = [
                        MarketDepthItem(
                            quantity=item.get('quantity', 0),
                            price=item.get('price', 0.0),
                            orders=item.get('orders', 0)
                        )
                        for item in depth_data.get('sell', [])
                    ]
                    
                    depth = MarketDepth(buy=buy_depth, sell=sell_depth)
                
                # Create KiteTick model
                # Field names match exactly what KiteTicker library returns
                tick = KiteTick(
                    tradable=raw_tick_data.get('tradable', True),
                    mode=raw_tick_data.get('mode', 'quote'),
                    instrument_token=raw_tick_data['instrument_token'],
                    last_price=raw_tick_data.get('last_price'),
                    last_traded_quantity=raw_tick_data.get('last_traded_quantity'),
                    average_traded_price=raw_tick_data.get('average_traded_price'),
                    volume_traded=raw_tick_data.get('volume_traded'),
                    total_buy_quantity=raw_tick_data.get('total_buy_quantity'),
                    total_sell_quantity=raw_tick_data.get('total_sell_quantity'),
                    ohlc=raw_tick_data.get('ohlc'),
                    change=raw_tick_data.get('change'),
                    last_trade_time=raw_tick_data.get('last_trade_time'),
                    timestamp=raw_tick_data.get('timestamp') or raw_tick_data.get('exchange_timestamp'),
                    oi=raw_tick_data.get('oi'),
                    oi_day_high=raw_tick_data.get('oi_day_high'),
                    oi_day_low=raw_tick_data.get('oi_day_low'),
                    depth=depth
                )
                
                # Validate tick
                if not validate_tick(tick):
                    self.invalid_tick_count += 1
                    continue
                
                self.valid_tick_count += 1
                
                # Enrich tick with metadata and derived metrics
                enriched_tick = enrich_tick(tick, self.instruments_cache)
                
                # Add to batch buffer
                self.tick_buffer.append(enriched_tick.to_dict())
            
            except Exception as e:
                logger.error(
                    "tick_processing_failed",
                    error=str(e),
                    instrument_token=raw_tick_data.get('instrument_token')
                )
                self.invalid_tick_count += 1
        
        # Check if we should flush batch
        should_flush = (
            len(self.tick_buffer) >= self.BATCH_SIZE or
            (time.time() - self.last_publish_time) >= self.BATCH_TIMEOUT
        )
        
        if should_flush and self.tick_buffer:
            self._flush_tick_buffer()
        
        # Log statistics every 100 ticks
        if self.tick_count % 100 == 0:
            self._log_statistics()
    
    def on_close(self, ws, code, reason):
        """Callback when WebSocket connection is closed"""
        logger.warning(
            "websocket_closed",
            code=code,
            reason=reason
        )
        
        # Flush remaining ticks before closing
        if self.tick_buffer:
            logger.info("flushing_remaining_ticks_on_close", count=len(self.tick_buffer))
            self._flush_tick_buffer()
        
        self._log_statistics()
    
    def on_error(self, ws, code, reason):
        """Callback when WebSocket error occurs"""
        logger.error(
            "websocket_error",
            code=code,
            reason=reason
        )
        
        # Crash on 403 Forbidden (authentication error)
        # This typically means token is invalid/expired
        # Wait 5 minutes before crashing to allow for temporary issues
        if code == 403:
            logger.critical(
                "authentication_error_403",
                message="Access token invalid or expired. Waiting 5 minutes before crashing to trigger restart with fresh token.",
                backoff_seconds=300
            )
            
            # Send Slack notification immediately so user can refresh token
            domain = os.getenv("DOMAIN", "localhost")
            protocol = "https" if os.getenv("ENVIRONMENT") == "production" else "http"
            login_url = f"{protocol}://{domain}/"
            
            slack_message = (
                "*Kite Authentication Error (403)*\n\n"
                f"The access token is invalid or expired. Please login to refresh:\n"
                f"{login_url}\n\n"
                f"Service will crash and restart in 5 minutes if token is not refreshed."
            )
            
            send_slack_notification(self.slack_webhook_url, slack_message, "🚨")
            
            time.sleep(300)  # 5-minute backoff
            logger.critical("crashing_due_to_403_error")
            sys.exit(1)
    
    def on_reconnect(self, ws, attempts_count):
        """Callback when WebSocket attempts to reconnect"""
        self.reconnect_attempts = attempts_count
        
        logger.info(
            "websocket_reconnecting",
            attempt=attempts_count
        )
    
    def on_noreconnect(self, ws):
        """Callback when WebSocket gives up reconnecting"""
        logger.critical(
            "websocket_reconnect_failed",
            message="Max reconnection attempts reached. Crashing to trigger Docker restart.",
            max_attempts=self.max_reconnect_attempts
        )
        
        # Flush remaining ticks before crashing
        if self.tick_buffer:
            logger.info("flushing_remaining_ticks_before_crash", count=len(self.tick_buffer))
            self._flush_tick_buffer()
        
        sys.exit(1)
    
    def _flush_tick_buffer(self):
        """Flush buffered ticks to RabbitMQ"""
        if not self.tick_buffer:
            return
        
        buffer_to_publish = self.tick_buffer.copy()
        batch_size = len(buffer_to_publish)
        
        try:
            # Publish batch to RabbitMQ
            success_count = self.publisher.publish_batch(buffer_to_publish)
            
            if success_count > 0:
                self.published_count += success_count
                self.batch_publish_count += 1
                self.tick_buffer = []
                self.last_publish_time = time.time()
                
                logger.debug(
                    "batch_published",
                    batch_size=batch_size,
                    successful=success_count,
                    total_batches=self.batch_publish_count
                )
            else:
                logger.warning(
                    "batch_publish_failed",
                    batch_size=batch_size
                )
        
        except Exception as e:
            logger.error(
                "batch_publish_error",
                error=str(e),
                batch_size=batch_size
            )
    
    def _log_statistics(self):
        """Log ingestion statistics"""
        elapsed_time = time.time() - self.start_time
        tps = self.tick_count / elapsed_time if elapsed_time > 0 else 0
        
        logger.info(
            "ingestion_statistics",
            total_ticks=self.tick_count,
            valid_ticks=self.valid_tick_count,
            invalid_ticks=self.invalid_tick_count,
            published_ticks=self.published_count,
            batch_count=self.batch_publish_count,
            elapsed_seconds=round(elapsed_time, 2),
            ticks_per_second=round(tps, 2),
            buffered_ticks=len(self.tick_buffer)
        )
    
    def start(self):
        """
        Start WebSocket connection with auto-reconnect
        This is a blocking call
        """
        logger.info("starting_websocket_connection")
        
        try:
            # Start ticker (blocking)
            self.kws.connect(threaded=False)
        
        except KeyboardInterrupt:
            logger.info("websocket_interrupted_by_user")
            self.stop()
        
        except Exception as e:
            logger.error("websocket_start_failed", error=str(e))
            raise
    
    def stop(self):
        """Stop WebSocket connection gracefully"""
        logger.info("stopping_websocket_connection")
        
        try:
            # Flush remaining ticks
            if self.tick_buffer:
                logger.info("flushing_remaining_ticks_on_stop", count=len(self.tick_buffer))
                self._flush_tick_buffer()
            
            if self.kws:
                self.kws.close()
            
            self._log_statistics()
            
            logger.info("websocket_stopped")
        
        except Exception as e:
            logger.error("websocket_stop_error", error=str(e))
