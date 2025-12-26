"""
KiteConnect WebSocket Handler
Manages WebSocket connection to Kite and processes incoming ticks
"""

import time
import structlog
from typing import List, Dict
from kiteconnect import KiteTicker
from models import KiteTick, MarketDepth, MarketDepthItem
from validator import validate_tick
from enricher import enrich_tick, InstrumentInfo
from publisher import RabbitMQPublisher

logger = structlog.get_logger()


class KiteWebSocketHandler:
    """
    Handles KiteConnect WebSocket connection and tick processing
    """
    
    MODE_LTP = "ltp"
    MODE_QUOTE = "quote"
    MODE_FULL = "full"
    
    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments: List[int],
        publisher: RabbitMQPublisher,
        instruments_cache: Dict[int, InstrumentInfo]
    ):
        """
        Initialize WebSocket handler
        
        Args:
            api_key: Kite API key
            access_token: Access token from Redis
            instruments: List of instrument tokens to subscribe
            publisher: RabbitMQ publisher instance
            instruments_cache: Dictionary of instrument metadata
        """
        self.api_key = api_key
        self.access_token = access_token
        self.instruments = instruments
        self.publisher = publisher
        self.instruments_cache = instruments_cache
        
        # Statistics
        self.tick_count = 0
        self.valid_tick_count = 0
        self.invalid_tick_count = 0
        self.published_count = 0
        self.start_time = time.time()
        
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
                
                # Publish to RabbitMQ
                success = self.publisher.publish(enriched_tick.to_dict())
                
                if success:
                    self.published_count += 1
                else:
                    logger.warning(
                        "publish_failed",
                        instrument_token=tick.instrument_token
                    )
            
            except Exception as e:
                logger.error(
                    "tick_processing_failed",
                    error=str(e),
                    instrument_token=raw_tick_data.get('instrument_token')
                )
                self.invalid_tick_count += 1
        
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
        
        self._log_statistics()
    
    def on_error(self, ws, code, reason):
        """Callback when WebSocket error occurs"""
        logger.error(
            "websocket_error",
            code=code,
            reason=reason
        )
    
    def on_reconnect(self, ws, attempts_count):
        """Callback when WebSocket attempts to reconnect"""
        self.reconnect_attempts = attempts_count
        
        logger.info(
            "websocket_reconnecting",
            attempt=attempts_count
        )
    
    def on_noreconnect(self, ws):
        """Callback when WebSocket gives up reconnecting"""
        logger.error(
            "websocket_reconnect_failed",
            message="Max reconnection attempts reached"
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
            elapsed_seconds=round(elapsed_time, 2),
            ticks_per_second=round(tps, 2)
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
            if self.kws:
                self.kws.close()
            
            self._log_statistics()
            
            logger.info("websocket_stopped")
        
        except Exception as e:
            logger.error("websocket_stop_error", error=str(e))
