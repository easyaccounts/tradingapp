"""
Ingestion Service - Main Entry Point
Connects to KiteConnect or Dhan WebSocket and ingests real-time tick data

Environment Variables:
    DATA_SOURCE: "kite" (default) or "dhan"
    KITE_API_KEY: For Kite source
    DHAN_API_KEY: For Dhan source (legacy)
    DHAN_CLIENT_ID: For Dhan source (legacy)
"""

import sys
import os
import signal
import time
import asyncio
import logging
import structlog
import redis
from config import config
from publisher import RabbitMQPublisher
from enricher import load_instruments_cache, dhan_tick_to_enriched

# Conditional imports based on data source
DATA_SOURCE = os.getenv('DATA_SOURCE', 'kite').lower()

if DATA_SOURCE == 'dhan':
    from dhan_auth import get_dhan_credentials, get_websocket_url
    from dhan_websocket import DhanWebSocketClient
    logger_context = {"data_source": "dhan"}
else:
    from kite_auth import get_access_token, check_token_validity
    from kite_websocket import KiteWebSocketHandler
    logger_context = {"data_source": "kite"}

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, config.LOG_LEVEL.upper())),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False
)

logger = structlog.get_logger()

# Global handler for graceful shutdown
websocket_handler = None
publisher = None
dhan_websocket_client = None


def on_dhan_tick(tick_data: dict):
    """
    Callback for Dhan ticks
    Enriches and publishes to RabbitMQ
    """
    global publisher, instruments_cache
    
    try:
        from models import DhanTick
        
        # Parse tick into DhanTick model
        dhan_tick = DhanTick(**tick_data)
        
        # Enrich tick
        enriched = dhan_tick_to_enriched(dhan_tick, instruments_cache)
        
        if enriched:
            # Publish to RabbitMQ
            if publisher:
                publisher.publish(enriched.model_dump())
        else:
            # Log if enrichment failed (security_id not found)
            pass
    
    except Exception as e:
        logger.error("dhan_tick_processing_error", error=str(e))


def on_dhan_connect():
    """Callback for Dhan WebSocket connection"""
    logger.info("dhan_websocket_connected")


def on_dhan_error(error: Exception):
    """Callback for Dhan WebSocket error"""
    logger.error("dhan_websocket_error", error=str(error))


def on_dhan_close(code: int, reason: str):
    """Callback for Dhan WebSocket close"""
    logger.warning("dhan_websocket_closed", code=code, reason=reason)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global websocket_handler, dhan_websocket_client
    
    logger.info(
        "shutdown_signal_received",
        signal=signal.Signals(signum).name
    )
    
    # Stop WebSocket based on data source
    if DATA_SOURCE == 'dhan' and dhan_websocket_client:
        asyncio.run(dhan_websocket_client.disconnect())
    elif websocket_handler:
        websocket_handler.stop()
    
    # Close publisher
    if publisher:
        publisher.close()
    
    logger.info("ingestion_service_stopped")
    sys.exit(0)


def main():
    """Main entry point for ingestion service"""
    global websocket_handler, publisher, dhan_websocket_client, instruments_cache
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info(
        "ingestion_service_starting",
        data_source=DATA_SOURCE,
        config=str(config)
    )
    
    # Validate environment
    try:
        logger.info("validating_configuration")
        
        # Validate based on data source
        if DATA_SOURCE == 'dhan':
            # Dhan validation: check token file exists
            from dhan_auth import check_dhan_token_validity
            if not check_dhan_token_validity():
                logger.error(
                    "dhan_token_not_found",
                    message="Please authenticate via web interface and place dhan_token.json in /app/data/"
                )
                sys.exit(1)
            logger.info("dhan_token_valid")
        
        else:
            # Kite validation: check access token in Redis
            if not check_token_validity(config.REDIS_URL):
                domain = os.getenv("DOMAIN", "localhost")
                protocol = "https" if os.getenv("ENVIRONMENT") == "production" else "http"
                logger.error(
                    "access_token_not_found",
                    message=f"Please authenticate via web interface: {protocol}://{domain}/"
                )
                sys.exit(1)
            
            # Get access token
            logger.info("retrieving_access_token")
            access_token = get_access_token(config.REDIS_URL)
            logger.info("access_token_retrieved_successfully")
    
    except Exception as e:
        logger.error("initialization_failed", error=str(e))
        sys.exit(1)
    
    # Main loop - single attempt with fail-fast on errors
    # Docker restart policy will handle service restarts
    try:
        # Initialize Redis client for fallback
        logger.info("connecting_to_redis")
        redis_client = redis.from_url(config.REDIS_URL, decode_responses=False)
        redis_client.ping()
        logger.info("redis_connected")
        
        # Load instruments cache from database
        logger.info("loading_instruments_cache_from_database")
        instruments_cache = load_instruments_cache(config.DATABASE_URL, redis_client)
        
        if not instruments_cache:
            logger.warning(
                "instruments_cache_empty",
                message="Run scripts/update_instruments.py to populate cache"
            )
        else:
            logger.info(
                "instruments_cache_loaded",
                count=len(instruments_cache)
            )
        
        # Initialize RabbitMQ publisher
        logger.info("initializing_rabbitmq_publisher")
        publisher = RabbitMQPublisher(config.RABBITMQ_URL)
        logger.info("rabbitmq_publisher_ready")
        
        # Initialize WebSocket based on data source
        if DATA_SOURCE == 'dhan':
            # Build Dhan instruments list from cache
            dhan_instruments = []
            for token, info in instruments_cache.items():
                if info.security_id and info.source == 'dhan':
                    # Map exchange to Dhan exchange segment
                    exchange_map = {
                        'NSE': 0,  # NSE_EQ
                        'NFO': 1,  # NSE_FNO
                        'BSE': 3,  # BSE_EQ
                        'BFO': 4,  # BSE_FNO
                        'MCX': 6,  # MCX_COM
                        'CDS': 7,  # NSE_CURRENCY
                    }
                    exchange_segment = exchange_map.get(info.exchange, 1)  # Default to NFO
                    
                    dhan_instruments.append({
                        'security_id': info.security_id,
                        'exchange_segment': exchange_segment
                    })
            
            logger.info(
                "dhan_instruments_prepared",
                count=len(dhan_instruments),
                sample=dhan_instruments[:3] if dhan_instruments else []
            )
            
            if not dhan_instruments:
                logger.error("no_dhan_instruments_found", message="No active Dhan instruments in database")
                sys.exit(1)
            
            # Initialize Dhan WebSocket client
            logger.info("initializing_dhan_websocket_client")
            dhan_websocket_client = DhanWebSocketClient(
                on_tick=on_dhan_tick,
                on_connect=on_dhan_connect,
                on_error=on_dhan_error,
                on_close=on_dhan_close
            )
            
            logger.info("dhan_websocket_client_initialized")
            logger.info("starting_dhan_websocket_connection")
            
            # Start Dhan connection (async)
            async def run_dhan():
                # Start connection
                connect_task = asyncio.create_task(dhan_websocket_client.start())
                
                # Wait for connection
                await asyncio.sleep(2)
                
                # Subscribe to instruments
                if dhan_websocket_client.is_connected:
                    try:
                        await dhan_websocket_client.subscribe(
                            dhan_instruments,
                            mode='full'
                        )
                        logger.info("instruments_subscribed", count=len(dhan_instruments))
                    except Exception as e:
                        logger.error("subscription_failed", error=str(e))
                
                # Keep running
                await connect_task
            
            # Run async event loop
            asyncio.run(run_dhan())
        
        else:
            # Initialize Kite WebSocket handler (original code)
            logger.info(
                "initializing_websocket_handler",
                instruments=config.INSTRUMENTS
            )
            
            websocket_handler = KiteWebSocketHandler(
                api_key=config.KITE_API_KEY,
                access_token=access_token,
                instruments=config.INSTRUMENTS,
                publisher=publisher,
                instruments_cache=instruments_cache,
                slack_webhook_url=config.SLACK_WEBHOOK_URL
            )
            
            logger.info("websocket_handler_initialized")
            
            # Start WebSocket (blocking call)
            logger.info("starting_websocket_connection")
            websocket_handler.start()
            
            # If we reach here, connection closed normally
            logger.info("websocket_connection_closed_normally")
    
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_received")
    
    except Exception as e:
        logger.critical(
            "fatal_ingestion_error",
            error=str(e),
            message="Crashing to trigger Docker restart"
        )
        
        # Cleanup before crash
        if publisher:
            publisher.close()
        if 'redis_client' in locals():
            redis_client.close()
        
        sys.exit(1)
    
    finally:
        # Normal cleanup
        if publisher:
            publisher.close()
        if 'redis_client' in locals():
            redis_client.close()
    
    logger.info("ingestion_service_exiting")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical("fatal_error", error=str(e))
        sys.exit(1)
