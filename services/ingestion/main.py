"""
Ingestion Service - Main Entry Point
Connects to KiteConnect WebSocket and ingests real-time tick data
"""

import sys
import os
import signal
import time
import logging
import structlog
import redis
from config import config
from kite_auth import get_access_token, check_token_validity
from publisher import RabbitMQPublisher
from enricher import load_instruments_cache
from kite_websocket import KiteWebSocketHandler

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


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(
        "shutdown_signal_received",
        signal=signal.Signals(signum).name
    )
    
    # Stop WebSocket
    if websocket_handler:
        websocket_handler.stop()
    
    # Close publisher
    if publisher:
        publisher.close()
    
    logger.info("ingestion_service_stopped")
    sys.exit(0)


def main():
    """Main entry point for ingestion service"""
    global websocket_handler, publisher
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info(
        "ingestion_service_starting",
        config=str(config)
    )
    
    # Validate environment
    try:
        logger.info("validating_configuration")
        
        # Check if access token exists in Redis
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
    
    # Main loop with auto-restart
    restart_count = 0
    max_restarts = 10
    restart_delay = 10  # seconds
    
    while restart_count < max_restarts:
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
            
            # Initialize WebSocket handler
            logger.info(
                "initializing_websocket_handler",
                instruments=config.INSTRUMENTS
            )
            
            websocket_handler = KiteWebSocketHandler(
                api_key=config.KITE_API_KEY,
                access_token=access_token,
                instruments=config.INSTRUMENTS,
                publisher=publisher,
                instruments_cache=instruments_cache
            )
            
            logger.info("websocket_handler_initialized")
            
            # Start WebSocket (blocking call)
            logger.info("starting_websocket_connection")
            websocket_handler.start()
            
            # If we reach here, connection closed normally
            logger.info("websocket_connection_closed_normally")
            break
        
        except KeyboardInterrupt:
            logger.info("keyboard_interrupt_received")
            break
        
        except Exception as e:
            restart_count += 1
            logger.error(
                "ingestion_error",
                error=str(e),
                restart_count=restart_count,
                max_restarts=max_restarts
            )
            
            if restart_count < max_restarts:
                logger.info(
                    "restarting_ingestion_service",
                    delay_seconds=restart_delay
                )
                time.sleep(restart_delay)
                
                # Exponential backoff (max 60 seconds)
                restart_delay = min(restart_delay * 2, 60)
            else:
                logger.error(
                    "max_restarts_reached",
                    message="Giving up after multiple failures"
                )
                sys.exit(1)
        
        finally:
            # Cleanup
            if publisher:
                publisher.close()
            if redis_client:
                redis_client.close()
    
    logger.info("ingestion_service_exiting")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical("fatal_error", error=str(e))
        sys.exit(1)
