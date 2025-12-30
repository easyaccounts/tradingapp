#!/usr/bin/env python3
"""
Ingestion Service - Main Entry Point
Connects to KiteConnect WebSocket and ingests real-time tick data
"""

import sys
import os
import signal
import time
import logging
import json
from datetime import datetime, time as dt_time
import pytz
import structlog
import redis
import threading
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
redis_health_client = None
health_update_thread = None
stop_health_updates = False


def is_market_hours():
    """Check if current time is within market hours (9:15 AM - 3:30 PM IST)"""
    ist = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist)
    
    # Check if it's a weekday (Monday=0, Sunday=6)
    if now_ist.weekday() >= 5:  # Saturday or Sunday
        return False
    
    market_start = dt_time(9, 15)
    market_end = dt_time(15, 30)
    current_time = now_ist.time()
    
    return market_start <= current_time <= market_end


def publish_health_status():
    """Background thread to publish health status to Redis every 15 seconds"""
    global stop_health_updates, websocket_handler, redis_health_client
    
    while not stop_health_updates:
        try:
            if websocket_handler and redis_health_client:
                # Calculate health status
                now = time.time()
                last_tick_age = (now - websocket_handler.last_tick_time) if websocket_handler.last_tick_time else None
                
                # Determine status
                if not websocket_handler.websocket_connected:
                    status = 'unhealthy'
                    reason = 'WebSocket disconnected'
                elif websocket_handler.error_403_count > 0:
                    status = 'unhealthy'
                    reason = f'Auth failed ({websocket_handler.error_403_count} x 403 errors)'
                elif websocket_handler.reconnect_attempts >= websocket_handler.max_reconnect_attempts:
                    status = 'unhealthy'
                    reason = 'Max reconnection attempts reached'
                elif last_tick_age and last_tick_age > 120 and is_market_hours():
                    # No ticks for 2 minutes during market hours
                    status = 'degraded'
                    reason = f'No ticks for {int(last_tick_age)}s during market hours'
                elif last_tick_age and last_tick_age > 300:
                    # No ticks for 5 minutes (even off hours, something might be wrong)
                    status = 'degraded'
                    reason = f'No ticks for {int(last_tick_age)}s'
                else:
                    status = 'healthy'
                    reason = 'WebSocket connected, receiving ticks'
                
                health_data = {\n                    'status': status,\n                    'reason': reason,\n                    'websocket_connected': websocket_handler.websocket_connected,\n                    'last_tick_time': websocket_handler.last_tick_time,\n                    'last_tick_age_seconds': int(last_tick_age) if last_tick_age else None,\n                    'tick_count': websocket_handler.tick_count,\n                    'valid_tick_count': websocket_handler.valid_tick_count,\n                    'published_count': websocket_handler.published_count,\n                    'error_403_count': websocket_handler.error_403_count,\n                    'reconnect_attempts': websocket_handler.reconnect_attempts,\n                    'is_market_hours': is_market_hours(),\n                    'updated_at': now\n                }\n                \n                # Write to Redis with 60 second TTL\n                redis_health_client.setex(\n                    'health:ingestion',\n                    60,\n                    json.dumps(health_data)\n                )\n                \n                logger.debug('health_status_published', status=status)\n            \n        except Exception as e:\n            logger.error('health_publish_failed', error=str(e))\n        \n        time.sleep(15)  # Update every 15 seconds


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global stop_health_updates
    
    logger.info(
        "shutdown_signal_received",
        signal=signal.Signals(signum).name
    )
    
    # Stop health updates
    stop_health_updates = True
    
    # Stop WebSocket
    if websocket_handler:
        websocket_handler.stop()
    
    # Close publisher
    if publisher:
        publisher.close()
    
    # Close Redis health client
    if redis_health_client:
        redis_health_client.close()
    
    logger.info("ingestion_service_stopped")
    sys.exit(0)


def main():
    """Main entry point for ingestion service"""
    global websocket_handler, publisher, redis_health_client, health_update_thread
    
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
            
            # Initialize Redis health client (separate connection for health updates)
            redis_health_client = redis.from_url(config.REDIS_URL, decode_responses=True)
            redis_health_client.ping()
            logger.info("redis_health_client_connected")
            
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
            
            # Start health status publishing thread
            logger.info("starting_health_status_publisher")
            health_update_thread = threading.Thread(target=publish_health_status, daemon=True)
            health_update_thread.start()
            logger.info("health_status_publisher_started")
            
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
