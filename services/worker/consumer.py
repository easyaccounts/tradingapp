"""
RabbitMQ Consumer for Tick Data
Consumes messages from ticks_queue and writes directly to database
"""

import os
import sys
import json
import signal
import time
import pika
import structlog
import logging
from typing import Dict, Any, List
from db_writer import bulk_insert_ticks, test_connection

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()

# Configuration
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
QUEUE_NAME = "ticks_queue"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", 5))
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", 100))

# Global state
tick_batch = []
last_flush_time = time.time()
should_stop = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global should_stop
    logger.info("shutdown_signal_received", signal=signal.Signals(signum).name)
    should_stop = True


def flush_batch():
    """Flush current batch to database"""
    global tick_batch, last_flush_time
    
    if not tick_batch:
        return
    
    try:
        start_time = time.time()
        batch_size = len(tick_batch)
        
        logger.info("flushing_batch", size=batch_size)
        
        # Insert to database
        rows_inserted = bulk_insert_ticks(tick_batch)
        
        elapsed = time.time() - start_time
        
        logger.info(
            "batch_flushed",
            inserted=rows_inserted,
            batch_size=batch_size,
            elapsed_seconds=round(elapsed, 2)
        )
        
        # Clear batch
        tick_batch = []
        last_flush_time = time.time()
        
    except Exception as e:
        logger.error("batch_flush_failed", error=str(e), batch_size=len(tick_batch))
        # Clear batch anyway to avoid infinite retry
        tick_batch = []
        last_flush_time = time.time()


def process_message(ch, method, properties, body):
    """Process a single message from RabbitMQ"""
    global tick_batch, last_flush_time
    
    try:
        # Parse JSON
        tick_data = json.loads(body)
        
        # Add to batch
        tick_batch.append(tick_data)
        
        # Check if we should flush
        should_flush = (
            len(tick_batch) >= BATCH_SIZE or
            (time.time() - last_flush_time) >= BATCH_TIMEOUT
        )
        
        if should_flush:
            flush_batch()
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except json.JSONDecodeError as e:
        logger.error("invalid_json", error=str(e))
        # Reject and discard bad message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
    except Exception as e:
        logger.error("message_processing_failed", error=str(e))
        # Reject and requeue for retry
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    """Main consumer loop"""
    global should_stop
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("consumer_starting")
    
    # Validate configuration
    if not RABBITMQ_URL:
        logger.error("rabbitmq_url_not_configured")
        sys.exit(1)
    
    # Test database connection
    if not test_connection():
        logger.error("database_connection_failed")
        sys.exit(1)
    
    logger.info("database_connected")
    
    # Connect to RabbitMQ
    max_retries = 10
    retry_delay = 5
    
    connection = None
    channel = None
    
    for attempt in range(max_retries):
        try:
            logger.info("connecting_to_rabbitmq", attempt=attempt + 1)
            
            parameters = pika.URLParameters(RABBITMQ_URL)
            parameters.heartbeat = 600
            parameters.blocked_connection_timeout = 300
            
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Set QoS - prefetch messages
            channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            
            logger.info(
                "rabbitmq_connected",
                queue=QUEUE_NAME,
                prefetch=PREFETCH_COUNT
            )
            break
            
        except Exception as e:
            logger.error("rabbitmq_connection_failed", attempt=attempt + 1, error=str(e))
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error("rabbitmq_connection_exhausted")
                sys.exit(1)
    
    # Verify connection and channel
    if not connection or not channel:
        logger.error("connection_not_established")
        sys.exit(1)
    
    # Start consuming
    try:
        logger.info("starting_consumer", queue=QUEUE_NAME)
        
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=process_message,
            auto_ack=False  # Manual acknowledgment
        )
        
        # Consume with periodic check for shutdown
        while not should_stop:
            try:
                connection.process_data_events(time_limit=1)
                
                # Periodic flush check
                if (time.time() - last_flush_time) >= BATCH_TIMEOUT and tick_batch:
                    flush_batch()
                    
            except Exception as e:
                logger.error("consume_error", error=str(e))
                time.sleep(1)
        
        # Graceful shutdown
        logger.info("shutting_down")
        
        # Flush remaining batch
        if tick_batch:
            logger.info("flushing_remaining_batch", size=len(tick_batch))
            flush_batch()
        
        # Close connection
        if channel and channel.is_open:
            channel.close()
        if connection and connection.is_open:
            connection.close()
        
        logger.info("consumer_stopped")
        
    except Exception as e:
        logger.error("consumer_error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
