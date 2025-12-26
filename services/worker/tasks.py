"""
Celery Tasks
Processes tick data and performs maintenance operations
"""

import os
import json
import time
import logging
import redis
import structlog
from typing import List, Dict
from celery import Task
from celery_app import app
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

# Configuration from environment
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", 5))
REDIS_URL = os.getenv("REDIS_URL")

# In-memory batch accumulator
# In production, consider using Redis for distributed batching
tick_batch = []
last_flush_time = time.time()


class DatabaseTask(Task):
    """Base task class with database connection management"""
    
    _db_connected = None
    
    @property
    def db_connected(self):
        if self._db_connected is None:
            self._db_connected = test_connection()
        return self._db_connected


@app.task(
    bind=True,
    base=DatabaseTask,
    max_retries=3,
    default_retry_delay=5,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True
)
def process_tick(self, tick_data: Dict) -> Dict:
    """
    Process a single tick and add to batch
    Flushes to database when batch size or timeout is reached
    
    Args:
        tick_data: Dictionary containing enriched tick data
    
    Returns:
        dict: Processing result
    """
    global tick_batch, last_flush_time
    
    try:
        # Add tick to batch
        tick_batch.append(tick_data)
        
        # Check if we should flush
        should_flush = (
            len(tick_batch) >= BATCH_SIZE or
            (time.time() - last_flush_time) >= BATCH_TIMEOUT
        )
        
        if should_flush:
            result = flush_tick_batch()
            return result
        
        return {
            "status": "queued",
            "batch_size": len(tick_batch),
            "instrument_token": tick_data.get("instrument_token")
        }
    
    except Exception as e:
        logger.error(
            "process_tick_failed",
            error=str(e),
            instrument_token=tick_data.get("instrument_token")
        )
        raise


def flush_tick_batch() -> Dict:
    """
    Flush accumulated ticks to database
    
    Returns:
        dict: Flush result with counts
    """
    global tick_batch, last_flush_time
    
    if not tick_batch:
        return {"status": "skipped", "reason": "empty_batch"}
    
    try:
        batch_size = len(tick_batch)
        
        logger.info(
            "flushing_tick_batch",
            batch_size=batch_size
        )
        
        # Bulk insert to database
        start_time = time.time()
        rows_inserted = bulk_insert_ticks(tick_batch)
        elapsed_time = time.time() - start_time
        
        logger.info(
            "batch_flush_successful",
            rows_inserted=rows_inserted,
            batch_size=batch_size,
            elapsed_seconds=round(elapsed_time, 3),
            inserts_per_second=round(rows_inserted / elapsed_time, 2) if elapsed_time > 0 else 0
        )
        
        # Clear batch
        tick_batch = []
        last_flush_time = time.time()
        
        return {
            "status": "success",
            "rows_inserted": rows_inserted,
            "batch_size": batch_size,
            "elapsed_seconds": round(elapsed_time, 3)
        }
    
    except Exception as e:
        logger.error(
            "batch_flush_failed",
            error=str(e),
            batch_size=len(tick_batch)
        )
        
        # Don't clear batch on failure - will retry
        raise


@app.task(bind=True, max_retries=3)
def check_token_expiry(self):
    """
    Check Kite access token expiry and send alerts
    Runs daily via Celery Beat
    
    Returns:
        dict: Token status
    """
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        
        # Get token TTL
        ttl = redis_client.ttl("kite_access_token")
        
        if ttl < 0:
            logger.error(
                "token_not_found_or_expired",
                message="Please re-authenticate via web interface"
            )
            return {"status": "expired", "ttl": 0}
        
        # Warning if less than 6 hours remaining
        if ttl < 21600:  # 6 hours
            logger.warning(
                "token_expiring_soon",
                ttl_seconds=ttl,
                ttl_hours=round(ttl / 3600, 2),
                message="Please plan to re-authenticate soon"
            )
            
            return {
                "status": "expiring_soon",
                "ttl_seconds": ttl,
                "ttl_hours": round(ttl / 3600, 2)
            }
        
        logger.info(
            "token_check_passed",
            ttl_seconds=ttl,
            ttl_hours=round(ttl / 3600, 2)
        )
        
        return {
            "status": "valid",
            "ttl_seconds": ttl,
            "ttl_hours": round(ttl / 3600, 2)
        }
    
    except Exception as e:
        logger.error("token_expiry_check_failed", error=str(e))
        raise


@app.task
def cleanup_old_results():
    """
    Clean up old Celery result data from Redis
    Runs daily via Celery Beat
    
    Returns:
        dict: Cleanup result
    """
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        
        # Get all celery result keys
        pattern = "celery-task-meta-*"
        keys = redis_client.keys(pattern)
        
        deleted_count = 0
        
        for key in keys:
            # Delete keys older than 1 hour
            ttl = redis_client.ttl(key)
            if ttl < 0 or ttl > 3600:
                redis_client.delete(key)
                deleted_count += 1
        
        logger.info(
            "cleanup_old_results_completed",
            total_keys=len(keys),
            deleted_keys=deleted_count
        )
        
        return {
            "status": "success",
            "total_keys": len(keys),
            "deleted_keys": deleted_count
        }
    
    except Exception as e:
        logger.error("cleanup_failed", error=str(e))
        raise


@app.task(bind=True)
def health_check(self):
    """
    Worker health check task
    
    Returns:
        dict: Health status
    """
    try:
        # Test database connection
        db_ok = test_connection()
        
        # Test Redis connection
        redis_client = redis.from_url(REDIS_URL)
        redis_ok = redis_client.ping()
        
        return {
            "status": "healthy" if (db_ok and redis_ok) else "unhealthy",
            "database": "ok" if db_ok else "failed",
            "redis": "ok" if redis_ok else "failed",
            "worker_id": self.request.id
        }
    
    except Exception as e:
        logger.error("health_check_failed", error=str(e))
        return {
            "status": "unhealthy",
            "error": str(e)
        }


# RabbitMQ consumer task (alternative approach)
@app.task(bind=True, max_retries=3)
def consume_tick_from_queue(self, message_body: str):
    """
    Consume tick message from RabbitMQ and process
    This is an alternative to direct Celery task calls
    
    Args:
        message_body: JSON string from RabbitMQ
    
    Returns:
        dict: Processing result
    """
    try:
        # Parse message
        tick_data = json.loads(message_body)
        
        # Process tick
        result = process_tick(tick_data)
        
        return result
    
    except json.JSONDecodeError as e:
        logger.error("invalid_json_in_message", error=str(e))
        raise
    
    except Exception as e:
        logger.error("consume_tick_failed", error=str(e))
        raise


# Periodic task to force flush if needed
@app.task
def force_flush_batch():
    """
    Force flush the current batch regardless of size
    Useful for end-of-day or maintenance operations
    
    Returns:
        dict: Flush result
    """
    try:
        result = flush_tick_batch()
        return result
    except Exception as e:
        logger.error("force_flush_failed", error=str(e))
        raise
