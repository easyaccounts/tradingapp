"""
Health Check Routes
Monitors connectivity to all critical services (PostgreSQL, Redis, RabbitMQ)
"""

import os
import logging
from typing import Dict

from fastapi import APIRouter, HTTPException
import redis
import psycopg2
import pika

logger = logging.getLogger(__name__)

router = APIRouter()

# Service connection URLs from environment
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")


async def check_postgresql() -> Dict[str, str]:
    """
    Check PostgreSQL/PgBouncer connection
    
    Returns:
        dict: Status and connection info
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Execute a simple query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        # Check if TimescaleDB is available
        cursor.execute("SELECT extversion FROM pg_extension WHERE extname='timescaledb';")
        timescale_version = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy",
            "service": "PostgreSQL",
            "timescaledb": timescale_version[0] if timescale_version else "not installed",
            "message": "Database connection successful"
        }
    
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "service": "PostgreSQL",
            "error": str(e),
            "message": "Database connection failed"
        }


async def check_redis() -> Dict[str, str]:
    """
    Check Redis connection
    
    Returns:
        dict: Status and connection info
    """
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        
        # Ping Redis
        pong = redis_client.ping()
        
        # Get Redis info
        info = redis_client.info("server")
        
        redis_client.close()
        
        return {
            "status": "healthy",
            "service": "Redis",
            "version": info.get("redis_version", "unknown"),
            "uptime_seconds": info.get("uptime_in_seconds", 0),
            "message": "Redis connection successful"
        }
    
    except Exception as e:
        logger.error(f"Redis health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "service": "Redis",
            "error": str(e),
            "message": "Redis connection failed"
        }


async def check_rabbitmq() -> Dict[str, str]:
    """
    Check RabbitMQ connection
    
    Returns:
        dict: Status and connection info
    """
    try:
        # Parse RabbitMQ URL
        parameters = pika.URLParameters(RABBITMQ_URL)
        
        # Create connection with timeout
        connection = pika.BlockingConnection(parameters)
        
        # Check if connection is open
        if connection.is_open:
            channel = connection.channel()
            
            # Declare a test queue (passive=True means don't create, just check)
            try:
                channel.queue_declare(queue='ticks_queue', passive=True)
                queue_exists = True
            except:
                queue_exists = False
            
            connection.close()
            
            return {
                "status": "healthy",
                "service": "RabbitMQ",
                "ticks_queue_exists": queue_exists,
                "message": "RabbitMQ connection successful"
            }
        else:
            raise Exception("Connection not open")
    
    except Exception as e:
        logger.error(f"RabbitMQ health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "service": "RabbitMQ",
            "error": str(e),
            "message": "RabbitMQ connection failed"
        }


@router.get("/health")
async def health_check():
    """
    Comprehensive health check for all services
    
    Returns:
        dict: Health status of PostgreSQL, Redis, RabbitMQ, and overall system
    
    Example:
        GET /health
        Response: {
            "status": "healthy",
            "services": {
                "postgresql": {...},
                "redis": {...},
                "rabbitmq": {...}
            }
        }
    """
    try:
        # Check all services
        postgresql_status = await check_postgresql()
        redis_status = await check_redis()
        rabbitmq_status = await check_rabbitmq()
        
        # Determine overall health
        all_healthy = all([
            postgresql_status["status"] == "healthy",
            redis_status["status"] == "healthy",
            rabbitmq_status["status"] == "healthy"
        ])
        
        overall_status = "healthy" if all_healthy else "degraded"
        
        # Count unhealthy services
        unhealthy_count = sum([
            1 for s in [postgresql_status, redis_status, rabbitmq_status]
            if s["status"] == "unhealthy"
        ])
        
        response = {
            "status": overall_status,
            "timestamp": "NOW()",  # Will be replaced by actual timestamp
            "services": {
                "postgresql": postgresql_status,
                "redis": redis_status,
                "rabbitmq": rabbitmq_status
            },
            "summary": {
                "total_services": 3,
                "healthy_services": 3 - unhealthy_count,
                "unhealthy_services": unhealthy_count
            }
        }
        
        # Log if any service is unhealthy
        if not all_healthy:
            logger.warning(f"Health check: {unhealthy_count} service(s) unhealthy")
        
        return response
    
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Health check failed: {str(e)}"
        )


@router.get("/health/postgresql")
async def health_check_postgresql():
    """
    Check only PostgreSQL health
    
    Returns:
        dict: PostgreSQL health status
    """
    return await check_postgresql()


@router.get("/health/redis")
async def health_check_redis():
    """
    Check only Redis health
    
    Returns:
        dict: Redis health status
    """
    return await check_redis()


@router.get("/health/rabbitmq")
async def health_check_rabbitmq():
    """
    Check only RabbitMQ health
    
    Returns:
        dict: RabbitMQ health status
    """
    return await check_rabbitmq()


@router.get("/health/liveness")
async def liveness_probe():
    """
    Kubernetes liveness probe - checks if application is running
    Returns 200 if API is alive (doesn't check dependencies)
    
    Returns:
        dict: Simple alive status
    """
    return {"status": "alive", "service": "api"}


@router.get("/health/readiness")
async def readiness_probe():
    """
    Kubernetes readiness probe - checks if application can handle requests
    Returns 200 only if all critical services are healthy
    
    Returns:
        dict: Readiness status
    
    Raises:
        HTTPException: 503 if any critical service is unhealthy
    """
    postgresql_status = await check_postgresql()
    redis_status = await check_redis()
    
    # RabbitMQ is not critical for API readiness, only for ingestion
    all_ready = all([
        postgresql_status["status"] == "healthy",
        redis_status["status"] == "healthy"
    ])
    
    if not all_ready:
        raise HTTPException(
            status_code=503,
            detail="Service not ready - critical dependencies unavailable"
        )
    
    return {
        "status": "ready",
        "service": "api",
        "message": "All critical services are healthy"
    }
