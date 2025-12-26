"""
Kite Authentication Handler
Retrieves and validates access token from Redis
"""

import os
import redis
import structlog
from typing import Optional

logger = structlog.get_logger()

REDIS_TOKEN_KEY = "kite_access_token"


def get_access_token(redis_url: str) -> str:
    """
    Retrieve Kite access token from Redis
    
    Args:
        redis_url: Redis connection URL
    
    Returns:
        str: Access token
    
    Raises:
        Exception: If token not found or Redis connection fails
    """
    try:
        redis_client = redis.from_url(redis_url, decode_responses=True)
        token = redis_client.get(REDIS_TOKEN_KEY)
        
        if not token:
            domain = os.getenv("DOMAIN", "localhost")
            protocol = "https" if os.getenv("ENVIRONMENT") == "production" else "http"
            logger.error("access_token_not_found", key=REDIS_TOKEN_KEY)
            raise Exception(
                f"Kite access token not found in Redis (key: {REDIS_TOKEN_KEY}). "
                f"Please authenticate via the web interface first: {protocol}://{domain}/"
            )
        
        # Get remaining TTL
        ttl = redis_client.ttl(REDIS_TOKEN_KEY)
        
        logger.info(
            "access_token_retrieved",
            token_length=len(token),
            ttl_seconds=ttl,
            ttl_hours=round(ttl / 3600, 2)
        )
        
        # Warn if token is about to expire
        if ttl < 3600:  # Less than 1 hour
            logger.warning(
                "access_token_expiring_soon",
                ttl_seconds=ttl,
                message="Token will expire soon. Please re-authenticate."
            )
        
        redis_client.close()
        return token
    
    except redis.ConnectionError as e:
        logger.error("redis_connection_failed", error=str(e))
        raise Exception(f"Failed to connect to Redis: {e}")
    except Exception as e:
        logger.error("access_token_retrieval_failed", error=str(e))
        raise


def check_token_validity(redis_url: str) -> bool:
    """
    Check if access token exists and is valid
    
    Args:
        redis_url: Redis connection URL
    
    Returns:
        bool: True if token exists and has TTL > 0
    """
    try:
        redis_client = redis.from_url(redis_url, decode_responses=True)
        
        # Check if token exists
        token = redis_client.get(REDIS_TOKEN_KEY)
        if not token:
            logger.warning("token_check_failed", reason="token_not_found")
            return False
        
        # Check TTL
        ttl = redis_client.ttl(REDIS_TOKEN_KEY)
        if ttl <= 0:
            logger.warning("token_check_failed", reason="token_expired")
            return False
        
        logger.info("token_check_passed", ttl_seconds=ttl)
        redis_client.close()
        return True
    
    except Exception as e:
        logger.error("token_validity_check_failed", error=str(e))
        return False


def get_token_ttl(redis_url: str) -> Optional[int]:
    """
    Get remaining TTL of access token
    
    Args:
        redis_url: Redis connection URL
    
    Returns:
        int: TTL in seconds, or None if token doesn't exist
    """
    try:
        redis_client = redis.from_url(redis_url, decode_responses=True)
        ttl = redis_client.ttl(REDIS_TOKEN_KEY)
        redis_client.close()
        
        if ttl < 0:
            return None
        
        return ttl
    
    except Exception as e:
        logger.error("ttl_retrieval_failed", error=str(e))
        return None


def refresh_token_ttl(redis_url: str, expiry_seconds: int = 86400) -> bool:
    """
    Refresh the TTL of existing access token
    
    Args:
        redis_url: Redis connection URL
        expiry_seconds: New TTL in seconds (default: 24 hours)
    
    Returns:
        bool: True if successful
    """
    try:
        redis_client = redis.from_url(redis_url, decode_responses=True)
        
        # Get existing token
        token = redis_client.get(REDIS_TOKEN_KEY)
        if not token:
            logger.error("refresh_failed", reason="token_not_found")
            return False
        
        # Set new expiry
        redis_client.expire(REDIS_TOKEN_KEY, expiry_seconds)
        
        logger.info("token_ttl_refreshed", new_ttl_seconds=expiry_seconds)
        redis_client.close()
        return True
    
    except Exception as e:
        logger.error("ttl_refresh_failed", error=str(e))
        return False
