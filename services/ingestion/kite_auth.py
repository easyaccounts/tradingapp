"""
Kite Authentication Handler
Retrieves and validates access token from file or Redis
"""

import os
import redis
import structlog
from typing import Optional

logger = structlog.get_logger()

REDIS_TOKEN_KEY = "access_token"
TOKEN_FILE_PATH = "/app/data/access_token.txt"
TOKEN_EXPIRY_SECONDS = 86400  # 24 hours


def read_token_from_file() -> Optional[str]:
    """Read access token from persistent file"""
    try:
        if os.path.exists(TOKEN_FILE_PATH):
            with open(TOKEN_FILE_PATH, 'r') as f:
                token = f.read().strip()
            return token if token else None
        return None
    except Exception as e:
        logger.error("failed_to_read_token_file", error=str(e))
        return None


def get_access_token(redis_url: str) -> str:
    """
    Retrieve Kite access token from file (primary) or Redis (fallback)
    Syncs token from file to Redis if Redis is missing it
    
    Args:
        redis_url: Redis connection URL
    
    Returns:
        str: Access token
    
    Raises:
        Exception: If token not found
    """
    try:
        # Try reading from file first (primary source)
        token = read_token_from_file()
        
        if token:
            logger.info(
                "access_token_retrieved_from_file",
                token_length=len(token)
            )
            
            # Sync to Redis if not present or expired
            try:
                redis_client = redis.from_url(redis_url, decode_responses=True)
                redis_token = redis_client.get(REDIS_TOKEN_KEY)
                
                if not redis_token:
                    # Token exists in file but not in Redis - sync it
                    redis_client.setex(
                        REDIS_TOKEN_KEY,
                        TOKEN_EXPIRY_SECONDS,
                        token
                    )
                    logger.info(
                        "token_synced_to_redis",
                        message="Token from file synced to Redis with 24h TTL"
                    )
                
                redis_client.close()
            except Exception as redis_error:
                logger.warning(
                    "redis_sync_failed",
                    error=str(redis_error),
                    message="Token retrieved from file but Redis sync failed"
                )
            
            return token
        
        # Fallback to Redis
        redis_client = redis.from_url(redis_url, decode_responses=True)
        token = redis_client.get(REDIS_TOKEN_KEY)
        
        if not token:
            domain = os.getenv("DOMAIN", "localhost")
            protocol = "https" if os.getenv("ENVIRONMENT") == "production" else "http"
            logger.error("access_token_not_found", key=REDIS_TOKEN_KEY)
            raise Exception(
                f"Kite access token not found in file or Redis. "
                f"Please authenticate via the web interface first: {protocol}://{domain}/"
            )
        
        # Get remaining TTL
        ttl = redis_client.ttl(REDIS_TOKEN_KEY)
        
        logger.info(
            "access_token_retrieved_from_redis",
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
        
        # Save token from Redis to file for persistence
        try:
            os.makedirs(os.path.dirname(TOKEN_FILE_PATH), exist_ok=True)
            with open(TOKEN_FILE_PATH, 'w') as f:
                f.write(token)
            logger.info("token_saved_to_file_from_redis")
        except Exception as file_error:
            logger.warning("failed_to_save_token_to_file", error=str(file_error))
        
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
    Checks file first (primary), then Redis (fallback)
    Syncs token to Redis if found in file but not in Redis
    
    Args:
        redis_url: Redis connection URL
    
    Returns:
        bool: True if token exists in file or Redis
    """
    try:
        # Check file first (primary source)
        token = read_token_from_file()
        
        if token:
            logger.info("token_check_passed", source="file")
            
            # Sync to Redis if not present
            try:
                redis_client = redis.from_url(redis_url, decode_responses=True)
                redis_token = redis_client.get(REDIS_TOKEN_KEY)
                
                if not redis_token:
                    redis_client.setex(
                        REDIS_TOKEN_KEY,
                        TOKEN_EXPIRY_SECONDS,
                        token
                    )
                    logger.info("token_synced_to_redis_from_validation")
                
                redis_client.close()
            except Exception as redis_error:
                logger.warning(
                    "redis_sync_during_validation_failed",
                    error=str(redis_error)
                )
            
            return True
        
        # Fallback to Redis check
        redis_client = redis.from_url(redis_url, decode_responses=True)
        
        # Check if token exists in Redis
        token = redis_client.get(REDIS_TOKEN_KEY)
        if not token:
            logger.warning("token_check_failed", reason="token_not_found_in_file_or_redis")
            redis_client.close()
            return False
        
        # Check TTL
        ttl = redis_client.ttl(REDIS_TOKEN_KEY)
        if ttl <= 0:
            logger.warning("token_check_failed", reason="redis_token_expired")
            redis_client.close()
            return False
        
        logger.info("token_check_passed", source="redis", ttl_seconds=ttl)
        
        # Save to file for persistence
        try:
            os.makedirs(os.path.dirname(TOKEN_FILE_PATH), exist_ok=True)
            with open(TOKEN_FILE_PATH, 'w') as f:
                f.write(token)
            logger.info("token_saved_to_file_from_redis_validation")
        except Exception as file_error:
            logger.warning("failed_to_save_token_to_file", error=str(file_error))
        
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
