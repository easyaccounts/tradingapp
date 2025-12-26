"""
Kite Authentication Routes
Handles KiteConnect OAuth flow and token management
"""

import os
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse
from kiteconnect import KiteConnect
import redis

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize Redis client
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(redis_url, decode_responses=True)

# Initialize KiteConnect
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")
# Construct redirect URL from domain
DOMAIN = os.getenv("DOMAIN", "localhost")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
protocol = "https" if ENVIRONMENT == "production" else "http"
port = "" if ENVIRONMENT == "production" else ":8000"
KITE_REDIRECT_URL = os.getenv("KITE_REDIRECT_URL", f"{protocol}://{DOMAIN}{port}/api/kite/callback")

if not KITE_API_KEY or not KITE_API_SECRET:
    logger.error("KITE_API_KEY and KITE_API_SECRET must be set in environment variables")
    raise EnvironmentError("Missing Kite API credentials")

kite = KiteConnect(api_key=KITE_API_KEY)

# Constants
REDIS_TOKEN_KEY = "kite_access_token"
TOKEN_EXPIRY_SECONDS = 86400  # 24 hours

# Token file path (persisted via volume mount)
TOKEN_FILE_PATH = "/app/data/access_token.txt"


def write_token_to_file(token: str):
    """Write access token to persistent file"""
    try:
        os.makedirs(os.path.dirname(TOKEN_FILE_PATH), exist_ok=True)
        with open(TOKEN_FILE_PATH, 'w') as f:
            f.write(token)
        logger.info(f"Access token written to {TOKEN_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to write token to file: {e}")


def read_token_from_file() -> Optional[str]:
    """Read access token from persistent file"""
    try:
        if os.path.exists(TOKEN_FILE_PATH):
            with open(TOKEN_FILE_PATH, 'r') as f:
                token = f.read().strip()
            return token if token else None
        return None
    except Exception as e:
        logger.error(f"Failed to read token from file: {e}")
        return None


@router.get("/login-url")
async def get_login_url():
    """
    Generate KiteConnect login URL for OAuth flow
    
    Returns:
        dict: Contains the login_url to redirect user to
    
    Example:
        GET /api/kite/login-url
        Response: {"login_url": "https://kite.zerodha.com/connect/login?..."}
    """
    try:
        login_url = kite.login_url()
        logger.info("Generated Kite login URL")
        
        return {
            "login_url": login_url,
            "message": "Redirect user to this URL for authentication"
        }
    
    except Exception as e:
        logger.error(f"Failed to generate login URL: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate login URL: {str(e)}"
        )


@router.get("/callback")
async def kite_callback(
    request_token: Optional[str] = Query(None, description="Request token from Kite"),
    status: Optional[str] = Query(None, description="Status from Kite (success/error)")
):
    """
    Kite OAuth callback endpoint
    Exchanges request_token for access_token and stores in Redis
    
    Args:
        request_token: Temporary token from Kite after user authorization
        status: Status of the authentication (success/error)
    
    Returns:
        RedirectResponse: Redirects to success or error page
    
    Example:
        GET /api/kite/callback?request_token=abc123&status=success
    """
    # Check for errors in callback
    if status == "error" or not request_token:
        logger.error(f"Kite authentication failed: status={status}")
        raise HTTPException(
            status_code=400,
            detail="Authentication failed or denied by user"
        )
    
    try:
        # Exchange request_token for access_token
        logger.info(f"Exchanging request token: {request_token[:10]}...")
        
        data = kite.generate_session(
            request_token=request_token,
            api_secret=KITE_API_SECRET
        )
        
        access_token = data["access_token"]
        logger.info("Successfully obtained access token")
        
        # Store access_token in file (persisted)
        write_token_to_file(access_token)
        
        # Also store in Redis for scripts and caching
        try:
            redis_client.setex(
                REDIS_TOKEN_KEY,
                TOKEN_EXPIRY_SECONDS,
                access_token
            )
            redis_client.setex(
                "kite_user_profile",
                TOKEN_EXPIRY_SECONDS,
                str(data.get("user_id", "unknown"))
            )
            logger.info("Access token stored in Redis with 24h expiry")
        except Exception as redis_error:
            logger.warning(f"Failed to store token in Redis: {redis_error}")
            # Redis is optional, continue anyway
        
        # Redirect to success page
        return RedirectResponse(url="/success.html", status_code=302)
    
    except Exception as e:
        logger.error(f"Failed to exchange request token: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to authenticate with Kite: {str(e)}"
        )


@router.get("/status")
async def check_auth_status():
    """
    Check if Kite access token is present and valid
    
    Returns:
        dict: Authentication status and token TTL
    
    Example:
        GET /api/kite/status
        Response: {
            "authenticated": true,
            "ttl_seconds": 82340,
            "user_id": "ABC123"
        }
    """
    try:
        # Check if token exists in file (primary source)
        token = read_token_from_file()
        
        # Fallback to Redis if file is empty
        if not token:
            token = redis_client.get(REDIS_TOKEN_KEY)
            # If found in Redis, save to file for persistence
            if token:
                write_token_to_file(token)
        
        if not token:
            return {
                "authenticated": False,
                "ttl_seconds": 0,
                "message": "No access token found. Please authenticate."
            }
        
        # Get remaining TTL from Redis (if available)
        ttl = redis_client.ttl(REDIS_TOKEN_KEY)
        user_id = redis_client.get("kite_user_profile")
        
        # Check if token is about to expire (< 1 hour)
        if ttl < 3600:
            logger.warning(f"Access token expiring soon! TTL: {ttl} seconds")
        
        return {
            "authenticated": True,
            "ttl_seconds": ttl,
            "user_id": user_id,
            "expires_in_hours": round(ttl / 3600, 2),
            "message": "Access token is valid"
        }
    
    except Exception as e:
        logger.error(f"Failed to check auth status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check authentication status: {str(e)}"
        )


@router.delete("/logout")
async def logout():
    """
    Invalidate current access token by removing from Redis
    
    Returns:
        dict: Logout confirmation
    
    Example:
        DELETE /api/kite/logout
        Response: {"message": "Successfully logged out"}
    """
    try:
        # Delete token from Redis
        deleted = redis_client.delete(REDIS_TOKEN_KEY, "kite_user_profile")
        
        if deleted:
            logger.info("User logged out successfully")
            return {
                "message": "Successfully logged out",
                "tokens_deleted": deleted
            }
        else:
            return {
                "message": "No active session found",
                "tokens_deleted": 0
            }
    
    except Exception as e:
        logger.error(f"Failed to logout: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to logout: {str(e)}"
        )


@router.get("/profile")
async def get_user_profile():
    """
    Get authenticated user's Kite profile
    Requires valid access token in Redis
    
    Returns:
        dict: User profile information from Kite
    
    Example:
        GET /api/kite/profile
        Response: {
            "user_id": "ABC123",
            "user_name": "John Doe",
            "email": "john@example.com",
            "broker": "ZERODHA"
        }
    """
    try:
        # Get access token from Redis
        access_token = redis_client.get(REDIS_TOKEN_KEY)
        
        if not access_token:
            raise HTTPException(
                status_code=401,
                detail="Not authenticated. Please login first."
            )
        
        # Set access token and fetch profile
        kite.set_access_token(access_token)
        profile = kite.profile()
        
        logger.info(f"Retrieved profile for user: {profile.get('user_id')}")
        
        return {
            "user_id": profile.get("user_id"),
            "user_name": profile.get("user_name"),
            "email": profile.get("email"),
            "broker": profile.get("broker"),
            "exchanges": profile.get("exchanges", []),
            "products": profile.get("products", []),
            "order_types": profile.get("order_types", [])
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch user profile: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch profile: {str(e)}"
        )
