"""
Dhan Authentication Routes
Handles Dhan OAuth flow and token management
"""

import os
import logging
from typing import Optional
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import RedirectResponse
import redis
import requests

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize Redis client
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(redis_url, decode_responses=True)

# Slack webhook
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Dhan API Configuration
DHAN_API_KEY = os.getenv("DHAN_API_KEY")
DHAN_API_SECRET = os.getenv("DHAN_API_SECRET")
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

# Construct redirect URL from domain
DOMAIN = os.getenv("DOMAIN", "localhost")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
protocol = "https" if ENVIRONMENT == "production" else "http"
port = "" if ENVIRONMENT == "production" else ":8000"
DHAN_REDIRECT_URL = os.getenv("DHAN_REDIRECT_URL", f"{protocol}://{DOMAIN}{port}/api/dhan/callback")

if not DHAN_API_KEY or not DHAN_API_SECRET or not DHAN_CLIENT_ID:
    logger.warning("DHAN_API_KEY, DHAN_API_SECRET, and DHAN_CLIENT_ID should be set in environment variables")

# Constants
REDIS_CONSENT_KEY = "dhan_consent_app_id"
REDIS_TOKEN_KEY = "dhan_access_token"
REDIS_TOKEN_EXPIRY_KEY = "dhan_token_expiry"
TOKEN_EXPIRY_SECONDS = 86400  # 24 hours

# Token file path (persisted via volume mount)
TOKEN_FILE_PATH = "/app/data/dhan_token.json"


def write_token_to_file(access_token: str, expiry: str):
    """Write access token to persistent file"""
    import json
    from datetime import datetime
    
    try:
        os.makedirs(os.path.dirname(TOKEN_FILE_PATH), exist_ok=True)
        
        data = {
            'access_token': access_token,
            'expiry': expiry,
            'client_id': DHAN_CLIENT_ID,
            'generated_at': datetime.now().isoformat()
        }
        
        with open(TOKEN_FILE_PATH, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Access token written to {TOKEN_FILE_PATH}")
    except Exception as e:
        logger.error(f"Failed to write token to file: {e}")


def send_token_notification(provider: str, client_name: str, expiry: str):
    """Send Slack notification when token is saved"""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return
    
    try:
        now = datetime.now()
        expiry_24h = now + timedelta(hours=24)
        
        message = {
            "text": f"✅ *{provider} Token Saved*",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{provider} Authentication Successful*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Provider:*\n{provider}"},
                        {"type": "mrkdwn", "text": f"*Client:*\n{client_name}"},
                        {"type": "mrkdwn", "text": f"*Saved At:*\n{now.strftime('%Y-%m-%d %H:%M:%S IST')}"},
                        {"type": "mrkdwn", "text": f"*Expires At:*\n{expiry_24h.strftime('%Y-%m-%d %H:%M:%S IST')}"},
                        {"type": "mrkdwn", "text": f"*Valid For:*\n24 hours"},
                        {"type": "mrkdwn", "text": f"*Token File:*\n`{TOKEN_FILE_PATH}`"}
                    ]
                }
            ]
        }
        
        response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=5)
        if response.status_code == 200:
            logger.info(f"Token notification sent to Slack for {provider}")
        else:
            logger.error(f"Slack notification failed: {response.status_code}")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")


def read_token_from_file() -> Optional[dict]:
    """Read access token from persistent file"""
    import json
    
    try:
        if os.path.exists(TOKEN_FILE_PATH):
            with open(TOKEN_FILE_PATH, 'r') as f:
                data = json.load(f)
            return data
        return None
    except Exception as e:
        logger.error(f"Failed to read token from file: {e}")
        return None


@router.get("/login-url")
async def get_login_url():
    """
    Generate Dhan OAuth login URL
    
    Step 1: Generate consent to get consentAppId
    Step 2: Return browser URL for user login
    
    Returns:
        dict: Contains the login_url to redirect user to
    
    Example:
        GET /api/dhan/login-url
        Response: {"login_url": "https://auth.dhan.co/login/consentApp-login?..."}
    """
    try:
        if not DHAN_API_KEY or not DHAN_API_SECRET:
            raise HTTPException(
                status_code=500,
                detail="Dhan API credentials not configured. Set DHAN_API_KEY and DHAN_API_SECRET."
            )
        
        # Step 1: Generate consent
        response = requests.post(
            f'https://auth.dhan.co/app/generate-consent?client_id={DHAN_CLIENT_ID}',
            headers={
                'app_id': DHAN_API_KEY,
                'app_secret': DHAN_API_SECRET
            },
            timeout=10
        )
        
        if response.status_code != 200:
            logger.error(f"Consent generation failed: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate consent: {response.text}"
            )
        
        data = response.json()
        consent_app_id = data.get('consentAppId')
        
        if not consent_app_id:
            raise HTTPException(
                status_code=500,
                detail="ConsentAppId not received from Dhan"
            )
        
        # Store consentAppId in Redis for callback verification (5 min expiry)
        redis_client.setex(REDIS_CONSENT_KEY, 300, consent_app_id)
        
        # Step 2: Generate browser login URL
        login_url = f"https://auth.dhan.co/login/consentApp-login?consentAppId={consent_app_id}"
        
        logger.info(f"Generated Dhan login URL with consentAppId: {consent_app_id}")
        
        return {
            "login_url": login_url,
            "message": "Redirect user to this URL for authentication",
            "consent_app_id": consent_app_id
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate login URL: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate login URL: {str(e)}"
        )


@router.get("/callback")
async def dhan_callback(
    tokenId: Optional[str] = Query(None, description="Token ID from Dhan after login")
):
    """
    Dhan OAuth callback endpoint
    
    Step 3: Exchange tokenId for access_token and store
    
    Args:
        tokenId: Token ID from Dhan after successful authentication
    
    Returns:
        RedirectResponse: Redirects to success page
    
    Example:
        GET /api/dhan/callback?tokenId=abc123xyz
    """
    if not tokenId:
        logger.error("Dhan authentication callback received without tokenId")
        raise HTTPException(
            status_code=400,
            detail="Authentication failed: tokenId not provided"
        )
    
    try:
        if not DHAN_API_KEY or not DHAN_API_SECRET:
            raise HTTPException(
                status_code=500,
                detail="Dhan API credentials not configured"
            )
        
        # Step 3: Consume consent to get access token
        logger.info(f"Exchanging tokenId: {tokenId[:10]}...")
        
        response = requests.get(
            f'https://auth.dhan.co/app/consumeApp-consent?tokenId={tokenId}',
            headers={
                'app_id': DHAN_API_KEY,
                'app_secret': DHAN_API_SECRET
            },
            timeout=10
        )
        
        if response.status_code != 200:
            logger.error(f"Token exchange failed: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to exchange tokenId: {response.text}"
            )
        
        data = response.json()
        access_token = data.get('accessToken')
        expiry_time = data.get('expiryTime')
        client_name = data.get('dhanClientName', 'Unknown')
        
        if not access_token:
            raise HTTPException(
                status_code=500,
                detail="Access token not received from Dhan"
            )
        
        logger.info(f"Successfully obtained access token for {client_name}")
        
        # Store access_token in file (persisted)
        write_token_to_file(access_token, expiry_time)
        
        # Send Slack notification
        send_token_notification("Dhan", client_name, expiry_time)
        
        # Also store in Redis for caching
        try:
            redis_client.setex(
                REDIS_TOKEN_KEY,
                TOKEN_EXPIRY_SECONDS,
                access_token
            )
            redis_client.setex(
                REDIS_TOKEN_EXPIRY_KEY,
                TOKEN_EXPIRY_SECONDS,
                expiry_time or ""
            )
            redis_client.setex(
                "dhan_client_name",
                TOKEN_EXPIRY_SECONDS,
                client_name
            )
            
            # Store tokenId for future use with auth module
            redis_client.setex(
                "dhan_token_id",
                TOKEN_EXPIRY_SECONDS * 30,  # 30 days
                tokenId
            )
            
            logger.info("Access token stored in Redis and file")
        except Exception as redis_error:
            logger.warning(f"Failed to store token in Redis: {redis_error}")
            # Redis is optional, continue anyway
        
        # Redirect to success page
        return RedirectResponse(url="/success.html?provider=dhan", status_code=302)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to exchange tokenId: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to authenticate with Dhan: {str(e)}"
        )


@router.get("/status")
async def check_auth_status():
    """
    Check if Dhan access token is present and valid
    
    Returns:
        dict: Authentication status and token details
    
    Example:
        GET /api/dhan/status
        Response: {
            "authenticated": true,
            "client_name": "JOHN DOE",
            "expiry": "2025-12-30T09:00:00"
        }
    """
    try:
        # Check if token exists in file (primary source)
        token_data = read_token_from_file()
        
        if token_data and token_data.get('access_token'):
            token = token_data['access_token']
            expiry = token_data.get('expiry', 'Unknown')
            
            # Sync to Redis if not there
            redis_token = redis_client.get(REDIS_TOKEN_KEY)
            if not redis_token:
                redis_client.setex(REDIS_TOKEN_KEY, TOKEN_EXPIRY_SECONDS, token)
                redis_client.setex(REDIS_TOKEN_EXPIRY_KEY, TOKEN_EXPIRY_SECONDS, expiry)
            
            # Get client name from Redis or token data
            client_name = redis_client.get("dhan_client_name") or token_data.get('client_id', 'Unknown')
            
            return {
                "authenticated": True,
                "client_id": token_data.get('client_id'),
                "client_name": client_name,
                "expiry": expiry,
                "generated_at": token_data.get('generated_at')
            }
        
        # Fallback to Redis if file is empty
        token = redis_client.get(REDIS_TOKEN_KEY)
        
        if token:
            expiry = redis_client.get(REDIS_TOKEN_EXPIRY_KEY) or "Unknown"
            client_name = redis_client.get("dhan_client_name") or "Unknown"
            
            return {
                "authenticated": True,
                "client_name": client_name,
                "expiry": expiry
            }
        
        # No token found
        return {
            "authenticated": False,
            "message": "Not authenticated with Dhan"
        }
    
    except Exception as e:
        logger.error(f"Failed to check auth status: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check authentication status: {str(e)}"
        )


@router.get("/token")
async def get_token():
    """
    Get current Dhan access token
    
    Returns:
        dict: Access token if available
    
    Example:
        GET /api/dhan/token
        Response: {"access_token": "eyJ...", "expiry": "2025-12-30T09:00:00"}
    """
    try:
        # Try file first
        token_data = read_token_from_file()
        
        if token_data and token_data.get('access_token'):
            return {
                "access_token": token_data['access_token'],
                "expiry": token_data.get('expiry'),
                "source": "file"
            }
        
        # Fallback to Redis
        token = redis_client.get(REDIS_TOKEN_KEY)
        expiry = redis_client.get(REDIS_TOKEN_EXPIRY_KEY)
        
        if token:
            return {
                "access_token": token,
                "expiry": expiry,
                "source": "redis"
            }
        
        raise HTTPException(
            status_code=404,
            detail="No access token found. Please authenticate first."
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve token: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve token: {str(e)}"
        )
