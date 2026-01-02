"""
Dhan Authentication Module
Reads Dhan credentials from shared volume (/app/data/dhan_token.json)
Pattern from depth_collector implementation
"""

import os
import json
import structlog
from typing import Dict, Optional
from datetime import datetime

logger = structlog.get_logger()


def get_dhan_credentials(token_file: Optional[str] = None) -> Dict[str, str]:
    """
    Read Dhan credentials from token file
    
    Token file structure:
    {
        "access_token": "eyJ...",
        "client_id": "1001234567",
        "expiry": "2026-01-15T00:00:00"  # Optional
    }
    
    Args:
        token_file: Path to token file (default: /app/data/dhan_token.json)
    
    Returns:
        Dict with 'access_token' and 'client_id'
    
    Raises:
        FileNotFoundError: If token file doesn't exist
        ValueError: If token file is malformed
    """
    if token_file is None:
        # Check for container path first, fallback to host path
        if os.path.exists('/app/data/dhan_token.json'):
            token_file = '/app/data/dhan_token.json'
        else:
            token_file = os.path.expanduser('/opt/tradingapp/data/dhan_token.json')
    
    logger.info("reading_dhan_credentials", token_file=token_file)
    
    if not os.path.exists(token_file):
        logger.error("dhan_token_file_not_found", token_file=token_file)
        raise FileNotFoundError(
            f"Dhan token file not found: {token_file}. "
            "Please authenticate via web interface: https://zopilot.in/api/dhan/login"
        )
    
    try:
        with open(token_file, 'r') as f:
            data = json.load(f)
        
        access_token = data.get('access_token')
        client_id = data.get('client_id')
        
        if not access_token or not client_id:
            raise ValueError("Token file must contain 'access_token' and 'client_id'")
        
        # Check expiry if present
        expiry = data.get('expiry')
        if expiry:
            try:
                expiry_dt = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
                if datetime.now(expiry_dt.tzinfo) > expiry_dt:
                    logger.warning(
                        "dhan_token_expired",
                        expiry=expiry,
                        message="Token may have expired. Please re-authenticate."
                    )
            except Exception as e:
                logger.warning("expiry_parse_failed", error=str(e))
        
        logger.info(
            "dhan_credentials_loaded",
            client_id=client_id,
            token_length=len(access_token)
        )
        
        return {
            'access_token': access_token,
            'client_id': client_id
        }
    
    except json.JSONDecodeError as e:
        logger.error("invalid_json_in_token_file", error=str(e))
        raise ValueError(f"Invalid JSON in token file: {e}")
    
    except Exception as e:
        logger.error("failed_to_read_credentials", error=str(e))
        raise


def check_dhan_token_validity(token_file: Optional[str] = None) -> bool:
    """
    Check if Dhan token file exists and is valid
    
    Args:
        token_file: Path to token file (optional)
    
    Returns:
        bool: True if token is valid, False otherwise
    """
    try:
        credentials = get_dhan_credentials(token_file)
        return bool(credentials.get('access_token') and credentials.get('client_id'))
    except Exception:
        return False


def get_websocket_url(access_token: str, client_id: str, version: int = 2, auth_type: int = 2) -> str:
    """
    Construct Dhan WebSocket URL with authentication parameters
    
    Args:
        access_token: Dhan access token
        client_id: Dhan client ID
        version: API version (default: 2)
        auth_type: Authentication type (default: 2)
    
    Returns:
        str: Complete WebSocket URL
    """
    return f"wss://api-feed.dhan.co?version={version}&token={access_token}&clientId={client_id}&authType={auth_type}"
