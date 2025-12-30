"""
Dhan API Authentication Manager
Handles token generation, storage, and auto-refresh using API key/secret
"""

import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict
import requests


class DhanAuthManager:
    """Manages Dhan API authentication with auto-refresh"""
    
    def __init__(self, 
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 client_id: Optional[str] = None,
                 token_file: str = './data/dhan_token.json'):
        """
        Initialize auth manager
        
        Args:
            api_key: Dhan API key (from environment or direct)
            api_secret: Dhan API secret
            client_id: Dhan client ID
            token_file: Path to store token data
        """
        self.api_key = api_key or os.getenv('DHAN_API_KEY')
        self.api_secret = api_secret or os.getenv('DHAN_API_SECRET')
        self.client_id = client_id or os.getenv('DHAN_CLIENT_ID')
        
        # Try multiple token file locations (VPS absolute path, relative path, /app/data for containers)
        possible_paths = [
            token_file,
            '/opt/tradingapp/data/dhan_token.json',  # VPS host path
            '/app/data/dhan_token.json'  # Docker container path
        ]
        
        self.token_file = None
        for path in possible_paths:
            p = Path(path)
            if p.exists():
                self.token_file = p
                break
        
        # If no existing file found, use the provided path (will be created)
        if not self.token_file:
            self.token_file = Path(token_file)
        
        # Ensure token directory exists
        self.token_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Token data
        self.access_token = None
        self.token_expiry = None
        
    def get_valid_token(self) -> str:
        """
        Get a valid access token (from cache or generate new)
        
        Returns:
            Valid access token
        """
        # Try to load from file
        if self._load_token_from_file():
            if self._is_token_valid():
                print(f"✓ Using cached token (expires: {self.token_expiry.strftime('%Y-%m-%d %H:%M:%S')})")
                return self.access_token
        
        # Token expired or doesn't exist - refresh or regenerate
        print("Token expired or missing, generating new token...")
        
        if self.api_key and self.api_secret:
            # Use API key flow
            self._generate_token_with_api_key()
        else:
            # Fall back to manual token from environment
            manual_token = os.getenv('DHAN_ACCESS_TOKEN')
            if manual_token:
                print("⚠ Using manual access token from environment (24-hour validity)")
                self.access_token = manual_token
                self.token_expiry = datetime.now() + timedelta(hours=23)
                self._save_token_to_file()
            else:
                raise ValueError("No authentication method available. Provide API_KEY/SECRET or ACCESS_TOKEN")
        
        return self.access_token
    
    def _load_token_from_file(self) -> bool:
        """Load token from file if exists"""
        if not self.token_file.exists():
            return False
        
        try:
            with open(self.token_file, 'r') as f:
                data = json.load(f)
            
            self.access_token = data.get('access_token')
            expiry_str = data.get('expiry')
            
            # Load client_id from file if not set
            if not self.client_id:
                self.client_id = data.get('client_id')
            
            if self.access_token and expiry_str:
                self.token_expiry = datetime.fromisoformat(expiry_str)
                return True
        except Exception as e:
            print(f"Error loading token file: {e}")
        
        return False
    
    def _save_token_to_file(self):
        """Save token to file"""
        try:
            data = {
                'access_token': self.access_token,
                'expiry': self.token_expiry.isoformat() if self.token_expiry else None,
                'client_id': self.client_id,
                'generated_at': datetime.now().isoformat()
            }
            
            with open(self.token_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"✓ Token saved to {self.token_file}")
        except Exception as e:
            print(f"Warning: Could not save token to file: {e}")
    
    def _is_token_valid(self) -> bool:
        """Check if token is still valid (with 1-hour buffer)"""
        if not self.access_token or not self.token_expiry:
            return False
        
        # Consider token invalid if less than 1 hour remaining
        buffer = timedelta(hours=1)
        return datetime.now() + buffer < self.token_expiry
    
    def _generate_token_with_api_key(self):
        """
        Generate access token using API key/secret flow
        
        This requires the tokenId from the OAuth flow.
        For automated systems, tokenId should be obtained once and reused.
        """
        # Check if we have a stored tokenId
        token_id = self._get_stored_token_id()
        
        if token_id:
            # Try to consume consent with existing tokenId
            if self._consume_consent(token_id):
                return
        
        # If no tokenId or consumption failed, need to do full OAuth flow
        print("⚠ Full OAuth flow required - this needs manual browser login once")
        print("Steps:")
        print("1. Generate consent")
        print("2. Open browser URL and login")
        print("3. Save tokenId for future use")
        
        # Generate consent
        consent_app_id = self._generate_consent()
        if not consent_app_id:
            raise Exception("Failed to generate consent")
        
        # Provide login URL
        login_url = f"https://auth.dhan.co/login/consentApp-login?consentAppId={consent_app_id}"
        print(f"\n{'='*80}")
        print(f"MANUAL STEP REQUIRED (one-time)")
        print(f"{'='*80}")
        print(f"\n1. Open this URL in browser:\n   {login_url}")
        print(f"\n2. Login with your Dhan credentials")
        print(f"\n3. After successful login, you'll be redirected with tokenId")
        print(f"\n4. Copy the tokenId from URL and set as environment variable:")
        print(f"   export DHAN_TOKEN_ID=<your_token_id>")
        print(f"\n5. Restart the service")
        print(f"\n{'='*80}\n")
        
        raise Exception("Manual OAuth step required. Follow instructions above.")
    
    def _generate_consent(self) -> Optional[str]:
        """Generate consent for API key auth"""
        try:
            response = requests.post(
                f'https://auth.dhan.co/app/generate-consent?client_id={self.client_id}',
                headers={
                    'app_id': self.api_key,
                    'app_secret': self.api_secret
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                consent_app_id = data.get('consentAppId')
                print(f"✓ Consent generated: {consent_app_id}")
                return consent_app_id
            else:
                print(f"✗ Consent generation failed: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"✗ Error generating consent: {e}")
            return None
    
    def _consume_consent(self, token_id: str) -> bool:
        """Consume consent to get access token"""
        try:
            response = requests.get(
                f'https://auth.dhan.co/app/consumeApp-consent?tokenId={token_id}',
                headers={
                    'app_id': self.api_key,
                    'app_secret': self.api_secret
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('accessToken')
                expiry_str = data.get('expiryTime')
                
                if self.access_token and expiry_str:
                    # Parse expiry: "2025-09-23T12:37:23"
                    self.token_expiry = datetime.fromisoformat(expiry_str)
                    self._save_token_to_file()
                    print(f"✓ Access token obtained (expires: {expiry_str})")
                    return True
            else:
                print(f"✗ Token consumption failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"✗ Error consuming consent: {e}")
            return False
    
    def _get_stored_token_id(self) -> Optional[str]:
        """Get tokenId from environment or file"""
        # Check environment first
        token_id = os.getenv('DHAN_TOKEN_ID')
        if token_id:
            return token_id
        
        # Check token file for stored tokenId
        if self.token_file.exists():
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                return data.get('token_id')
            except:
                pass
        
        return None
    
    def refresh_token(self) -> bool:
        """
        Refresh access token (if supported by current token)
        
        Returns:
            True if refresh successful
        """
        if not self.access_token:
            return False
        
        try:
            response = requests.post(
                'https://api.dhan.co/v2/RenewToken',
                headers={
                    'access-token': self.access_token,
                    'dhanClientId': self.client_id
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                new_token = data.get('accessToken')
                
                if new_token:
                    self.access_token = new_token
                    self.token_expiry = datetime.now() + timedelta(hours=24)
                    self._save_token_to_file()
                    print(f"✓ Token refreshed successfully")
                    return True
            else:
                print(f"Token refresh failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error refreshing token: {e}")
            return False


def get_dhan_credentials() -> Dict[str, str]:
    """
    Get Dhan credentials for WebSocket connection
    
    Returns:
        Dictionary with access_token and client_id
    """
    auth_manager = DhanAuthManager()
    
    try:
        access_token = auth_manager.get_valid_token()
        return {
            'access_token': access_token,
            'client_id': auth_manager.client_id
        }
    except Exception as e:
        print(f"Authentication failed: {e}")
        raise


# Example usage
if __name__ == '__main__':
    print("Testing Dhan Authentication...")
    
    auth = DhanAuthManager()
    token = auth.get_valid_token()
    
    print(f"\nAccess Token: {token[:50]}...")
    print(f"Client ID: {auth.client_id}")
    print(f"Token Expiry: {auth.token_expiry}")
