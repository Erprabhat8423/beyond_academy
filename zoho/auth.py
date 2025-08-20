import requests
import os
import logging
import time
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Token cache
_token_cache = {
    'access_token': None,
    'expires_at': None
}

def get_access_token(force_refresh=False):
    """
    Get access token for Zoho CRM API using refresh token with caching
    
    Args:
        force_refresh: Force refresh token even if cached token is valid
        
    Returns:
        str: Valid access token
    """
    try:
        # Check if we have a valid cached token
        if not force_refresh and _token_cache['access_token'] and _token_cache['expires_at']:
            if datetime.now() < _token_cache['expires_at']:
                logger.info("Using cached access token")
                return _token_cache['access_token']
        
        url = os.getenv("ZOHO_TOKEN_URL")
        refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
        client_id = os.getenv("ZOHO_CLIENT_ID")
        client_secret = os.getenv("ZOHO_CLIENT_SECRET")
        
        # Log the values (without sensitive data) for debugging
        logger.info(f"Requesting new token from: {url}")
        logger.info(f"Client ID: {client_id[:10]}..." if client_id else "Client ID: None")
        
        if not all([url, refresh_token, client_id, client_secret]):
            missing = []
            if not url: missing.append("ZOHO_TOKEN_URL")
            if not refresh_token: missing.append("ZOHO_REFRESH_TOKEN")
            if not client_id: missing.append("ZOHO_CLIENT_ID")
            if not client_secret: missing.append("ZOHO_CLIENT_SECRET")
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        payload = {
            'refresh_token': refresh_token,
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'refresh_token'
        }
        
        # Add retry logic for rate limiting
        max_retries = 3
        base_wait_time = 60  # Start with 60 seconds
        
        for attempt in range(max_retries):
            try:
                response = requests.post(url, data=payload, timeout=120)
                
                if response.status_code == 429 or (response.status_code == 400 and "too many requests" in response.text.lower()):
                    if attempt < max_retries - 1:
                        wait_time = base_wait_time * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Rate limited. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} attempts")
                        raise Exception(f"Rate limited by Zoho API. Please wait 15-30 minutes before retrying.")
                
                if response.status_code != 200:
                    logger.error(f"Token request failed with status {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    
                response.raise_for_status()
                break
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed, retrying in {base_wait_time} seconds: {e}")
                    time.sleep(base_wait_time)
                    continue
                else:
                    raise
        
        token_data = response.json()
        if 'access_token' not in token_data:
            logger.error(f"No access_token in response: {token_data}")
            raise ValueError("Invalid token response - no access_token found")
        
        # Cache the token (Zoho tokens typically expire in 1 hour)
        expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
        _token_cache['access_token'] = token_data['access_token']
        _token_cache['expires_at'] = datetime.now() + timedelta(seconds=expires_in - 300)  # Refresh 5 min early
        
        logger.info(f"Successfully obtained new access token (expires in {expires_in} seconds)")
        return token_data['access_token']
        
    except Exception as e:
        logger.error(f"Error getting access token: {e}")
        raise

def clear_token_cache():
    """Clear the cached token to force refresh on next request"""
    global _token_cache
    _token_cache = {
        'access_token': None,
        'expires_at': None
    }
    logger.info("Token cache cleared")
