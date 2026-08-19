"""
OAuth 2.0 Authentication Helper for AWS Glue REST API Connectors.

Supports:
- OAuth 2.0 Client Credentials Grant (grant_type=client_credentials)
- OAuth 2.0 Resource Owner Password Grant (grant_type=password)
- OAuth 2.0 Refresh Token Grant (grant_type=refresh_token)
- Automatic Access Token Caching & Expiration Handling
"""

import json
import time
import logging
import urllib.request
import urllib.parse
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class OAuth2Client:
    """
    Handles OAuth 2.0 token acquisition, caching, and auto-refresh for REST API connectors.
    """
    _token_cache: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def get_access_token(cls, secret_dict: Dict[str, Any], force_refresh: bool = False) -> str:
        """
        Retrieves a valid OAuth 2.0 access token from cache or token endpoint.

        Args:
            secret_dict (dict): Secrets Manager credentials dictionary containing:
                                - token_url (required)
                                - client_id, client_secret
                                - grant_type ('client_credentials', 'password', 'refresh_token')
                                - username, password (if grant_type='password')
                                - refresh_token (if grant_type='refresh_token')
                                - scope (optional)
            force_refresh (bool): Force a new token request.

        Returns:
            str: Valid OAuth 2.0 access token string.
        """
        # Check if direct access_token or bearer_token is already provided
        direct_token = secret_dict.get('access_token') or secret_dict.get('bearer_token') or secret_dict.get('api_token')
        token_url = secret_dict.get('token_url')

        if not token_url:
            if direct_token:
                logger.info("Using static OAuth 2.0 / Bearer token provided in secret.")
                return direct_token
            raise ValueError("OAuth 2.0 configuration error: 'token_url' or 'access_token' must be specified in secret.")

        cache_key = f"{token_url}:{secret_dict.get('client_id', '')}:{secret_dict.get('username', '')}"

        # Return cached token if still valid
        if not force_refresh and cache_key in cls._token_cache:
            cached_data = cls._token_cache[cache_key]
            # Consider expired if within 60s of expiry
            if time.time() < cached_data['expires_at'] - 60:
                logger.info("Using cached valid OAuth 2.0 access token.")
                return cached_data['access_token']

        # Acquire new token
        logger.info(f"Acquiring new OAuth 2.0 access token from: '{token_url}'...")
        grant_type = secret_dict.get('grant_type', 'client_credentials').lower()

        payload = {'grant_type': grant_type}

        if secret_dict.get('client_id'):
            payload['client_id'] = secret_dict['client_id']
        if secret_dict.get('client_secret'):
            payload['client_secret'] = secret_dict['client_secret']
        if secret_dict.get('scope'):
            payload['scope'] = secret_dict['scope']

        if grant_type == 'password':
            if not secret_dict.get('username') or not secret_dict.get('password'):
                raise ValueError("OAuth 2.0 Password Grant requires 'username' and 'password'.")
            payload['username'] = secret_dict['username']
            payload['password'] = secret_dict['password']
        elif grant_type == 'refresh_token':
            if not secret_dict.get('refresh_token'):
                raise ValueError("OAuth 2.0 Refresh Token Grant requires 'refresh_token'.")
            payload['refresh_token'] = secret_dict['refresh_token']

        encoded_payload = urllib.parse.urlencode(payload).encode('utf-8')
        req = urllib.request.Request(
            token_url,
            data=encoded_payload,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
                'User-Agent': 'AWS-Glue-OAuth2-Client/1.0'
            },
            method='POST'
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                res_body = response.read().decode('utf-8')
                token_data = json.loads(res_body)

                access_token = token_data.get('access_token')
                if not access_token:
                    raise ValueError(f"OAuth 2.0 response from '{token_url}' did not contain 'access_token'. Response: {res_body}")

                expires_in = int(token_data.get('expires_in', 3600))
                cls._token_cache[cache_key] = {
                    'access_token': access_token,
                    'expires_at': time.time() + expires_in
                }
                logger.info(f"Successfully acquired OAuth 2.0 access token (expires in {expires_in}s).")
                return access_token

        except Exception as err:
            logger.error(f"Failed to acquire OAuth 2.0 token from '{token_url}': {err}")
            raise
