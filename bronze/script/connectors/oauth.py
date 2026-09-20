"""
OAuth 2.0 Token Client — AWS Glue Data Pipeline Connectors.

Required keys in Secrets Manager:
  token_url    : Token endpoint URL  (e.g. https://api.example.com/oauth/token)
  grant_type   : 'client_credentials' | 'password' | 'refresh_token'
  client_id    : Client identifier
  client_secret: Client secret
  scope        : Optional OAuth scope string
  username     : Required when grant_type = 'password'
  password     : Required when grant_type = 'password'
  refresh_token: Required when grant_type = 'refresh_token'

Tokens are cached in-memory and reused until 60 seconds before expiry.
"""

import json
import time
import logging
import urllib.request
import urllib.parse
from typing import Any, Dict

logger = logging.getLogger(__name__)


class OAuth2Client:
    """Acquires and caches OAuth 2.0 access tokens."""

    _cache: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def get_access_token(cls, secret_dict: Dict[str, Any], force_refresh: bool = False) -> str:
        """
        Returns a valid access token, using cache when possible.

        Args:
            secret_dict:   Credentials from Secrets Manager. Must contain 'token_url'.
            force_refresh: Skip cache and acquire a new token.

        Raises:
            ValueError: If required keys are missing or the token response is invalid.
            RuntimeError: If the token endpoint returns an HTTP error.
        """
        token_url = secret_dict.get('token_url')
        if not token_url:
            raise ValueError(
                "OAuth 2.0 error: 'token_url' is missing in Secrets Manager. "
                "Add 'token_url' with the value of your OAuth token endpoint URL."
            )

        cache_key = f"{token_url}:{secret_dict.get('client_id', '')}:{secret_dict.get('username', '')}"

        if not force_refresh and cache_key in cls._cache:
            cached = cls._cache[cache_key]
            if time.time() < cached['expires_at'] - 60:
                return cached['access_token']

        grant_type = (secret_dict.get('grant_type') or 'client_credentials').lower()
        payload: Dict[str, str] = {'grant_type': grant_type}

        for key in ('client_id', 'client_secret', 'scope'):
            if secret_dict.get(key):
                payload[key] = secret_dict[key]

        if grant_type == 'password':
            for key in ('username', 'password'):
                if not secret_dict.get(key):
                    raise ValueError(
                        f"OAuth password grant requires '{key}' in Secrets Manager."
                    )
            payload['username'] = secret_dict['username']
            payload['password'] = secret_dict['password']

        elif grant_type == 'refresh_token':
            if not secret_dict.get('refresh_token'):
                raise ValueError(
                    "OAuth refresh_token grant requires 'refresh_token' in Secrets Manager."
                )
            payload['refresh_token'] = secret_dict['refresh_token']

        logger.info(f"Requesting OAuth 2.0 token from '{token_url}' (grant: {grant_type}).")
        req = urllib.request.Request(
            token_url,
            data=urllib.parse.urlencode(payload).encode('utf-8'),
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json',
                'User-Agent': 'AWS-Glue-Connector/1.0',
            },
            method='POST',
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                token_data = json.loads(resp.read().decode('utf-8'))
        except Exception as err:
            raise RuntimeError(
                f"OAuth 2.0 token request to '{token_url}' failed: {err}"
            ) from err

        access_token = token_data.get('access_token')
        if not access_token:
            raise ValueError(
                f"OAuth 2.0 response from '{token_url}' did not return 'access_token'. "
                "Verify 'client_id', 'client_secret', and 'grant_type' in Secrets Manager."
            )

        expires_in = int(token_data.get('expires_in', 3600))
        cls._cache[cache_key] = {
            'access_token': access_token,
            'expires_at': time.time() + expires_in,
        }
        logger.info(f"OAuth 2.0 token acquired (expires in {expires_in}s).")
        return access_token
