"""
HTTP Client — AWS Glue Data Pipeline Connectors.

Authentication: set 'auth_type' in Secrets Manager.
  auth_type = 'basic'   → requires 'username', 'password'
  auth_type = 'oauth'   → requires 'token_url' (+ 'client_id', 'client_secret', 'grant_type')
  auth_type = 'oauth2'  → alias for 'oauth'
  auth_type = 'api_key' → requires 'api_key'; optional 'api_key_header' (default: 'x-api-key')

Retry policy (built-in, no configuration required):
  HTTP 429, 500, 502, 503, 504 → retried up to max_retries with exponential backoff.
  Respects 'Retry-After' header on HTTP 429.
  HTTP 401 with OAuth → refreshes token once, then retries.
"""

import gzip
import json
import time
import logging
import base64
import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
from typing import Any, Dict, Optional

from .oauth import OAuth2Client

logger = logging.getLogger(__name__)

_VALID_AUTH_TYPES = ('basic', 'oauth', 'oauth2', 'api_key')


class HTTPClient:
    """HTTP GET client with authentication, retry, and exponential backoff."""

    @staticmethod
    def _build_auth_header(
        secret_dict: Dict[str, Any],
        force_oauth_refresh: bool = False,
    ) -> Dict[str, str]:
        """
        Builds the Authorization header from secret_dict.

        Canonical key: 'auth_type' in Secrets Manager.
        If 'auth_type' is absent, auto-detects from credential keys and logs a warning.
        Supports 'basic', 'oauth', 'oauth2', 'api_key'.
        """
        raw_auth_type = str(secret_dict.get('auth_type', '')).lower().strip()

        if not raw_auth_type:
            if secret_dict.get('token_url') or secret_dict.get('grant_type'):
                raw_auth_type = 'oauth2'
            elif secret_dict.get('username') and secret_dict.get('password'):
                raw_auth_type = 'basic'
            elif secret_dict.get('api_key'):
                raw_auth_type = 'api_key'
            else:
                raise ValueError(
                    "Secrets Manager is missing required key 'auth_type'. "
                    f"Add 'auth_type' with one of: {_VALID_AUTH_TYPES}."
                )
            logger.warning(
                f"'auth_type' not set in Secrets Manager — auto-detected as '{raw_auth_type}'. "
                f"Set 'auth_type': '{raw_auth_type}' explicitly to remove this warning."
            )

        # Normalize aliases: oauth2/oauth_2/oauth-2 -> oauth, apikey/api-key -> api_key
        if raw_auth_type in ('oauth', 'oauth2', 'oauth_2', 'oauth-2'):
            auth_type = 'oauth'
        elif raw_auth_type in ('api_key', 'apikey', 'api-key'):
            auth_type = 'api_key'
        elif raw_auth_type == 'basic':
            auth_type = 'basic'
        else:
            raise ValueError(
                f"Invalid 'auth_type': '{raw_auth_type}' in Secrets Manager. "
                f"Allowed values: {_VALID_AUTH_TYPES}."
            )

        if auth_type == 'basic':
            username = secret_dict.get('username')
            password = secret_dict.get('password')
            if not username:
                raise ValueError("auth_type=basic requires 'username' in Secrets Manager.")
            if not password:
                raise ValueError("auth_type=basic requires 'password' in Secrets Manager.")
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {'Authorization': f'Basic {encoded}'}

        if auth_type == 'oauth':
            token = OAuth2Client.get_access_token(secret_dict, force_refresh=force_oauth_refresh)
            return {'Authorization': f'Bearer {token}'}

        # api_key
        api_key = secret_dict.get('api_key')
        if not api_key:
            raise ValueError("auth_type=api_key requires 'api_key' in Secrets Manager.")
        header_name = secret_dict.get('api_key_header', 'x-api-key')
        return {header_name: api_key}

    @staticmethod
    def get(
        url: str,
        secret_dict: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
    ) -> Any:
        """
        Performs an authenticated HTTP GET request.

        Args:
            url:         Full request URL.
            secret_dict: Credentials from Secrets Manager.
            headers:     Additional headers (e.g. Assistant-Name for Moveworks).
            max_retries: Number of retries on transient errors (default: 3).

        Returns:
            Parsed JSON response (dict or list).

        Raises:
            HTTPError:  On non-retryable HTTP errors.
            URLError:   If network errors persist after all retries.
        """
        request_headers: Dict[str, str] = {
            'Accept': 'application/json',
            'User-Agent': 'AWS-Glue-Connector/1.0',
        }
        request_headers.update(HTTPClient._build_auth_header(secret_dict))
        if headers:
            request_headers.update(headers)

        backoff = 2.0
        oauth_refreshed = False
        max_429_retries = 10
        retries_429 = 0
        attempt = 0

        while True:
            attempt += 1
            req = urllib.request.Request(
                url.replace(' ', '%20'),
                headers=request_headers,
                method='GET',
            )
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                    body = gzip.decompress(raw) if resp.info().get('Content-Encoding') == 'gzip' else raw
                    return json.loads(body.decode('utf-8')) if body else {}

            except HTTPError as err:
                code = err.code

                if code == 401 and not oauth_refreshed:
                    logger.warning("HTTP 401 — refreshing OAuth token and retrying.")
                    oauth_refreshed = True
                    request_headers.update(
                        HTTPClient._build_auth_header(secret_dict, force_oauth_refresh=True)
                    )
                    continue

                if code == 429:
                    retries_429 += 1
                    if retries_429 <= max_429_retries:
                        retry_after = err.headers.get('Retry-After')
                        if retry_after and retry_after.isdigit():
                            wait = float(retry_after)
                        else:
                            import random
                            wait = 60.0 + random.uniform(1.0, 2.0)
                        logger.warning(
                            f"HTTP 429 Rate Limited on '{url}'. Moveworks org rate limit reached. "
                            f"Sleeping {wait:.1f}s before retry (attempt {retries_429}/{max_429_retries})..."
                        )
                        time.sleep(wait)
                        continue
                    logger.error(f"HTTP 429: Exceeded max rate-limit retries ({max_429_retries}) for '{url}'.")
                    raise

                if code in (500, 502, 503, 504) and attempt <= max_retries:
                    retry_after = err.headers.get('Retry-After')
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else backoff
                    logger.warning(
                        f"HTTP {code} on attempt {attempt}/{max_retries}. "
                        f"Retrying in {wait:.1f}s..."
                    )
                    time.sleep(wait)
                    backoff *= 2.0
                    continue

                logger.error(f"HTTP {code} for '{url}': {err.reason}")
                raise

            except (URLError, TimeoutError) as err:
                if attempt <= max_retries:
                    logger.warning(
                        f"Network error on attempt {attempt}/{max_retries}: {err}. "
                        f"Retrying in {backoff:.1f}s..."
                    )
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                logger.error(f"Network error after {max_retries} retries: {err}")
                raise
