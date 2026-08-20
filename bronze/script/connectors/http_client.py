"""
HTTP Client Utility for AWS Glue REST API Connectors.

Supports:
- HTTP Basic Authentication (username & password via Authorization: Basic <base64>)
- OAuth 2.0 Bearer Authentication (via OAuth2Client token manager and Authorization: Bearer <token>)
- Custom API Key Headers
- Automatic 401 Unauthorized Token Refresh & Retry
- Exponential Backoff for 429 & 5xx Rate Limits
"""

import json
import time
import logging
import base64
import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
from typing import Dict, Any, Optional
from .oauth import OAuth2Client

logger = logging.getLogger(__name__)


class HTTPClient:
    """
    HTTP GET client supporting Basic Auth, OAuth 2.0 (Bearer), and API Key headers.
    """

    @staticmethod
    def build_auth_header(secret_dict: Dict[str, Any], force_oauth_refresh: bool = False) -> Dict[str, str]:
        """
        Builds Authorization HTTP headers based on secret credentials.
        Dynamically determines whether to use Basic Auth or OAuth 2.0.

        Args:
            secret_dict (dict): Secrets Manager credentials dictionary.
            force_oauth_refresh (bool): Force refresh of OAuth 2.0 access token.

        Returns:
            dict: Authorization headers dictionary.
        """
        headers = {}
        auth_type = (secret_dict.get('auth_type') or '').lower()

        # 1. Explicit Basic Auth OR (username + password provided WITHOUT token_url/grant_type)
        is_basic = (
            auth_type in ('basic', 'basic_auth') or
            (
                secret_dict.get('username') and
                secret_dict.get('password') and
                not secret_dict.get('token_url') and
                not secret_dict.get('grant_type') and
                not secret_dict.get('auth_type')
            )
        )

        if is_basic:
            username = secret_dict.get('username', '')
            password = secret_dict.get('password', '')
            user_pass = f"{username}:{password}".encode('utf-8')
            b64_credentials = base64.b64encode(user_pass).decode('utf-8')
            headers['Authorization'] = f"Basic {b64_credentials}"
            logger.info(f"Configured HTTP Authorization: Basic Auth (User: '{username}')")
            return headers

        # 2. OAuth 2.0 or Bearer Token (token_url, grant_type, access_token, bearer_token)
        has_oauth = (
            auth_type in ('oauth', 'oauth2', 'bearer') or
            secret_dict.get('token_url') or
            secret_dict.get('grant_type') or
            secret_dict.get('access_token') or
            secret_dict.get('bearer_token')
        )

        if has_oauth:
            try:
                access_token = OAuth2Client.get_access_token(secret_dict, force_refresh=force_oauth_refresh)
                headers['Authorization'] = f"Bearer {access_token}"
                logger.info("Configured HTTP Authorization: OAuth 2.0 Bearer Token")
                return headers
            except Exception as oauth_err:
                logger.warning(f"OAuth 2.0 token resolution failed ({oauth_err}). Trying fallback headers...")

        # 3. Custom API Key Header
        api_key = secret_dict.get('api_key')
        if api_key:
            header_name = secret_dict.get('api_key_header', 'x-api-key')
            headers[header_name] = api_key
            logger.info(f"Configured HTTP Authorization Header: {header_name}")
            return headers

        logger.warning("No explicit Basic Auth or OAuth 2.0 credentials found in secret_dict. Proceeding without Auth header.")
        return headers

    @staticmethod
    def get(
        url: str,
        secret_dict: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 3
    ) -> Any:
        """
        Executes an HTTP GET request with retries, exponential backoff, and 401 OAuth token refresh.

        Args:
            url (str): Target REST API endpoint URL.
            secret_dict (dict): Credentials dictionary from AWS Secrets Manager.
            headers (dict, optional): Custom HTTP headers.
            max_retries (int): Retry limit for transient errors.

        Returns:
            Any: Parsed JSON response body.
        """
        request_headers = {
            'Accept': 'application/json',
            'User-Agent': 'AWS-Glue-Python-Shell-Ingestion/1.0'
        }

        # Build initial Auth headers
        auth_headers = HTTPClient.build_auth_header(secret_dict)
        request_headers.update(auth_headers)

        if headers:
            request_headers.update(headers)

        attempt = 0
        backoff_seconds = 2.0
        oauth_refreshed = False

        while attempt <= max_retries:
            attempt += 1
            clean_url = url.replace(' ', '%20')
            req = urllib.request.Request(clean_url, headers=request_headers, method='GET')

            try:
                logger.debug(f"Executing HTTP GET request (attempt {attempt}): {url}")
                with urllib.request.urlopen(req, timeout=60) as response:
                    res_body = response.read().decode('utf-8')
                    return json.loads(res_body) if res_body else {}

            except HTTPError as http_err:
                status_code = http_err.code

                # Handle 401 Unauthorized (Trigger OAuth 2.0 token refresh once)
                if status_code == 401 and not oauth_refreshed and (secret_dict.get('token_url') or secret_dict.get('grant_type')):
                    logger.warning("HTTP 401 Unauthorized encountered. Refreshing OAuth 2.0 access token...")
                    oauth_refreshed = True
                    new_auth = HTTPClient.build_auth_header(secret_dict, force_oauth_refresh=True)
                    request_headers.update(new_auth)
                    time.sleep(1.0)
                    continue

                if status_code in (429, 500, 502, 503, 504) and attempt <= max_retries:
                    retry_after = http_err.headers.get('Retry-After')
                    sleep_time = float(retry_after) if (retry_after and retry_after.isdigit()) else backoff_seconds
                    logger.warning(f"HTTP GET {status_code} encountered on attempt {attempt}/{max_retries}. Retrying in {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                    backoff_seconds *= 2.0
                else:
                    logger.error(f"HTTP GET Error {status_code} for URL '{url}': {http_err.reason}")
                    raise

            except (URLError, TimeoutError) as net_err:
                if attempt <= max_retries:
                    logger.warning(f"Network error '{net_err}' on HTTP GET attempt {attempt}/{max_retries}. Retrying in {backoff_seconds:.1f}s...")
                    time.sleep(backoff_seconds)
                    backoff_seconds *= 2.0
                else:
                    logger.error(f"Network error persisted on HTTP GET after {max_retries} attempts: {net_err}")
                    raise
