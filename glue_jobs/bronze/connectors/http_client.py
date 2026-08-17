"""
HTTP GET Client Utility for AWS Glue REST API Connectors.

STRICT GUARANTEE: Exclusively executes HTTP GET requests.
No HTTP POST requests are ever used for authentication, payload queries, or API verification.
"""

import json
import time
import logging
import base64
import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class HTTPClient:
    """
    HTTP GET-only client for managing API authentication and data extractions.
    Uses standard library urllib (native to AWS Glue Python Shell).
    """

    @staticmethod
    def build_auth_header(secret_dict: Dict[str, Any]) -> Dict[str, str]:
        """
        Builds HTTP GET Authorization headers based on secret credentials provided in AWS Secrets Manager.
        Supports Bearer Tokens, API Keys, and Basic Auth — ALL via HTTP GET headers.

        Args:
            secret_dict (dict): Parsed secret JSON containing:
                                - api_token / bearer_token / token
                                - OR username & password
                                - OR api_key

        Returns:
            dict: Authorization header dictionary.
        """
        headers = {}

        # 1. Bearer Token / API Token
        token = secret_dict.get('api_token') or secret_dict.get('bearer_token') or secret_dict.get('token')
        if token:
            headers['Authorization'] = f"Bearer {token}"
            logger.info("Configured HTTP GET Authorization: Bearer Token")
            return headers

        # 2. HTTP Basic Authentication (Username + Password)
        username = secret_dict.get('username')
        password = secret_dict.get('password')
        if username and password:
            user_pass = f"{username}:{password}".encode('utf-8')
            b64_credentials = base64.b64encode(user_pass).decode('utf-8')
            headers['Authorization'] = f"Basic {b64_credentials}"
            logger.info("Configured HTTP GET Authorization: Basic Auth")
            return headers

        # 3. Custom API Key Header
        api_key = secret_dict.get('api_key')
        if api_key:
            header_name = secret_dict.get('api_key_header', 'x-api-key')
            headers[header_name] = api_key
            logger.info(f"Configured HTTP GET Authorization Header: {header_name}")
            return headers

        logger.warning("No explicit auth token/basic credentials found in secret_dict. Proceeding with standard headers.")
        return headers

    @staticmethod
    def get(
        url: str,
        secret_dict: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 3
    ) -> Any:
        """
        Executes strictly an HTTP GET request with retries and exponential backoff.

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

        # Inject Authorization headers built for HTTP GET
        auth_headers = HTTPClient.build_auth_header(secret_dict)
        request_headers.update(auth_headers)

        if headers:
            request_headers.update(headers)

        attempt = 0
        backoff_seconds = 2.0

        while attempt <= max_retries:
            attempt += 1
            # STRICT ENFORCEMENT: method='GET'
            req = urllib.request.Request(url, headers=request_headers, method='GET')

            try:
                logger.debug(f"Executing HTTP GET request (attempt {attempt}): {url}")
                with urllib.request.urlopen(req, timeout=60) as response:
                    res_body = response.read().decode('utf-8')
                    return json.loads(res_body) if res_body else {}

            except HTTPError as http_err:
                status_code = http_err.code

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
