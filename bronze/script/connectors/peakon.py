"""
Workday Peakon Employee Voice REST API Connector — AWS Glue Data Pipeline.

Extracts survey answers, employees, engagement metrics, drivers, and segments from Peakon.
Supports both:
  1. Peakon Custom Application Access Token (automatically exchanged for JWT Bearer token).
  2. Pre-exchanged JWT Bearer token.

Cursor pagination via JSON:API links.next.
Delta extraction via responseAnsweredAt / updated timestamps.

Required Secrets Manager keys:
  api_token    : Peakon Custom Application Access Token (or JWT Bearer token)
  api_base_url : (Optional) Peakon instance base URL (default: https://api.peakon.com)
  batch_size   : (Optional) Max answers per page (default: 2000, max: 20000)

Required bronze_config.json keys (source_systems.peakon):
  base_url             : Fallback Peakon base URL if not in secret
  response_records_key : JSON key containing records (default: 'data')
  batch_size           : Records per page (default: 2000)
"""

import json
import time
import logging
import urllib.request
import urllib.parse
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


def _parse_ts(ts_val: Any) -> Optional[datetime]:
    """Parses various ISO and date string formats into timezone-aware UTC datetime."""
    if not ts_val:
        return None
    s = str(ts_val).strip()
    if s.endswith('Z') or s.endswith('z'):
        s = s[:-1] + '+00:00'
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


class PeakonConnector:
    """
    Workday Peakon Employee Voice API connector.
    Handles automatic JWT authentication, JSON:API unwrapping, delta date filtering,
    and cursor pagination via links.next.
    """

    _jwt_cache: Dict[str, Tuple[str, float]] = {}

    @classmethod
    def _get_bearer_token(cls, base_url: str, raw_token: str) -> str:
        """
        Returns a valid JWT Bearer token for Peakon API requests.
        If the token is already a JWT (starts with 'ey' and contains two dots), it is used directly.
        Otherwise, exchanges the Custom App Access Token at /api/v1/auth/application.
        """
        clean_token = raw_token.strip()

        # If already a JWT bearer token, use directly
        if clean_token.startswith('ey') and clean_token.count('.') == 2:
            return clean_token

        # Check in-memory token cache (valid for 24 hours, refreshed 60s before)
        now = time.time()
        if clean_token in cls._jwt_cache:
            cached_jwt, expires_at = cls._jwt_cache[clean_token]
            if now < expires_at - 60:
                return cached_jwt

        auth_url = f"{base_url.rstrip('/')}/api/v1/auth/application"
        payload = json.dumps({"token": clean_token, "persists": True}).encode("utf-8")
        req = urllib.request.Request(
            auth_url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "UAX-DataLake-Pipeline/1.0",
            },
            method="POST",
        )

        logger.info(f"Authenticating with Peakon at '{auth_url}'...")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp_data = json.loads(resp.read().decode("utf-8"))
        except HTTPError as http_err:
            body = ""
            try:
                body = http_err.read().decode("utf-8")
            except Exception:
                pass
            raise RuntimeError(
                f"Peakon authentication failed (HTTP {http_err.code}): {http_err.reason}. "
                f"Response body: {body}. Verify 'api_token' in AWS Secrets Manager."
            ) from http_err
        except Exception as err:
            raise RuntimeError(f"Peakon authentication error: {err}") from err

        jwt_token = resp_data.get("data", {}).get("id")
        if not jwt_token:
            raise ValueError(
                f"Peakon auth response missing 'data.id'. Received: {resp_data}"
            )

        # Cache token for 24 hours (persists=True session token)
        cls._jwt_cache[clean_token] = (jwt_token, now + 86400)
        logger.info("Successfully acquired Peakon JWT Bearer token.")
        return jwt_token

    @classmethod
    def _execute_request_with_retry(
        cls,
        url: str,
        bearer_token: str,
        max_retries: int = 4
    ) -> Dict[str, Any]:
        """Executes GET request with exponential backoff on HTTP 429 rate limits or transient errors."""
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Accept": "application/json",
            "User-Agent": "UAX-DataLake-Pipeline/1.0",
        }

        for attempt in range(1, max_retries + 1):
            req = urllib.request.Request(url, headers=headers, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except HTTPError as err:
                if err.code == 429:
                    retry_after = err.headers.get("Retry-After")
                    sleep_sec = int(retry_after) if retry_after and retry_after.isdigit() else (5 * attempt)
                    logger.warning(
                        f"Peakon rate limit (HTTP 429) hit on '{url}'. Backing off {sleep_sec}s (attempt {attempt}/{max_retries})..."
                    )
                    time.sleep(sleep_sec)
                    continue
                elif err.code in (500, 502, 503, 504) and attempt < max_retries:
                    sleep_sec = 2 ** attempt
                    logger.warning(
                        f"Peakon server error (HTTP {err.code}) on '{url}'. Retrying in {sleep_sec}s (attempt {attempt}/{max_retries})..."
                    )
                    time.sleep(sleep_sec)
                    continue
                else:
                    body = ""
                    try:
                        body = err.read().decode("utf-8")
                    except Exception:
                        pass
                    raise RuntimeError(
                        f"Peakon API call failed (HTTP {err.code}): {err.reason} on {url}. Body: {body}"
                    ) from err
            except (URLError, TimeoutError) as net_err:
                if attempt < max_retries:
                    sleep_sec = 2 ** attempt
                    logger.warning(f"Peakon network error: {net_err}. Retrying in {sleep_sec}s...")
                    time.sleep(sleep_sec)
                    continue
                raise

        raise RuntimeError(f"Peakon API call to '{url}' failed after {max_retries} attempts.")

    @classmethod
    def fetch_delta(
        cls,
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Extracts records from Workday Peakon updated since last_load_date.
        Supports full cold starts and incremental delta filtering.
        Streams records to S3 chunks via on_chunk_callback.
        """
        if not table_name or not table_name.strip():
            raise ValueError("Peakon connector: 'table_name' is required.")

        config = source_config or {}

        # 1. Resolve Credentials & Token
        api_token = (
            secret_dict.get("api_token")
            or secret_dict.get("token")
            or secret_dict.get("access_token")
            or secret_dict.get("bearer_token")
        )
        if not api_token:
            raise ValueError(
                "Peakon connector: Missing 'api_token' in AWS Secrets Manager. "
                "Store your Peakon Custom Application Access Token in Secrets Manager."
            )

        base_url = (
            secret_dict.get("api_base_url")
            or config.get("base_url")
            or "https://api.peakon.com"
        ).rstrip("/")

        bearer_token = cls._get_bearer_token(base_url, api_token)

        # 2. Resolve Table Endpoint, Response Key, and Pagination parameters
        endpoint = ConfigLoader.get_table_endpoint("peakon", table_name, config)
        response_key = config.get("response_records_key", "data")
        batch_size = int(secret_dict.get("batch_size") or config.get("batch_size", 2000))
        # Cap per_page at Peakon's API maximum of 20000
        batch_size = min(max(batch_size, 1), 20000)

        # Construct initial request URL
        clean_endpoint = endpoint.strip()
        if "?" in clean_endpoint:
            next_url = f"{base_url}{clean_endpoint}&per_page={batch_size}"
        else:
            next_url = f"{base_url}{clean_endpoint}?per_page={batch_size}"

        if custom_query and str(custom_query).strip():
            cq = str(custom_query).strip()
            separator = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{separator}{cq}"

        # 3. Setup Delta Filtering Timestamps
        is_full_load = (not last_load_date) or str(last_load_date).strip().startswith("1900")
        cutoff_dt = _parse_ts(last_load_date) if not is_full_load else None

        upper_bound_str = config.get("upper_bound")
        is_high_date = bool(
            upper_bound_str and (
                str(upper_bound_str).strip().startswith("9999")
                or str(upper_bound_str).strip().startswith("9998")
            )
        )
        upper_bound_dt = _parse_ts(upper_bound_str) if (upper_bound_str and not is_high_date) else None

        logger.info(
            f"[Peakon/{table_name}] Starting extraction from '{base_url}' | "
            f"Mode: {'FULL_LOAD' if is_full_load else 'DELTA'} | "
            f"since: {last_load_date} | upper_bound: {upper_bound_str or 'None'} | "
            f"response_records_key: {response_key} | batch_size: {batch_size}"
        )

        all_records: List[Dict[str, Any]] = []
        records_buffer: List[Dict[str, Any]] = []
        total_extracted = 0
        total_filtered_out = 0
        part_number = 1
        page_num = 1

        while next_url:
            logger.info(f"[Peakon/{table_name}] Page {page_num}: Fetching from {next_url}...")
            resp_data = cls._execute_request_with_retry(next_url, bearer_token)

            if not isinstance(resp_data, dict):
                raise TypeError(
                    f"[Peakon/{table_name}] Expected dict response from API, got {type(resp_data)}."
                )

            if response_key not in resp_data:
                raise KeyError(
                    f"[Peakon/{table_name}] Response key '{response_key}' not found in API response. "
                    f"Available top-level keys: {list(resp_data.keys())}. "
                    "Update 'response_records_key' in bronze_config.json (source_systems.peakon.response_records_key)."
                )

            # Response JSON:API data array extracted via configured response_key
            raw_items = resp_data[response_key]
            if isinstance(raw_items, dict):
                raw_items = [raw_items]
            elif not isinstance(raw_items, list):
                raw_items = []

            page_valid_records = []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue

                # Unwrap JSON:API structure: id + type + attributes
                attrs = item.get("attributes")
                record: Dict[str, Any] = {
                    "id": item.get("id"),
                    "type": item.get("type", table_name),
                }
                if isinstance(attrs, dict):
                    record.update(attrs)
                else:
                    record.update(item)

                # Delta date filtering
                # Check candidate timestamp fields
                rec_ts_str = (
                    record.get("responseAnsweredAt")
                    or record.get("updated_at")
                    or record.get("updatedAt")
                    or record.get("created_at")
                    or record.get("createdAt")
                    or record.get("employmentStart")
                )

                if not is_full_load and cutoff_dt and rec_ts_str:
                    rec_dt = _parse_ts(rec_ts_str)
                    if rec_dt and rec_dt < cutoff_dt:
                        total_filtered_out += 1
                        continue

                if upper_bound_dt and rec_ts_str:
                    rec_dt = _parse_ts(rec_ts_str)
                    if rec_dt and rec_dt > upper_bound_dt:
                        total_filtered_out += 1
                        continue

                page_valid_records.append(record)

            total_extracted += len(page_valid_records)
            logger.info(
                f"[Peakon/{table_name}] Page {page_num}: Processed {len(raw_items)} items "
                f"({len(page_valid_records)} accepted, {total_extracted} total accepted so far)."
            )

            # Buffer and stream via callback
            if on_chunk_callback:
                records_buffer.extend(page_valid_records)
                if len(records_buffer) >= s3_chunk_size:
                    on_chunk_callback(records_buffer, part_number)
                    part_number += 1
                    records_buffer.clear()
            else:
                all_records.extend(page_valid_records)

            # Follow JSON:API links.next cursor
            links = resp_data.get("links", {})
            next_url = links.get("next") if isinstance(links, dict) else None
            page_num += 1

        # Flush any remaining buffered records to S3 callback
        if on_chunk_callback and records_buffer:
            on_chunk_callback(records_buffer, part_number)
            records_buffer.clear()

        logger.info(
            f"[Peakon/{table_name}] Extraction completed: {total_extracted} records accepted "
            f"({total_filtered_out} records skipped prior to watermark '{last_load_date}')."
        )

        return all_records if not on_chunk_callback else []
