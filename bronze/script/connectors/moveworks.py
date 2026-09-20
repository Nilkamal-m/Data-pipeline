"""
Moveworks Records API Connector — AWS Glue Data Pipeline.

This connector is the ONLY place that contains Moveworks-specific logic:
  - Rate limiting (0s for interactions/conversations, 2s for users)
  - Cursor pagination via @odata.nextLink
  - Time-window sharding for parallel extraction
  - Thread-safe callback wrapping

The orchestrator (uax_bronze_load.py) calls only fetch_delta() and is
unaware of whether parallel or sequential execution was used.

Required Secrets Manager keys:
  auth_type    : 'oauth'
  token_url    : Moveworks OAuth token endpoint
  client_id    : Client identifier
  client_secret: Client secret
  grant_type   : 'client_credentials'
  api_base_url : Moveworks API base URL (e.g. https://api.moveworks.ai)
  batch_size   : Records per API page (max 500)

Required bronze_config.json keys (source_systems.moveworks):
  base_url          : Fallback API base URL if not in secret
  assistant_name    : Moveworks assistant identifier
  parallel_processing.enabled         : true | false
  parallel_processing.max_workers     : int (recommended: 5)
  parallel_processing.shard_window_days: int (recommended: 15)

Official API reference:
  https://docs.moveworks.com/ai-assistant/data-api/
"""

import time
import logging
import threading
import urllib.parse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)

# Inter-page delay per endpoint (seconds on HTTP 200 OK).
# Source: Official Moveworks sample script.
# Users requires 2s; all other entities (interactions, conversations, etc.) require 0s.
_PAGE_DELAY: Dict[str, float] = {
    'users': 2.0,
}


class MoveworksConnector:
    """
    Connector for the Moveworks Records API.

    Public method:  fetch_delta()  — called by uax_bronze_load.py (signature is frozen).
    Private methods: _fetch_parallel(), _fetch_single_window(), _build_shards().
    """

    # ── Public Interface ──────────────────────────────────────────────────────

    @staticmethod
    def fetch_delta(
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Extracts Moveworks records updated since last_load_date.

        Routes to parallel or sequential execution based on
        source_config['parallel_processing']['enabled'].
        The caller does not need to know which path is taken.

        Args:
            last_load_date:     Watermark timestamp — inclusive start (ISO 8601 UTC).
            secret_dict:        Credentials from AWS Secrets Manager.
            table_name:         Moveworks entity (e.g. 'interactions', 'users').
            source_config:      Source config block from bronze_config.json.
            custom_query:       Optional OData filter override from CLI.
            on_chunk_callback:  Called with (records, part_number) per chunk.
            s3_chunk_size:      Records buffered before flushing via callback.

        Returns:
            List of records when on_chunk_callback is None.
            Empty list when callback is provided (data is streamed).
        """
        if not table_name or not table_name.strip():
            raise ValueError("Moveworks connector: 'table_name' is required.")
        if not last_load_date or not last_load_date.strip():
            raise ValueError(
                f"Moveworks connector: 'last_load_date' is required for entity '{table_name}'."
            )

        config = source_config or {}

        # ── Resolve connection parameters ──────────────────────────────────────
        api_base_url = secret_dict.get('api_base_url') or config.get('base_url')
        if not api_base_url:
            raise ValueError(
                f"Moveworks connector for '{table_name}': API base URL is not configured. "
                "Set 'api_base_url' in Secrets Manager, or 'base_url' in "
                "bronze_config.json (source_systems.moveworks.base_url)."
            )
        base_url = str(api_base_url).strip().rstrip('/')

        assistant_name = config.get('assistant_name') or secret_dict.get('assistant_name')
        if not assistant_name:
            raise ValueError(
                f"Moveworks connector for '{table_name}': 'assistant_name' is not configured. "
                "Set 'assistant_name' in bronze_config.json "
                "(source_systems.moveworks.assistant_name)."
            )

        endpoint     = ConfigLoader.get_table_endpoint('moveworks', table_name, config)
        response_key = config.get('response_records_key')
        if not response_key:
            raise ValueError(
                f"Moveworks connector for '{table_name}': 'response_records_key' is not set. "
                "Add 'response_records_key' (typically 'value') to "
                "bronze_config.json (source_systems.moveworks.response_records_key)."
            )
        batch_size = secret_dict.get('batch_size') or config.get('batch_size')
        if not batch_size:
            raise ValueError(
                f"Moveworks connector for '{table_name}': 'batch_size' is not configured. "
                "Set 'batch_size' (max 500) in Secrets Manager or in bronze_config.json "
                "(source_systems.moveworks.batch_size)."
            )
        limit = min(int(batch_size), 500)

        custom_headers = {'Assistant-Name': str(assistant_name).strip()}

        # ── Route: parallel or sequential ─────────────────────────────────────
        parallel_cfg      = config.get('parallel_processing', {})
        parallel_enabled  = bool(parallel_cfg.get('enabled', False))
        max_workers       = int(parallel_cfg.get('max_workers', 5))
        shard_window_days = int(parallel_cfg.get('shard_window_days', 15))
        configured_ub     = config.get('upper_bound')
        upper_bound       = (
            str(configured_ub).strip()
            if configured_ub and str(configured_ub).strip()
            else datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        )

        if parallel_enabled and on_chunk_callback:
            logger.info(
                f"[{table_name}] PARALLEL | workers={max_workers} | "
                f"shard={shard_window_days}d | {last_load_date} → {upper_bound}"
            )
            MoveworksConnector._fetch_parallel(
                lower_bound=last_load_date,
                upper_bound=upper_bound,
                base_url=base_url,
                endpoint=endpoint,
                response_key=response_key,
                limit=limit,
                secret_dict=secret_dict,
                custom_headers=custom_headers,
                table_name=table_name,
                source_config=config,
                custom_query=custom_query,
                on_chunk_callback=on_chunk_callback,
                s3_chunk_size=s3_chunk_size,
                max_workers=max_workers,
                shard_window_days=shard_window_days,
            )
            return []

        logger.info(f"[{table_name}] SEQUENTIAL | lower_bound={last_load_date} | upper_bound={upper_bound}")
        return MoveworksConnector._fetch_single_window(
            lower_bound=last_load_date,
            upper_bound=upper_bound,
            base_url=base_url,
            endpoint=endpoint,
            response_key=response_key,
            limit=limit,
            secret_dict=secret_dict,
            custom_headers=custom_headers,
            table_name=table_name,
            source_config=config,
            custom_query=custom_query,
            on_chunk_callback=on_chunk_callback,
            s3_chunk_size=s3_chunk_size,
        )

    # ── Private: Parallel orchestration ───────────────────────────────────────

    @staticmethod
    def _fetch_parallel(
        lower_bound: str,
        upper_bound: str,
        base_url: str,
        endpoint: str,
        response_key: str,
        limit: int,
        secret_dict: Dict[str, Any],
        custom_headers: Dict[str, str],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str],
        on_chunk_callback: Callable[[List[Dict[str, Any]], int], None],
        s3_chunk_size: int,
        max_workers: int,
        shard_window_days: int,
    ) -> None:
        """
        Runs each time-window shard concurrently via ThreadPoolExecutor.

        Thread safety: wraps on_chunk_callback with an internal lock so the
        main file's S3-write callback is never called concurrently. Part numbers
        are globally sequential across all shards.
        """
        shards = MoveworksConnector._build_shards(lower_bound, upper_bound, shard_window_days)
        logger.info(f"[{table_name}] {len(shards)} shard(s) × {shard_window_days} days.")

        callback_lock = threading.Lock()
        part_counter  = [0]
        total_records = [0]

        def safe_callback(chunk: List[Dict[str, Any]], _: int) -> None:
            with callback_lock:
                part_counter[0]  += 1
                total_records[0] += len(chunk)
                on_chunk_callback(chunk, part_counter[0])

        errors: List[Tuple[int, str, str, str]] = []

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f'mw_{table_name[:8]}') as pool:
            future_map = {
                pool.submit(
                    MoveworksConnector._fetch_single_window,
                    lb, ub, base_url, endpoint, response_key, limit,
                    secret_dict, custom_headers, table_name, source_config,
                    custom_query, safe_callback, s3_chunk_size,
                ): (i, lb, ub)
                for i, (lb, ub) in enumerate(shards, start=1)
            }

            for future in as_completed(future_map):
                shard_id, lb, ub = future_map[future]
                try:
                    future.result()
                    logger.info(f"[{table_name}] Shard {shard_id}/{len(shards)} done | {lb} → {ub}")
                except Exception as err:
                    logger.error(
                        f"[{table_name}] Shard {shard_id}/{len(shards)} FAILED | {lb} → {ub} | {err}",
                        exc_info=True,
                    )
                    errors.append((shard_id, lb, ub, str(err)))

        if errors:
            windows = [(lb, ub) for _, lb, ub, _ in errors]
            raise RuntimeError(
                f"[{table_name}] {len(errors)} of {len(shards)} shard(s) failed. "
                f"Failed windows: {windows}. Successful shards are already staged."
            )

        logger.info(f"[{table_name}] All shards complete. Total records: {total_records[0]}.")

    # ── Private: Single time-window fetch ─────────────────────────────────────

    @staticmethod
    def _fetch_single_window(
        lower_bound: str,
        upper_bound: Optional[str],
        base_url: str,
        endpoint: str,
        response_key: str,
        limit: int,
        secret_dict: Dict[str, Any],
        custom_headers: Dict[str, str],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str],
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]],
        s3_chunk_size: int,
    ) -> List[Dict[str, Any]]:
        """
        Fetches all records for [lower_bound, upper_bound] using @odata.nextLink cursor pagination.

        Pagination: follows @odata.nextLink from the server response.
        First request sends query parameters; subsequent requests use the nextLink URL verbatim.
        No $skip offset arithmetic.

        Pacing: endpoint-specific delay from _PAGE_DELAY (0s for most, 2s for /users).
        HTTP 429/5xx backoff is handled by HTTPClient — not duplicated here.
        """
        query_filter = ConfigLoader.get_table_query_filter(
            source_system='moveworks',
            table_name=table_name,
            last_load_date=lower_bound,
            custom_query_cli=custom_query,
            source_config=source_config,
            upper_bound=upper_bound,
        )

        first_params = {'$orderby': 'last_updated_time desc', '$top': str(limit)}
        if query_filter and query_filter.strip():
            first_params['$filter'] = query_filter.strip()

        url = f"{base_url.rstrip('/')}{endpoint}"
        pacing = _PAGE_DELAY.get(table_name.strip().lower(), 0.0)

        all_records:    List[Dict[str, Any]] = []
        records_buffer: List[Dict[str, Any]] = []
        total = 0
        page  = 0
        part  = 1
        next_url: Optional[str] = url
        first_request = True

        logger.info(
            f"[{table_name}] lower_bound={lower_bound} | upper_bound={upper_bound or 'open'} | "
            f"pacing={pacing}s | filter: {query_filter}"
        )

        while next_url:
            page += 1
            full_url = (next_url + '?' + urllib.parse.urlencode(first_params)) if first_request else next_url
            first_request = False

            logger.info(f"[{table_name}] Page {page}: {full_url}")
            response = HTTPClient.get(url=full_url, secret_dict=secret_dict, headers=custom_headers)

            if isinstance(response, list):
                batch = response
            elif isinstance(response, dict):
                batch = response.get(response_key)
                if not isinstance(batch, list):
                    raise KeyError(
                        f"[{table_name}] Response key '{response_key}' not found or not a list. "
                        f"Available keys: {list(response.keys())}. "
                        f"Update 'response_records_key' in bronze_config.json."
                    )
            else:
                raise TypeError(f"[{table_name}] Unexpected API response type: {type(response)}")

            total += len(batch)
            logger.info(f"[{table_name}] Page {page}: {len(batch)} records | total: {total}")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    on_chunk_callback(records_buffer, part)
                    records_buffer = []
                    part += 1
            else:
                all_records.extend(batch)

            next_url = response.get('@odata.nextLink') if isinstance(response, dict) else None
            if next_url and pacing > 0:
                time.sleep(pacing)

        if on_chunk_callback and records_buffer:
            on_chunk_callback(records_buffer, part)

        logger.info(
            f"[{table_name}] Done | lower_bound={lower_bound} | "
            f"upper_bound={upper_bound or 'open'} | pages={page} | total={total}"
        )
        return all_records if not on_chunk_callback else []

    # ── Private: Timestamp parsing and shard builder ──────────────────────────

    @staticmethod
    def _parse_ts(ts: str) -> datetime:
        """
        Parses ISO-8601 or 'YYYY-MM-DD HH:MM:SS' string into a timezone-aware UTC datetime.
        Handles both '9999-01-01 00:00:00' and '9999-01-01T00:00:00Z'.
        """
        s = str(ts).strip()
        if s.endswith('Z') or s.endswith('z'):
            s = s[:-1] + '+00:00'
        elif ' ' in s and '+' not in s and '-' not in s[10:]:
            s = s.replace(' ', 'T') + '+00:00'
        elif 'T' in s and '+' not in s and '-' not in s[10:]:
            s = s + '+00:00'
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    @staticmethod
    def _build_shards(lower_bound: str, upper_bound: str, window_days: int) -> List[Tuple[str, str]]:
        """
        Divides [lower_bound, upper_bound] into non-overlapping windows of window_days.

        Each window: (shard_lower, shard_upper).
        Adjacent shards share a boundary — the ge/le filter ensures no overlap or gap.
        Caps open-ended or sentinel upper_bound (e.g. 9999-01-01 00:00:00) at current UTC time
        to prevent memory exhaustion from millions of shards.
        """
        fmt     = '%Y-%m-%dT%H:%M:%SZ'
        start   = MoveworksConnector._parse_ts(lower_bound)
        end     = MoveworksConnector._parse_ts(upper_bound)
        now_utc = datetime.now(timezone.utc)

        # Sentinel protection: cap open-ended upper bound (e.g. 9999-01-01) or future timestamps to current UTC time
        if end.year >= 9000 or end > now_utc:
            logger.info(
                f"Capping open-ended / future upper_bound ({upper_bound}) to current UTC time: "
                f"{now_utc.strftime(fmt)}"
            )
            end = now_utc

        step    = timedelta(days=window_days)
        shards: List[Tuple[str, str]] = []
        cursor  = start
        while cursor < end:
            shard_upper = min(cursor + step, end)
            shards.append((cursor.strftime(fmt), shard_upper.strftime(fmt)))
            cursor = shard_upper
        return shards
