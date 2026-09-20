"""
Genesys Cloud REST API Connector — AWS Glue Data Pipeline.

Required Secrets Manager keys:
  auth_type    : 'oauth'
  api_base_url : Genesys Cloud API base URL (e.g. https://api.mypurecloud.com)
  token_url    : OAuth token endpoint
  client_id    : Client identifier
  client_secret: Client secret
  grant_type   : 'client_credentials'
  batch_size   : Records per page

Required bronze_config.json keys (source_systems.genesys):
  base_url             : Fallback API base URL if not in secret
  response_records_key : JSON key containing records (typically 'entities')
  batch_size           : Records per page
"""

import logging
import urllib.parse
from typing import Any, Callable, Dict, List, Optional

from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class GenesysConnector:
    """
    Genesys Cloud API connector. Supports delta extraction with page-number pagination.
    Streams large datasets via chunked S3 callbacks.
    """

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
        Extracts Genesys records updated since last_load_date.

        Args:
            last_load_date:    Watermark timestamp — inclusive start.
            secret_dict:       Credentials from Secrets Manager.
            table_name:        Genesys entity name (e.g. 'conversations').
            source_config:     Source config block from bronze_config.json.
            custom_query:      Optional extra query params appended to the URL.
            on_chunk_callback: Called with (records, part_number) per chunk.
            s3_chunk_size:     Records buffered before flushing via callback.
        """
        if not table_name or not table_name.strip():
            raise ValueError("Genesys connector: 'table_name' is required.")
        if not last_load_date or not last_load_date.strip():
            raise ValueError(
                f"Genesys connector: 'last_load_date' is required for entity '{table_name}'."
            )

        config = source_config or {}

        base_url = secret_dict.get('api_base_url') or config.get('base_url')
        if not base_url:
            raise ValueError(
                f"Genesys connector for '{table_name}': API base URL is not configured. "
                "Set 'api_base_url' in Secrets Manager, or 'base_url' in "
                "bronze_config.json (source_systems.genesys.base_url)."
            )
        base_url = str(base_url).strip().rstrip('/')

        endpoint     = ConfigLoader.get_table_endpoint('genesys', table_name, config)
        response_key = config.get('response_records_key')
        if not response_key:
            raise ValueError(
                f"Genesys connector for '{table_name}': 'response_records_key' is not set. "
                "Add 'response_records_key' (typically 'entities') to "
                "bronze_config.json (source_systems.genesys.response_records_key)."
            )

        batch_size = secret_dict.get('batch_size') or config.get('batch_size')
        if not batch_size:
            raise ValueError(
                f"Genesys connector for '{table_name}': 'batch_size' is not configured. "
                "Set 'batch_size' in Secrets Manager or in bronze_config.json "
                "(source_systems.genesys.batch_size)."
            )
        page_size = int(batch_size)

        all_records:    List[Dict[str, Any]] = []
        records_buffer: List[Dict[str, Any]] = []
        total    = 0
        part     = 1
        page_num = 1

        logger.info(
            f"[Genesys/{table_name}] Starting extraction from '{base_url}' | "
            f"since: {last_load_date} | page_size: {page_size}"
        )

        while True:
            encoded_date = urllib.parse.quote(last_load_date)
            extra = f"&{custom_query.strip()}" if (custom_query and custom_query.strip()) else ""
            api_url = (
                f"{base_url}{endpoint}?"
                f"pageSize={page_size}&pageNumber={page_num}&interval={encoded_date}{extra}"
            )

            response = HTTPClient.get(url=api_url, secret_dict=secret_dict)

            if not isinstance(response, dict) or response_key not in response:
                raise KeyError(
                    f"[Genesys/{table_name}] Response key '{response_key}' not found. "
                    f"Available keys: {list(response.keys()) if isinstance(response, dict) else type(response)}. "
                    "Update 'response_records_key' in bronze_config.json."
                )

            batch = response[response_key]
            if not isinstance(batch, list):
                raise TypeError(
                    f"[Genesys/{table_name}] Expected list at '{response_key}', got {type(batch)}."
                )

            total += len(batch)
            logger.info(f"[Genesys/{table_name}] Page {page_num}: {len(batch)} records | total: {total}")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    on_chunk_callback(records_buffer, part)
                    records_buffer = []
                    part += 1
            else:
                all_records.extend(batch)

            if len(batch) < page_size:
                break
            page_num += 1

        if on_chunk_callback and records_buffer:
            on_chunk_callback(records_buffer, part)

        logger.info(f"[Genesys/{table_name}] Done. Total: {total}")
        return all_records if not on_chunk_callback else []
