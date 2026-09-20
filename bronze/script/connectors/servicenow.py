"""
ServiceNow REST API Connector — AWS Glue Data Pipeline.

Required Secrets Manager keys:
  auth_type    : 'basic' or 'oauth'
  api_base_url : ServiceNow instance URL (e.g. https://mycompany.service-now.com)
  username     : (when auth_type=basic) ServiceNow username
  password     : (when auth_type=basic) ServiceNow password
  token_url    : (when auth_type=oauth) OAuth token endpoint
  client_id    : (when auth_type=oauth) Client identifier
  client_secret: (when auth_type=oauth) Client secret
  batch_size   : Records per page (default in config)

Required bronze_config.json keys (source_systems.servicenow):
  base_url             : Fallback instance URL if not in secret
  response_records_key : JSON key containing records (typically 'result')
  batch_size           : Records per page
"""

import logging
from typing import Any, Callable, Dict, List, Optional

from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class ServiceNowConnector:
    """
    ServiceNow Table API connector. Supports delta extraction with offset pagination.
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
        Extracts rows from a ServiceNow table updated since last_load_date.

        Args:
            last_load_date:    Watermark timestamp — inclusive start.
            secret_dict:       Credentials from Secrets Manager.
            table_name:        ServiceNow table name (e.g. 'incident').
            source_config:     Source config block from bronze_config.json.
            custom_query:      Optional sysparm_query override from CLI.
            on_chunk_callback: Called with (records, part_number) per chunk.
            s3_chunk_size:     Records buffered before flushing via callback.
        """
        if not table_name or not table_name.strip():
            raise ValueError("ServiceNow connector: 'table_name' is required.")
        if not last_load_date or not last_load_date.strip():
            raise ValueError(
                f"ServiceNow connector: 'last_load_date' is required for table '{table_name}'."
            )

        config = source_config or {}

        base_url = secret_dict.get('api_base_url') or config.get('base_url')
        if not base_url:
            raise ValueError(
                f"ServiceNow connector for '{table_name}': Instance URL is not configured. "
                "Set 'api_base_url' in Secrets Manager, or 'base_url' in "
                "bronze_config.json (source_systems.servicenow.base_url)."
            )
        base_url = str(base_url).strip().rstrip('/')

        endpoint     = ConfigLoader.get_table_endpoint('servicenow', table_name, config)
        query_filter = ConfigLoader.get_table_query_filter(
            'servicenow', table_name, last_load_date, custom_query, config,
            upper_bound=config.get('upper_bound')
        )

        response_key = config.get('response_records_key')
        if not response_key:
            raise ValueError(
                f"ServiceNow connector for '{table_name}': 'response_records_key' is not set. "
                "Add 'response_records_key' (typically 'result') to "
                "bronze_config.json (source_systems.servicenow.response_records_key)."
            )

        batch_size = secret_dict.get('batch_size') or config.get('batch_size')
        if not batch_size:
            raise ValueError(
                f"ServiceNow connector for '{table_name}': 'batch_size' is not configured. "
                "Set 'batch_size' in Secrets Manager or in bronze_config.json "
                "(source_systems.servicenow.batch_size)."
            )
        limit = int(batch_size)

        all_records:    List[Dict[str, Any]] = []
        records_buffer: List[Dict[str, Any]] = []
        total = 0
        part  = 1
        offset = 0

        logger.info(
            f"[ServiceNow/{table_name}] Starting extraction from '{base_url}' | "
            f"filter: {query_filter} | page_size: {limit}"
        )

        while True:
            api_url = (
                f"{base_url}{endpoint}?"
                f"sysparm_query={query_filter}^ORDERBYsys_updated_on"
                f"&sysparm_limit={limit}&sysparm_offset={offset}"
            )
            response = HTTPClient.get(url=api_url, secret_dict=secret_dict)

            if not isinstance(response, dict) or response_key not in response:
                raise KeyError(
                    f"[ServiceNow/{table_name}] Response key '{response_key}' not found. "
                    f"Available keys: {list(response.keys()) if isinstance(response, dict) else type(response)}. "
                    "Update 'response_records_key' in bronze_config.json."
                )

            batch = response[response_key]
            if not isinstance(batch, list):
                raise TypeError(
                    f"[ServiceNow/{table_name}] Expected list at '{response_key}', got {type(batch)}."
                )

            total += len(batch)
            logger.info(f"[ServiceNow/{table_name}] offset={offset}: {len(batch)} records | total: {total}")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    on_chunk_callback(records_buffer, part)
                    records_buffer = []
                    part += 1
            else:
                all_records.extend(batch)

            if len(batch) < limit:
                break
            offset += limit

        if on_chunk_callback and records_buffer:
            on_chunk_callback(records_buffer, part)

        logger.info(f"[ServiceNow/{table_name}] Done. Total: {total}")
        return all_records if not on_chunk_callback else []
