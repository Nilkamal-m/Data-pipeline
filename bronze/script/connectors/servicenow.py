"""
ServiceNow REST API Connector for AWS Glue Data Pipeline.

Strict Policy: No hardcoded default URLs, credentials, or table names.
Raises explicit ValueError if any required parameter is missing to prevent wrong data extraction.
"""

import logging
from typing import List, Dict, Any, Optional, Callable
from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class ServiceNowConnector:
    """
    Dynamic ServiceNow REST API Connector using HTTP GET requests.
    Supports memory-safe streaming of large datasets via chunked callbacks.
    """

    @staticmethod
    def fetch_delta(
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Extracts raw records for any standard or custom ServiceNow table updated since last_load_date.
        Raises ValueError if required parameters or base URLs are missing.
        """
        if not table_name or not table_name.strip():
            raise ValueError("ServiceNow connector error: 'table_name' parameter is required and cannot be empty.")

        if not last_load_date or not last_load_date.strip():
            raise ValueError(f"ServiceNow connector error: 'last_load_date' is required for table '{table_name}'.")

        config = source_config or {}
        
        # Resolve Base URL strictly (no dummy hardcoded defaults)
        base_url = secret_dict.get('instance_url') or secret_dict.get('base_url') or config.get('base_url')
        if not base_url or not str(base_url).strip():
            raise ValueError(
                f"ServiceNow connector error for table '{table_name}': "
                f"Base URL ('base_url' / 'instance_url') is missing in Secrets Manager and bronze_config.json."
            )
        base_url = str(base_url).strip().rstrip('/')

        endpoint = ConfigLoader.get_table_endpoint('servicenow', table_name, config)
        query_filter = ConfigLoader.get_table_query_filter('servicenow', table_name, last_load_date, custom_query, config)
        
        response_key = config.get('response_records_key') or secret_dict.get('response_records_key') or 'result'
        limit = secret_dict.get('batch_size') or config.get('batch_size') or 1000
        limit = int(limit)

        target_table = table_name.strip()
        records_buffer = []
        all_records = []
        total_extracted = 0
        part_number = 1
        offset = 0
        has_more = True

        logger.info(f"Extracting ServiceNow table '{target_table}' from '{base_url}' (API limit: {limit}, chunk threshold: {s3_chunk_size})...")

        while has_more:
            query = f"sysparm_query={query_filter}^ORDERBYsys_updated_on&sysparm_limit={limit}&sysparm_offset={offset}"
            api_url = f"{base_url}{endpoint}?{query}"

            response = HTTPClient.get(
                url=api_url,
                secret_dict=secret_dict
            )

            if response_key not in response and not isinstance(response, list):
                raise KeyError(
                    f"ServiceNow response for table '{target_table}' missing expected records key '{response_key}'. "
                    f"Available response keys: {list(response.keys()) if isinstance(response, dict) else type(response)}"
                )

            batch = response.get(response_key, response) if isinstance(response, dict) else response
            if not isinstance(batch, list):
                raise TypeError(f"Expected records list for ServiceNow table '{target_table}', got: {type(batch)}")

            batch_count = len(batch)
            total_extracted += batch_count

            logger.info(f"ServiceNow offset {offset}: fetched {batch_count} records (Total extracted: {total_extracted})")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    logger.info(f"Chunk threshold reached ({len(records_buffer)} records). Flushing part {part_number} to S3...")
                    on_chunk_callback(records_buffer, part_number)
                    records_buffer = []
                    part_number += 1
            else:
                all_records.extend(batch)

            if batch_count < limit:
                has_more = False
                logger.info(f"Reached end of ServiceNow table '{target_table}' delta extraction.")
            else:
                offset += limit

        # Flush any remaining buffer to S3
        if on_chunk_callback and records_buffer:
            logger.info(f"Flushing final part {part_number} ({len(records_buffer)} records) to S3...")
            on_chunk_callback(records_buffer, part_number)
            records_buffer = []

        logger.info(f"Finished extraction for ServiceNow table '{target_table}'. Total records: {total_extracted}")
        return all_records if not on_chunk_callback else []
