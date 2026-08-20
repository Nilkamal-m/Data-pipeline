"""
Genesys Cloud REST API Connector for AWS Glue Data Pipeline.

Strict Policy: No hardcoded default URLs, credentials, or table names.
Raises explicit ValueError if any required parameter is missing to prevent wrong data extraction.
"""

import logging
from typing import List, Dict, Any, Optional, Callable
import urllib.parse
from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class GenesysConnector:
    """
    Connector for Genesys Cloud API delta ingestion supporting memory-safe chunked S3 streaming.
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
        Extracts Genesys records updated since last_load_date.
        Raises ValueError if required parameters or base URLs are missing.
        """
        if not table_name or not table_name.strip():
            raise ValueError("Genesys connector error: 'table_name' parameter is required and cannot be empty.")

        if not last_load_date or not last_load_date.strip():
            raise ValueError(f"Genesys connector error: 'last_load_date' is required for entity '{table_name}'.")

        config = source_config or {}

        # Resolve Base URL strictly (no dummy hardcoded defaults)
        base_url = secret_dict.get('api_base_url') or secret_dict.get('base_url') or config.get('base_url')
        if not base_url or not str(base_url).strip():
            raise ValueError(
                f"Genesys connector error for entity '{table_name}': "
                f"Base URL ('base_url' / 'api_base_url') is missing in Secrets Manager and bronze_config.json."
            )
        base_url = str(base_url).strip().rstrip('/')

        endpoint = ConfigLoader.get_table_endpoint('genesys', table_name, config)
        response_key = config.get('response_records_key') or secret_dict.get('response_records_key') or 'entities'
        page_size = secret_dict.get('batch_size') or config.get('batch_size') or 1000
        page_size = int(page_size)

        target_entity = table_name.strip()
        records_buffer = []
        all_records = []
        total_extracted = 0
        part_number = 1
        page_number = 1
        has_more = True

        logger.info(f"Extracting Genesys entity '{target_entity}' from '{base_url}' (page size: {page_size}, chunk threshold: {s3_chunk_size})...")

        while has_more:
            encoded_date = urllib.parse.quote(last_load_date)
            extra_query = f"&{custom_query.strip()}" if (custom_query and custom_query.strip()) else ""
            api_url = f"{base_url}{endpoint}?pageSize={page_size}&pageNumber={page_number}&interval={encoded_date}{extra_query}"

            response = HTTPClient.get(
                url=api_url,
                secret_dict=secret_dict
            )

            if response_key not in response and not isinstance(response, list):
                # Fallback check for common Genesys entity keys if standard response_key is absent
                alt_keys = [k for k in ('entities', 'conversations', 'users', 'queues') if k in response]
                if alt_keys:
                    batch = response[alt_keys[0]]
                else:
                    raise KeyError(
                        f"Genesys response for entity '{target_entity}' missing expected records key '{response_key}'. "
                        f"Available response keys: {list(response.keys()) if isinstance(response, dict) else type(response)}"
                    )
            else:
                batch = response.get(response_key, response) if isinstance(response, dict) else response

            if not isinstance(batch, list):
                raise TypeError(f"Expected records list for Genesys entity '{target_entity}', got: {type(batch)}")

            batch_count = len(batch)
            total_extracted += batch_count

            logger.info(f"Genesys entity '{target_entity}' [page {page_number}]: fetched {batch_count} records (Total: {total_extracted})")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    logger.info(f"Chunk threshold reached ({len(records_buffer)} records). Flushing part {part_number} to S3...")
                    on_chunk_callback(records_buffer, part_number)
                    records_buffer = []
                    part_number += 1
            else:
                all_records.extend(batch)

            if batch_count < page_size:
                has_more = False
                logger.info(f"Reached end of Genesys entity '{target_entity}' delta extraction.")
            else:
                page_number += 1

        if on_chunk_callback and records_buffer:
            logger.info(f"Flushing final part {part_number} ({len(records_buffer)} records) to S3...")
            on_chunk_callback(records_buffer, part_number)
            records_buffer = []

        logger.info(f"Finished extraction for Genesys entity '{target_entity}'. Total records: {total_extracted}")
        return all_records if not on_chunk_callback else []
