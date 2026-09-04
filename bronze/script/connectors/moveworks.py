"""
Moveworks Records API Connector for AWS Glue Data Pipeline.

Key Features:
- Encodes OData querystring dictionaries using urllib.parse.urlencode.
- Enforces 500 max record limit per API call ($top=500).
- Applies a mandatory 10-second delay between subsequent API calls to comply with Moveworks rate limits.
- Automatically iterates via $skip offset until 100% of all delta records are extracted.
"""

import time
import logging
import urllib.parse
from typing import List, Dict, Any, Optional, Callable
from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class MoveworksConnector:
    """
    Connector for Moveworks Records API delta ingestion supporting memory-safe chunked S3 streaming.
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
        Extracts Moveworks records updated/created since last_load_date.
        Raises ValueError if required parameters or base URLs are missing.
        """
        if not table_name or not table_name.strip():
            raise ValueError("Moveworks connector error: 'table_name' parameter is required and cannot be empty.")

        if not last_load_date or not last_load_date.strip():
            raise ValueError(f"Moveworks connector error: 'last_load_date' is required for entity '{table_name}'.")

        config = source_config or {}

        # Resolve Base URL strictly (no dummy hardcoded defaults)
        base_url = secret_dict.get('api_base_url') or secret_dict.get('base_url') or config.get('base_url')
        if not base_url or not str(base_url).strip():
            raise ValueError(
                f"Moveworks connector error for entity '{table_name}': "
                f"Base URL ('base_url' / 'api_base_url') is missing in Secrets Manager and bronze_config.json."
            )
        base_url = str(base_url).strip().rstrip('/')

        endpoint = ConfigLoader.get_table_endpoint('moveworks', table_name, config)
        response_key = config.get('response_records_key') or secret_dict.get('response_records_key') or 'value'

        # Moveworks maximum limit per API call is 500 records
        limit = int(secret_dict.get('batch_size') or config.get('batch_size') or 500)
        limit = min(limit, 500)

        # Mandatory 10-second delay between API calls for rate limiting
        api_delay_seconds = int(config.get('api_delay_seconds') or secret_dict.get('api_delay_seconds') or 10)

        # Resolve Assistant-Name header (mandatory for Moveworks Assistant API)
        assistant_name = (
            secret_dict.get('assistant_name') or
            secret_dict.get('Assistant-Name') or
            config.get('assistant_name') or
            'acmecorp-conversations-rest-api'
        )
        custom_headers = {}
        if assistant_name:
            custom_headers['Assistant-Name'] = str(assistant_name).strip()

        target_entity = table_name.strip()
        records_buffer = []
        all_records = []
        total_extracted = 0
        part_number = 1
        skip = 0
        page_count = 0
        has_more = True

        # Resolve delta query filter (e.g., last_updated_time gt '...' or custom filter)
        query_filter = ConfigLoader.get_table_query_filter('moveworks', table_name, last_load_date, custom_query, config)

        logger.info(
            f"Extracting Moveworks entity '{target_entity}' from '{base_url}' via endpoint '{endpoint}' "
            f"(Assistant-Name: '{assistant_name}', Max records/call: {limit}, 10s delay, chunk threshold: {s3_chunk_size})..."
        )

        # Base endpoint URL (e.g. https://api.moveworks.ai/export/v1/records/{table_name} or /assistant/v1/...)
        url = f"{base_url.rstrip('/')}{endpoint}"

        while has_more:
            page_count += 1

            # Simplified query parameters matching Moveworks API specification
            query_params = {
                "$count": "true",
                "$orderby": "last_updated_time desc",
                "$top": str(limit),
                "$skip": str(skip)
            }
            if query_filter and str(query_filter).strip():
                query_params["$filter"] = str(query_filter).strip()

            # Construct full URL with encoded parameters
            full_url = url + "?" + urllib.parse.urlencode(query_params)
            logger.info(f"Fetching Moveworks records [Page {page_count}, $skip={skip}] from: {full_url}")

            response = HTTPClient.get(
                url=full_url,
                secret_dict=secret_dict,
                headers=custom_headers
            )

            # Resolve records array from response payload
            if response_key not in response and not isinstance(response, list):
                alt_keys = [k for k in ('value', 'records', 'data', 'entities') if k in response]
                if alt_keys:
                    batch = response[alt_keys[0]]
                else:
                    raise KeyError(
                        f"Moveworks response for entity '{target_entity}' missing expected records key '{response_key}'. "
                        f"Available response keys: {list(response.keys()) if isinstance(response, dict) else type(response)}"
                    )
            else:
                batch = response.get(response_key, response) if isinstance(response, dict) else response

            if not isinstance(batch, list):
                raise TypeError(f"Expected records list for Moveworks entity '{target_entity}', got: {type(batch)}")

            batch_count = len(batch)
            total_extracted += batch_count

            logger.info(f"Moveworks entity '{target_entity}' [Page {page_count}]: fetched {batch_count} records (Total extracted: {total_extracted})")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    logger.info(f"Chunk threshold reached ({len(records_buffer)} records). Flushing part {part_number} to S3...")
                    on_chunk_callback(records_buffer, part_number)
                    records_buffer = []
                    part_number += 1
            else:
                all_records.extend(batch)

            # Check if additional pages remain
            raw_next_link = (
                response.get('@odata.nextLink') or 
                response.get('next_link') or 
                (response.get('paging', {}).get('next') if isinstance(response.get('paging'), dict) else None)
            )

            # If full batch returned (500 records) or nextLink exists, iterate to next page after rate-limit delay
            if (batch_count >= limit or raw_next_link) and batch_count > 0:
                skip += limit
                logger.info(f"More records remain ({batch_count} returned). Pausing {api_delay_seconds} seconds for rate limiting before fetching next page ($skip={skip})...")
                time.sleep(api_delay_seconds)
            else:
                has_more = False
                logger.info(f"Reached end of Moveworks entity '{target_entity}' delta extraction. Total extracted: {total_extracted} records.")

        if on_chunk_callback and records_buffer:
            logger.info(f"Flushing final part {part_number} ({len(records_buffer)} records) to S3...")
            on_chunk_callback(records_buffer, part_number)
            records_buffer = []

        logger.info(f"Finished extraction for Moveworks entity '{target_entity}'. Total records: {total_extracted}")
        return all_records if not on_chunk_callback else []
