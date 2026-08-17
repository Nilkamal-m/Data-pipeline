import logging
from typing import List, Dict, Any, Optional, Callable
from .http_client import HTTPClient
from config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class ServiceNowConnector:
    """
    Dynamic ServiceNow REST API Connector using HTTP GET requests.
    Supports memory-safe streaming of large datasets (>1,000 to millions of records) via chunked callbacks.
    """

    @staticmethod
    def fetch_delta(
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str = "incident",
        source_config: Dict[str, Any] = None,
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Extracts raw records for any standard or custom ServiceNow table updated since last_load_date.

        Args:
            last_load_date (str): High-Water Mark ISO timestamp.
            secret_dict (dict): Secrets Manager credentials.
            table_name (str): Target ServiceNow table.
            source_config (dict, optional): Centralized configuration.
            custom_query (str, optional): Explicit custom query filter.
            on_chunk_callback (callable, optional): Streaming callback invoked when buffer reaches s3_chunk_size.
            s3_chunk_size (int): Max records per memory chunk before flushing to S3.

        Returns:
            list: List of extracted records if callback is None, or empty list if streamed via callback.
        """
        config = source_config or {}
        base_url = secret_dict.get('instance_url') or config.get('base_url', 'https://your-instance.service-now.com')
        base_url = base_url.rstrip('/')

        endpoint = ConfigLoader.get_table_endpoint('servicenow', table_name, config)
        query_filter = ConfigLoader.get_table_query_filter('servicenow', table_name, last_load_date, custom_query, config)
        response_key = config.get('response_records_key', 'result')

        # API limit per HTTP GET request (ServiceNow recommends 1,000)
        limit = secret_dict.get('batch_size') or config.get('batch_size', 1000)

        target_table = table_name.strip()
        records_buffer = []
        all_records = []
        total_extracted = 0
        part_number = 1
        offset = 0
        has_more = True

        logger.info(f"Extracting ServiceNow table '{target_table}' (API page size: {limit}, S3 chunk threshold: {s3_chunk_size})...")

        while has_more:
            query = f"sysparm_query={query_filter}^ORDERBYsys_updated_on&sysparm_limit={limit}&sysparm_offset={offset}"
            api_url = f"{base_url}{endpoint}?{query}"

            response = HTTPClient.get(
                url=api_url,
                secret_dict=secret_dict
            )

            batch = response.get(response_key, [])
            batch_count = len(batch)
            total_extracted += batch_count

            logger.info(f"ServiceNow offset {offset}: fetched {batch_count} records (Total extracted: {total_extracted})")

            if on_chunk_callback:
                records_buffer.extend(batch)
                # If buffer exceeds chunk threshold, flush to S3 immediately
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
