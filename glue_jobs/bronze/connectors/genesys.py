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
        table_name: str = "conversations",
        source_config: Dict[str, Any] = None,
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Extracts Genesys records updated since last_load_date.
        """
        config = source_config or {}
        base_url = secret_dict.get('api_base_url') or config.get('base_url', 'https://api.mypurecloud.com')
        base_url = base_url.rstrip('/')

        endpoint = ConfigLoader.get_table_endpoint('genesys', table_name, config)
        response_key = config.get('response_records_key', 'entities')

        target_entity = table_name.strip()
        records_buffer = []
        all_records = []
        total_extracted = 0
        part_number = 1
        page_number = 1
        page_size = secret_dict.get('batch_size') or config.get('batch_size', 100)
        has_more = True

        logger.info(f"Extracting Genesys entity '{target_entity}' (page size: {page_size}, chunk threshold: {s3_chunk_size})...")

        while has_more:
            encoded_date = urllib.parse.quote(last_load_date)
            extra_query = f"&{custom_query.strip()}" if (custom_query and custom_query.strip()) else ""
            api_url = f"{base_url}{endpoint}?pageSize={page_size}&pageNumber={page_number}&interval={encoded_date}{extra_query}"

            response = HTTPClient.get(
                url=api_url,
                secret_dict=secret_dict
            )

            batch = response.get(response_key, response.get('conversations', []))
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
