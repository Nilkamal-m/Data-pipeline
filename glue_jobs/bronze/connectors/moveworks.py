import logging
from typing import List, Dict, Any, Optional, Callable
import urllib.parse
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
        table_name: str = "interactions",
        source_config: Dict[str, Any] = None,
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Extracts Moveworks records updated since last_load_date.
        """
        config = source_config or {}
        base_url = secret_dict.get('api_base_url') or config.get('base_url', 'https://api.moveworks.ai')
        base_url = base_url.rstrip('/')

        endpoint = ConfigLoader.get_table_endpoint('moveworks', table_name, config)
        response_key = config.get('response_records_key', 'value')

        target_entity = table_name.strip()
        records_buffer = []
        all_records = []
        total_extracted = 0
        part_number = 1
        encoded_date = urllib.parse.quote(last_load_date)

        if custom_query and custom_query.strip():
            query_str = f"{custom_query.strip()} and updated_at gt '{encoded_date}'" if "updated_at" not in custom_query else custom_query.strip()
        else:
            query_str = f"updated_at gt '{encoded_date}'"

        next_url = f"{base_url}{endpoint}?$filter={query_str}"
        page_count = 0

        logger.info(f"Extracting Moveworks entity '{target_entity}' (chunk threshold: {s3_chunk_size})...")

        while next_url:
            page_count += 1
            response = HTTPClient.get(
                url=next_url,
                secret_dict=secret_dict
            )

            batch = response.get(response_key, response.get('records', []))
            batch_count = len(batch)
            total_extracted += batch_count

            logger.info(f"Moveworks entity '{target_entity}' [page {page_count}]: fetched {batch_count} records (Total: {total_extracted})")

            if on_chunk_callback:
                records_buffer.extend(batch)
                if len(records_buffer) >= s3_chunk_size:
                    logger.info(f"Chunk threshold reached ({len(records_buffer)} records). Flushing part {part_number} to S3...")
                    on_chunk_callback(records_buffer, part_number)
                    records_buffer = []
                    part_number += 1
            else:
                all_records.extend(batch)

            raw_next_link = (
                response.get('@odata.nextLink') or 
                response.get('next_link') or 
                (response.get('paging', {}).get('next') if isinstance(response.get('paging'), dict) else None)
            )

            if raw_next_link:
                if raw_next_link.startswith('/'):
                    next_url = f"{base_url}{raw_next_link}"
                else:
                    next_url = raw_next_link
            else:
                next_url = None
                logger.info(f"Reached end of Moveworks entity '{target_entity}' delta extraction.")

        if on_chunk_callback and records_buffer:
            logger.info(f"Flushing final part {part_number} ({len(records_buffer)} records) to S3...")
            on_chunk_callback(records_buffer, part_number)
            records_buffer = []

        logger.info(f"Finished extraction for Moveworks entity '{target_entity}'. Total records: {total_extracted}")
        return all_records if not on_chunk_callback else []
