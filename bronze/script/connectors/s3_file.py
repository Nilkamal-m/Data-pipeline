"""
S3 File Source Connector: Extracts flat files, CSV, JSON, Text, or Parquet files
from external/source S3 buckets and streams records incrementally to Bronze.
"""

import logging
import json
import csv
import io
import boto3
from typing import Callable, Optional, Dict, Any
from datetime import datetime, timezone

logger = logging.getLogger("S3FileConnector")


class S3FileConnector:
    """
    Ingestion connector for reading files directly from external or internal S3 buckets.
    Supports CSV, Flat, Text, JSON, NDJSON, and Parquet file formats.
    """

    @classmethod
    def fetch_delta(
        cls,
        last_load_date: str,
        secret_dict: dict,
        table_name: str,
        source_config: dict,
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[list, int], None]] = None,
        s3_chunk_size: int = 10000
    ) -> None:
        """
        Scans source S3 bucket for new/updated files since last_load_date and streams records.
        """
        source_bucket = source_config.get('source_bucket')
        if not source_bucket:
            raise ValueError(f"S3 File Ingestion Error: 'source_bucket' must be configured in bronze_config.json for table '{table_name}'.")

        # Resolve file prefix or pattern for table
        prefix_template = source_config.get('file_prefix_template', 'raw/{table_name}/')
        file_prefix = prefix_template.format(table_name=table_name)
        
        table_patterns = source_config.get('table_file_patterns', {})
        file_pattern = table_patterns.get(table_name, '')

        file_format = (source_config.get('file_format') or 'csv').lower()
        delimiter = source_config.get('delimiter', ',')
        has_header = source_config.get('has_header', True)
        encoding = source_config.get('encoding', 'utf-8')

        # Initialize boto3 S3 client (uses cross-account AWS keys if provided in secret_dict, else IAM role)
        if secret_dict.get('aws_access_key_id') and secret_dict.get('aws_secret_access_key'):
            s3_src_client = boto3.client(
                's3',
                aws_access_key_id=secret_dict['aws_access_key_id'],
                aws_secret_access_key=secret_dict['aws_secret_access_key'],
                aws_session_token=secret_dict.get('aws_session_token')
            )
        else:
            s3_src_client = boto3.client('s3')

        # Parse HWM timestamp threshold
        try:
            hwm_dt = datetime.fromisoformat(last_load_date.replace('Z', '+00:00'))
        except Exception:
            hwm_dt = datetime.min.replace(tzinfo=timezone.utc)

        logger.info(f"Scanning source S3 bucket 's3://{source_bucket}/{file_prefix}' for files modified after {last_load_date}...")

        # Paginate through source bucket objects
        paginator = s3_src_client.get_paginator('list_objects_v2')
        matching_keys = []

        for page in paginator.paginate(Bucket=source_bucket, Prefix=file_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Skip directory markers
                if key.endswith('/'):
                    continue

                # Filter by file pattern if specified
                if file_pattern and not key.lower().endswith(file_pattern.lower()):
                    continue

                # Check modification date against High-Water Mark
                obj_mtime = obj['LastModified']
                if obj_mtime > hwm_dt:
                    matching_keys.append((key, obj_mtime))

        # Sort matching files by modification time
        matching_keys.sort(key=lambda x: x[1])
        logger.info(f"Found {len(matching_keys)} new/updated files in 's3://{source_bucket}/{file_prefix}'.")

        if not matching_keys:
            return

        records_accumulator = []
        part_number = 1

        for key, mtime in matching_keys:
            logger.info(f"Reading file: 's3://{source_bucket}/{key}' (Modified: {mtime})...")
            try:
                response = s3_src_client.get_object(Bucket=source_bucket, Key=key)
                body_bytes = response['Body'].read()

                records = cls.parse_file_bytes(
                    body_bytes=body_bytes,
                    file_format=file_format,
                    delimiter=delimiter,
                    has_header=has_header,
                    encoding=encoding,
                    source_key=key
                )

                records_accumulator.extend(records)

                # Flush accumulator when chunk threshold reached
                while len(records_accumulator) >= s3_chunk_size:
                    chunk = records_accumulator[:s3_chunk_size]
                    records_accumulator = records_accumulator[s3_chunk_size:]
                    if on_chunk_callback:
                        on_chunk_callback(chunk, part_number)
                    part_number += 1

            except Exception as err:
                logger.error(f"Error reading/parsing file 's3://{source_bucket}/{key}': {err}")
                raise

        # Flush remaining records
        if records_accumulator:
            if on_chunk_callback:
                on_chunk_callback(records_accumulator, part_number)

    @classmethod
    def parse_file_bytes(
        cls,
        body_bytes: bytes,
        file_format: str,
        delimiter: str = ",",
        has_header: bool = True,
        encoding: str = "utf-8",
        source_key: str = ""
    ) -> list:
        """
        Parses byte contents of a file into a list of record dictionaries.
        """
        fmt = file_format.strip().lower()
        text_content = body_bytes.decode(encoding, errors='replace')

        records = []

        if fmt in ('csv', 'flat', 'tsv', 'txt_delimited'):
            sep = '\t' if fmt == 'tsv' else delimiter
            lines = text_content.splitlines()
            if not lines:
                return []

            if has_header:
                reader = csv.DictReader(lines, delimiter=sep)
                for row in reader:
                    records.append(dict(row))
            else:
                reader = csv.reader(lines, delimiter=sep)
                for idx, row in enumerate(reader):
                    rec = {f"col_{i}": val for i, val in enumerate(row)}
                    records.append(rec)

        elif fmt in ('json', 'ndjson', 'jsonlines'):
            lines = text_content.splitlines()
            # Try newline-delimited JSON first
            is_ndjson = False
            for line in lines:
                line_str = line.strip()
                if not line_str:
                    continue
                try:
                    obj = json.loads(line_str)
                    if isinstance(obj, dict):
                        records.append(obj)
                        is_ndjson = True
                except Exception:
                    break

            if not is_ndjson or not records:
                # Try parsing as full JSON array or object
                full_obj = json.loads(text_content)
                if isinstance(full_obj, list):
                    records = full_obj
                elif isinstance(full_obj, dict):
                    # Check for result wrapper
                    if 'data' in full_obj and isinstance(full_obj['data'], list):
                        records = full_obj['data']
                    elif 'records' in full_obj and isinstance(full_obj['records'], list):
                        records = full_obj['records']
                    else:
                        records = [full_obj]

        elif fmt == 'text':
            lines = text_content.splitlines()
            for idx, line in enumerate(lines, start=1):
                records.append({
                    "line_number": idx,
                    "line_content": line,
                    "source_file": source_key
                })

        elif fmt == 'parquet':
            import pandas as pd
            buffer = io.BytesIO(body_bytes)
            df = pd.read_parquet(buffer)
            records = df.to_dict(orient='records')

        else:
            raise ValueError(f"Unsupported file_format: '{file_format}'. Allowed: csv, flat, text, json, ndjson, parquet")

        return records
