"""
S3 File Source Connector — AWS Glue Data Pipeline.

Extracts flat files (CSV, JSON, NDJSON, Parquet, Text) from an external
or internal S3 bucket and streams records to the Bronze layer.

Required bronze_config.json keys (source_systems.<name>):
  source_bucket      : S3 bucket to read files from
  file_prefix        : S3 key prefix for all files of this source (e.g. 'raw/hr/')
  file_format        : 'csv' | 'tsv' | 'json' | 'ndjson' | 'parquet' | 'text'
  fetch_mode         : 'all' — all files modified after watermark (default)
                       'latest' — only the single most recently modified file
  table_paths        : Optional per-table path overrides { table_name: "path/prefix/" }
  table_fetch_modes  : Optional per-table fetch_mode overrides { table_name: "latest" }
  delimiter          : Column delimiter for CSV/TSV (default: ',')
  has_header         : Whether CSV has a header row (default: true)
  encoding           : File encoding (default: 'utf-8')

Cross-account S3 access: if Secrets Manager contains 'aws_access_key_id' and
'aws_secret_access_key', a cross-account S3 client is used; otherwise the
Glue execution role (IAM) is used.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3

logger = logging.getLogger(__name__)

_SUPPORTED_FORMATS = ('csv', 'tsv', 'json', 'ndjson', 'parquet', 'text')
_VALID_FETCH_MODES = ('all', 'latest')


class S3FileConnector:
    """
    Ingestion connector for reading files from S3 source buckets.
    Supports two fetch modes: 'all' (all new files) or 'latest' (most recent file only).
    """

    @classmethod
    def fetch_delta(
        cls,
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str] = None,
        on_chunk_callback: Optional[Callable[[List[Dict[str, Any]], int], None]] = None,
        s3_chunk_size: int = 10000,
    ) -> None:
        """
        Scans the source S3 path for files and streams records to Bronze.

        fetch_mode = 'all'   : Reads all files modified after last_load_date.
        fetch_mode = 'latest': Reads only the most recently modified file in the path,
                               regardless of the watermark. Useful for full-refresh feeds
                               (e.g. daily employee export).
        """
        config = source_config or {}

        source_bucket = config.get('source_bucket')
        if not source_bucket:
            raise ValueError(
                f"S3FileConnector for '{table_name}': 'source_bucket' is not configured. "
                "Add 'source_bucket' to bronze_config.json "
                f"(source_systems.<name>.source_bucket)."
            )

        # Per-table path override: tables.<table_name>.file_path > table_paths > global file_prefix
        tables_dict = config.get('tables', {})
        table_cfg = tables_dict.get(table_name, {}) if isinstance(tables_dict, dict) else {}
        table_paths = config.get('table_paths', {})
        file_prefix = table_cfg.get('file_path') or table_paths.get(table_name) or config.get('file_prefix', f'raw/{table_name}/')

        # Per-table fetch_mode override: tables.<table_name>.fetch_mode > table_fetch_modes > global fetch_mode
        table_modes = config.get('table_fetch_modes', {})
        raw_mode = table_cfg.get('fetch_mode') or table_modes.get(table_name) or config.get('fetch_mode', 'all')
        fetch_mode = str(raw_mode).strip().lower()
        if fetch_mode not in _VALID_FETCH_MODES:
            raise ValueError(
                f"S3FileConnector for '{table_name}': invalid 'fetch_mode' = '{fetch_mode}'. "
                f"Allowed: {_VALID_FETCH_MODES}. "
                "Set 'fetch_mode' in bronze_config.json (source_systems.<name>.fetch_mode)."
            )

        file_format = (config.get('file_format') or 'csv').lower()
        if file_format not in _SUPPORTED_FORMATS:
            raise ValueError(
                f"S3FileConnector for '{table_name}': unsupported 'file_format' = '{file_format}'. "
                f"Allowed: {_SUPPORTED_FORMATS}."
            )
        delimiter  = config.get('delimiter', ',')
        has_header = bool(config.get('has_header', True))
        encoding   = config.get('encoding', 'utf-8')

        # Build S3 client (cross-account if explicit credentials provided)
        if secret_dict.get('aws_access_key_id') and secret_dict.get('aws_secret_access_key'):
            s3 = boto3.client(
                's3',
                aws_access_key_id=secret_dict['aws_access_key_id'],
                aws_secret_access_key=secret_dict['aws_secret_access_key'],
                aws_session_token=secret_dict.get('aws_session_token'),
            )
        else:
            s3 = boto3.client('s3')

        # Parse watermark and optional upper_bound (support 'YYYY-MM-DD HH:MM:SS' and ISO-8601)
        try:
            hwm_str = str(last_load_date).strip()
            if hwm_str.endswith('Z') or hwm_str.endswith('z'):
                hwm_str = hwm_str[:-1] + '+00:00'
            elif ' ' in hwm_str and '+' not in hwm_str and '-' not in hwm_str[10:]:
                hwm_str = hwm_str.replace(' ', 'T') + '+00:00'
            elif 'T' in hwm_str and '+' not in hwm_str and '-' not in hwm_str[10:]:
                hwm_str = hwm_str + '+00:00'
            hwm = datetime.fromisoformat(hwm_str)
            if hwm.tzinfo is None:
                hwm = hwm.replace(tzinfo=timezone.utc)
        except Exception:
            hwm = datetime.min.replace(tzinfo=timezone.utc)

        upper_bound_str = config.get('upper_bound')
        ub_dt = None
        if upper_bound_str and str(upper_bound_str).strip():
            try:
                ub_clean = str(upper_bound_str).strip()
                if ub_clean.endswith('Z') or ub_clean.endswith('z'):
                    ub_clean = ub_clean[:-1] + '+00:00'
                elif ' ' in ub_clean and '+' not in ub_clean and '-' not in ub_clean[10:]:
                    ub_clean = ub_clean.replace(' ', 'T') + '+00:00'
                elif 'T' in ub_clean and '+' not in ub_clean and '-' not in ub_clean[10:]:
                    ub_clean = ub_clean + '+00:00'
                ub_dt = datetime.fromisoformat(ub_clean)
                if ub_dt.tzinfo is None:
                    ub_dt = ub_dt.replace(tzinfo=timezone.utc)
            except Exception:
                logger.warning(f"[S3File/{table_name}] Could not parse upper_bound '{upper_bound_str}', ignoring.")

        # List all matching files
        candidates: List[Tuple[str, datetime]] = []
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=source_bucket, Prefix=file_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/'):
                    continue
                mtime = obj['LastModified']
                within_ub = (ub_dt is None) or (mtime <= ub_dt)
                if (fetch_mode == 'latest' or mtime > hwm) and within_ub:
                    candidates.append((key, mtime))

        if not candidates:
            logger.info(
                f"[S3File/{table_name}] No files found in "
                f"s3://{source_bucket}/{file_prefix} (fetch_mode={fetch_mode})."
            )
            return

        # Apply fetch_mode
        if fetch_mode == 'latest':
            selected = [max(candidates, key=lambda x: x[1])]
            logger.info(f"[S3File/{table_name}] fetch_mode=latest → reading: {selected[0][0]}")
        else:
            selected = sorted(candidates, key=lambda x: x[1])
            logger.info(
                f"[S3File/{table_name}] fetch_mode=all → {len(selected)} file(s) "
                f"in s3://{source_bucket}/{file_prefix} modified after {last_load_date}."
            )

        buffer: List[Dict[str, Any]] = []
        part = 1

        for key, mtime in selected:
            logger.info(f"[S3File/{table_name}] Reading: s3://{source_bucket}/{key} (modified: {mtime})")
            try:
                body_bytes = s3.get_object(Bucket=source_bucket, Key=key)['Body'].read()
                records    = cls._parse(body_bytes, file_format, delimiter, has_header, encoding, key)
            except Exception as err:
                logger.error(f"[S3File/{table_name}] Failed to read/parse '{key}': {err}")
                raise

            buffer.extend(records)
            while len(buffer) >= s3_chunk_size:
                if on_chunk_callback:
                    on_chunk_callback(buffer[:s3_chunk_size], part)
                buffer = buffer[s3_chunk_size:]
                part  += 1

        if buffer and on_chunk_callback:
            on_chunk_callback(buffer, part)

    @classmethod
    def _parse(
        cls,
        body_bytes: bytes,
        file_format: str,
        delimiter: str,
        has_header: bool,
        encoding: str,
        source_key: str,
    ) -> List[Dict[str, Any]]:
        """Parses raw file bytes into a list of record dicts."""
        text = body_bytes.decode(encoding, errors='replace')

        if file_format in ('csv', 'tsv'):
            sep  = '\t' if file_format == 'tsv' else delimiter
            lines = text.splitlines()
            if not lines:
                return []
            if has_header:
                return [dict(row) for row in csv.DictReader(lines, delimiter=sep)]
            return [
                {f'col_{i}': v for i, v in enumerate(row)}
                for row in csv.reader(lines, delimiter=sep)
            ]

        if file_format in ('json', 'ndjson'):
            records: List[Dict[str, Any]] = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        records.append(obj)
                except json.JSONDecodeError:
                    break
            if records:
                return records
            # Fallback: full JSON array
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                for key in ('data', 'records', 'value', 'entities'):
                    if key in parsed and isinstance(parsed[key], list):
                        return parsed[key]
                return [parsed]
            return []

        if file_format == 'text':
            return [
                {'line_number': i, 'line_content': line, 'source_file': source_key}
                for i, line in enumerate(text.splitlines(), start=1)
            ]

        if file_format == 'parquet':
            import pandas as pd
            return pd.read_parquet(io.BytesIO(body_bytes)).to_dict(orient='records')

        raise ValueError(f"Unsupported file_format: '{file_format}'.")
