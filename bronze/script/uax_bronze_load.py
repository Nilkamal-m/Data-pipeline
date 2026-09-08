"""
AWS Glue Python Shell Script: Modular & Scalable Bronze REST API / DB Incremental Ingestion Engine

Key Features:
- Supports multi-table dynamic extraction in a single job execution.
- Strict Load Date Enforcement: Throws an explicit error if initial load date is null/missing (no arbitrary past record fallbacks).
- Memory-Safe Streaming: Flushes extraction batches to S3 chunk files immediately when reaching threshold.
- Atomic Staging Promotion: Writes chunk artifacts into isolated _staging/ execution directories.
- Atomic State Management: Updates S3 metadata state file ONLY after data extraction and S3 promotion succeed.
- Custom Metric Emission: Reports CloudWatch custom metrics (UAX/DataPipeline/Ingestion).
- Error Handling Mode: Supports CONTINUE_ON_ERROR vs HALT_ON_ERROR policies.
- Recursive JSON Flattening: Flattens nested JSON payloads into flat column schemas before Parquet serialization.
"""

import sys
import os
import json
import logging
import boto3
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# ------------------------------------------------------------------------------
# High-Performance, Non-Duplicating CloudWatch Log Configuration
# ------------------------------------------------------------------------------
class FlushStreamHandler(logging.StreamHandler):
    """Guarantees immediate line flush to avoid AWS Glue / CloudWatch line interleaving."""
    def emit(self, record):
        super().emit(record)
        self.flush()


class CleanLogFormatter(logging.Formatter):
    """
    Clean, modern log formatter optimized for AWS CloudWatch console viewing.
    - Eliminates duplicate handlers & line collisions.
    - Provides standardized timestamp [YYYY-MM-DD HH:MM:SS UTC] and 5-char aligned severity.
    - If the message has multiple lines (e.g. ASCII cards/banners), the first line carries
      the full summary line for CloudWatch collapsed list view, and following lines preserve
      clean ASCII box indentation.
    """
    def format(self, record):
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        level = record.levelname.ljust(5)
        msg = record.getMessage()

        if "\n" in msg:
            lines = msg.split("\n")
            first_line = f"{ts} | {level} | {lines[0]}"
            rest = "\n".join(lines[1:])
            return f"{first_line}\n{rest}"

        return f"{ts} | {level} | {msg}"


# Configure single unified handler on root logger to avoid duplicate log outputs in AWS Glue
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for h in list(root_logger.handlers):
    root_logger.removeHandler(h)

stdout_handler = FlushStreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(CleanLogFormatter())
root_logger.addHandler(stdout_handler)

# Force stdout line buffering in Python environment if available
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

logger = logging.getLogger("uax_bronze_load")
logger.setLevel(logging.INFO)
logger.propagate = True

# Ensure script directory and Glue extraPython paths are on sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
for path in [script_dir, os.getcwd(), "/tmp/extraPython", "/tmp"]:
    if os.path.exists(path) and path not in sys.path:
        sys.path.insert(0, path)

# Auto-discover and extract connectors.zip across Glue script directories (e.g. /tmp/glue-python-scripts-*)
candidate_zips = set()
for search_dir in [script_dir, "/tmp", "/tmp/extraPython", os.getcwd()]:
    if os.path.exists(search_dir):
        for root, _, files in os.walk(search_dir):
            if "connectors.zip" in files:
                candidate_zips.add(os.path.join(root, "connectors.zip"))

for candidate in candidate_zips:
    try:
        import zipfile
        target_dir = os.path.join(script_dir, "connectors")
        os.makedirs(target_dir, exist_ok=True)
        with zipfile.ZipFile(candidate, 'r') as zf:
            namelist = zf.namelist()
            has_subfolder = any(name.startswith("connectors/") for name in namelist)
            if has_subfolder:
                zf.extractall(script_dir)
            else:
                zf.extractall(target_dir)
        logger.info(f"Auto-extracted zip artifact '{candidate}' into '{script_dir}'.")
        break
    except Exception as ze:
        logger.warning(f"Could not auto-extract '{candidate}': {ze}")

from config_loader import ConfigLoader

try:
    from connectors import get_connector
except ModuleNotFoundError:
    # Resilient fallback if connectors module files are unzipped directly at sys.path root
    try:
        import connectors
        get_connector = getattr(connectors, 'get_connector')
    except Exception as err:
        logger.error(f"Failed to import 'connectors' package. Current sys.path: {sys.path}")
        raise ModuleNotFoundError(f"Cannot find 'connectors' module in sys.path: {err}")

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')


def get_secret(secret_name: str) -> dict:
    """
    Fetches API credential secret payload from AWS Secrets Manager.
    Returns dictionary with optional manual hardcoded fallbacks using .get('key', 'default_val').
    """
    sec_payload = {}
    if secret_name and secret_name.strip():
        logger.info(f"Fetching secret payload for '{secret_name}' from AWS Secrets Manager...")
        secrets_client = boto3.client('secretsmanager')
        try:
            response = secrets_client.get_secret_value(SecretId=secret_name)
            secret_str = response.get('SecretString')
            if secret_str:
                sec_payload = json.loads(secret_str)
        except ClientError as err:
            logger.warning(f"Could not fetch secret '{secret_name}' ({err}). Proceeding with manual fallbacks.")

    def _val(key: str, default: str) -> str:
        v = sec_payload.get(key)
        if v and str(v).strip() and not str(v).startswith("CHANGE_ME") and not str(v).startswith("YOUR_"):
            return str(v).strip()
        return default

    return {
        "auth_type": "oauth2",
        "grant_type": "client_credentials",
        "client_id": "YOUR_MOVEWORKS_CLIENT_ID_HERE",
        "client_secret": "YOUR_MOVEWORKS_CLIENT_SECRET_HERE",
        "token_url": "https://api.moveworks.ai/oauth/v1/token",
        "assistant_name": _val('assistant_name', 'acmecorp-conversations-rest-api'),
        "scope": _val('scope', 'export:read'),
        "username": _val('username', ''),
        "password": _val('password', '')
    }


def parse_arguments() -> dict:
    """
    Parses CLI arguments passed by Step Functions, AWS Glue Job Run, or manual triggers.
    Enforces strict 3-tier parameter precedence:
      1. Glue CLI Argument (--KEY value or --KEY=value) [HIGHEST PRIORITY]
      2. Config File (bronze_config.json or S3 config) [SECOND PRIORITY]
      3. Code Hardcoded Defaults [LOWEST PRIORITY]
    """
    arg_dict = {}
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg.startswith('--'):
            key_val = arg[2:]
            if '=' in key_val:
                k, v = key_val.split('=', 1)
                arg_dict[k.strip()] = v.strip()
                i += 1
            else:
                k = key_val.strip()
                v = ''
                if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                    v = sys.argv[i + 1].strip()
                    i += 2
                else:
                    i += 1
                arg_dict[k] = v
        else:
            i += 1

    def get_cli_arg(*names, default=None):
        """Case-insensitive CLI argument lookup helper."""
        lower_names = [n.lower() for n in names]
        for k, v in arg_dict.items():
            if k.lower() in lower_names and v is not None and str(v).strip():
                return str(v).strip()
        return default

    # 1. Source System (Required)
    source_system = get_cli_arg('SOURCE_SYSTEM', 'source_system', 'SOURCE', 'source')
    if not source_system:
        logger.error("Missing required parameter '--SOURCE_SYSTEM'. Please pass '--SOURCE_SYSTEM'.")
        raise ValueError("Missing required argument '--SOURCE_SYSTEM'. Example usage: --SOURCE_SYSTEM moveworks")

    source_system_clean = source_system.strip().lower()
    config_s3_path = get_cli_arg('CONFIG_S3_PATH', 'config_s3_path')

    # Load centralized Bronze configuration
    full_config = ConfigLoader.load_config(config_s3_path=config_s3_path, s3_client=s3_client)
    pipeline_defaults = ConfigLoader.get_pipeline_defaults(full_config)
    source_config = ConfigLoader.get_source_config(source_system_clean, full_config)

    # -------------------------------------------------------------
    # STRICT 3-TIER PARAMETER PRECEDENCE (CLI > Config > Code Default)
    # -------------------------------------------------------------

    # Table List: CLI parameter takes highest priority over config and code defaults
    cli_tables = get_cli_arg('SOURCE_TABLE_NAME', 'source_table_name', 'TABLE_NAME', 'table_name', 'TABLES', 'tables', 'TABLE_NAMES', 'table_names', 'TABLE', 'table')
    if cli_tables:
        table_list = [t.strip() for t in cli_tables.split(',') if t.strip()]
        logger.info(f"[PARAM PRECEDENCE] Table List resolved from GLUE CLI (Priority 1): {table_list}")
    elif source_config.get('default_tables'):
        table_list = list(source_config['default_tables'])
        logger.info(f"[PARAM PRECEDENCE] Table List resolved from CONFIG FILE (Priority 2): {table_list}")
    else:
        raise ValueError(
            f"CRITICAL CONFIG ERROR: 'default_tables' is missing or empty for source system '{source_system_clean}' "
            f"in bronze_config.json (source_systems.{source_system_clean}.default_tables) and was not provided via CLI "
            f"(--SOURCE_TABLE_NAME). Please configure at least one target table in bronze_config.json."
        )

    # Batch Size
    cli_batch = get_cli_arg('BATCH_SIZE', 'batch_size')
    if cli_batch:
        batch_size = int(cli_batch)
        source_config['batch_size'] = batch_size
        logger.info(f"[PARAM PRECEDENCE] 'batch_size' resolved from GLUE CLI (Priority 1): {batch_size}")
    elif source_config.get('batch_size'):
        batch_size = int(source_config['batch_size'])
        logger.info(f"[PARAM PRECEDENCE] 'batch_size' resolved from CONFIG (Priority 2): {batch_size}")
    else:
        batch_size = int(pipeline_defaults.get('batch_size', 500 if source_system_clean == 'moveworks' else 1000))
        source_config['batch_size'] = batch_size
        logger.info(f"[PARAM PRECEDENCE] 'batch_size' resolved from CODE DEFAULT (Priority 3): {batch_size}")

    # Base URL
    cli_base_url = get_cli_arg('BASE_URL', 'base_url')
    if cli_base_url:
        source_config['base_url'] = cli_base_url
        logger.info(f"[PARAM PRECEDENCE] 'base_url' resolved from GLUE CLI (Priority 1): {cli_base_url}")

    # Secret Name
    cli_secret = get_cli_arg('SECRET_NAME', 'secret_name')
    if cli_secret:
        secret_name = cli_secret
        logger.info(f"[PARAM PRECEDENCE] 'secret_name' resolved from GLUE CLI (Priority 1): {secret_name}")
    elif source_config.get('secret_name'):
        secret_name = source_config['secret_name']
        logger.info(f"[PARAM PRECEDENCE] 'secret_name' resolved from CONFIG (Priority 2): {secret_name}")
    else:
        secret_name = ''

    # Custom Query
    custom_query = get_cli_arg('CUSTOM_QUERY', 'custom_query')

    # Initial Load Date CLI Override
    initial_load_date_cli = get_cli_arg('INITIAL_LOAD_DATE', 'initial_load_date')

    # S3 Buckets: CLI > Config > Env / Code Default
    bronze_bucket = get_cli_arg('BRONZE_BUCKET', 'bronze_bucket', default=pipeline_defaults.get('bronze_bucket') or os.environ.get('BRONZE_BUCKET', 'uax-datalake-dev-bucket'))
    state_bucket = get_cli_arg('STATE_BUCKET', 'state_bucket', default=pipeline_defaults.get('state_bucket') or bronze_bucket)

    # S3 Chunk Size
    cli_chunk = get_cli_arg('S3_CHUNK_SIZE', 's3_chunk_size')
    s3_chunk_size = int(cli_chunk) if cli_chunk else int(pipeline_defaults.get('s3_chunk_size', 10000))

    # Output Format & Parquet Compression
    output_format = (get_cli_arg('OUTPUT_FORMAT', 'output_format') or pipeline_defaults.get('output_format', 'parquet')).lower()
    parquet_compression = (get_cli_arg('PARQUET_COMPRESSION', 'parquet_compression') or pipeline_defaults.get('parquet_compression', 'snappy')).lower()

    # Error Handling Mode
    error_handling_mode = (get_cli_arg('ERROR_HANDLING_MODE', 'error_handling_mode') or pipeline_defaults.get('error_handling_mode', 'CONTINUE_ON_ERROR')).upper()

    # CloudWatch Namespace
    cloudwatch_namespace = get_cli_arg('CLOUDWATCH_NAMESPACE', 'cloudwatch_namespace') or pipeline_defaults.get('cloudwatch_namespace', 'UAX/DataPipeline/Ingestion')

    # Response Records Key
    cli_rec_key = get_cli_arg('RESPONSE_RECORDS_KEY', 'response_records_key')
    if cli_rec_key:
        source_config['response_records_key'] = cli_rec_key

    # Flatten Nested JSON
    cli_flatten = get_cli_arg('FLATTEN_NESTED_JSON', 'flatten_nested_json')
    if cli_flatten is not None:
        source_config['flatten_nested_json'] = (cli_flatten.lower() == 'true')
    elif 'flatten_nested_json' not in source_config:
        source_config['flatten_nested_json'] = pipeline_defaults.get('flatten_nested_json', True)

    # Flatten Separator
    cli_sep = get_cli_arg('FLATTEN_SEPARATOR', 'flatten_separator')
    if cli_sep:
        source_config['flatten_separator'] = cli_sep
    elif 'flatten_separator' not in source_config:
        source_config['flatten_separator'] = pipeline_defaults.get('flatten_separator', '_')

    # Assistant-Name (for Moveworks)
    cli_assistant = get_cli_arg('ASSISTANT_NAME', 'assistant_name')
    if cli_assistant:
        source_config['assistant_name'] = cli_assistant

    # Job Name
    job_name = get_cli_arg('JOB_NAME', 'job_name', default=f"glue-incremental-load-{source_system_clean}")

    # Bronze Data Prefix: CLI > Config > Code Default ('bronze/data')
    bronze_data_prefix = (
        get_cli_arg('BRONZE_DATA_PREFIX', 'bronze_data_prefix', 'BRONZE_PREFIX', 'bronze_prefix')
        or pipeline_defaults.get('bronze_data_prefix')
        or pipeline_defaults.get('bronze_prefix', 'bronze/data')
    ).strip('/')

    # Glue Catalog & Crawler Configuration (Option B: Unified Lake Database with raw_tbl_ Table Prefix)
    catalog_config = pipeline_defaults.get('glue_catalog', {})
    glue_catalog_enabled = (
        get_cli_arg('SYNC_GLUE_CATALOG', 'sync_glue_catalog', 'GLUE_CATALOG_ENABLED', 'glue_catalog_enabled')
        or str(catalog_config.get('enabled', True))
    ).lower() == 'true'

    glue_database_name = (
        get_cli_arg('GLUE_DATABASE', 'glue_database', 'GLUE_DB_NAME', 'glue_db_name')
        or catalog_config.get('database_name')
    )
    if glue_catalog_enabled and (not glue_database_name or not str(glue_database_name).strip()):
        raise ValueError(
            "CRITICAL CONFIG ERROR: 'database_name' is missing or empty in bronze_config.json "
            "(pipeline_defaults.glue_catalog.database_name) and was not provided via CLI. "
            "Please configure 'database_name' (e.g. 'uax_datalake_db_dev')."
        )
    if glue_database_name:
        glue_database_name = str(glue_database_name).strip()

    glue_table_prefix = (
        get_cli_arg('GLUE_TABLE_PREFIX', 'glue_table_prefix', 'TABLE_PREFIX', 'table_prefix')
        or catalog_config.get('table_prefix')
    )
    if not glue_table_prefix or not str(glue_table_prefix).strip():
        raise ValueError(
            "CRITICAL CONFIG ERROR: 'table_prefix' is missing or empty in bronze_config.json "
            "(pipeline_defaults.glue_catalog.table_prefix) and was not provided via CLI. "
            "Please configure 'table_prefix' (e.g. 'raw_tbl_')."
        )
    glue_table_prefix = str(glue_table_prefix).strip()

    bronze_crawler_name = (
        get_cli_arg('BRONZE_CRAWLER_NAME', 'bronze_crawler_name', 'CRAWLER_NAME', 'crawler_name')
        or catalog_config.get('crawler_name')
    )

    trigger_crawler = (
        get_cli_arg('TRIGGER_CRAWLER', 'trigger_crawler')
        or str(catalog_config.get('trigger_crawler', True))
    ).lower() == 'true'

    sync_watermark_table = (
        get_cli_arg('SYNC_WATERMARK_TABLE', 'sync_watermark_table')
        or str(catalog_config.get('sync_watermark_table', True))
    ).lower() == 'true'

    watermark_table_name = (
        get_cli_arg('WATERMARK_TABLE_NAME', 'watermark_table_name')
        or catalog_config.get('watermark_table_name')
    )
    if sync_watermark_table and (not watermark_table_name or not str(watermark_table_name).strip()):
        raise ValueError(
            "CRITICAL CONFIG ERROR: 'watermark_table_name' is missing or empty in bronze_config.json "
            "(pipeline_defaults.glue_catalog.watermark_table_name) and was not provided via CLI. "
            "Please configure 'watermark_table_name' (e.g. 'raw_tbl_watermarks')."
        )
    if watermark_table_name:
        watermark_table_name = str(watermark_table_name).strip()

    parsed_params = {
        'JOB_NAME': job_name,
        'SOURCE_SYSTEM': source_system_clean,
        'TABLE_LIST': table_list,
        'CUSTOM_QUERY': custom_query,
        'SECRET_NAME': secret_name,
        'BRONZE_BUCKET': bronze_bucket,
        'STATE_BUCKET': state_bucket,
        'BRONZE_DATA_PREFIX': bronze_data_prefix,
        'INITIAL_LOAD_DATE_CLI': initial_load_date_cli,
        'S3_CHUNK_SIZE': s3_chunk_size,
        'OUTPUT_FORMAT': output_format,
        'PARQUET_COMPRESSION': parquet_compression,
        'ERROR_HANDLING_MODE': error_handling_mode,
        'CLOUDWATCH_NAMESPACE': cloudwatch_namespace,
        'GLUE_CATALOG_ENABLED': glue_catalog_enabled,
        'GLUE_DATABASE_NAME': glue_database_name,
        'GLUE_TABLE_PREFIX': glue_table_prefix,
        'BRONZE_CRAWLER_NAME': bronze_crawler_name,
        'TRIGGER_CRAWLER': trigger_crawler,
        'SYNC_WATERMARK_TABLE': sync_watermark_table,
        'WATERMARK_TABLE_NAME': watermark_table_name,
        'SOURCE_CONFIG': source_config,
        'PIPELINE_DEFAULTS': pipeline_defaults
    }

    logger.info(f"Resolved Parameters: {json.dumps({k: v for k, v in parsed_params.items() if k != 'SOURCE_CONFIG'})}")
    return parsed_params


# ---------------------------------------------------------
# Helper Functions, Serialization & CloudWatch Metrics
# ---------------------------------------------------------
def flatten_and_expand_record(record: dict, parent_key: str = '', sep: str = '_') -> list:
    """
    Recursively flattens nested dictionaries and explodes arrays of objects into multiple individual rows.
    If a record contains an array of objects (e.g. external_ids), generates N rows—one for each item in the array.
    """
    base_dict = {}
    list_of_dicts = []
    list_key_name = None

    for k, v in record.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
        if isinstance(v, dict):
            sub_flat = flatten_dict_single(v, parent_key=new_key, sep=sep)
            base_dict.update(sub_flat)
        elif isinstance(v, list):
            if v and isinstance(v[0], dict) and not list_key_name:
                list_key_name = new_key
                list_of_dicts = v
            else:
                base_dict[new_key] = json.dumps(v) if v is not None else None
        else:
            base_dict[new_key] = v

    if not list_of_dicts:
        return [base_dict]

    expanded_rows = []
    for item in list_of_dicts:
        row_dict = base_dict.copy()
        if isinstance(item, dict):
            item_flat = flatten_dict_single(item, parent_key=list_key_name, sep=sep)
            row_dict.update(item_flat)
        expanded_rows.append(row_dict)

    return expanded_rows


def flatten_dict_single(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Flattens a single dictionary object recursively.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
        if isinstance(v, dict):
            items.extend(flatten_dict_single(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v) if v is not None else None))
        else:
            items.append((new_key, v))
    return dict(items)


def flatten_dict(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Wrapper for single dictionary flattening.
    """
    return flatten_dict_single(d, parent_key=parent_key, sep=sep)


def serialize_chunk_to_bytes(records_chunk: list, output_format: str = "parquet", parquet_compression: str = "snappy") -> tuple:
    """
    Serializes a record chunk into bytes according to configured format (parquet or json).
    """
    fmt = output_format.strip().lower()
    if fmt == "parquet":
        try:
            import pandas as pd
            import io
            df = pd.DataFrame(records_chunk)
            buffer = io.BytesIO()
            df.to_parquet(buffer, compression=parquet_compression, index=False)
            return buffer.getvalue(), "application/x-parquet", ".parquet"
        except Exception as err:
            logger.warning(f"Parquet serialization via pandas failed ({err}). Falling back to JSON format.")
            fmt = "json"

    json_bytes = json.dumps(records_chunk, indent=2).encode('utf-8')
    return json_bytes, "application/json", ".json"


def emit_cloudwatch_metrics(
    namespace: str,
    source_system: str,
    table_name: str,
    records_count: int,
    duration_seconds: float,
    success: bool
) -> None:
    """
    Emits custom ingestion metrics to AWS CloudWatch.
    """
    try:
        cw_client = boto3.client('cloudwatch')
        dimensions = [
            {'Name': 'SourceSystem', 'Value': source_system},
            {'Name': 'TableName', 'Value': table_name}
        ]
        metric_data = [
            {
                'MetricName': 'RecordsIngested',
                'Dimensions': dimensions,
                'Value': float(records_count),
                'Unit': 'Count'
            },
            {
                'MetricName': 'IngestionDurationSeconds',
                'Dimensions': dimensions,
                'Value': round(float(duration_seconds), 3),
                'Unit': 'Seconds'
            },
            {
                'MetricName': 'TableExtractionSuccess',
                'Dimensions': dimensions,
                'Value': 1.0 if success else 0.0,
                'Unit': 'Count'
            }
        ]
        logger.info(f"Emitting CloudWatch custom metrics to '{namespace}' for {source_system}/{table_name}")
        cw_client.put_metric_data(Namespace=namespace, MetricData=metric_data)
    except Exception as err:
        logger.warning(f"Failed to emit CloudWatch metrics: {err}")


def get_table_state_key(source_system: str, table_name: str) -> str:
    """Returns the S3 metadata key for a given source system and table name: metadata/bronze/<source>/<table>/watermark.json."""
    return f"metadata/bronze/{source_system}/{table_name}/watermark.json"


def get_last_load_date(
    state_bucket: str,
    state_key: str,
    source_system: str,
    table_name: str,
    cli_initial_date: Optional[str] = None,
    source_config: Optional[dict] = None
) -> str:
    """
    Fetches the last successful load timestamp for a table from S3 metadata JSON (watermark.json).
    ONLY if the watermark state file is NOT present in S3 does it fall back to bronze_config.json table_initial_load_dates.
    Strictly throws ValueError if load date is NULL/missing.
    """
    s3_path = f"s3://{state_bucket}/{state_key}"
    try:
        logger.info(f"Checking for High-Water Mark state file at '{s3_path}'...")
        response = s3_client.get_object(Bucket=state_bucket, Key=state_key)
        state_content = response['Body'].read().decode('utf-8')
        state_data = json.loads(state_content)
        
        last_load_date = state_data.get('last_load_date')
        if last_load_date and str(last_load_date).strip():
            logger.info(f"HIGH-WATER MARK FOUND in S3 metadata ({s3_path}): '{last_load_date}' for table '{table_name}'. (Bypassing bronze_config.json initial_load_dates)")
            return str(last_load_date).strip()

    except ClientError as err:
        error_code = err.response.get('Error', {}).get('Code')
        if error_code in ('NoSuchKey', '404'):
            logger.info(f"Watermark state file NOT present in S3 at '{s3_path}'. Falling back to bronze_config.json 'table_initial_load_dates' for initial run...")
        else:
            logger.error(f"Error reading state file from '{s3_path}': {err}")
            raise

    # Watermark NOT present in S3 -> Fall back to table_initial_load_dates in bronze_config.json
    logger.info(f"Resolving initial load date for table '{table_name}' from bronze_config.json...")
    return ConfigLoader.get_table_initial_load_date(
        source_system=source_system,
        table_name=table_name,
        cli_initial_date=cli_initial_date,
        source_config=source_config
    )


def update_last_load_date(
    state_bucket: str,
    state_key: str,
    source_system: str,
    table_name: str,
    current_run_time: str,
    total_records: int,
    table_prefix: str
) -> None:
    """
    Writes/Updates the High-Water Mark JSON metadata file for a specific table in S3.
    Formats table_name with the centralized table_prefix (e.g. raw_tbl_incident).
    """
    if not table_prefix or not str(table_prefix).strip():
        raise ValueError(
            "CRITICAL CONFIG ERROR: 'table_prefix' must be provided and non-empty for update_last_load_date. "
            "Please ensure 'table_prefix' is configured in bronze_config.json or passed via CLI."
        )
    s3_path = f"s3://{state_bucket}/{state_key}"
    prefix = str(table_prefix).strip()
    formatted_table_name = table_name if table_name.startswith(prefix) else f"{prefix}{table_name}"
    state_payload = {
        "source_system": source_system,
        "table_name": formatted_table_name,
        "last_load_date": current_run_time,
        "last_status": "SUCCESS",
        "records_ingested": total_records,
        "updated_at": current_run_time
    }
    
    try:
        logger.info(f"Updating state file at '{s3_path}' with payload: {state_payload}")
        s3_client.put_object(
            Bucket=state_bucket,
            Key=state_key,
            Body=json.dumps(state_payload, indent=2).encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Successfully updated S3 state file at '{s3_path}'")
    except ClientError as err:
        logger.error(f"Failed to update S3 state file at '{s3_path}': {err}")
        raise


def promote_staging_to_bronze(bucket_name: str, staging_prefix: str, final_partition_prefix: str) -> None:
    """
    Atomically copies extraction chunk files from temporary STAGING folder to final Bronze partition folder.
    """
    try:
        logger.info(f"Promoting staging artifacts from s3://{bucket_name}/{staging_prefix} to s3://{bucket_name}/{final_partition_prefix}")
        paginator = s3_client.get_paginator('list_objects_v2')
        copied_keys = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix=staging_prefix):
            for obj in page.get('Contents', []):
                staging_key = obj['Key']
                filename = os.path.basename(staging_key)
                target_key = f"{final_partition_prefix}{filename}"

                # Copy object atomically
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': staging_key},
                    Key=target_key
                )
                copied_keys.append(staging_key)
                logger.info(f"Promoted: s3://{bucket_name}/{target_key}")

        # Clean up staging files after successful copy
        if copied_keys:
            for i in range(0, len(copied_keys), 1000):
                chunk = [{'Key': k} for k in copied_keys[i:i + 1000]]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': chunk})
            logger.info("Staging cleanup completed successfully.")

    except Exception as err:
        logger.error(f"Failed to promote staging files to Bronze partition prefix: {err}")
        raise


def cleanup_failed_staging(bucket_name: str, staging_prefix: str) -> None:
    """
    Removes temporary uncommitted staging files if an extraction fails mid-way.
    """
    try:
        logger.info(f"Cleaning up failed staging artifacts at s3://{bucket_name}/{staging_prefix}")
        paginator = s3_client.get_paginator('list_objects_v2')
        objects_to_delete = []

        for page in paginator.paginate(Bucket=bucket_name, Prefix=staging_prefix):
            for obj in page.get('Contents', []):
                objects_to_delete.append({'Key': obj['Key']})

        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                chunk = objects_to_delete[i:i + 1000]
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': chunk})
            logger.info("Failed staging artifacts purged successfully.")
    except Exception as err:
        logger.warning(f"Error purging failed staging artifacts at '{staging_prefix}': {err}")


def save_execution_log(
    state_bucket: str,
    source_system: str,
    execution_id: str,
    execution_log: dict
) -> None:
    """
    Persists a comprehensive JSON execution log in S3 for auditing, monitoring, and Athena querying.
    Writes both the timestamped log and a 'latest_execution.json' pointer.
    """
    log_json = json.dumps(execution_log, indent=2, default=str)
    
    # 1. Timestamped execution log
    log_key = f"metadata/logs/bronze/{source_system}/execution_{execution_id}.json"
    try:
        s3_client.put_object(
            Bucket=state_bucket,
            Key=log_key,
            Body=log_json.encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Saved Execution Audit Log to S3: s3://{state_bucket}/{log_key}")
    except Exception as err:
        logger.warning(f"Could not save Execution Audit Log to '{log_key}': {err}")

    # 2. Latest execution pointer for quick inspection
    latest_key = f"metadata/logs/bronze/{source_system}/latest_execution.json"
    try:
        s3_client.put_object(
            Bucket=state_bucket,
            Key=latest_key,
            Body=log_json.encode('utf-8'),
            ContentType="application/json"
        )
    except Exception as err:
        logger.warning(f"Could not update 'latest_execution.json': {err}")


# ------------------------------------------------------------------------------
# AWS Glue Data Catalog & Crawler Management
# ------------------------------------------------------------------------------
def infer_glue_column_type(val: Any) -> str:
    """Infers AWS Glue Data Catalog column type from a sample Python data value."""
    if isinstance(val, bool):
        return 'boolean'
    elif isinstance(val, int):
        return 'bigint'
    elif isinstance(val, float):
        return 'double'
    elif isinstance(val, (dict, list)):
        return 'string'
    return 'string'


def ensure_glue_database(database_name: str) -> None:
    """Ensures the target database exists in AWS Glue Data Catalog."""
    try:
        glue_client.get_database(Name=database_name)
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('EntityNotFoundException', 'NoSuchEntityException'):
            try:
                glue_client.create_database(
                    DatabaseInput={
                        'Name': database_name,
                        'Description': 'AWS Glue Data Catalog Database for UAX Data Lake Bronze & Silver Layers'
                    }
                )
                logger.info(f"Created AWS Glue Catalog Database: '{database_name}'")
            except ClientError as ce:
                if ce.response.get('Error', {}).get('Code') != 'AlreadyExistsException':
                    logger.warning(f"Could not create Glue database '{database_name}': {ce}")
        else:
            logger.warning(f"Could not check Glue database '{database_name}': {e}")


def sync_bronze_catalog_table(
    database_name: str,
    table_prefix: str,
    source_system: str,
    table_name: str,
    bronze_bucket: str,
    bronze_data_prefix: str,
    partition_date: datetime,
    sample_record: Optional[dict] = None,
    output_format: str = "parquet",
    ingested_at: Optional[str] = None
) -> str:
    """
    Creates/updates AWS Glue Data Catalog table for Bronze raw data and registers the execution partition.
    Naming format: <database_name>.<table_prefix><table_name> (e.g. uax_datalake_db_dev.raw_tbl_incident).
    Location: s3://{bronze_bucket}/{bronze_data_prefix}/{source_system}/{table_name}/
    Partition: _ingested_at=<ISO_TIMESTAMP> (Single partition on _ingested_at)
    """
    catalog_table_name = f"{table_prefix}{table_name}"
    table_location = f"s3://{bronze_bucket}/{bronze_data_prefix}/{source_system}/{table_name}/"
    
    ingested_at_str = ingested_at or partition_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    partition_location = f"{table_location}_ingested_at={ingested_at_str}/"

    is_parquet = (output_format.lower() == 'parquet')
    input_fmt = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' if is_parquet else 'org.apache.hadoop.mapred.TextInputFormat'
    output_fmt = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' if is_parquet else 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    serde_lib = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' if is_parquet else 'org.openx.data.jsonserde.JsonSerDe'

    # Build schema columns from sample record if available
    columns = []
    if sample_record and isinstance(sample_record, dict):
        for k, v in sample_record.items():
            clean_k = str(k).strip().lower().replace(" ", "_").replace("-", "_")
            # Partition keys must NOT be declared in StorageDescriptor.Columns in AWS Glue Data Catalog
            if clean_k not in ('_ingested_at', 'year', 'month', 'day'):
                columns.append({'Name': clean_k, 'Type': infer_glue_column_type(v)})

    # Ensure technical audit columns (_source_system, _table_name, _execution_id) are present
    existing_col_names = {c['Name'] for c in columns}
    for audit_col in ('_source_system', '_table_name', '_execution_id'):
        if audit_col not in existing_col_names:
            columns.append({'Name': audit_col, 'Type': 'string'})

    if not columns:
        columns = [
            {'Name': 'payload', 'Type': 'string'},
            {'Name': '_source_system', 'Type': 'string'},
            {'Name': '_table_name', 'Type': 'string'},
            {'Name': '_execution_id', 'Type': 'string'}
        ]

    # Single partition on _ingested_at
    partition_keys = [
        {'Name': '_ingested_at', 'Type': 'string'}
    ]

    storage_desc = {
        'Columns': columns,
        'Location': table_location,
        'InputFormat': input_fmt,
        'OutputFormat': output_fmt,
        'Compressed': is_parquet,
        'NumberOfBuckets': -1,
        'SerdeInfo': {
            'SerializationLibrary': serde_lib,
            'Parameters': {'serialization.format': '1'}
        }
    }

    ensure_glue_database(database_name)

    # 1. Ensure Table exists in Glue Catalog with single partition key ['_ingested_at']
    try:
        existing_table = glue_client.get_table(DatabaseName=database_name, Name=catalog_table_name)
        current_pkeys = [pk.get('Name') for pk in existing_table.get('Table', {}).get('PartitionKeys', [])]
        if current_pkeys != ['_ingested_at']:
            logger.info(
                f"Existing table '{catalog_table_name}' has outdated partition keys {current_pkeys}. "
                f"Recreating Glue Catalog table with single partition key ['_ingested_at']..."
            )
            try:
                glue_client.delete_table(DatabaseName=database_name, Name=catalog_table_name)
                logger.info(f"Deleted outdated Glue Catalog table: {database_name}.{catalog_table_name}")
            except Exception as del_err:
                logger.warning(f"Could not delete old table {catalog_table_name}: {del_err}")
            raise ClientError({'Error': {'Code': 'EntityNotFoundException'}}, 'GetTable')
        else:
            logger.info(f"Glue Catalog Table verified: {database_name}.{catalog_table_name} with partition key ['_ingested_at']")
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('EntityNotFoundException', 'NoSuchEntityException'):
            try:
                glue_client.create_table(
                    DatabaseName=database_name,
                    TableInput={
                        'Name': catalog_table_name,
                        'Description': f"Bronze raw data table for {source_system}/{table_name}",
                        'PartitionKeys': partition_keys,
                        'TableType': 'EXTERNAL_TABLE',
                        'Parameters': {
                            'EXTERNAL': 'TRUE',
                            'has_encrypted_data': 'true',
                            'classification': output_format.lower()
                        },
                        'StorageDescriptor': storage_desc
                    }
                )
                logger.info(f"Created AWS Glue Catalog Table: {database_name}.{catalog_table_name} at '{table_location}' with partition key ['_ingested_at']")
            except ClientError as ce:
                if ce.response.get('Error', {}).get('Code') != 'AlreadyExistsException':
                    logger.warning(f"Failed to create Glue Catalog table '{catalog_table_name}': {ce}")
        else:
            logger.warning(f"Error checking table '{catalog_table_name}': {e}")

    # 2. Register/Update the Single Partition for this execution run (_ingested_at)
    partition_storage = dict(storage_desc)
    partition_storage['Location'] = partition_location
    try:
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName=catalog_table_name,
            PartitionInput={
                'Values': [ingested_at_str],
                'StorageDescriptor': partition_storage,
                'Parameters': {}
            }
        )
        logger.info(f"Registered Glue Catalog Partition: {database_name}.{catalog_table_name} [_ingested_at={ingested_at_str}] -> '{partition_location}'")
    except ClientError as pe:
        code = pe.response.get('Error', {}).get('Code')
        if code == 'AlreadyExistsException':
            try:
                glue_client.update_partition(
                    DatabaseName=database_name,
                    TableName=catalog_table_name,
                    PartitionValueList=[ingested_at_str],
                    PartitionInput={
                        'Values': [ingested_at_str],
                        'StorageDescriptor': partition_storage,
                        'Parameters': {}
                    }
                )
                logger.info(f"Updated existing Glue Catalog Partition: {database_name}.{catalog_table_name} [_ingested_at={ingested_at_str}]")
            except Exception as ue:
                logger.warning(f"Could not update partition: {ue}")
        else:
            logger.warning(f"Could not register partition in Glue Catalog: {pe}")

    return f"{database_name}.{catalog_table_name}"


def sync_watermark_catalog_table(
    database_name: str,
    watermark_table_name: str,
    state_bucket: str,
    state_prefix: str = "metadata/bronze"
) -> str:
    """
    Creates/Ensures an Athena-queryable AWS Glue Catalog table for all Bronze High-Water Mark state files.
    Location: s3://{state_bucket}/{state_prefix}/
    Using recursive directory scanning so all watermark.json files across sources and tables are queried.
    Query in Athena: SELECT * FROM <database_name>.<watermark_table_name>;
    """
    clean_prefix = state_prefix.strip('/')
    watermark_location = f"s3://{state_bucket}/{clean_prefix}/"

    columns = [
        {'Name': 'source_system', 'Type': 'string'},
        {'Name': 'table_name', 'Type': 'string'},
        {'Name': 'last_load_date', 'Type': 'string'},
        {'Name': 'last_status', 'Type': 'string'},
        {'Name': 'records_ingested', 'Type': 'bigint'},
        {'Name': 'updated_at', 'Type': 'string'}
    ]

    storage_desc = {
        'Columns': columns,
        'Location': watermark_location,
        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
        'Compressed': False,
        'NumberOfBuckets': -1,
        'SerdeInfo': {
            'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe',
            'Parameters': {
                'ignore.malformed.json': 'true',
                'mapping.source_system': 'source_system',
                'mapping.table_name': 'table_name',
                'mapping.last_load_date': 'last_load_date',
                'mapping.last_status': 'last_status',
                'mapping.records_ingested': 'records_ingested',
                'mapping.updated_at': 'updated_at'
            }
        }
    }

    ensure_glue_database(database_name)

    try:
        glue_client.get_table(DatabaseName=database_name, Name=watermark_table_name)
        logger.info(f"Watermark Catalog Table verified: {database_name}.{watermark_table_name}")
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('EntityNotFoundException', 'NoSuchEntityException'):
            try:
                glue_client.create_table(
                    DatabaseName=database_name,
                    TableInput={
                        'Name': watermark_table_name,
                        'Description': 'Athena queryable table for all Bronze High-Water Mark state files',
                        'TableType': 'EXTERNAL_TABLE',
                        'Parameters': {
                            'EXTERNAL': 'TRUE',
                            'classification': 'json',
                            'recursive.directories': 'true'
                        },
                        'StorageDescriptor': storage_desc
                    }
                )
                logger.info(f"Created Athena Watermark Catalog Table: {database_name}.{watermark_table_name} at '{watermark_location}'")
            except ClientError as ce:
                if ce.response.get('Error', {}).get('Code') != 'AlreadyExistsException':
                    logger.warning(f"Failed to create Watermark Catalog table '{watermark_table_name}': {ce}")
        else:
            logger.warning(f"Error checking Watermark table '{watermark_table_name}': {e}")

    return f"{database_name}.{watermark_table_name}"


def trigger_glue_crawler(crawler_name: str) -> None:
    """
    Triggers AWS Glue Crawler if configured. Handles already-running crawler gracefully.
    """
    if not crawler_name or not crawler_name.strip():
        return

    c_name = crawler_name.strip()
    try:
        glue_client.start_crawler(Name=c_name)
        logger.info(f"Triggered AWS Glue Crawler '{c_name}' to crawl latest table partitions.")
    except ClientError as ce:
        err_code = ce.response.get('Error', {}).get('Code')
        if err_code == 'CrawlerRunningException':
            logger.info(f"Glue Crawler '{c_name}' is already RUNNING. Latest data will be cataloged.")
        elif err_code in ('EntityNotFoundException', 'NoSuchEntityException'):
            logger.info(f"Glue Crawler '{c_name}' not yet provisioned. Table & partition are already synced via Glue Catalog API.")
        else:
            logger.warning(f"Could not trigger Glue Crawler '{c_name}': {ce}")


# ---------------------------------------------------------
# Main Execution Handler
# ---------------------------------------------------------
def main():
    params = parse_arguments()
    
    source_system = params['SOURCE_SYSTEM']
    table_list = params['TABLE_LIST']
    custom_query = params['CUSTOM_QUERY']
    secret_name = params['SECRET_NAME']
    bronze_bucket = params['BRONZE_BUCKET']
    state_bucket = params['STATE_BUCKET']
    bronze_data_prefix = params.get('BRONZE_DATA_PREFIX', 'bronze/data')
    initial_load_date_cli = params['INITIAL_LOAD_DATE_CLI']
    s3_chunk_size = params['S3_CHUNK_SIZE']
    output_format = params['OUTPUT_FORMAT']
    parquet_compression = params['PARQUET_COMPRESSION']
    error_handling_mode = params['ERROR_HANDLING_MODE']
    cloudwatch_namespace = params['CLOUDWATCH_NAMESPACE']
    source_config = params['SOURCE_CONFIG']
    pipeline_defaults = params.get('PIPELINE_DEFAULTS', {})
    
    execution_start_utc = datetime.now(timezone.utc)
    current_run_time = execution_start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    execution_id = execution_start_utc.strftime('%Y%m%d_%H%M%S')
    partition_prefix = f"_ingested_at={current_run_time}"

    # Glue Catalog & Crawler Configuration (Unified Lake Database with raw_tbl_ Table Prefix)
    glue_catalog_enabled = params.get('GLUE_CATALOG_ENABLED', True)
    glue_database_name = params.get('GLUE_DATABASE_NAME')
    glue_table_prefix = params['GLUE_TABLE_PREFIX']
    bronze_crawler_name = params.get('BRONZE_CRAWLER_NAME')
    trigger_crawler = params.get('TRIGGER_CRAWLER', True)
    sync_watermark_table = params.get('SYNC_WATERMARK_TABLE', True)
    watermark_table_name = params.get('WATERMARK_TABLE_NAME')

    start_banner = (
        f"[JOB START] UAX BRONZE INGESTION | Source: {source_system.upper()} | Tables: {', '.join(table_list)} | Mode: {error_handling_mode}\n"
        "+================================================================================+\n"
        "|                  UAX DATA LAKE - BRONZE INGESTION ENGINE                       |\n"
        "+================================================================================+\n"
        f"|  Execution ID       : {execution_id:<57}|\n"
        f"|  Source System      : {source_system.upper():<57}|\n"
        f"|  Target Tables      : {', '.join(table_list):<57}|\n"
        f"|  Output Format      : {f'{output_format.upper()} (Compression: {parquet_compression})':<57}|\n"
        f"|  Error Handling     : {error_handling_mode:<57}|\n"
        f"|  Bronze Bucket      : {f's3://{bronze_bucket}/':<57}|\n"
        f"|  Bronze Data Path   : {f's3://{bronze_bucket}/{bronze_data_prefix}/{source_system}/':<57}|\n"
        f"|  State S3 Bucket    : {f's3://{state_bucket}/':<57}|\n"
        f"|  Glue Database      : {glue_database_name:<57}|\n"
        f"|  Catalog Prefix     : {glue_table_prefix:<57}|\n"
        f"|  Bronze Crawler     : {bronze_crawler_name if bronze_crawler_name else 'N/A':<57}|\n"
        f"|  Watermark Athena   : {watermark_table_name:<57}|\n"
        f"|  Start Time (UTC)   : {current_run_time:<57}|\n"
        "+================================================================================+"
    )
    logger.info(start_banner)

    # Fetch API secret credentials from Secrets Manager
    secret_dict = get_secret(secret_name)

    # Load matching connector class (supports direct source_system or config type mapping)
    connector_cls = get_connector(source_system, source_config)
    logger.info(f"Loaded connector class: {connector_cls.__name__}")

    failed_tables = []
    table_stats = []

    # Loop through each requested table dynamically
    for table_idx, table_name in enumerate(table_list, start=1):
        table_start_time = datetime.now(timezone.utc)
        table_header = (
            f"[TABLE START] {table_name} [{table_idx}/{len(table_list)}] | Source: {source_system} | Exec: {execution_id} | Timestamp: {current_run_time}\n"
            "+--------------------------------------------------------------------------------+\n"
            f"| >>> [{table_idx}/{len(table_list)}] PROCESSING TABLE: {table_name.upper()} (Source: {source_system})\n"
            f"|     Execution ID       : {execution_id}\n"
            f"|     Table Start (UTC)  : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            "+--------------------------------------------------------------------------------+"
        )
        logger.info(table_header)

        state_key = get_table_state_key(source_system, table_name)
        
        try:
            # Resolves last load date or table-specific initial_load_date (Throws ValueError if load date is missing/null)
            last_load_date = get_last_load_date(
                state_bucket=state_bucket,
                state_key=state_key,
                source_system=source_system,
                table_name=table_name,
                cli_initial_date=initial_load_date_cli,
                source_config=source_config
            )
        except Exception as load_date_err:
            logger.error(f"Cannot process table '{table_name}': {load_date_err}")
            failed_tables.append((table_name, str(load_date_err)))
            table_stats.append({
                "table_name": table_name,
                "status": "FAILED",
                "date_range": {
                    "start_date": None,
                    "end_date": current_run_time
                },
                "records_fetched": 0,
                "chunks_written": 0,
                "duration_seconds": 0.0,
                "s3_destination": f"s3://{bronze_bucket}/{bronze_data_prefix}/{source_system}/{table_name}/{partition_prefix}/",
                "error_message": str(load_date_err)
            })
            if error_handling_mode == 'HALT_ON_ERROR':
                raise
            else:
                continue

        staging_prefix = f"_staging/exec_{execution_id}/{source_system}/{table_name}/"
        final_partition_prefix = f"{bronze_data_prefix}/{source_system}/{table_name}/{partition_prefix}/"

        total_table_records = 0
        parts_written = 0
        first_sample_record = None

        # Memory-safe callback function writing to isolated STAGING directory
        def chunk_writer_callback(records_chunk: list, part_num: int):
            nonlocal total_table_records, parts_written, first_sample_record
            if not records_chunk:
                return

            flatten_enabled = source_config.get('flatten_nested_json', True)
            flatten_sep = source_config.get('flatten_separator', '_')

            processed_chunk = []
            # Flatten nested JSON and enrich with Audit Metadata
            for record in records_chunk:
                if isinstance(record, dict):
                    expanded_recs = flatten_and_expand_record(record, sep=flatten_sep) if flatten_enabled else [record]
                    for rec in expanded_recs:
                        rec['_ingested_at'] = current_run_time
                        rec['_source_system'] = source_system
                        rec['_table_name'] = table_name
                        rec['_execution_id'] = execution_id
                        processed_chunk.append(rec)
                else:
                    processed_chunk.append(record)

            records_chunk = processed_chunk
            if first_sample_record is None and records_chunk:
                first_sample_record = records_chunk[0]
            total_table_records += len(records_chunk)
            parts_written += 1
            
            # Serialize chunk to Parquet or JSON bytes
            file_bytes, content_type, file_ext = serialize_chunk_to_bytes(
                records_chunk=records_chunk,
                output_format=output_format,
                parquet_compression=parquet_compression
            )
            
            staging_key = f"{staging_prefix}delta_{execution_id}_part_{part_num:04d}{file_ext}"
            
            logger.info(f"Writing part {part_num} ({len(records_chunk)} records) to STAGING: s3://{bronze_bucket}/{staging_key}")
            s3_client.put_object(
                Bucket=bronze_bucket,
                Key=staging_key,
                Body=file_bytes,
                ContentType=content_type
            )

        try:
            # Extract delta records writing to STAGING area
            connector_cls.fetch_delta(
                last_load_date=last_load_date,
                secret_dict=secret_dict,
                table_name=table_name,
                source_config=source_config,
                custom_query=custom_query,
                on_chunk_callback=chunk_writer_callback,
                s3_chunk_size=s3_chunk_size
            )

            duration_sec = (datetime.now(timezone.utc) - table_start_time).total_seconds()

            # If extraction completed successfully, promote staging if records exist and update High-Water Mark in S3
            if total_table_records > 0:
                logger.info(f"Table '{table_name}' extraction succeeded ({total_table_records} records in {duration_sec:.2f}s). Promoting staging to Bronze...")
                promote_staging_to_bronze(bronze_bucket, staging_prefix, final_partition_prefix)

                # Sync table schema and execution partition to Glue Data Catalog directly
                if glue_catalog_enabled:
                    sync_bronze_catalog_table(
                        database_name=glue_database_name,
                        table_prefix=glue_table_prefix,
                        source_system=source_system,
                        table_name=table_name,
                        bronze_bucket=bronze_bucket,
                        bronze_data_prefix=bronze_data_prefix,
                        partition_date=execution_start_utc,
                        sample_record=first_sample_record,
                        output_format=output_format,
                        ingested_at=current_run_time
                    )
                    # Trigger Glue Crawler if configured to crawl latest table folder
                    if trigger_crawler and bronze_crawler_name:
                        trigger_glue_crawler(bronze_crawler_name)
            else:
                logger.info(f"Table '{table_name}' extraction completed cleanly with 0 new records since {last_load_date}.")
                cleanup_failed_staging(bronze_bucket, staging_prefix)

            # Create or update High-Water Mark watermark state file in S3 with current execution timestamp
            update_last_load_date(state_bucket, state_key, source_system, table_name, current_run_time, total_table_records, table_prefix=glue_table_prefix)
            logger.info(f"Table '{table_name}' High-Water Mark watermark file updated/created in S3 ({state_key}) with timestamp {current_run_time}.")

            # Ensure Athena-queryable Watermark Catalog Table is synced
            if glue_catalog_enabled and sync_watermark_table:
                sync_watermark_catalog_table(
                    database_name=glue_database_name,
                    watermark_table_name=watermark_table_name,
                    state_bucket=state_bucket,
                    state_prefix=source_config.get('state_prefix') or pipeline_defaults.get('state_prefix', 'metadata/bronze')
                )

            # Record table execution details
            table_stats.append({
                "table_name": table_name,
                "status": "SUCCESS",
                "date_range": {
                    "start_date": last_load_date,
                    "end_date": current_run_time
                },
                "records_fetched": total_table_records,
                "chunks_written": parts_written,
                "duration_seconds": round(duration_sec, 2),
                "s3_destination": f"s3://{bronze_bucket}/{final_partition_prefix}",
                "error_message": None
            })

            # High-visibility table extraction summary block in CloudWatch logs
            table_end_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            catalog_table_display = f"{glue_database_name}.{glue_table_prefix}{table_name}" if glue_catalog_enabled else "N/A"
            summary_card = (
                f"[TABLE SUMMARY] {table_name} | SUCCESS | Records: {total_table_records:,} | Chunks: {parts_written} | Duration: {duration_sec:.2f}s | Range: {last_load_date} -> {current_run_time}\n"
                "+================================================================================+\n"
                f"|  TABLE EXTRACTION COMPLETED: {table_name} [SUCCESS]\n"
                "+--------------------------------------------------------------------------------+\n"
                f"|  * Source System    : {source_system}\n"
                f"|  * Table Name       : {table_name}\n"
                f"|  * Status           : SUCCESS\n"
                f"|  * Extraction Range : {last_load_date}  -->  {current_run_time}\n"
                f"|  * Records Ingested : {total_table_records:,}\n"
                f"|  * Chunks Written   : {parts_written}\n"
                f"|  * Table Duration   : {duration_sec:.2f}s\n"
                f"|  * Start Time (UTC) : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
                f"|  * End Time (UTC)   : {table_end_time_str}\n"
                f"|  * Catalog Table    : {catalog_table_display}\n"
                f"|  * Watermark S3 Key : {state_key}\n"
                f"|  * S3 Destination   : s3://{bronze_bucket}/{final_partition_prefix}\n"
                "+================================================================================+"
            )
            logger.info(summary_card)

            # Report custom CloudWatch metrics
            emit_cloudwatch_metrics(
                namespace=cloudwatch_namespace,
                source_system=source_system,
                table_name=table_name,
                records_count=total_table_records,
                duration_seconds=duration_sec,
                success=True
            )

        except Exception as table_err:
            duration_sec = (datetime.now(timezone.utc) - table_start_time).total_seconds()
            logger.error(f"FAILURE during extraction for table '{table_name}': {table_err}")
            
            # Clean up uncommitted staging artifacts
            cleanup_failed_staging(bronze_bucket, staging_prefix)

            table_stats.append({
                "table_name": table_name,
                "status": "FAILED",
                "date_range": {
                    "start_date": last_load_date if 'last_load_date' in locals() else None,
                    "end_date": current_run_time
                },
                "records_fetched": 0,
                "chunks_written": 0,
                "duration_seconds": round(duration_sec, 2),
                "s3_destination": f"s3://{bronze_bucket}/{final_partition_prefix}",
                "error_message": str(table_err)
            })

            failed_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            failed_card = (
                f"[TABLE FAILED] {table_name} | FAILED | Duration: {duration_sec:.2f}s | Error: {str(table_err)[:60]}\n"
                "+================================================================================+\n"
                f"|  TABLE EXTRACTION FAILED: {table_name} [FAILED]\n"
                "+--------------------------------------------------------------------------------+\n"
                f"|  * Source System    : {source_system}\n"
                f"|  * Table Name       : {table_name}\n"
                f"|  * Status           : FAILED\n"
                f"|  * Extraction Range : {last_load_date if 'last_load_date' in locals() else 'N/A'}  -->  {current_run_time}\n"
                f"|  * Error Details    : {table_err}\n"
                f"|  * Table Duration   : {duration_sec:.2f}s\n"
                f"|  * Start Time (UTC) : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
                f"|  * Failed At (UTC)  : {failed_time_str}\n"
                "+================================================================================+"
            )
            logger.error(failed_card)
            
            # Report failure metric to CloudWatch
            emit_cloudwatch_metrics(
                namespace=cloudwatch_namespace,
                source_system=source_system,
                table_name=table_name,
                records_count=0,
                duration_seconds=duration_sec,
                success=False
            )

            failed_tables.append((table_name, str(table_err)))

            if error_handling_mode == 'HALT_ON_ERROR':
                logger.error(f"Error handling mode is HALT_ON_ERROR. Halting execution immediately.")
                # Persist partial execution log before raising
                halt_log = {
                    "execution_id": execution_id,
                    "job_name": params['JOB_NAME'],
                    "source_system": source_system,
                    "status": "FAILED",
                    "start_time": current_run_time,
                    "end_time": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "duration_seconds": round((datetime.now(timezone.utc) - execution_start_utc).total_seconds(), 2),
                    "total_records_ingested": sum(t.get('records_fetched', 0) for t in table_stats),
                    "tables_count": len(table_list),
                    "tables_succeeded": len(table_list) - len(failed_tables),
                    "tables_failed": len(failed_tables),
                    "tables": table_stats,
                    "error": str(table_err)
                }
                save_execution_log(state_bucket, source_system, execution_id, halt_log)
                raise table_err

    # -------------------------------------------------------------
    # Overall Job Execution Summary & S3 Audit Log Persistence
    # -------------------------------------------------------------
    execution_end_utc = datetime.now(timezone.utc)
    total_job_duration = (execution_end_utc - execution_start_utc).total_seconds()
    overall_status = "FAILED" if failed_tables else "SUCCESS"
    total_records_all = sum(t.get('records_fetched', 0) for t in table_stats)

    execution_log = {
        "execution_id": execution_id,
        "job_name": params['JOB_NAME'],
        "source_system": source_system,
        "status": overall_status,
        "start_time": current_run_time,
        "end_time": execution_end_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "duration_seconds": round(total_job_duration, 2),
        "total_records_ingested": total_records_all,
        "tables_count": len(table_list),
        "tables_succeeded": len(table_list) - len(failed_tables),
        "tables_failed": len(failed_tables),
        "tables": table_stats,
        "parameters": {
            "source_system": source_system,
            "table_list": table_list,
            "batch_size": params['SOURCE_CONFIG'].get('batch_size'),
            "output_format": output_format,
            "parquet_compression": parquet_compression,
            "bronze_bucket": bronze_bucket,
            "state_bucket": state_bucket,
            "error_handling_mode": error_handling_mode
        }
    }

    # Save comprehensive execution log to S3 for auditing and debugging
    save_execution_log(
        state_bucket=state_bucket,
        source_system=source_system,
        execution_id=execution_id,
        execution_log=execution_log
    )

    # Print high-visibility overall summary to CloudWatch
    breakdown_lines = []
    for t in table_stats:
        status_tag = "[OK]  " if t['status'] == 'SUCCESS' else "[FAIL]"
        breakdown_lines.append(
            f"|  {status_tag} {t['table_name']:<20} | Records: {t['records_fetched']:>8,} | "
            f"Chunks: {t['chunks_written']:>2} | Time: {t['duration_seconds']:>6.2f}s | Status: {t['status']}"
        )
        if t.get('error_message'):
            breakdown_lines.append(f"|         └── Error: {t['error_message']}")

    breakdown_str = "\n".join(breakdown_lines)

    overall_card = (
        f"[JOB REPORT] UAX BRONZE INGESTION | Status: {overall_status} | Records: {total_records_all:,} | Tables: {len(table_list) - len(failed_tables)}/{len(table_list)} | Duration: {total_job_duration:.2f}s\n"
        "+================================================================================+\n"
        "|                  BRONZE INGESTION FINAL EXECUTION REPORT                       |\n"
        "+================================================================================+\n"
        f"|  Execution ID          : {execution_id}\n"
        f"|  Source System         : {source_system.upper()}\n"
        f"|  Overall Job Status    : {overall_status}\n"
        f"|  Total Tables          : {len(table_list)} (Succeeded: {len(table_list) - len(failed_tables)}, Failed: {len(failed_tables)})\n"
        f"|  Total Records Ingested: {total_records_all:,}\n"
        f"|  Start Time (UTC)      : {current_run_time}\n"
        f"|  End Time (UTC)        : {execution_end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"|  Total Job Duration    : {total_job_duration:.2f}s\n"
        "+--------------------------------------------------------------------------------+\n"
        "|  TABLE EXECUTION BREAKDOWN:\n"
        f"{breakdown_str}\n"
        "+--------------------------------------------------------------------------------+\n"
        "|  S3 PERSISTENCE LOCATIONS:\n"
        f"|  * Raw Data Path       : s3://{bronze_bucket}/{bronze_data_prefix}/{source_system}/\n"
        f"|  * Watermark State     : s3://{state_bucket}/metadata/bronze/{source_system}/\n"
        f"|  * S3 Execution Log    : s3://{state_bucket}/metadata/logs/bronze/{source_system}/execution_{execution_id}.json\n"
        f"|  * Latest Log Pointer  : s3://{state_bucket}/metadata/logs/bronze/{source_system}/latest_execution.json\n"
        "+--------------------------------------------------------------------------------+\n"
        "|  GLUE CATALOG & ATHENA INTEGRATION:\n"
        f"|  * Glue Database       : {glue_database_name}\n"
        f"|  * Catalog Tables      : {glue_database_name}.{glue_table_prefix}<tablename>\n"
        f"|  * Watermark Athena Tbl: {glue_database_name}.{watermark_table_name}\n"
        f"|  * Bronze Crawler      : {bronze_crawler_name if bronze_crawler_name else 'N/A'}\n"
        "+================================================================================+"
    )
    logger.info(overall_card)

    # Final summary check
    if failed_tables:
        summary_msg = f"Incremental load completed with failures in {len(failed_tables)} table(s): {[t[0] for t in failed_tables]}"
        logger.error(summary_msg)
        raise RuntimeError(summary_msg)

    logger.info("All requested table extractions completed successfully.")


if __name__ == "__main__":
    main()
