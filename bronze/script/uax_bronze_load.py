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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("uax_bronze_load")
logger.setLevel(logging.INFO)
# Ensure stdout handler exists so logs appear in AWS Glue CloudWatch logs immediately
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)
logging.getLogger().setLevel(logging.INFO)

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
    cli_tables = get_cli_arg('TABLE_NAME', 'table_name', 'TABLES', 'tables', 'TABLE_NAMES', 'table_names', 'TABLE', 'table')
    if cli_tables:
        table_list = [t.strip() for t in cli_tables.split(',') if t.strip()]
        logger.info(f"[PARAM PRECEDENCE] Table List resolved from GLUE CLI (Priority 1): {table_list}")
    elif source_config.get('default_tables'):
        table_list = list(source_config['default_tables'])
        logger.info(f"[PARAM PRECEDENCE] Table List resolved from CONFIG FILE (Priority 2): {table_list}")
    else:
        table_list = ['conversations'] if source_system_clean == 'moveworks' else ['incident']
        logger.info(f"[PARAM PRECEDENCE] Table List resolved from CODE DEFAULT (Priority 3): {table_list}")

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
        'SOURCE_CONFIG': source_config
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


def update_last_load_date(state_bucket: str, state_key: str, source_system: str, table_name: str, current_run_time: str, total_records: int) -> None:
    """
    Writes/Updates the High-Water Mark JSON metadata file for a specific table in S3.
    """
    s3_path = f"s3://{state_bucket}/{state_key}"
    state_payload = {
        "source_system": source_system,
        "table_name": table_name,
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
    
    execution_start_utc = datetime.now(timezone.utc)
    current_run_time = execution_start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
    execution_id = execution_start_utc.strftime('%Y%m%d_%H%M%S')
    partition_prefix = execution_start_utc.strftime('year=%Y/month=%m/day=%d')

    logger.info(f"Starting execution run '{execution_id}' for '{source_system}' tables {table_list} at {current_run_time}")
    logger.info(f"Output Format: '{output_format.upper()}' (Compression: '{parquet_compression}'), Error Policy: '{error_handling_mode}'")

    # Fetch API secret credentials from Secrets Manager
    secret_dict = get_secret(secret_name)

    # Load matching connector class (supports direct source_system or config type mapping)
    connector_cls = get_connector(source_system, source_config)
    logger.info(f"Loaded connector class: {connector_cls.__name__}")

    failed_tables = []
    table_stats = []

    # Loop through each requested table dynamically
    for table_name in table_list:
        table_start_time = datetime.now(timezone.utc)
        logger.info(f"\n========================================================")
        logger.info(f" Processing Table: '{table_name}' (Source: '{source_system}')")
        logger.info(f" Execution ID: '{execution_id}'")
        logger.info(f"========================================================")

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

        # Memory-safe callback function writing to isolated STAGING directory
        def chunk_writer_callback(records_chunk: list, part_num: int):
            nonlocal total_table_records, parts_written
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
            else:
                logger.info(f"Table '{table_name}' extraction completed cleanly with 0 new records since {last_load_date}.")
                cleanup_failed_staging(bronze_bucket, staging_prefix)

            # Create or update High-Water Mark watermark state file in S3 with current execution timestamp
            update_last_load_date(state_bucket, state_key, source_system, table_name, current_run_time, total_table_records)
            logger.info(f"Table '{table_name}' High-Water Mark watermark file updated/created in S3 ({state_key}) with timestamp {current_run_time}.")

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
            logger.info("\n" + "=" * 80)
            logger.info(f" TABLE EXTRACTION SUMMARY: {table_name}")
            logger.info("-" * 80)
            logger.info(f" Source System    : {source_system}")
            logger.info(f" Table Name       : {table_name}")
            logger.info(f" Status           : SUCCESS")
            logger.info(f" Date Range       : {last_load_date}  -->  {current_run_time}")
            logger.info(f" Records Fetched  : {total_table_records:,}")
            logger.info(f" Chunks Written   : {parts_written}")
            logger.info(f" Duration         : {duration_sec:.2f}s")
            logger.info(f" S3 Destination   : s3://{bronze_bucket}/{final_partition_prefix}")
            logger.info("=" * 80 + "\n")

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

            logger.error("\n" + "=" * 80)
            logger.error(f" TABLE EXTRACTION FAILED: {table_name}")
            logger.error("-" * 80)
            logger.error(f" Source System    : {source_system}")
            logger.error(f" Table Name       : {table_name}")
            logger.error(f" Status           : FAILED")
            logger.error(f" Date Range       : {last_load_date if 'last_load_date' in locals() else 'N/A'}  -->  {current_run_time}")
            logger.error(f" Error Details    : {table_err}")
            logger.error("=" * 80 + "\n")
            
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
    logger.info("\n" + "=" * 80)
    logger.info("              AWS GLUE BRONZE INGESTION EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f" Execution ID       : {execution_id}")
    logger.info(f" Source System      : {source_system}")
    logger.info(f" Overall Status     : {overall_status}")
    logger.info(f" Total Records      : {total_records_all:,}")
    logger.info(f" Tables Processed   : {len(table_list)} (Succeeded: {len(table_list) - len(failed_tables)}, Failed: {len(failed_tables)})")
    logger.info(f" Start Time (UTC)   : {current_run_time}")
    logger.info(f" End Time (UTC)     : {execution_end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}")
    logger.info(f" Total Duration     : {total_job_duration:.2f}s")
    logger.info(f" S3 Execution Log   : s3://{state_bucket}/metadata/logs/bronze/{source_system}/execution_{execution_id}.json")
    logger.info("-" * 80)
    logger.info(" TABLE-BY-TABLE BREAKDOWN:")
    for t in table_stats:
        status_icon = "[OK]" if t['status'] == 'SUCCESS' else "[FAIL]"
        date_info = f"{t['date_range']['start_date']} to {t['date_range']['end_date']}" if t['date_range']['start_date'] else "N/A"
        logger.info(
            f"   {status_icon} {t['table_name']:<20} | Status: {t['status']:<7} | "
            f"Records: {t['records_fetched']:>7,} | Date: {date_info} | "
            f"Time: {t['duration_seconds']:>5.2f}s"
        )
        if t.get('error_message'):
            logger.info(f"        -> Error: {t['error_message']}")
    logger.info("=" * 80 + "\n")

    # Final summary check
    if failed_tables:
        summary_msg = f"Incremental load completed with failures in {len(failed_tables)} table(s): {[t[0] for t in failed_tables]}"
        logger.error(summary_msg)
        raise RuntimeError(summary_msg)

    logger.info("\nAll requested table extractions completed successfully.")


if __name__ == "__main__":
    main()
