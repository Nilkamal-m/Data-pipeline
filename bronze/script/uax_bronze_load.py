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

# Ensure script directory is on sys.path for ConfigLoader & Connector imports
script_dir = os.path.dirname(os.path.abspath(__file__))
for path in [script_dir, os.getcwd(), "/tmp/extraPython"]:
    if os.path.exists(path) and path not in sys.path:
        sys.path.insert(0, path)

from config_loader import ConfigLoader
from connectors import get_connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("uax_bronze_load")

s3_client = boto3.client('s3')


def get_secret(secret_name: str) -> dict:
    """
    Fetches API credential secret payload from AWS Secrets Manager.
    Returns empty dict if secret_name is empty or not specified.
    """
    if not secret_name or secret_name.strip() == "":
        logger.info("No secret_name specified. Proceeding without Secrets Manager payload.")
        return {}

    logger.info(f"Fetching secret payload for '{secret_name}' from AWS Secrets Manager...")
    secrets_client = boto3.client('secretsmanager')
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_str = response.get('SecretString')
        if not secret_str:
            raise ValueError(f"Secret '{secret_name}' contains no SecretString payload.")
        return json.loads(secret_str)
    except ClientError as err:
        logger.warning(f"Could not fetch secret '{secret_name}' ({err}). Proceeding with empty secret dictionary.")
        return {}


def parse_arguments() -> dict:
    """
    Parses CLI arguments passed by Step Functions or AWS Glue Job Run.
    """
    arg_dict = {}
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg.startswith('--'):
            key = arg[2:]
            val = sys.argv[i + 1] if (i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--')) else ''
            arg_dict[key] = val
            i += 2
        else:
            i += 1

    source_system = arg_dict.get('SOURCE_SYSTEM')
    if not source_system:
        logger.error("Missing required parameter '--SOURCE_SYSTEM'. Please pass '--SOURCE_SYSTEM' from Step Functions.")
        raise ValueError("Missing required argument '--SOURCE_SYSTEM'. Example usage: --SOURCE_SYSTEM servicenow")

    source_system_clean = source_system.strip().lower()
    config_s3_path = arg_dict.get('CONFIG_S3_PATH')
    
    # Load centralized Bronze configuration
    full_config = ConfigLoader.load_config(config_s3_path=config_s3_path, s3_client=s3_client)
    pipeline_defaults = ConfigLoader.get_pipeline_defaults(full_config)
    source_config = ConfigLoader.get_source_config(source_system_clean, full_config)

    # Allow CLI parameters to dynamically override bronze_config.json values for manual testing / Step Functions
    if arg_dict.get('BASE_URL'):
        source_config['base_url'] = arg_dict['BASE_URL'].strip()
        logger.info(f"CLI Parameter Override: 'base_url' -> '{source_config['base_url']}'")

    if arg_dict.get('BATCH_SIZE'):
        source_config['batch_size'] = int(arg_dict['BATCH_SIZE'])
        logger.info(f"CLI Parameter Override: 'batch_size' -> {source_config['batch_size']}")

    if arg_dict.get('RESPONSE_RECORDS_KEY'):
        source_config['response_records_key'] = arg_dict['RESPONSE_RECORDS_KEY'].strip()
        logger.info(f"CLI Parameter Override: 'response_records_key' -> '{source_config['response_records_key']}'")

    if arg_dict.get('FLATTEN_NESTED_JSON'):
        source_config['flatten_nested_json'] = (arg_dict['FLATTEN_NESTED_JSON'].strip().lower() == 'true')
        logger.info(f"CLI Parameter Override: 'flatten_nested_json' -> {source_config['flatten_nested_json']}")

    if arg_dict.get('FLATTEN_SEPARATOR'):
        source_config['flatten_separator'] = arg_dict['FLATTEN_SEPARATOR'].strip()
        logger.info(f"CLI Parameter Override: 'flatten_separator' -> '{source_config['flatten_separator']}'")

    # Resolve table list
    raw_tables = arg_dict.get('TABLE_NAME') or arg_dict.get('TABLES') or arg_dict.get('TABLE_NAMES')
    if raw_tables:
        table_list = [t.strip() for t in raw_tables.split(',') if t.strip()]
    else:
        table_list = source_config.get('default_tables', ['incident'])

    custom_query = arg_dict.get('CUSTOM_QUERY')
    job_name = arg_dict.get('JOB_NAME', f"glue-incremental-load-{source_system_clean}")
    secret_name = arg_dict.get('SECRET_NAME', '')
    bronze_bucket = arg_dict.get('BRONZE_BUCKET', os.environ.get('BRONZE_BUCKET', 'uax-datalake-dev-bucket'))
    state_bucket = arg_dict.get('STATE_BUCKET', bronze_bucket)
    initial_load_date_cli = arg_dict.get('INITIAL_LOAD_DATE')
    s3_chunk_size = int(arg_dict.get('S3_CHUNK_SIZE') or pipeline_defaults.get('s3_chunk_size', 10000))
    output_format = (arg_dict.get('OUTPUT_FORMAT') or pipeline_defaults.get('output_format', 'parquet')).lower()
    parquet_compression = (arg_dict.get('PARQUET_COMPRESSION') or pipeline_defaults.get('parquet_compression', 'snappy')).lower()
    error_handling_mode = (arg_dict.get('ERROR_HANDLING_MODE') or pipeline_defaults.get('error_handling_mode', 'CONTINUE_ON_ERROR')).upper()
    cloudwatch_namespace = arg_dict.get('CLOUDWATCH_NAMESPACE') or pipeline_defaults.get('cloudwatch_namespace', 'UAX/DataPipeline/Ingestion')

    parsed_params = {
        'JOB_NAME': job_name,
        'SOURCE_SYSTEM': source_system_clean,
        'TABLE_LIST': table_list,
        'CUSTOM_QUERY': custom_query,
        'SECRET_NAME': secret_name,
        'BRONZE_BUCKET': bronze_bucket,
        'STATE_BUCKET': state_bucket,
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
def flatten_dict(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Recursively flattens nested JSON dictionaries into a single level key-value map.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, str(v) if v is not None else None))
        else:
            items.append((new_key, v))
    return dict(items)


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
    """Returns the S3 metadata key for a given source system and table name."""
    return f"metadata/{source_system}/{table_name}/watermark.json"


def get_last_load_date(
    state_bucket: str,
    state_key: str,
    source_system: str,
    table_name: str,
    cli_initial_date: Optional[str] = None,
    source_config: Optional[dict] = None
) -> str:
    """
    Fetches the last successful load timestamp for a table from S3 metadata JSON.
    If state file does not exist, fetches table-specific initial_load_date.
    Strictly throws ValueError if load date is NULL/missing (no arbitrary past record fallbacks).
    """
    s3_path = f"s3://{state_bucket}/{state_key}"
    try:
        logger.info(f"Fetching High-Water Mark state file from '{s3_path}'")
        response = s3_client.get_object(Bucket=state_bucket, Key=state_key)
        state_content = response['Body'].read().decode('utf-8')
        state_data = json.loads(state_content)
        
        last_load_date = state_data.get('last_load_date')
        if last_load_date and str(last_load_date).strip():
            logger.info(f"Retrieved High-Water Mark from '{s3_path}': {last_load_date}")
            return str(last_load_date).strip()

    except ClientError as err:
        error_code = err.response.get('Error', {}).get('Code')
        if error_code in ('NoSuchKey', '404'):
            logger.warning(f"No previous state file at '{s3_path}'. Resolving initial load date for first run...")
        else:
            logger.error(f"Error reading state file from '{s3_path}': {err}")
            raise

    # Resolve table-specific initial_load_date from config/CLI (Throws ValueError if missing)
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
            if error_handling_mode == 'HALT_ON_ERROR':
                raise
            else:
                failed_tables.append((table_name, str(load_date_err)))
                continue

        staging_prefix = f"_staging/exec_{execution_id}/{source_system}/{table_name}/"
        final_partition_prefix = f"bronze/{source_system}/{table_name}/{partition_prefix}/"

        total_table_records = 0

        # Memory-safe callback function writing to isolated STAGING directory
        def chunk_writer_callback(records_chunk: list, part_num: int):
            nonlocal total_table_records
            if not records_chunk:
                return

            flatten_enabled = source_config.get('flatten_nested_json', True)
            flatten_sep = source_config.get('flatten_separator', '_')

            processed_chunk = []
            # Flatten nested JSON and enrich with Audit Metadata
            for record in records_chunk:
                if isinstance(record, dict):
                    rec = flatten_dict(record, sep=flatten_sep) if flatten_enabled else record
                    rec['_ingested_at'] = current_run_time
                    rec['_source_system'] = source_system
                    rec['_table_name'] = table_name
                    rec['_execution_id'] = execution_id
                    processed_chunk.append(rec)
                else:
                    processed_chunk.append(record)

            records_chunk = processed_chunk
            total_table_records += len(records_chunk)
            
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

            # If extraction completed successfully, atomically promote staging -> bronze
            if total_table_records > 0:
                logger.info(f"Table '{table_name}' extraction succeeded ({total_table_records} records in {duration_sec:.2f}s). Promoting staging to Bronze...")
                promote_staging_to_bronze(bronze_bucket, staging_prefix, final_partition_prefix)

                # Update High-Water Mark ONLY after staging promotion succeeds
                update_last_load_date(state_bucket, state_key, source_system, table_name, current_run_time, total_table_records)
                logger.info(f"Table '{table_name}' successfully ingested into Bronze with High-Water Mark {current_run_time}.")
            else:
                logger.info(f"Table '{table_name}' extraction completed cleanly with 0 new records since {last_load_date}.")
                cleanup_failed_staging(bronze_bucket, staging_prefix)

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
                raise table_err

    # Final summary check
    if failed_tables:
        summary_msg = f"Incremental load completed with failures in {len(failed_tables)} table(s): {[t[0] for t in failed_tables]}"
        logger.error(summary_msg)
        raise RuntimeError(summary_msg)

    logger.info("\nAll requested table extractions completed successfully.")


if __name__ == "__main__":
    main()
