"""
AWS Lambda Helper Function: Trigger & Monitor AWS Glue Ingestion & Silver ETL Jobs

Purpose:
  Enables triggering and optional synchronous monitoring of AWS Glue Bronze & Silver jobs
  via AWS Lambda console, CLI, or API invocation when direct AWS Glue Console access is restricted.

Supported Glue Jobs:
  - Bronze Ingestion Job: uax-datalake-bronze-ingestion-dev (or env DEFAULT_BRONZE_JOB)
  - Silver Iceberg ETL Job: uax-datalake-silver-etl-dev (or env DEFAULT_SILVER_JOB)

Payload Schema (JSON):
{
    "layer": "bronze",                                 # Optional: "bronze" or "silver" (Defaults to "bronze")
    "job_name": "uax-datalake-bronze-ingestion-dev",   # Optional explicit job name override
    "source_system": "servicenow",                     # Required: servicenow, moveworks, genesys, postgresql, mysql
    "source_table_name": "incident",                   # Optional: single string ("incident"), list (["incident", "sys_user"]), or comma-separated ("incident,sys_user")
    "secret_name": "uax-datalake/servicenow-credentials-dev", # Optional secret name override
    "custom_query": "",                                # Optional custom query override
    "full_refresh": false,                             # Optional for Silver: true to ignore watermarks
    "watermark_enabled": true,                         # Optional for Silver: default true
    "wait_until_completion": true,                    # Optional: true (default) or false (async)
    "poll_interval_seconds": 10,                       # Optional polling interval in seconds
    "timeout_seconds": 600                             # Optional maximum polling timeout
}
"""

import os
import json
import time
import logging
import boto3
from typing import Dict, Any
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')

# Terminal status codes for AWS Glue Job Runs
TERMINAL_STATES = {'SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'}

DEFAULT_BRONZE_JOB = os.environ.get('DEFAULT_BRONZE_JOB', 'uax-datalake-bronze-ingestion-dev')
DEFAULT_SILVER_JOB = os.environ.get('DEFAULT_SILVER_JOB', 'uax-datalake-silver-etl-dev')


def build_glue_arguments(event: Dict[str, Any]) -> Dict[str, str]:
    """
    Constructs CLI arguments dictionary passed to AWS Glue start_job_run API.
    Supports both Bronze Ingestion and Silver Iceberg ETL parameters.
    """
    glue_args = {}

    # Source System (Required by both Bronze and Silver Glue scripts)
    source_system = event.get('source_system') or event.get('SOURCE_SYSTEM')
    if not source_system:
        raise ValueError(
            "Missing required field 'source_system' in event payload. "
            "Example: {'source_system': 'servicenow', 'layer': 'bronze'}"
        )
    
    glue_args['--SOURCE_SYSTEM'] = str(source_system).strip().lower()

    # Source Table Name(s) resolution
    # Supports:
    #   - String: "raw_tbl_interactions"
    #   - List/Array: ["raw_tbl_interactions", "raw_tbl_conversations"]
    #   - Comma-separated string: "raw_tbl_interactions, raw_tbl_conversations"
    raw_source_tables = (
        event.get('source_table_name')
        if event.get('source_table_name') is not None
        else event.get('SOURCE_TABLE_NAME')
        if event.get('SOURCE_TABLE_NAME') is not None
        else event.get('source_table_names')
        if event.get('source_table_names') is not None
        else event.get('SOURCE_TABLE_NAMES')
        if event.get('SOURCE_TABLE_NAMES') is not None
        else event.get('table_name')
        if event.get('table_name') is not None
        else event.get('TABLE_NAME')
        if event.get('TABLE_NAME') is not None
        else event.get('tables')
        if event.get('tables') is not None
        else event.get('TABLES')
    )

    if raw_source_tables is not None:
        if isinstance(raw_source_tables, (list, tuple, set)):
            # If passed as a JSON array / Python list: ["raw_tbl_interactions", "raw_tbl_conversations"]
            cleaned_tables = [str(t).strip() for t in raw_source_tables if str(t).strip()]
            formatted_tables = ','.join(cleaned_tables)
        elif isinstance(raw_source_tables, str):
            # If passed as a single table or comma-separated string: "raw_tbl_interactions, raw_tbl_conversations"
            cleaned_tables = [t.strip() for t in raw_source_tables.split(',') if t.strip()]
            formatted_tables = ','.join(cleaned_tables)
        else:
            formatted_tables = str(raw_source_tables).strip()

        if formatted_tables:
            glue_args['--SOURCE_TABLE_NAME'] = formatted_tables
            # Dual-populate --TABLE_NAME so downstream Glue jobs accept either parameter
            glue_args['--TABLE_NAME'] = formatted_tables

    # Parameter mappings for Bronze Ingestion and Silver Iceberg ETL
    param_mappings = {
        # Common parameters
        'conf': '--conf',
        '--conf': '--conf',
        'CONF': '--conf',
        '--CONF': '--conf',
        'secret_name': '--SECRET_NAME',
        'SECRET_NAME': '--SECRET_NAME',
        'custom_query': '--CUSTOM_QUERY',
        'CUSTOM_QUERY': '--CUSTOM_QUERY',
        # Bronze specific parameters
        'config_s3_path': '--CONFIG_S3_PATH',
        'CONFIG_S3_PATH': '--CONFIG_S3_PATH',
        'initial_load_date': '--INITIAL_LOAD_DATE_CLI',
        'INITIAL_LOAD_DATE': '--INITIAL_LOAD_DATE_CLI',
        'initial_load_date_cli': '--INITIAL_LOAD_DATE_CLI',
        'INITIAL_LOAD_DATE_CLI': '--INITIAL_LOAD_DATE_CLI',
        'batch_size': '--BATCH_SIZE',
        'BATCH_SIZE': '--BATCH_SIZE',
        's3_chunk_size': '--S3_CHUNK_SIZE',
        'S3_CHUNK_SIZE': '--S3_CHUNK_SIZE',
        'output_format': '--OUTPUT_FORMAT',
        'OUTPUT_FORMAT': '--OUTPUT_FORMAT',
        'error_handling_mode': '--ERROR_HANDLING_MODE',
        'ERROR_HANDLING_MODE': '--ERROR_HANDLING_MODE',
        'trigger_crawler': '--TRIGGER_CRAWLER',
        'TRIGGER_CRAWLER': '--TRIGGER_CRAWLER',
        # Silver specific parameters
        'silver_config_s3_path': '--SILVER_CONFIG_S3_PATH',
        'SILVER_CONFIG_S3_PATH': '--SILVER_CONFIG_S3_PATH',
        'data_lake_bucket': '--DATA_LAKE_BUCKET',
        'DATA_LAKE_BUCKET': '--DATA_LAKE_BUCKET',
        'glue_database': '--GLUE_DATABASE',
        'GLUE_DATABASE': '--GLUE_DATABASE',
        'table_prefix': '--TABLE_PREFIX',
        'TABLE_PREFIX': '--TABLE_PREFIX',
        'watermark_enabled': '--WATERMARK_ENABLED',
        'WATERMARK_ENABLED': '--WATERMARK_ENABLED',
        'watermark_column': '--WATERMARK_COLUMN',
        'WATERMARK_COLUMN': '--WATERMARK_COLUMN',
        'full_refresh': '--FULL_REFRESH',
        'FULL_REFRESH': '--FULL_REFRESH',
        'nkey': '--NKEY',
        'NKEY': '--NKEY',
        'nkeys': '--NKEYS',
        'NKEYS': '--NKEYS',
        'deduplication_order_by': '--DEDUPLICATION_ORDER_BY',
        'DEDUPLICATION_ORDER_BY': '--DEDUPLICATION_ORDER_BY',
        'merge_strategy': '--MERGE_STRATEGY',
        'MERGE_STRATEGY': '--MERGE_STRATEGY',
        'scd_type': '--SCD_TYPE',
        'SCD_TYPE': '--SCD_TYPE',
        'deduplication_strategy': '--DEDUPLICATION_STRATEGY',
        'DEDUPLICATION_STRATEGY': '--DEDUPLICATION_STRATEGY'
    }

    for event_key, glue_arg_key in param_mappings.items():
        val = event.get(event_key)
        if val is not None and str(val).strip() != '':
            glue_args[glue_arg_key] = str(val).strip()

    # Allow custom arbitrary arguments passed via 'arguments' dictionary
    if 'arguments' in event and isinstance(event['arguments'], dict):
        for k, v in event['arguments'].items():
            arg_key = k if k.startswith('--') else f"--{k}"
            glue_args[arg_key] = str(v).strip()

    return glue_args


def poll_glue_job_run(job_name: str, run_id: str, poll_interval: int, timeout_seconds: int) -> Dict[str, Any]:
    """
    Synchronously polls Glue job run status until terminal state or timeout is reached.
    """
    logger.info(f"Polling Glue job status for '{job_name}' (RunID: {run_id}) every {poll_interval}s...")
    start_time = time.time()

    while True:
        elapsed = int(time.time() - start_time)
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Glue job '{job_name}' run '{run_id}' exceeded timeout limit of {timeout_seconds}s. "
                f"Job is still running in AWS Glue."
            )

        response = glue_client.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)
        job_run = response.get('JobRun', {})
        state = job_run.get('JobRunState', 'UNKNOWN')
        error_msg = job_run.get('ErrorMessage')
        exec_seconds = job_run.get('ExecutionTime', elapsed)

        logger.info(f"[{elapsed}s] Job '{job_name}' state: {state}")

        if state in TERMINAL_STATES:
            return {
                'JobState': state,
                'ExecutionTimeSeconds': exec_seconds,
                'ErrorMessage': error_msg,
                'LogGroupName': job_run.get('LogGroupName', '/aws-glue/jobs/output'),
                'CompletedOn': str(job_run.get('CompletedOn', ''))
            }

        time.sleep(poll_interval)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda entrypoint.
    """
    logger.info(f"Received invocation event: {json.dumps(event, default=str)}")

    try:
        # Layer resolution: "bronze" or "silver"
        layer = str(event.get('layer', 'bronze')).strip().lower()

        # Job Name resolution: explicit override > environment defaults
        job_name = event.get('job_name')
        if not job_name:
            if layer == 'silver':
                job_name = DEFAULT_SILVER_JOB
            elif layer == 'bronze':
                job_name = DEFAULT_BRONZE_JOB
            else:
                raise ValueError(f"Invalid layer '{layer}'. Expected 'bronze' or 'silver'.")

        # Build Glue command-line arguments (--SOURCE_SYSTEM, --SOURCE_TABLE_NAME, etc.)
        glue_args = build_glue_arguments(event)
        logger.info(f"Triggering Glue Job: '{job_name}' with arguments: {json.dumps(glue_args)}")

        # Trigger Glue Job Run
        run_response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=glue_args
        )
        run_id = run_response['JobRunId']
        logger.info(f"Successfully started Glue Job '{job_name}' with RunId: {run_id}")

        # Execution mode: Synchronous (wait_until_completion=True) or Asynchronous (wait_until_completion=False)
        wait_until_completion = event.get('wait_until_completion', True)
        if isinstance(wait_until_completion, str):
            wait_until_completion = wait_until_completion.strip().lower() in ('true', '1', 'yes')

        poll_interval = int(event.get('poll_interval_seconds', 10))
        timeout_seconds = int(event.get('timeout_seconds', 540))

        if not wait_until_completion:
            # Asynchronous return (HTTP 202 Accepted)
            logger.info("Asynchronous mode selected. Returning 202 Accepted immediately.")
            return {
                'statusCode': 202,
                'body': json.dumps({
                    'message': 'Glue job started asynchronously.',
                    'job_name': job_name,
                    'job_run_id': run_id,
                    'status': 'STARTING',
                    'arguments': glue_args
                })
            }

        # Synchronous polling loop
        final_result = poll_glue_job_run(
            job_name=job_name,
            run_id=run_id,
            poll_interval=poll_interval,
            timeout_seconds=timeout_seconds
        )

        job_state = final_result['JobState']
        is_success = (job_state == 'SUCCEEDED')
        status_code = 200 if is_success else 500

        response_body = {
            'job_name': job_name,
            'job_run_id': run_id,
            'job_status': job_state,
            'execution_time_seconds': final_result.get('ExecutionTimeSeconds', 0),
            'source_system': glue_args.get('--SOURCE_SYSTEM'),
            'source_table_name': glue_args.get('--SOURCE_TABLE_NAME', 'ALL_CONFIGURED'),
            'cloudwatch_log_group': final_result.get('LogGroupName', '/aws-glue/jobs/output'),
            'error_message': final_result.get('ErrorMessage', '') if not is_success else None
        }

        logger.info(f"Execution complete. Final Status: {job_state}")
        return {
            'statusCode': status_code,
            'body': json.dumps(response_body)
        }

    except Exception as err:
        logger.error(f"Lambda execution error: {str(err)}", exc_info=True)
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': str(err),
                'message': 'Failed to trigger or monitor AWS Glue Job'
            })
        }
