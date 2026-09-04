"""
AWS Lambda Helper Function: Trigger & Monitor AWS Glue Ingestion & Silver ETL Jobs

Purpose:
  Enables triggering and optional synchronous monitoring of AWS Glue Bronze & Silver jobs
  via AWS Lambda console or Lambda API invocation when AWS Glue Console access is restricted.

Payload Schema (JSON):
{
    "layer": "bronze",                                 # Optional: "bronze" or "silver" (Defaults to "bronze")
    "job_name": "uax-datalake-bronze-ingestion-dev",   # Optional explicit job name override
    "source_system": "servicenow",                      # Required (servicenow, moveworks, genesys)
    "table_name": "incident",                          # Optional (e.g. "incident" or "incident,sys_user")
    
    # Bronze specific options
    "secret_name": "uax-datalake/servicenow-credentials-dev",
    "bronze_bucket": "uax-datalake-dev-bucket",
    "custom_query": "",
    "initial_load_date": "2024-01-01T00:00:00Z",
    "batch_size": "1000",
    "base_url": "",
    "output_format": "parquet",
    
    # Silver specific options
    "data_lake_bucket": "uax-datalake-dev-bucket",
    "glue_database": "uax-datalake-db-dev",
    "silver_config_s3_path": "s3://uax-datalake-dev-bucket/silver/script/config/silver_config.json",
    
    # Execution & Monitoring control
    "error_handling_mode": "CONTINUE_ON_ERROR",
    "wait_until_completion": true,
    "poll_interval_seconds": 10,
    "timeout_seconds": 600
}
"""

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

DEFAULT_BRONZE_JOB = 'uax-datalake-bronze-ingestion-dev'
DEFAULT_SILVER_JOB = 'uax-datalake-silver-iceberg-etl-dev'


def resolve_job_name(event: Dict[str, Any]) -> str:
    """
    Resolves target Glue job name based on explicit 'job_name' or 'layer' ('bronze' / 'silver').
    """
    if event.get('job_name') or event.get('JOB_NAME'):
        return str(event.get('job_name') or event.get('JOB_NAME')).strip()
    
    layer = str(event.get('layer', 'bronze')).strip().lower()
    if layer == 'silver':
        return DEFAULT_SILVER_JOB
    return DEFAULT_BRONZE_JOB


def build_glue_arguments(event: Dict[str, Any]) -> Dict[str, str]:
    """
    Translates input event parameters into Glue argument dictionary format with '--' prefix.
    Supports arguments for both Bronze ingestion and Silver PySpark Iceberg ETL jobs.
    """
    glue_args = {}

    # Source System (Required by Glue scripts)
    source_system = event.get('source_system') or event.get('SOURCE_SYSTEM')
    if not source_system:
        raise ValueError("Missing required field 'source_system' in event payload. Example: {'source_system': 'servicenow'}")
    
    glue_args['--SOURCE_SYSTEM'] = str(source_system).strip().lower()

    # Dynamic parameter mappings for both Bronze & Silver Glue jobs
    param_mappings = {
        # Table list
        'table_name': '--TABLE_NAME',
        'tables': '--TABLE_NAME',
        'TABLE_NAME': '--TABLE_NAME',
        
        # Bronze options
        'secret_name': '--SECRET_NAME',
        'SECRET_NAME': '--SECRET_NAME',
        'bronze_bucket': '--BRONZE_BUCKET',
        'BRONZE_BUCKET': '--BRONZE_BUCKET',
        'state_bucket': '--STATE_BUCKET',
        'STATE_BUCKET': '--STATE_BUCKET',
        'custom_query': '--CUSTOM_QUERY',
        'CUSTOM_QUERY': '--CUSTOM_QUERY',
        'initial_load_date': '--INITIAL_LOAD_DATE',
        'INITIAL_LOAD_DATE': '--INITIAL_LOAD_DATE',
        'batch_size': '--BATCH_SIZE',
        'BATCH_SIZE': '--BATCH_SIZE',
        'base_url': '--BASE_URL',
        'BASE_URL': '--BASE_URL',
        'output_format': '--OUTPUT_FORMAT',
        'OUTPUT_FORMAT': '--OUTPUT_FORMAT',
        'config_s3_path': '--CONFIG_S3_PATH',
        'CONFIG_S3_PATH': '--CONFIG_S3_PATH',

        # Silver options
        'data_lake_bucket': '--DATA_LAKE_BUCKET',
        'DATA_LAKE_BUCKET': '--DATA_LAKE_BUCKET',
        'glue_database': '--GLUE_DATABASE',
        'GLUE_DATABASE': '--GLUE_DATABASE',
        'silver_config_s3_path': '--SILVER_CONFIG_S3_PATH',
        'SILVER_CONFIG_S3_PATH': '--SILVER_CONFIG_S3_PATH',

        # Common options
        'error_handling_mode': '--ERROR_HANDLING_MODE',
        'ERROR_HANDLING_MODE': '--ERROR_HANDLING_MODE'
    }

    for event_key, glue_arg_key in param_mappings.items():
        val = event.get(event_key)
        if val is not None and str(val).strip() != '':
            glue_args[glue_arg_key] = str(val).strip()

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
            logger.warning(f"Polling timed out after {elapsed}s. Glue job is still executing in background.")
            return {
                'JobState': 'POLL_TIMEOUT',
                'ExecutionTimeSeconds': elapsed,
                'ErrorMessage': f"Lambda monitoring timed out after {timeout_seconds} seconds. Glue Job ID '{run_id}' continues running in AWS Glue."
            }

        try:
            response = glue_client.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)
            job_run = response.get('JobRun', {})
            state = job_run.get('JobRunState', 'UNKNOWN')
            execution_time = job_run.get('ExecutionTime', elapsed)
            error_message = job_run.get('ErrorMessage', '')
            log_group = job_run.get('LogGroupName', '')

            logger.info(f"Glue Job Status [{elapsed}s elapsed]: State='{state}'")

            if state in TERMINAL_STATES:
                return {
                    'JobState': state,
                    'ExecutionTimeSeconds': execution_time,
                    'ErrorMessage': error_message,
                    'LogGroupName': log_group
                }

        except ClientError as err:
            logger.error(f"Error fetching Glue job run status: {err}")
            raise

        time.sleep(poll_interval)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entry point.
    """
    logger.info(f"Received Lambda event payload: {json.dumps(event)}")

    try:
        # Resolve target Glue Job Name (Bronze or Silver)
        job_name = resolve_job_name(event)
        
        # Build Glue command-line arguments (--SOURCE_SYSTEM, --TABLE_NAME, etc.)
        glue_args = build_glue_arguments(event)
        
        logger.info(f"Triggering Glue Job '{job_name}' with Arguments: {json.dumps(glue_args)}")

        # Trigger Glue Job execution via boto3 SDK
        start_response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=glue_args
        )
        
        run_id = start_response.get('JobRunId')
        logger.info(f"Glue Job successfully triggered! JobRunId: '{run_id}'")

        # Parse polling control flags
        wait_until_completion = event.get('wait_until_completion', True)
        poll_interval = int(event.get('poll_interval_seconds', 10))
        timeout_seconds = int(event.get('timeout_seconds', 540))  # Default 9 mins

        if not wait_until_completion:
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
            'table_name': glue_args.get('--TABLE_NAME', 'DEFAULT'),
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
