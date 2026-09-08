"""
AWS Lambda Helper Function: Trigger & Monitor AWS Glue Jobs & Execute Athena Queries

Purpose:
  1. Trigger and monitor AWS Glue Bronze Ingestion & Silver Iceberg ETL jobs.
  2. Execute queries directly on Amazon Athena (Iceberg / Glue Data Catalog tables)
     and output formatted results into CloudWatch / Lambda execution logs.

Supported Operations:
  A. Athena Query Execution (New):
     Pass "query", "athena_query", or "sql" in the event payload to execute any SQL query
     against Athena and inspect the result table in the Lambda logs.

  B. AWS Glue Job Triggering (Existing):
     - Bronze Ingestion Job: uax-datalake-bronze-ingestion-dev (or env DEFAULT_BRONZE_JOB)
     - Silver Iceberg ETL Job: uax-datalake-silver-etl-dev (or env DEFAULT_SILVER_JOB)

Athena Query Payload Schema (JSON):
{
    "query": "SELECT * FROM uax_datalake_db_dev.raw_tbl_incident LIMIT 10", # Required SQL query (pass <database>.<table_name> directly in query)
    "max_results": 50,                                                      # Optional: maximum rows to display in log (default: 50)
    "workgroup": "uax-datalake-workgroup-dev",                              # Optional: Athena workgroup (default: uax-datalake-workgroup-dev)
    "database": "uax_datalake_db_dev",                                      # Optional: only needed if not passing <database>.<table_name> in query
    "timeout_seconds": 120                                                  # Optional: query execution timeout (default: 120s)
}

Glue Job Payload Schema (JSON):
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
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')
athena_client = boto3.client('athena')

# Terminal status codes for AWS Glue Job Runs
TERMINAL_STATES = {'SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'}

DEFAULT_BRONZE_JOB = os.environ.get('DEFAULT_BRONZE_JOB', 'uax-datalake-bronze-ingestion-dev')
DEFAULT_SILVER_JOB = os.environ.get('DEFAULT_SILVER_JOB', 'uax-datalake-silver-etl-dev')

DEFAULT_ATHENA_DATABASE = os.environ.get('DEFAULT_ATHENA_DATABASE', 'uax_datalake_db_dev')
DEFAULT_ATHENA_WORKGROUP = os.environ.get('DEFAULT_ATHENA_WORKGROUP', 'uax-datalake-workgroup-dev')
DEFAULT_ATHENA_OUTPUT_LOCATION = os.environ.get('DEFAULT_ATHENA_OUTPUT_LOCATION', '')


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
        'crawler_name': '--CRAWLER_NAME',
        'CRAWLER_NAME': '--CRAWLER_NAME',
        'silver_crawler_name': '--SILVER_CRAWLER_NAME',
        'SILVER_CRAWLER_NAME': '--SILVER_CRAWLER_NAME',
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


def is_athena_query_event(event: Dict[str, Any]) -> bool:
    """
    Detects if the incoming Lambda payload is intended for Athena query execution.
    Recognizes:
      - Explicit query fields: "query", "athena_query", "sql"
      - Action: "query", "athena", "run_query"
      - Layer: "athena"
    """
    for q_key in ('query', 'athena_query', 'sql', 'QUERY', 'ATHENA_QUERY', 'SQL'):
        if event.get(q_key) and str(event[q_key]).strip():
            return True
    layer = str(event.get('layer', '')).strip().lower()
    action = str(event.get('action', '')).strip().lower()
    return layer == 'athena' or action in ('query', 'athena', 'run_query')


def execute_athena_query(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Executes an SQL query against Amazon Athena, polls for completion, formats
    the tabular results into the Lambda execution logs, and returns the records.
    """
    # 1. Resolve SQL Query string
    query_str = None
    for q_key in ('query', 'athena_query', 'sql', 'QUERY', 'ATHENA_QUERY', 'SQL'):
        if event.get(q_key) and str(event[q_key]).strip():
            query_str = str(event[q_key]).strip()
            break

    if not query_str:
        raise ValueError(
            "Missing SQL query in payload. Please provide 'query' (e.g., {'query': 'SELECT * FROM uax_datalake_db_dev.raw_tbl_incident LIMIT 10'})."
        )

    # 2. Resolve Database, Workgroup, Output Location
    # Database is completely optional if your query passes <database>.<table_name> directly (e.g. uax_datalake_db_dev.raw_tbl_incident)
    database = (
        event.get('database')
        or event.get('db')
        or event.get('glue_database')
        or event.get('DATABASE')
        or DEFAULT_ATHENA_DATABASE
    )
    database = str(database).strip() if database else 'uax_datalake_db_dev'

    workgroup = (
        event.get('workgroup')
        or event.get('athena_workgroup')
        or event.get('WORKGROUP')
        or DEFAULT_ATHENA_WORKGROUP
    )
    workgroup = str(workgroup).strip() if workgroup else 'uax-datalake-workgroup-dev'

    output_location = (
        event.get('output_location')
        or event.get('s3_output')
        or event.get('OUTPUT_LOCATION')
        or DEFAULT_ATHENA_OUTPUT_LOCATION
    )
    output_location = str(output_location).strip() if output_location else ''

    # Region resolution: payload override > ATHENA_REGION env > default client
    athena_region = event.get('region') or event.get('athena_region') or os.environ.get('ATHENA_REGION')
    client = boto3.client('athena', region_name=athena_region.strip()) if athena_region and athena_region.strip() else athena_client

    max_results = int(event.get('max_results') or event.get('MAX_RESULTS') or 50)
    timeout_seconds = int(event.get('timeout_seconds') or event.get('TIMEOUT_SECONDS') or 120)
    poll_interval = float(event.get('poll_interval_seconds') or event.get('POLL_INTERVAL_SECONDS') or 1.0)

    logger.info("=" * 80)
    logger.info("STARTING AMAZON ATHENA QUERY EXECUTION")
    logger.info(f"Target Database : {database}")
    logger.info(f"Athena Workgroup: {workgroup}")
    logger.info(f"Athena Region   : {athena_region or 'Default'}")
    logger.info(f"Max Results Log : {max_results}")
    logger.info(f"Query String    :\n{query_str}")
    logger.info("=" * 80)

    # 3. Start Athena Query Execution
    start_params: Dict[str, Any] = {
        'QueryString': query_str,
        'WorkGroup': workgroup
    }
    if database:
        start_params['QueryExecutionContext'] = {'Database': database}
    
    result_config: Dict[str, Any] = {}
    if output_location:
        result_config['OutputLocation'] = output_location

    # Optional KMS encryption configuration
    encryption_option = event.get('encryption_option') or event.get('ENCRYPTION_OPTION')
    kms_key = event.get('kms_key') or event.get('KMS_KEY') or event.get('kms_key_arn')
    if encryption_option or kms_key:
        enc_dict: Dict[str, Any] = {'EncryptionOption': encryption_option or 'SSE_KMS'}
        if kms_key:
            enc_dict['KmsKey'] = kms_key
        result_config['EncryptionConfiguration'] = enc_dict

    if result_config:
        start_params['ResultConfiguration'] = result_config

    try:
        start_response = client.start_query_execution(**start_params)
    except ClientError as ce:
        err_msg = str(ce)
        # Workgroups with enforced output location may reject explicit ResultConfiguration
        if 'InvalidRequestException' in err_msg and 'workgroup' in err_msg.lower() and ('outputlocation' in err_msg.lower() or 'configuration' in err_msg.lower()):
            logger.info("Workgroup enforces output location. Retrying without explicit ResultConfiguration...")
            start_params.pop('ResultConfiguration', None)
            start_response = client.start_query_execution(**start_params)
        else:
            raise

    query_execution_id = start_response['QueryExecutionId']
    logger.info(f"Submitted to Athena. QueryExecutionId: {query_execution_id}")

    # 4. Polling loop
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            try:
                client.stop_query_execution(QueryExecutionId=query_execution_id)
            except Exception:
                pass
            raise TimeoutError(
                f"Athena query '{query_execution_id}' timed out after {timeout_seconds}s."
            )

        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution = response.get('QueryExecution', {})
        status_info = query_execution.get('Status', {})
        state = status_info.get('State', 'UNKNOWN')

        if state == 'SUCCEEDED':
            logger.info(f"Athena Query '{query_execution_id}' SUCCEEDED in {elapsed:.2f}s")
            break
        elif state in ('FAILED', 'CANCELLED'):
            reason = status_info.get('StateChangeReason', 'Unknown error')
            logger.error(f"Athena Query '{query_execution_id}' {state}: {reason}")
            return {
                'statusCode': 500 if state == 'FAILED' else 400,
                'body': json.dumps({
                    'query_execution_id': query_execution_id,
                    'status': state,
                    'query': query_str,
                    'database': database,
                    'workgroup': workgroup,
                    'error_message': reason
                })
            }

        time.sleep(poll_interval)

    # 5. Fetch Query Execution Statistics
    stats = query_execution.get('Statistics', {})
    exec_time_ms = stats.get('EngineExecutionTimeInMillis', 0)
    data_scanned_bytes = stats.get('DataScannedInBytes', 0)
    data_scanned_mb = data_scanned_bytes / (1024.0 * 1024.0)

    # 6. Fetch Query Results
    results_paginator = client.get_paginator('get_query_results')
    column_names: List[str] = []
    records: List[Dict[str, Any]] = []
    raw_table_rows: List[List[str]] = []
    is_first_page = True

    for page in results_paginator.paginate(QueryExecutionId=query_execution_id, PaginationConfig={'MaxItems': max_results}):
        result_set = page.get('ResultSet', {})
        rows = result_set.get('Rows', [])

        if not rows:
            continue

        start_row_idx = 0
        if is_first_page:
            # Row 0 contains column names
            column_names = [col.get('VarCharValue', f'col_{idx}') for idx, col in enumerate(rows[0].get('Data', []))]
            start_row_idx = 1
            is_first_page = False

        for r in rows[start_row_idx:]:
            row_data = r.get('Data', [])
            record_dict = {}
            record_values = []
            for idx, col_name in enumerate(column_names):
                val = row_data[idx].get('VarCharValue') if idx < len(row_data) else None
                record_dict[col_name] = val
                record_values.append(str(val) if val is not None else 'NULL')
            records.append(record_dict)
            raw_table_rows.append(record_values)

    # 7. Format & Print Pretty Table in Lambda Logs
    logger.info("=" * 80)
    logger.info("ATHENA QUERY EXECUTION RESULT SUMMARY")
    logger.info(f"Query           : {query_str}")
    logger.info(f"Database        : {database}")
    logger.info(f"WorkGroup       : {workgroup}")
    logger.info(f"Execution ID    : {query_execution_id}")
    logger.info(f"Engine Time     : {exec_time_ms} ms ({exec_time_ms / 1000.0:.2f} s)")
    logger.info(f"Data Scanned    : {data_scanned_bytes:,} bytes ({data_scanned_mb:.4f} MB)")
    logger.info(f"Rows Returned   : {len(records)} (max_results: {max_results})")
    logger.info("-" * 80)

    if column_names and raw_table_rows:
        col_widths = [max(len(col), 4) for col in column_names]
        for row in raw_table_rows:
            for idx, val in enumerate(row):
                col_widths[idx] = max(col_widths[idx], min(len(val), 50))
        col_widths = [min(w, 50) for w in col_widths]

        def _fmt_cell(val: str, width: int) -> str:
            clean = val.replace('\n', ' ').replace('\r', '')
            if len(clean) > width:
                return clean[:width - 3] + '...'
            return clean.ljust(width)

        header_line = " | ".join(_fmt_cell(col, col_widths[i]) for i, col in enumerate(column_names))
        sep_line = "-+-".join("-" * col_widths[i] for i in range(len(column_names)))

        logger.info(header_line)
        logger.info(sep_line)
        for row in raw_table_rows:
            row_line = " | ".join(_fmt_cell(val, col_widths[i]) for i, val in enumerate(row))
            logger.info(row_line)
        logger.info("-" * 80)
    else:
        logger.info("Query returned 0 data rows.")

    logger.info("JSON Records Output:")
    logger.info(json.dumps(records[:20], indent=2, default=str))
    logger.info("=" * 80)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'query_execution_id': query_execution_id,
            'status': 'SUCCEEDED',
            'query': query_str,
            'database': database,
            'workgroup': workgroup,
            'execution_time_ms': exec_time_ms,
            'data_scanned_bytes': data_scanned_bytes,
            'columns': column_names,
            'row_count': len(records),
            'records': records
        })
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda entrypoint.
    Supports:
      1. Athena query execution (if 'query', 'athena_query', or 'sql' is in event)
      2. AWS Glue Job Triggering & Monitoring (existing default)
    """
    logger.info(f"Received invocation event: {json.dumps(event, default=str)}")

    try:
        # Route 1: Athena Query Execution
        if is_athena_query_event(event):
            logger.info("Athena query request detected in payload. Routing to execute_athena_query...")
            return execute_athena_query(event, context)

        # Route 2: AWS Glue Job Triggering & Monitoring (Original Flow)
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
