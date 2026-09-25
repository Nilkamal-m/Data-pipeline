"""
AWS Lambda Helper Function: Trigger & Monitor AWS Glue Jobs & Execute Athena Queries

Purpose:
  1. Trigger and monitor AWS Glue Bronze Ingestion, Silver Iceberg ETL, and Gold Serving Mart jobs.
  2. Orchestrate end-to-end multi-layer pipeline runs (Bronze -> Silver -> Gold).
  3. Execute queries directly on Amazon Athena (Iceberg / Glue Data Catalog tables)
     and output formatted results into CloudWatch / Lambda execution logs.

Supported Operations:
  A. Athena Query Execution:
     Pass "query", "athena_query", or "sql" in the event payload to execute any SQL query
     against Athena and inspect the formatted result table in the Lambda logs.
     Payload: {"query": "SELECT * FROM uax_datalake_db_dev.raw_tbl_incident LIMIT 10"}

  B. Bronze Ingestion Glue Job:
     Payload: {"layer": "bronze", "source_system": "servicenow", "source_table_name": "incident"}

  C. Silver Iceberg ETL Glue Job:
     Payload: {"layer": "silver", "source_system": "servicenow", "source_table_name": "incident"}

  D. Gold Serving Layer Glue Job:
     Payload: {
         "layer": "gold",
         "source_system": "servicenow",
         "gold_schema": "enterprise_reporting",
         "rds_secret_name": "prod/rds/mysql_credentials"   # or "rds_password": "manual_password"
     }

  E. End-to-End Multi-Stage Pipeline Execution (Run All):
     Payload: {
         "layer": "all",                                    # Runs Bronze -> Silver -> Gold sequentially
         "source_system": "servicenow",
         "gold_schema": "enterprise_reporting",
         "rds_secret_name": "prod/rds/mysql_credentials"
     }
     Or custom stage selection:
     Payload: {
         "layers": ["silver", "gold"],                     # Runs Silver -> Gold
         "source_system": "servicenow",
         "gold_schema": "enterprise_reporting"
     }
"""

import os
import io
import re
import json
import time
import logging
import boto3
from typing import Dict, Any, List, Optional
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')
sfn_client = boto3.client('stepfunctions')

try:
    import pandas as pd
    HAS_PANDAS = True
except Exception:
    pd = None
    HAS_PANDAS = False

# Terminal status codes for AWS Glue Job Runs
TERMINAL_STATES = {'SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT'}

DEFAULT_BRONZE_JOB = os.environ.get('DEFAULT_BRONZE_JOB', 'uax-datalake-bronze-ingestion-dev')
DEFAULT_SILVER_JOB = os.environ.get('DEFAULT_SILVER_JOB', 'uax-datalake-silver-etl-dev')
DEFAULT_GOLD_JOB = os.environ.get('DEFAULT_GOLD_JOB', DEFAULT_SILVER_JOB)
DEFAULT_CRAWLER_NAME = os.environ.get('DEFAULT_CRAWLER_NAME', 'uax-datalake-bronze-crawler-dev')

DEFAULT_ATHENA_DATABASE = os.environ.get('DEFAULT_ATHENA_DATABASE', 'uax_datalake_db_dev')
DEFAULT_ATHENA_WORKGROUP = os.environ.get('DEFAULT_ATHENA_WORKGROUP', 'uax-datalake-workgroup-dev')
DEFAULT_ATHENA_OUTPUT_LOCATION = os.environ.get('DEFAULT_ATHENA_OUTPUT_LOCATION', '')
DEFAULT_STATE_MACHINE_ARN = os.environ.get('DEFAULT_STATE_MACHINE_ARN', '')

# Root directory of workspace for local SQL file lookups
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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
        'env': '--ENV',
        'ENV': '--ENV',
        'environment': '--ENV',
        'ENVIRONMENT': '--ENV',
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
        'DEDUPLICATION_STRATEGY': '--DEDUPLICATION_STRATEGY',
        # Gold Serving Layer parameters
        'gold_schema': '--GOLD_SCHEMA',
        'GOLD_SCHEMA': '--GOLD_SCHEMA',
        'rds_schema': '--GOLD_SCHEMA',
        'RDS_SCHEMA': '--GOLD_SCHEMA',
        'schema_name': '--GOLD_SCHEMA',
        'SCHEMA_NAME': '--GOLD_SCHEMA',
        'schema': '--GOLD_SCHEMA',
        'SCHEMA': '--GOLD_SCHEMA',
        'gold_target': '--GOLD_TARGET',
        'GOLD_TARGET': '--GOLD_TARGET',
        'gold_targets': '--GOLD_TARGETS',
        'GOLD_TARGETS': '--GOLD_TARGETS',
        'gold_config_s3_path': '--GOLD_CONFIG_S3_PATH',
        'GOLD_CONFIG_S3_PATH': '--GOLD_CONFIG_S3_PATH',
        'gold_config_path': '--GOLD_CONFIG_S3_PATH',
        'GOLD_CONFIG_PATH': '--GOLD_CONFIG_S3_PATH',
        'gold_config': '--GOLD_CONFIG_S3_PATH',
        'GOLD_CONFIG': '--GOLD_CONFIG_S3_PATH',
        'gold_config_json': '--GOLD_CONFIG_JSON',
        'GOLD_CONFIG_JSON': '--GOLD_CONFIG_JSON',
        'primary_key': '--PRIMARY_KEY',
        'PRIMARY_KEY': '--PRIMARY_KEY',
        'primary_keys': '--PRIMARY_KEY',
        'PRIMARY_KEYS': '--PRIMARY_KEY',
        'natural_key': '--NKEY',
        'NATURAL_KEY': '--NKEY',
        'natural_keys': '--NKEY',
        'NATURAL_KEYS': '--NKEY',
        'gold_query_s3_path': '--GOLD_QUERY_S3_PATH',
        'GOLD_QUERY_S3_PATH': '--GOLD_QUERY_S3_PATH',
        'gold_data_s3_path': '--GOLD_DATA_S3_PATH',
        'GOLD_DATA_S3_PATH': '--GOLD_DATA_S3_PATH',
        'rds_secret_name': '--RDS_SECRET_NAME',
        'RDS_SECRET_NAME': '--RDS_SECRET_NAME',
        'db_secret_name': '--RDS_SECRET_NAME',
        'DB_SECRET_NAME': '--RDS_SECRET_NAME',
        'db_secret': '--RDS_SECRET_NAME',
        'DB_SECRET': '--RDS_SECRET_NAME',
        'db_secrets': '--RDS_SECRET_NAME',
        'DB_SECRETS': '--RDS_SECRET_NAME',
        'rds_password': '--RDS_PASSWORD',
        'RDS_PASSWORD': '--RDS_PASSWORD',
        'rds_passwords': '--RDS_PASSWORD',
        'RDS_PASSWORDS': '--RDS_PASSWORD',
        'password': '--RDS_PASSWORD',
        'PASSWORD': '--RDS_PASSWORD',
        'passwords': '--RDS_PASSWORD',
        'PASSWORDS': '--RDS_PASSWORD',
        'db_password': '--RDS_PASSWORD',
        'DB_PASSWORD': '--RDS_PASSWORD',
        'db_passwords': '--RDS_PASSWORD',
        'DB_PASSWORDS': '--RDS_PASSWORD',
        'rds_pwd': '--RDS_PASSWORD',
        'RDS_PWD': '--RDS_PASSWORD',
        'pwd': '--RDS_PASSWORD',
        'PWD': '--RDS_PASSWORD',
        'rds_host': '--RDS_HOST',
        'RDS_HOST': '--RDS_HOST',
        'rds_url': '--RDS_HOST',
        'RDS_URL': '--RDS_HOST',
        'host': '--RDS_HOST',
        'HOST': '--RDS_HOST',
        'rds_port': '--RDS_PORT',
        'RDS_PORT': '--RDS_PORT',
        'port': '--RDS_PORT',
        'PORT': '--RDS_PORT',
        'rds_user': '--RDS_USER',
        'RDS_USER': '--RDS_USER',
        'rds_username': '--RDS_USER',
        'RDS_USERNAME': '--RDS_USER',
        'rds_uaername': '--RDS_USER',
        'RDS_UAERNAME': '--RDS_USER',
        'user': '--RDS_USER',
        'USER': '--RDS_USER',
        'connection_name': '--CONNECTION_NAME',
        'CONNECTION_NAME': '--CONNECTION_NAME',
        'glue_connection_name': '--CONNECTION_NAME',
        'GLUE_CONNECTION_NAME': '--CONNECTION_NAME',
        'external_columns': '--EXTERNAL_COLUMNS',
        'EXTERNAL_COLUMNS': '--EXTERNAL_COLUMNS',
        'process_layer': '--PROCESS_LAYER',
        'PROCESS_LAYER': '--PROCESS_LAYER',
        # Athena Workgroup parameters
        'athena_workgroup': '--ATHENA_WORKGROUP',
        'ATHENA_WORKGROUP': '--ATHENA_WORKGROUP',
        'workgroup': '--ATHENA_WORKGROUP',
        'WORKGROUP': '--ATHENA_WORKGROUP'
    }

    for event_key, glue_arg_key in param_mappings.items():
        val = event.get(event_key)
        if val is not None and str(val).strip() != '':
            glue_args[glue_arg_key] = str(val).strip()

    # Determine process layer (--PROCESS_LAYER strictly silver or gold for uax_silver_etl.py)
    layer = str(event.get('layer', '')).strip().lower()
    if layer == 'bronze':
        # Bronze ingestion job does not use --PROCESS_LAYER or gold/rds parameters
        glue_args.pop('--PROCESS_LAYER', None)
        for k in list(glue_args.keys()):
            if k.startswith('--GOLD_') or k.startswith('--RDS_'):
                glue_args.pop(k, None)
    elif layer == 'silver':
        glue_args['--PROCESS_LAYER'] = 'silver'
        for k in list(glue_args.keys()):
            if k.startswith('--GOLD_') or k.startswith('--RDS_'):
                glue_args.pop(k, None)
    elif layer == 'gold':
        glue_args['--PROCESS_LAYER'] = 'gold'
    elif not glue_args.get('--PROCESS_LAYER'):
        gold_identifiers = ('gold_schema', 'GOLD_SCHEMA', 'rds_schema', 'RDS_SCHEMA', 'schema_name', 'SCHEMA_NAME', 'db_secret', 'DB_SECRET', 'rds_secret_name')
        if any(k in event for k in gold_identifiers) or event.get('action') == 'gold':
            glue_args['--PROCESS_LAYER'] = 'gold'
        else:
            glue_args['--PROCESS_LAYER'] = 'silver'

    # Ensure Gold layer receives dedicated Athena workgroup if not specified
    if glue_args.get('--PROCESS_LAYER') in ('gold', 'both') or layer == 'gold':
        if not glue_args.get('--ATHENA_WORKGROUP') or glue_args.get('--ATHENA_WORKGROUP', '').lower() == 'primary':
            glue_args['--ATHENA_WORKGROUP'] = DEFAULT_ATHENA_WORKGROUP

    # Strict Gold Schema Validation (Enterprise Shared DB Policy - Zero Fallback)
    if glue_args.get('--PROCESS_LAYER') == 'gold':
        gold_schema = glue_args.get('--GOLD_SCHEMA')
        if not gold_schema or not str(gold_schema).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter 'gold_schema' (or 'rds_schema' / 'GOLD_SCHEMA') in event payload for Gold layer.\n"
                "In accordance with enterprise shared database policy, no fallback schema is permitted.\n"
                "Example: {'layer': 'gold', 'source_system': 'servicenow', 'rds_schema': 'enterprise_reporting'}"
            )

    # Auto-default --GOLD_CONFIG_S3_PATH for Gold layer if not explicitly specified
    if glue_args.get('--PROCESS_LAYER') in ('gold', 'both') or layer == 'gold':
        if not glue_args.get('--GOLD_CONFIG_S3_PATH') and not glue_args.get('--GOLD_CONFIG_JSON'):
            bucket = (
                glue_args.get('--DATA_LAKE_BUCKET')
                or os.environ.get('DATA_LAKE_BUCKET')
                or os.environ.get('DEFAULT_DATA_LAKE_BUCKET')
                or f"uax-datalake-{glue_args.get('--ENV', 'dev')}-bucket"
            )
            glue_args['--GOLD_CONFIG_S3_PATH'] = f"s3://{bucket}/gold/script/config/gold_config.json"

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


def is_crawler_event(event: Dict[str, Any]) -> bool:
    """
    Detects if the incoming Lambda payload is intended for Glue Crawler execution.
    Recognizes:
      - Explicit layer: "crawler", "glue_crawler", "crawl", "crawlers"
      - Explicit action: "crawler", "run_crawler", "start_crawler", "trigger_crawler", "crawl"
      - Presence of "crawler_name" / "CRAWLER_NAME" when layer is not a Glue job layer or athena
    Excludes multi-stage pipelines (e.g. layers: [...] or layer='all').
    """
    if isinstance(event.get('layers'), list):
        return False
    layer = str(event.get('layer', '')).strip().lower()
    if layer in ('all', 'pipeline', 'e2e', 'full'):
        return False
    action = str(event.get('action', '')).strip().lower()
    if action in ('run_all', 'pipeline', 'e2e', 'all'):
        return False

    if layer in ('crawler', 'glue_crawler', 'crawl', 'crawlers'):
        return True
    if action in ('crawler', 'run_crawler', 'start_crawler', 'trigger_crawler', 'crawl'):
        return True
    
    crawler_field = event.get('crawler_name') or event.get('CRAWLER_NAME') or event.get('crawler')
    if crawler_field and layer not in ('bronze', 'silver', 'gold', 'athena'):
        return True
    return False


def resolve_crawler_name(event: Dict[str, Any]) -> str:
    """
    Resolves the Glue crawler name from event payload or environment fallback.
    """
    crawler_name = (
        event.get('crawler_name')
        or event.get('CRAWLER_NAME')
        or event.get('crawler')
        or event.get('bronze_crawler_name')
        or event.get('silver_crawler_name')
    )
    if crawler_name and str(crawler_name).strip():
        return str(crawler_name).strip()

    source_system = event.get('source_system') or event.get('SOURCE_SYSTEM')
    env = event.get('env') or event.get('ENV') or os.environ.get('ENVIRONMENT', 'dev')

    layer = str(event.get('layer', '')).strip().lower()
    if 'silver' in layer:
        return f"uax-datalake-silver-crawler-{env}"
    elif source_system:
        return f"uax-datalake-bronze-crawler-{env}"
    return DEFAULT_CRAWLER_NAME


def poll_glue_crawler(crawler_name: str, poll_interval: int = 5, timeout_seconds: int = 540) -> Dict[str, Any]:
    """
    Synchronously polls AWS Glue Crawler status until it returns to READY state.
    """
    logger.info(f"Monitoring Glue Crawler '{crawler_name}' every {poll_interval}s (timeout: {timeout_seconds}s)...")
    start_time = time.time()

    # Small pause to allow AWS to transition state out of initial READY if just started
    time.sleep(min(2.0, float(poll_interval)))

    while True:
        elapsed = int(time.time() - start_time)
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Glue Crawler '{crawler_name}' exceeded timeout limit of {timeout_seconds}s. "
                f"Crawler is still running in AWS Glue."
            )

        resp = glue_client.get_crawler(Name=crawler_name)
        crawler_data = resp.get('Crawler', {})
        state = crawler_data.get('State', 'READY')  # READY, RUNNING, STOPPING
        last_crawl = crawler_data.get('LastCrawl', {})
        metrics = crawler_data.get('Metrics', {})
        crawl_status = last_crawl.get('Status', 'UNKNOWN')  # SUCCEEDED, CANCELLED, FAILED

        logger.info(f"[{elapsed}s] Crawler '{crawler_name}' state: {state} (LastCrawl Status: {crawl_status})")

        if state == 'READY':
            if crawl_status == 'FAILED':
                err_msg = last_crawl.get('ErrorMessage', 'Crawler run failed with unknown error')
                logger.error(f"Glue Crawler '{crawler_name}' FAILED: {err_msg}")
                return {
                    'Status': 'FAILED',
                    'State': 'READY',
                    'CrawlStatus': 'FAILED',
                    'ExecutionTimeSeconds': elapsed,
                    'ErrorMessage': err_msg,
                    'Metrics': metrics,
                    'LastCrawl': last_crawl
                }
            elif crawl_status == 'CANCELLED':
                err_msg = last_crawl.get('ErrorMessage', 'Crawler run was cancelled')
                logger.warning(f"Glue Crawler '{crawler_name}' CANCELLED: {err_msg}")
                return {
                    'Status': 'CANCELLED',
                    'State': 'READY',
                    'CrawlStatus': 'CANCELLED',
                    'ExecutionTimeSeconds': elapsed,
                    'ErrorMessage': err_msg,
                    'Metrics': metrics,
                    'LastCrawl': last_crawl
                }
            else:
                logger.info(f"Glue Crawler '{crawler_name}' SUCCEEDED in {elapsed}s.")
                return {
                    'Status': 'SUCCEEDED',
                    'State': 'READY',
                    'CrawlStatus': 'SUCCEEDED',
                    'ExecutionTimeSeconds': elapsed,
                    'Metrics': metrics,
                    'LastCrawl': last_crawl
                }

        time.sleep(poll_interval)


def trigger_and_monitor_crawler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Triggers an AWS Glue Crawler and optionally monitors it until completion.
    """
    crawler_name = resolve_crawler_name(event)
    logger.info(f"Starting AWS Glue Crawler: '{crawler_name}'")

    already_running = False
    try:
        glue_client.start_crawler(Name=crawler_name)
        logger.info(f"Successfully started Glue Crawler '{crawler_name}'.")
    except ClientError as ce:
        err_code = ce.response.get('Error', {}).get('Code')
        if err_code == 'CrawlerRunningException':
            logger.warning(f"Glue Crawler '{crawler_name}' is already running. Monitoring active crawl run.")
            already_running = True
        else:
            raise

    wait_until_completion = event.get('wait_until_completion', True)
    if isinstance(wait_until_completion, str):
        wait_until_completion = wait_until_completion.strip().lower() in ('true', '1', 'yes')

    poll_interval = int(event.get('poll_interval_seconds', 5))
    timeout_seconds = int(event.get('timeout_seconds', 540))

    if not wait_until_completion:
        logger.info("Asynchronous mode selected for Crawler. Returning 202 Accepted.")
        return {
            'statusCode': 202,
            'body': json.dumps({
                'status': 'STARTING' if not already_running else 'ALREADY_RUNNING',
                'message': f"Glue Crawler '{crawler_name}' started asynchronously.",
                'crawler_name': crawler_name
            })
        }

    crawl_res = poll_glue_crawler(
        crawler_name=crawler_name,
        poll_interval=poll_interval,
        timeout_seconds=timeout_seconds
    )

    is_success = crawl_res['Status'] == 'SUCCEEDED'
    metrics = crawl_res.get('Metrics', {})
    last_crawl = crawl_res.get('LastCrawl', {})

    response_payload = {
        'status': crawl_res['Status'],
        'crawler_name': crawler_name,
        'state': crawl_res.get('State', 'READY'),
        'crawl_status': crawl_res.get('CrawlStatus'),
        'execution_time_seconds': crawl_res.get('ExecutionTimeSeconds', 0),
        'tables_created': metrics.get('TablesCreated', 0),
        'tables_updated': metrics.get('TablesUpdated', 0),
        'tables_deleted': metrics.get('TablesDeleted', 0),
        'partitions_created': metrics.get('PartitionsCreated', 0),
        'partitions_updated': metrics.get('PartitionsUpdated', 0),
        'partitions_deleted': metrics.get('PartitionsDeleted', 0),
        'log_group': last_crawl.get('LogGroup', '/aws-glue/crawlers'),
        'log_stream': last_crawl.get('LogStream'),
        'error_message': crawl_res.get('ErrorMessage')
    }

    status_code = 200 if is_success else 500
    return {
        'statusCode': status_code,
        'body': json.dumps(response_payload, default=str)
    }


def is_step_function_event(event: Dict[str, Any]) -> bool:
    """
    Detects if the incoming Lambda payload is intended for AWS Step Functions execution.
    Recognizes:
      - 'action': 'step_function', 'stepfunction', 'state_machine', 'sfn', 'start_execution'
      - 'layer': 'step_function', 'stepfunction', 'state_machine', 'sfn'
      - Presence of 'state_machine_arn' or 'STATE_MACHINE_ARN'
    """
    action = str(event.get('action', '')).strip().lower()
    layer = str(event.get('layer', '')).strip().lower()
    if action in ('step_function', 'stepfunction', 'state_machine', 'sfn', 'start_execution'):
        return True
    if layer in ('step_function', 'stepfunction', 'state_machine', 'sfn'):
        return True
    if event.get('state_machine_arn') or event.get('STATE_MACHINE_ARN'):
        return True
    return False


def trigger_and_monitor_step_function(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Triggers an AWS Step Functions State Machine execution and optionally monitors it until completion.
    Only requires:
      {
        "action": "stepfunction",
        "stepfunction_name": "uax-pipeline-orchestrator-dev"
      }
    Zero external dependencies required; all pipeline parameters are configured within the Step Function.
    """
    state_machine_arn = (
        event.get('state_machine_arn')
        or event.get('STATE_MACHINE_ARN')
        or DEFAULT_STATE_MACHINE_ARN
    )

    state_machine_name = (
        event.get('stepfunction_name')
        or event.get('step_function_name')
        or event.get('state_machine_name')
        or event.get('name')
        or os.environ.get('DEFAULT_STATE_MACHINE_NAME', 'uax-pipeline-orchestrator-dev')
    )

    if not state_machine_arn:
        if state_machine_name.startswith('arn:aws:states:'):
            state_machine_arn = state_machine_name
        else:
            try:
                region = boto3.Session().region_name or os.environ.get('AWS_REGION', 'us-east-1')
                sts = boto3.client('sts')
                account_id = sts.get_caller_identity().get('Account', '123456789012')
                state_machine_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{state_machine_name}"
            except Exception:
                state_machine_arn = f"arn:aws:states:us-east-1:123456789012:stateMachine:{state_machine_name}"

    # Extract input payload to pass to Step Function (no forced defaults)
    control_keys = {
        'action', 'layer', 'state_machine_arn', 'STATE_MACHINE_ARN', 'state_machine_name',
        'stepfunction_name', 'step_function_name', 'name',
        'execution_name', 'wait_until_completion', 'poll_interval_seconds', 'timeout_seconds'
    }

    if isinstance(event.get('input'), dict):
        sfn_input = event['input']
    elif isinstance(event.get('payload'), dict):
        sfn_input = event['payload']
    else:
        # Pass only extra user-supplied parameters, if any (otherwise empty dict)
        sfn_input = {k: v for k, v in event.items() if k not in control_keys}

    # Generate unique execution name
    execution_name = event.get('execution_name')
    if not execution_name:
        import uuid
        ts = int(time.time())
        rnd = uuid.uuid4().hex[:6]
        base_name = state_machine_name.split(':')[-1]
        clean_name = re.sub(r'[^a-zA-Z0-9-_]', '', str(base_name))[:40]
        execution_name = f"{clean_name}-{ts}-{rnd}"

    logger.info(f"Triggering Step Function: {state_machine_arn}")
    logger.info(f"Execution Name: {execution_name}")
    logger.info(f"Input: {json.dumps(sfn_input, default=str)}")

    response = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(sfn_input, default=str)
    )

    execution_arn = response['executionArn']
    start_date = response['startDate'].isoformat() if hasattr(response['startDate'], 'isoformat') else str(response['startDate'])

    wait_until_completion = event.get('wait_until_completion', False)
    if not wait_until_completion:
        return {
            'statusCode': 202,
            'body': json.dumps({
                'message': 'Step Functions execution started successfully (asynchronous).',
                'state_machine_arn': state_machine_arn,
                'execution_arn': execution_arn,
                'execution_name': execution_name,
                'start_date': start_date,
                'status': 'RUNNING',
                'input': sfn_input
            }, default=str)
        }

    # Polling if wait_until_completion is True
    poll_interval = int(event.get('poll_interval_seconds', 10))
    timeout_seconds = int(event.get('timeout_seconds', 540))
    start_time = time.time()

    logger.info(f"Polling Step Function execution '{execution_arn}' every {poll_interval}s...")
    while True:
        elapsed = int(time.time() - start_time)
        if elapsed > timeout_seconds:
            raise TimeoutError(f"Step Function execution '{execution_name}' exceeded timeout of {timeout_seconds}s.")

        desc = sfn_client.describe_execution(executionArn=execution_arn)
        status = desc['status']
        logger.info(f"[{elapsed}s] Step Function execution status: {status}")

        if status in ('SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED'):
            status_code = 200 if status == 'SUCCEEDED' else 500
            output = None
            if 'output' in desc:
                try:
                    output = json.loads(desc['output'])
                except Exception:
                    output = desc['output']

            return {
                'statusCode': status_code,
                'body': json.dumps({
                    'status': status,
                    'execution_arn': execution_arn,
                    'execution_name': execution_name,
                    'duration_seconds': elapsed,
                    'stop_date': desc.get('stopDate', '').isoformat() if hasattr(desc.get('stopDate', ''), 'isoformat') else str(desc.get('stopDate', '')),
                    'output': output,
                    'error': desc.get('error'),
                    'cause': desc.get('cause')
                }, default=str)
            }

        time.sleep(poll_interval)



def is_catalog_maintenance_event(event: Dict[str, Any]) -> bool:
    """
    Detects if the incoming Lambda payload is intended for Glue Catalog schema maintenance,
    column deletion, or S3 Parquet sanitization.
    Recognizes:
      - 'action': 'delete_column', 'drop_column', 'remove_column', 'delete_from_parquet',
                  'sanitize_parquet', 'rewrite_parquet', 'delete_column_from_parquet',
                  'clean_parquet', 'fix_catalog_table', 'fix_columns', 'clean_catalog_table',
                  'update_schema', 'catalog_fix', 'sanitize_table'
      - 'layer': 'catalog', 'glue_catalog', 'parquet', 'sanitize'
      - Explicit field: 'exclude_columns', 'column_name', 'drop_column', 'delete_from_parquet', 'rewrite_parquet'
    """
def is_catalog_maintenance_event(event: Dict[str, Any]) -> bool:
    """Detects if event is requesting catalog or Parquet column deletion/sanitization."""
    if not isinstance(event, dict):
        return False
    action = str(event.get('action', '')).strip().lower()
    layer = str(event.get('layer', '')).strip().lower()
    return (
        action in (
            'delete_column', 'drop_column', 'remove_column',
            'delete_from_parquet', 'sanitize_parquet', 'rewrite_parquet',
            'delete_column_from_parquet', 'clean_parquet',
            'fix_catalog_table', 'fix_columns', 'clean_catalog_table',
            'update_schema', 'catalog_fix', 'sanitize_table',
            'delete', 'drop', 'clean', 'sanitize', 'rewrite'
        )
        or layer in ('catalog', 'glue_catalog', 'parquet', 'sanitize', 'clean')
        or 's3_path' in event
        or 's3_uri' in event
        or 'exclude_columns' in event
        or 'column_name' in event
        or 'drop_column' in event
        or 'columns' in event
        or 'delete_from_parquet' in event
        or 'rewrite_parquet' in event
    )



def fix_catalog_table_columns(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Deletes specified columns entirely from physical Apache Parquet files in S3.
    Reads each Parquet file, drops the target column(s) (e.g. _ingested_at), and rewrites
    the file in-place with snappy compression.
    Optionally syncs AWS Glue Catalog if the table exists, but never fails if the table is dropped.
    The user can then manually run the Glue Crawler to recreate the table cleanly.
    """
    # 1. Resolve column(s) to remove (default: _ingested_at)
    exclude_cols = set()
    for col_key in ('column_name', 'column', 'columns', 'exclude_columns', 'exclude_column', 'drop_columns', 'drop_column'):
        val = event.get(col_key)
        if val:
            if isinstance(val, str):
                exclude_cols.update(c.strip().lower() for c in val.split(',') if c.strip())
            elif isinstance(val, (list, tuple, set)):
                exclude_cols.update(str(c).strip().lower() for c in val if str(c).strip())

    if not exclude_cols:
        exclude_cols = {'_ingested_at'}

    # 2. Database and Table / S3 Path resolution
    database = (
        event.get('database')
        or event.get('database_name')
        or event.get('DATABASE')
        or event.get('DATABASE_NAME')
        or os.environ.get('DEFAULT_GLUE_DATABASE', 'uax_datalake_db_dev')
    )
    table_name = (
        event.get('table')
        or event.get('table_name')
        or event.get('TABLE')
        or event.get('TABLE_NAME')
    )
    s3_path = (
        event.get('s3_path')
        or event.get('s3_uri')
        or event.get('s3_location')
        or event.get('location')
        or event.get('path')
    )

    # If bucket and prefix were passed separately
    if not s3_path and event.get('bucket'):
        b = str(event['bucket']).strip().strip('/')
        p = str(event.get('prefix', '')).strip().strip('/')
        s3_path = f"s3://{b}/{p}/" if p else f"s3://{b}/"

    # Safely inspect Glue Catalog table if table_name is given (NEVER fail if table was dropped)
    table = None
    if table_name:
        try:
            logger.info(f"Checking Glue Catalog for table: {database}.{table_name}...")
            response = glue_client.get_table(DatabaseName=database, Name=table_name)
            table = response.get('Table')
            if not s3_path and table:
                table_loc = table.get('StorageDescriptor', {}).get('Location', '')
                if table_loc:
                    s3_path = table_loc
                    logger.info(f"Resolved S3 location from Glue Catalog: {s3_path}")
        except Exception as tbl_err:
            logger.info(f"Table '{table_name}' not found in Glue Catalog ({tbl_err}). Crawler will create it after S3 rewrite.")

    # Fallback S3 path resolution for standard Bronze layout if s3_path is still empty
    if not s3_path:
        target_tbl = table_name or 'raw_tbl_interactions'
        clean_tbl = target_tbl.replace('raw_tbl_', '').replace('tbl_', '')
        source_sys = event.get('source_system') or ('moveworks' if 'interaction' in clean_tbl or 'conversation' in clean_tbl else 'servicenow')
        env = event.get('env') or os.environ.get('ENVIRONMENT', 'dev')
        candidate_bucket = os.environ.get('DEFAULT_BRONZE_BUCKET') or f"uax-datalake-bronze-{env}"
        s3_path = f"s3://{candidate_bucket}/bronze/data/{source_sys}/{clean_tbl}/"
        logger.info(f"Constructed default Bronze S3 path: {s3_path}")


    # 3. Read Parquet files, delete column, and rewrite in-place
    parquet_summary = None
    if s3_path.startswith('s3://'):
        clean_s3 = s3_path[5:]
        parts = clean_s3.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        logger.info(f"Scanning S3 for Parquet files under s3://{bucket}/{prefix} to delete {list(exclude_cols)}...")
        try:
            try:
                import pyarrow.parquet as pq
                import pyarrow as pa
                has_arrow = True
            except ImportError:
                has_arrow = False

            if not has_arrow:
                msg = (
                    "PyArrow is not installed in the current Lambda runtime.\n"
                    "HOW TO FIX IN 10 SECONDS:\n"
                    "1. Go to AWS Lambda Console -> your Helper Lambda -> scroll to 'Layers' at the bottom.\n"
                    "2. Click 'Add a layer' -> choose 'AWS layers' -> select 'AWSSDKPandas-Python39' -> click Add.\n"
                    "OR run directly in AWS CloudShell / terminal: python3 lambda_function.py " + s3_path
                )
                logger.warning(msg)
                parquet_summary = {
                    'status': 'SKIPPED',
                    'reason': 'pyarrow_not_installed',
                    'message': msg
                }
            else:
                paginator = s3_client.get_paginator('list_objects_v2')
                parquet_keys = []
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get('Contents', []):
                        k = obj['Key']
                        if k.endswith('.parquet') and not k.endswith('/'):
                            parquet_keys.append(k)

                total_files = len(parquet_keys)
                logger.info(f"Found {total_files} Parquet file(s) under s3://{bucket}/{prefix}.")

                if total_files == 0:
                    parquet_summary = {
                        'status': 'NO_FILES_FOUND',
                        's3_location': f"s3://{bucket}/{prefix}",
                        'message': f"No .parquet files found under s3://{bucket}/{prefix}. Please check the S3 path."
                    }
                else:
                    def _sanitize_file(key: str) -> Dict[str, Any]:
                        resp = s3_client.get_object(Bucket=bucket, Key=key)
                        raw_bytes = resp['Body'].read()
                        arrow_table = pq.read_table(io.BytesIO(raw_bytes))
                        cols_to_drop = [c for c in arrow_table.column_names if c.strip().lower() in exclude_cols]
                        if cols_to_drop:
                            cleaned_table = arrow_table.drop(cols_to_drop)
                            out_buf = io.BytesIO()
                            pq.write_table(cleaned_table, out_buf, compression='snappy')
                            s3_client.put_object(
                                Bucket=bucket,
                                Key=key,
                                Body=out_buf.getvalue(),
                                ContentType='application/x-parquet'
                            )
                            return {'key': key, 'rewritten': True, 'dropped': cols_to_drop, 'rows': len(arrow_table)}
                        return {'key': key, 'rewritten': False, 'rows': len(arrow_table)}

                    rewritten_files = 0
                    total_records = 0
                    max_workers = min(16, (os.cpu_count() or 2) * 4)
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        future_to_key = {executor.submit(_sanitize_file, k): k for k in parquet_keys}
                        for future in as_completed(future_to_key):
                            res = future.result()
                            total_records += res.get('rows', 0)
                            if res.get('rewritten'):
                                rewritten_files += 1

                    success_msg = (
                        f"Successfully deleted column(s) {list(exclude_cols)} from all {rewritten_files} Parquet file(s) "
                        f"in S3 ({total_records} records sanitized). You can now manually trigger your Glue Crawler to create the table cleanly."
                    )
                    logger.info(f"✓ {success_msg}")
                    parquet_summary = {
                        'status': 'SUCCEEDED',
                        'files_scanned': total_files,
                        'files_rewritten': rewritten_files,
                        'total_records': total_records,
                        'deleted_columns': list(exclude_cols),
                        's3_location': f"s3://{bucket}/{prefix}",
                        'message': success_msg
                    }
        except Exception as rw_err:
            logger.error(f"S3 Parquet rewrite error: {rw_err}", exc_info=True)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'status': 'FAILED',
                    'error': str(rw_err),
                    'message': f"Failed rewriting Parquet files under s3://{bucket}/{prefix}"
                })
            }

    # 4. Optional: Update Glue Catalog if table exists (fail-safe)
    catalog_summary = None
    if table and table_name:
        try:
            sd = table.get('StorageDescriptor', {})
            original_cols = sd.get('Columns', [])
            cleaned_cols = [c for c in original_cols if c.get('Name', '').strip().lower() not in exclude_cols]
            removed_cols = [c.get('Name') for c in original_cols if c.get('Name', '').strip().lower() in exclude_cols]
            pkeys = [pk.get('Name') for pk in table.get('PartitionKeys', [])]

            if removed_cols:
                read_only_keys = [
                    'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
                    'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId',
                    'FederatedTable', 'Owner'
                ]
                table_input = {k: v for k, v in table.items() if k not in read_only_keys}
                table_input['StorageDescriptor']['Columns'] = cleaned_cols
                glue_client.update_table(DatabaseName=database, TableInput=table_input)
                catalog_summary = {
                    'status': 'SUCCEEDED',
                    'deleted_columns': removed_cols,
                    'partition_keys': pkeys,
                    'message': f"Updated existing Glue Catalog table {database}.{table_name}."
                }
            else:
                catalog_summary = {'status': 'NO_CHANGE', 'message': f"Table {table_name} catalog schema already clean."}
        except Exception as cat_err:
            logger.info(f"Catalog update skipped ({cat_err}); crawler will manage table schema.")

    resp_payload = {
        'status': 'SUCCEEDED',
        'message': f"Column(s) {list(exclude_cols)} deleted entirely from Parquet files in S3. Trigger crawler to create/refresh table.",
        'deleted_columns': list(exclude_cols),
        'parquet_sanitization': parquet_summary
    }
    if catalog_summary:
        resp_payload['catalog_update'] = catalog_summary

    return {
        'statusCode': 200,
        'body': json.dumps(resp_payload)
    }


def is_athena_query_event(event: Dict[str, Any]) -> bool:
    """
    Detects if the incoming Lambda payload is intended for Athena query execution.
    Recognizes:
      - Explicit query fields: "query", "athena_query", "sql", "query_file", "sql_file",
        "query_path", "sql_path", "sql_s3_path", "s3_query_uri", "query_s3_path", "query_name"
      - Action: "query", "athena", "run_query"
      - Layer: "athena"
    """
    for q_key in (
        'query', 'athena_query', 'sql', 'query_file', 'sql_file',
        'query_path', 'sql_path', 'sql_s3_path', 's3_query_uri',
        'query_s3_path', 'query_name', 'file_path', 'QUERY', 'ATHENA_QUERY', 'SQL'
    ):
        if event.get(q_key) and str(event[q_key]).strip():
            return True
    layer = str(event.get('layer', '')).strip().lower()
    action = str(event.get('action', '')).strip().lower()
    return layer == 'athena' or action in ('query', 'athena', 'run_query')


def strip_comments_from_sql(sql_text: str) -> str:
    """Removes single-line and multi-line comments from SQL text to verify if actual SQL statements remain."""
    clean = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
    clean = re.sub(r'--[^\r\n]*', '', clean)
    return clean.strip()


def split_sql_statements(sql_text: str) -> List[str]:
    """
    Parses and splits a multiline SQL query string into individual executable statements.
    Accurately handles:
      - Trailing semicolons (which cause Athena Presto/Trino syntax errors)
      - Semicolons inside single-quoted strings ('hello; world')
      - Semicolons inside double-quoted identifiers ("my;col")
      - Semicolons inside single-line (-- ...) and block (/* ... */) comments
      - Skips empty chunks and trailing comment-only sections
    """
    statements = []
    current: List[str] = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    n = len(sql_text)

    while i < n:
        ch = sql_text[i]
        next_ch = sql_text[i + 1] if i + 1 < n else ''

        # Line comment: starts with -- until newline
        if not in_single_quote and not in_double_quote and not in_block_comment and ch == '-' and next_ch == '-':
            in_line_comment = True
            current.append(ch)
            current.append(next_ch)
            i += 2
            continue
        elif in_line_comment and ch == '\n':
            in_line_comment = False
            current.append(ch)
            i += 1
            continue

        # Block comment: starts with /* until */
        elif not in_single_quote and not in_double_quote and not in_line_comment and ch == '/' and next_ch == '*':
            in_block_comment = True
            current.append(ch)
            current.append(next_ch)
            i += 2
            continue
        elif in_block_comment and ch == '*' and next_ch == '/':
            in_block_comment = False
            current.append(ch)
            current.append(next_ch)
            i += 2
            continue

        # String literals and statement terminator
        if not in_line_comment and not in_block_comment:
            if ch == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif ch == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif ch == ';' and not in_single_quote and not in_double_quote:
                stmt_str = "".join(current).strip().rstrip(';').strip()
                if strip_comments_from_sql(stmt_str):
                    statements.append(stmt_str)
                current = []
                i += 1
                continue

        current.append(ch)
        i += 1

    remaining_stmt = "".join(current).strip().rstrip(';').strip()
    if strip_comments_from_sql(remaining_stmt):
        statements.append(remaining_stmt)

    return statements


def resolve_sql_query(event: Dict[str, Any]) -> str:
    """
    Resolves an SQL query from event payload.
    Supports:
      1. Direct multiline SQL string: event['query'], event['sql'], event['athena_query']
      2. Local or repository file path: event['query_file'], event['sql_file'], event['query_path'], event['sql_path']
      3. S3 URI: 's3://bucket/key/to/query.sql' passed via query, sql_s3_path, s3_query_uri, etc.
      4. Named query lookup: event['query_name'] + event['source_system']
    Also performs template/parameter replacements if provided.
    """
    raw_query = None
    for q_key in (
        'query', 'sql', 'athena_query', 'query_file', 'sql_file',
        'query_path', 'sql_path', 'sql_s3_path', 's3_query_uri',
        'query_s3_path', 'file_path', 'QUERY', 'SQL', 'ATHENA_QUERY'
    ):
        val = event.get(q_key)
        if val and str(val).strip():
            raw_query = str(val).strip()
            break

    # If query_name is passed with source_system (e.g. query_name="v_interactions.sql", source_system="moveworks")
    if not raw_query and event.get('query_name'):
        q_name = str(event['query_name']).strip()
        src = str(event.get('source_system') or event.get('SOURCE_SYSTEM') or '').strip().lower()
        if src:
            potential_path = os.path.join(repo_root, "gold", "query", src, q_name)
            if os.path.isfile(potential_path):
                raw_query = potential_path
            else:
                raw_query = f"gold/query/{src}/{q_name}"

    if not raw_query:
        raise ValueError(
            "Missing SQL query in payload. Please provide 'query', 'sql', 'query_file', or 'sql_s3_path'.\n"
            "Example: {'query_file': 'gold/query/moveworks/v_interactions.sql'} or {'query': 'SELECT * FROM raw_tbl_interactions'}"
        )

    # Check if raw_query is an S3 URI (s3://bucket/path/to/query.sql)
    if raw_query.startswith('s3://'):
        logger.info(f"Loading SQL query from S3 URI: {raw_query}")
        s3_path = raw_query[5:]
        bucket_name, key_name = s3_path.split('/', 1)
        resp = s3_client.get_object(Bucket=bucket_name, Key=key_name)
        sql_content = resp['Body'].read().decode('utf-8')
    # Check if raw_query is a local file path (e.g. gold/query/moveworks/v_interactions.sql)
    elif os.path.isfile(raw_query):
        logger.info(f"Loading SQL query from local file path: {raw_query}")
        with open(raw_query, 'r', encoding='utf-8') as f:
            sql_content = f.read()
    elif os.path.isfile(os.path.join(repo_root, raw_query)):
        resolved_path = os.path.join(repo_root, raw_query)
        logger.info(f"Loading SQL query from workspace file path: {resolved_path}")
        with open(resolved_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
    else:
        # Inline SQL string (potentially multiline)
        sql_content = raw_query

    # Apply parameter / template substitution if parameters are supplied
    params = event.get('params') or event.get('parameters') or event.get('template_vars') or {}
    database = (
        event.get('database')
        or event.get('db')
        or event.get('glue_database')
        or event.get('DATABASE')
        or DEFAULT_ATHENA_DATABASE
    )
    source_system = event.get('source_system') or event.get('SOURCE_SYSTEM') or ''

    substitutions = dict(params)
    if 'database' not in substitutions and database:
        substitutions['database'] = database
    if 'source_system' not in substitutions and source_system:
        substitutions['source_system'] = source_system

    # Table replacements (e.g. mapping tbl_interactions -> raw_tbl_interactions if running against Bronze)
    table_replacements = event.get('table_replacements') or event.get('table_mapping') or {}
    if isinstance(table_replacements, dict):
        for old_t, new_t in table_replacements.items():
            sql_content = re.sub(rf'\b{re.escape(old_t)}\b', new_t, sql_content)

    for k, v in substitutions.items():
        val_str = str(v)
        sql_content = sql_content.replace(f"${{{k}}}", val_str)
        sql_content = sql_content.replace(f"{{{{{k}}}}}", val_str)
        sql_content = sql_content.replace(f"{{{k}}}", val_str)
        sql_content = sql_content.replace(f"<{k}>", val_str)
        sql_content = re.sub(rf':{re.escape(k)}\b', val_str, sql_content)

    return sql_content


def format_as_database_table(
    headers: List[str],
    rows: List[List[Any]],
    include_row_num: bool = True,
    max_col_width: Optional[int] = None
) -> str:
    """
    Renders tabular data as a clean SQL database CLI table (+-----+-----+).
    Shows all rows and columns in full.
    """
    if not headers and not rows:
        return "(0 rows returned)"

    display_headers = ["#"] + headers if include_row_num else list(headers)
    display_rows = []
    for idx, row in enumerate(rows, 1):
        if include_row_num:
            formatted_row = [str(idx)] + [str(v) if v is not None else 'NULL' for v in row]
        else:
            formatted_row = [str(v) if v is not None else 'NULL' for v in row]
        display_rows.append(formatted_row)

    col_widths = [len(h) for h in display_headers]
    for row in display_rows:
        for i, val in enumerate(row):
            clean_val = val.replace('\n', ' ').replace('\r', '')
            val_len = len(clean_val)
            if max_col_width and max_col_width > 0:
                val_len = min(val_len, max_col_width)
            col_widths[i] = max(col_widths[i], val_len)

    border_line = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"

    def _fmt_row(cells: List[str]) -> str:
        formatted_cells = []
        for i, c in enumerate(cells):
            clean = c.replace('\n', ' ').replace('\r', '')
            if max_col_width and max_col_width > 0 and len(clean) > col_widths[i]:
                clean = clean[:col_widths[i] - 3] + '...'
            formatted_cells.append(f" {clean.ljust(col_widths[i])} ")
        return "|" + "|".join(formatted_cells) + "|"

    lines = [
        border_line,
        _fmt_row(display_headers),
        border_line
    ]
    for r in display_rows:
        lines.append(_fmt_row(r))
    lines.append(border_line)
    lines.append(f"({len(display_rows)} row{'s' if len(display_rows) != 1 else ''} in set)")
    return "\n".join(lines)


def execute_athena_query(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Executes an SQL query against Amazon Athena, polls for completion, formats
    the tabular results into the Lambda execution logs, and returns the records.
    Supports multiline SQL queries, files (e.g. interactions.sql), S3 paths, and multi-statement queries.
    """
    # 1. Resolve SQL Query string (supports multiline, files, S3 URIs, and template params)
    raw_query_str = resolve_sql_query(event)
    statements = split_sql_statements(raw_query_str)
    if not statements:
        raise ValueError("No valid SQL statement found in query payload.")

    # 2. Resolve Database, Workgroup, Output Location
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

    athena_region = event.get('region') or event.get('athena_region') or os.environ.get('ATHENA_REGION')
    client = boto3.client('athena', region_name=athena_region.strip()) if athena_region and athena_region.strip() else athena_client

    raw_max = event.get('max_results') if event.get('max_results') is not None else event.get('MAX_RESULTS')
    if raw_max is None or str(raw_max).strip().lower() in ('none', 'all', '', '0', '-1'):
        max_results = None
    else:
        try:
            max_results = int(raw_max)
            if max_results <= 0:
                max_results = None
        except (ValueError, TypeError):
            max_results = None

    timeout_seconds = int(event.get('timeout_seconds') or event.get('TIMEOUT_SECONDS') or 120)
    poll_interval = float(event.get('poll_interval_seconds') or event.get('POLL_INTERVAL_SECONDS') or 1.0)

    logger.info("=" * 80)
    logger.info("STARTING AMAZON ATHENA QUERY EXECUTION")
    logger.info(f"Target Database : {database}")
    logger.info(f"Athena Workgroup: {workgroup}")
    logger.info(f"Athena Region   : {athena_region or 'Default'}")
    logger.info(f"Max Results     : {max_results if max_results is not None else 'ALL (No restriction)'}")
    logger.info(f"Total Statements: {len(statements)}")
    logger.info("=" * 80)

    execution_ids = []
    final_query_execution_id = None
    final_query_execution = None

    for stmt_idx, stmt in enumerate(statements, 1):
        # Sanitize query string: strip leading comments and trailing semicolons/whitespace so Athena's API
        # parser reliably detects the statement type (SELECT, WITH, CREATE, SHOW) and does NOT throw:
        # InvalidRequestException: Query of this type are not supported.
        clean_stmt = stmt.strip().rstrip(';').strip()
        clean_stmt = re.sub(r'^(?:\s*(?:--[^\r\n]*|/\*[\s\S]*?\*/)\s*)+', '', clean_stmt).strip()
        if not clean_stmt:
            logger.info(f"Skipping empty or comment-only statement chunk {stmt_idx}.")
            continue

        line_count = len(clean_stmt.splitlines())
        logger.info(f"--- [Statement {stmt_idx}/{len(statements)}] ({line_count} lines) ---")
        if line_count <= 20:
            logger.info(f"SQL:\n{clean_stmt}")
        else:
            first_5 = "\n".join(clean_stmt.splitlines()[:5])
            last_5 = "\n".join(clean_stmt.splitlines()[-5:])
            logger.info(f"SQL (showing preview of {line_count} lines):\n{first_5}\n...\n{last_5}")

        start_params: Dict[str, Any] = {
            'QueryString': clean_stmt,
            'WorkGroup': workgroup
        }
        if database:
            start_params['QueryExecutionContext'] = {'Database': database}
        if output_location:
            start_params['ResultConfiguration'] = {'OutputLocation': output_location}

        try:
            start_response = client.start_query_execution(**start_params)
        except ClientError as ce:
            err_msg = str(ce)
            if 'InvalidRequestException' in err_msg and 'workgroup' in err_msg.lower() and ('outputlocation' in err_msg.lower() or 'configuration' in err_msg.lower()):
                logger.info("Workgroup enforces output location. Retrying without explicit ResultConfiguration...")
                start_params.pop('ResultConfiguration', None)
                start_response = client.start_query_execution(**start_params)
            else:
                raise

        query_execution_id = start_response['QueryExecutionId']
        execution_ids.append(query_execution_id)
        final_query_execution_id = query_execution_id
        logger.info(f"Submitted to Athena. QueryExecutionId: {query_execution_id}")

        # Polling loop for current statement
        start_time = time.time()
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                try:
                    client.stop_query_execution(QueryExecutionId=query_execution_id)
                except Exception:
                    pass
                raise TimeoutError(
                    f"Athena query '{query_execution_id}' (Statement {stmt_idx}) timed out after {timeout_seconds}s."
                )

            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            query_execution = response.get('QueryExecution', {})
            status_info = query_execution.get('Status', {})
            state = status_info.get('State', 'UNKNOWN')

            if state == 'SUCCEEDED':
                logger.info(f"Statement {stmt_idx} ('{query_execution_id}') SUCCEEDED in {elapsed:.2f}s")
                final_query_execution = query_execution
                break
            elif state in ('FAILED', 'CANCELLED'):
                reason = status_info.get('StateChangeReason', 'Unknown error')
                logger.error(f"Athena Statement {stmt_idx} ('{query_execution_id}') {state}: {reason}")
                return {
                    'statusCode': 500 if state == 'FAILED' else 400,
                    'body': json.dumps({
                        'query_execution_id': query_execution_id,
                        'execution_ids': execution_ids,
                        'failed_statement_index': stmt_idx,
                        'total_statements': len(statements),
                        'status': state,
                        'query': stmt,
                        'database': database,
                        'workgroup': workgroup,
                        'error_message': reason
                    })
                }

            time.sleep(poll_interval)

    # Fetch results for the final statement
    stats = final_query_execution.get('Statistics', {}) if final_query_execution else {}
    exec_time_ms = stats.get('EngineExecutionTimeInMillis', 0)
    data_scanned_bytes = stats.get('DataScannedInBytes', 0)
    data_scanned_mb = data_scanned_bytes / (1024.0 * 1024.0)

    results_paginator = client.get_paginator('get_query_results')
    column_names: List[str] = []
    records: List[Dict[str, Any]] = []
    raw_table_rows: List[List[str]] = []
    is_first_page = True

    paginate_kwargs: Dict[str, Any] = {'QueryExecutionId': final_query_execution_id}
    if max_results is not None and max_results > 0:
        paginate_kwargs['PaginationConfig'] = {'MaxItems': max_results}

    for page in results_paginator.paginate(**paginate_kwargs):
        result_set = page.get('ResultSet', {})
        rows = result_set.get('Rows', [])

        if not rows:
            continue

        start_row_idx = 0
        if is_first_page:
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

    logger.info("=" * 80)
    logger.info("ATHENA QUERY EXECUTION RESULT SUMMARY")
    logger.info(f"Database        : {database}")
    logger.info(f"WorkGroup       : {workgroup}")
    logger.info(f"Execution ID    : {final_query_execution_id}")
    if len(execution_ids) > 1:
        logger.info(f"All ExecutionIDs: {', '.join(execution_ids)}")
    logger.info(f"Engine Time     : {exec_time_ms} ms ({exec_time_ms / 1000.0:.2f} s)")
    logger.info(f"Data Scanned    : {data_scanned_bytes:,} bytes ({data_scanned_mb:.4f} MB)")
    logger.info(f"Rows Returned   : {len(records)}" + (f" (capped at {max_results})" if max_results else " (ALL records)"))
    logger.info("-" * 80)

    if column_names and (records or raw_table_rows):
        if HAS_PANDAS:
            try:
                df = pd.DataFrame(records, columns=column_names)
                if not df.empty:
                    df.index = range(1, len(df) + 1)
                    df.index.name = '#'
                pd.set_option('display.max_columns', None)
                pd.set_option('display.max_rows', None)
                pd.set_option('display.width', 1000)
                pd.set_option('display.colheader_justify', 'left')
                pd.set_option('display.max_colwidth', None)

                logger.info("PANDAS DATAFRAME VIEW:")
                logger.info("\n" + df.to_string())
                logger.info(f"DataFrame Shape: {df.shape[0]} rows x {df.shape[1]} columns")
                logger.info("-" * 80)
            except Exception as df_err:
                logger.warning(f"Failed to render pandas DataFrame: {df_err}")

        logger.info("DATABASE TABLE VIEW:")
        db_table_str = format_as_database_table(
            headers=column_names,
            rows=raw_table_rows,
            include_row_num=True
        )
        logger.info("\n" + db_table_str)
        logger.info("-" * 80)
    else:
        logger.info("Query returned 0 data rows.")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'query_execution_id': final_query_execution_id,
            'execution_ids': execution_ids,
            'statements_executed': len(statements),
            'status': 'SUCCEEDED',
            'query': raw_query_str,
            'database': database,
            'workgroup': workgroup,
            'execution_time_ms': exec_time_ms,
            'data_scanned_bytes': data_scanned_bytes,
            'columns': column_names,
            'row_count': len(records),
            'records': records
        })
    }


def execute_pipeline_stages(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Executes a sequential multi-stage data pipeline (e.g. Bronze -> Silver -> Gold).
    Monitors each stage to completion, logs structured progress, and halts immediately if any stage fails.
    """
    pipeline_start_time = time.time()
    layers_requested = event.get('layers')
    if not layers_requested:
        # Default pipeline: bronze -> silver -> gold (or silver -> gold if skip_bronze is True)
        skip_bronze = event.get('skip_bronze', False)
        if isinstance(skip_bronze, str):
            skip_bronze = skip_bronze.strip().lower() in ('true', '1', 'yes')
        layers_requested = ['silver', 'gold'] if skip_bronze else ['bronze', 'silver', 'gold']

    cleaned_layers = [str(l).strip().lower() for l in layers_requested if str(l).strip()]
    logger.info(
        f"\n+================================================================================+\n"
        f"|  STARTING MULTI-STAGE DATA PIPELINE: {' -> '.join([l.upper() for l in cleaned_layers])}\n"
        f"+================================================================================+"
    )

    poll_interval = int(event.get('poll_interval_seconds', 10))
    timeout_seconds = int(event.get('timeout_seconds', 540))
    source_system = str(event.get('source_system') or event.get('SOURCE_SYSTEM') or '').strip().lower()

    stage_results = {}

    for idx, stage in enumerate(cleaned_layers, 1):
        logger.info(
            f"\n================================================================================\n"
            f"[PIPELINE STAGE {idx}/{len(cleaned_layers)}] Starting Layer: {stage.upper()}\n"
            f"================================================================================"
        )

        stage_event = dict(event)
        stage_event['layer'] = stage

        # Resolve stage type: crawler or glue job
        if stage in ('crawler', 'glue_crawler', 'crawl'):
            stage_crawler_name = resolve_crawler_name(stage_event)
            logger.info(f"Triggering Glue Crawler for stage '{stage}': '{stage_crawler_name}'")
            try:
                glue_client.start_crawler(Name=stage_crawler_name)
            except ClientError as ce:
                if ce.response.get('Error', {}).get('Code') == 'CrawlerRunningException':
                    logger.warning(f"Crawler '{stage_crawler_name}' is already running.")
                else:
                    raise

            crawl_res = poll_glue_crawler(
                crawler_name=stage_crawler_name,
                poll_interval=poll_interval,
                timeout_seconds=timeout_seconds
            )
            crawl_status = crawl_res['Status']
            stage_results[stage] = {
                'crawler_name': stage_crawler_name,
                'status': crawl_status,
                'execution_time_seconds': crawl_res.get('ExecutionTimeSeconds', 0),
                'metrics': crawl_res.get('Metrics', {}),
                'error_message': crawl_res.get('ErrorMessage')
            }
            if crawl_status != 'SUCCEEDED':
                total_time = int(time.time() - pipeline_start_time)
                err_msg = crawl_res.get('ErrorMessage') or f"Stage '{stage}' failed."
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'status': 'FAILED',
                        'failed_stage': stage,
                        'error_message': err_msg,
                        'total_duration_seconds': total_time,
                        'stage_results': stage_results
                    })
                }
            logger.info(f"[PIPELINE STAGE {idx}/{len(cleaned_layers)}] Stage '{stage.upper()}' SUCCEEDED in {crawl_res.get('ExecutionTimeSeconds', 0)}s.")
            continue

        if stage == 'bronze':
            stage_job = stage_event.get('bronze_job_name') or DEFAULT_BRONZE_JOB
        elif stage == 'silver':
            stage_job = stage_event.get('silver_job_name') or DEFAULT_SILVER_JOB
        elif stage == 'gold':
            stage_job = stage_event.get('gold_job_name') or DEFAULT_GOLD_JOB
        else:
            raise ValueError(f"Unknown pipeline stage layer: '{stage}'. Expected 'bronze', 'crawler', 'silver', or 'gold'.")

        stage_args = build_glue_arguments(stage_event)
        logger.info(f"Triggering Glue Job for stage '{stage}': '{stage_job}' with args: {json.dumps(stage_args)}")

        run_resp = glue_client.start_job_run(JobName=stage_job, Arguments=stage_args)
        run_id = run_resp['JobRunId']
        logger.info(f"Stage '{stage.upper()}' started with RunId: {run_id}")

        # Poll stage to completion
        poll_res = poll_glue_job_run(
            job_name=stage_job,
            run_id=run_id,
            poll_interval=poll_interval,
            timeout_seconds=timeout_seconds
        )

        job_state = poll_res['JobState']
        exec_duration = poll_res.get('ExecutionTimeSeconds', 0)

        stage_results[stage] = {
            'job_name': stage_job,
            'job_run_id': run_id,
            'status': job_state,
            'execution_time_seconds': exec_duration,
            'log_group': poll_res.get('LogGroupName'),
            'error_message': poll_res.get('ErrorMessage')
        }

        if job_state != 'SUCCEEDED':
            total_time = int(time.time() - pipeline_start_time)
            err_msg = poll_res.get('ErrorMessage') or f"Stage '{stage}' failed with state '{job_state}'."
            logger.error(
                f"\n+================================================================================+\n"
                f"|  PIPELINE FAILED AT STAGE '{stage.upper()}' (RunId: {run_id})\n"
                f"|  Error: {err_msg}\n"
                f"+================================================================================+"
            )
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'status': 'FAILED',
                    'failed_stage': stage,
                    'error_message': err_msg,
                    'total_duration_seconds': total_time,
                    'stage_results': stage_results
                })
            }

        logger.info(f"[PIPELINE STAGE {idx}/{len(cleaned_layers)}] Stage '{stage.upper()}' SUCCEEDED in {exec_duration}s.")

    total_pipeline_time = int(time.time() - pipeline_start_time)
    logger.info(
        f"\n+================================================================================+\n"
        f"|  PIPELINE EXECUTION COMPLETED SUCCESSFULLY IN {total_pipeline_time}s!\n"
        f"+================================================================================+"
    )
    return {
        'statusCode': 200,
        'body': json.dumps({
            'status': 'SUCCEEDED',
            'pipeline': ' -> '.join(cleaned_layers),
            'source_system': source_system,
            'total_duration_seconds': total_pipeline_time,
            'stage_results': stage_results
        })
    }


def sanitize_all_watermarks(bucket: str, prefixes: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Scans S3 for all watermark.json files under metadata/bronze and metadata/silver.
    Rewrites any multi-line / pretty-printed JSON into single-line NDJSON format.
    Fixes the issue where Athena returns NULL values for all columns.
    """
    if prefixes is None:
        prefixes = ["metadata/bronze", "metadata/silver"]

    results = {}
    total_fixed = 0
    total_scanned = 0

    for pfx in prefixes:
        clean_pfx = pfx.strip('/')
        scanned_in_pfx = 0
        fixed_in_pfx = 0
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=clean_pfx):
            for obj in page.get('Contents', []):
                key = obj.get('Key', '')
                if key.endswith('watermark.json'):
                    scanned_in_pfx += 1
                    try:
                        resp = s3_client.get_object(Bucket=bucket, Key=key)
                        raw = resp['Body'].read().decode('utf-8')
                        if '\n' in raw.strip():
                            parsed = json.loads(raw)
                            single_line = json.dumps(parsed) + '\n'
                            s3_client.put_object(
                                Bucket=bucket,
                                Key=key,
                                Body=single_line.encode('utf-8'),
                                ContentType="application/json"
                            )
                            fixed_in_pfx += 1
                            logger.info(f"[WATERMARK SANITIZER] Re-saved multi-line watermark to single-line JSON: s3://{bucket}/{key}")
                    except Exception as e:
                        logger.warning(f"Could not sanitize s3://{bucket}/{key}: {e}")

        results[clean_pfx] = {
            "scanned": scanned_in_pfx,
            "repaired_to_single_line": fixed_in_pfx
        }
        total_scanned += scanned_in_pfx
        total_fixed += fixed_in_pfx

    return {
        "status": "SUCCEEDED",
        "bucket": bucket,
        "total_scanned": total_scanned,
        "total_repaired": total_fixed,
        "prefix_details": results
    }


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """
    Main Lambda entrypoint.
    Supports:
      1. Athena query execution (if 'query', 'athena_query', or 'sql' is in event)
      2. Glue Catalog / Parquet sanitization (delete_from_parquet / s3_path / column_name)
      3. Watermark sanitization / repair (if action="sanitize_watermarks")
      4. Glue Crawler Trigger & Monitoring (if crawler_name / action="crawler")
      5. Multi-Stage Pipeline Execution (if layer="all" / "pipeline" / "e2e" or "layers" list)
      6. Single AWS Glue Job Triggering & Monitoring (Bronze, Silver, or Gold)
    """
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            pass

    if not isinstance(event, dict):
        event = {}

    if 'body' in event and isinstance(event.get('body'), str):
        try:
            parsed_body = json.loads(event['body'])
            if isinstance(parsed_body, dict):
                event = {**parsed_body, **event}
        except Exception:
            pass

    logger.info(f"Received invocation event: {json.dumps(event, default=str)}")


    try:
        # Route 0: Watermark Sanitization / Repair
        if event.get('action') in ('sanitize_watermarks', 'repair_watermarks', 'fix_watermarks'):
            bucket = event.get('bucket') or event.get('state_bucket') or event.get('data_lake_bucket') or os.environ.get('DATA_LAKE_BUCKET', '')
            if not bucket:
                raise ValueError("Missing required parameter 'bucket' (or 'state_bucket') for watermark sanitization.")
            prefixes = event.get('prefixes')
            res = sanitize_all_watermarks(bucket, prefixes)
            return {
                'statusCode': 200,
                'body': json.dumps(res)
            }

        # Route 0.5: AWS Step Functions State Machine Execution
        if is_step_function_event(event):
            logger.info("Step Functions request detected in payload. Routing to trigger_and_monitor_step_function...")
            return trigger_and_monitor_step_function(event, context)

        # Route 1: Athena Query Execution
        if is_athena_query_event(event):
            logger.info("Athena query request detected in payload. Routing to execute_athena_query...")
            return execute_athena_query(event, context)

        # Route 2: Glue Catalog Schema Maintenance (e.g. fix duplicate partition/data columns)
        if is_catalog_maintenance_event(event):
            logger.info("Catalog maintenance request detected in payload. Routing to fix_catalog_table_columns...")
            return fix_catalog_table_columns(event, context)

        # Route 3: Glue Crawler Trigger & Monitoring
        if is_crawler_event(event):
            logger.info("Glue Crawler request detected in payload. Routing to trigger_and_monitor_crawler...")
            return trigger_and_monitor_crawler(event, context)

        # Route 2: Multi-Stage Pipeline Execution ("all", "pipeline", "e2e", or layers list)
        layer = str(event.get('layer') or event.get('process_layer') or '').strip().lower()
        if event.get('action') in ('run_all', 'pipeline', 'e2e', 'all'):
            layer = 'all'
        elif event.get('action') in ('gold', 'silver', 'bronze'):
            layer = event['action'].strip().lower()

        if not layer or layer not in ('bronze', 'silver', 'gold', 'all', 'pipeline', 'e2e', 'full'):
            gold_identifiers = ('gold_schema', 'GOLD_SCHEMA', 'rds_schema', 'RDS_SCHEMA', 'schema_name', 'SCHEMA_NAME', 'db_secret', 'DB_SECRET', 'rds_secret_name', 'gold_target')
            if any(k in event for k in gold_identifiers):
                layer = 'gold'
            elif any(k in event for k in ('silver_config_s3_path', 'scd_type', 'deduplication_order_by')):
                layer = 'silver'
            else:
                layer = 'bronze'

        if layer in ('all', 'pipeline', 'e2e', 'full') or isinstance(event.get('layers'), list):
            logger.info("Multi-stage pipeline request detected. Routing to execute_pipeline_stages...")
            return execute_pipeline_stages(event, context)

        # Route 3: Single AWS Glue Job Triggering & Monitoring (Bronze, Silver, Gold)
        job_name = event.get('job_name')
        if not job_name:
            if layer == 'gold':
                job_name = DEFAULT_GOLD_JOB
            elif layer == 'silver':
                job_name = DEFAULT_SILVER_JOB
            elif layer == 'bronze':
                job_name = DEFAULT_BRONZE_JOB
            else:
                raise ValueError(
                    f"Invalid layer '{layer}'. Expected 'bronze', 'silver', 'gold', or 'all'."
                )

        # Build Glue command-line arguments (--SOURCE_SYSTEM, --SOURCE_TABLE_NAME, --PROCESS_LAYER, etc.)
        glue_args = build_glue_arguments(event)
        logger.info(f"Triggering Glue Job: '{job_name}' with arguments: {json.dumps(glue_args)}")

        # Trigger Glue Job Run
        run_response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=glue_args
        )
        run_id = run_response['JobRunId']
        logger.info(f"Successfully started Glue Job '{job_name}' (Layer: {layer.upper()}) with RunId: {run_id}")

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
                    'message': f'Glue job ({layer.upper()}) started asynchronously.',
                    'layer': layer,
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
            'layer': layer,
            'job_name': job_name,
            'job_run_id': run_id,
            'job_status': job_state,
            'execution_time_seconds': final_result.get('ExecutionTimeSeconds', 0),
            'source_system': glue_args.get('--SOURCE_SYSTEM'),
            'source_table_name': glue_args.get('--SOURCE_TABLE_NAME', 'ALL_CONFIGURED'),
            'process_layer': glue_args.get('--PROCESS_LAYER'),
            'gold_schema': glue_args.get('--GOLD_SCHEMA'),
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
                'message': 'Failed to execute operation in Helper Lambda'
            })
        }


if __name__ == '__main__':
    import sys
    # Direct execution support: python3 lambda_function.py <s3_path> [column_name]
    if len(sys.argv) > 1:
        target_s3 = sys.argv[1]
        col_to_drop = sys.argv[2] if len(sys.argv) > 2 else "_ingested_at"
        print(f"Running Parquet sanitization on: {target_s3} to remove '{col_to_drop}'...")
        res = lambda_handler({
            "action": "delete_from_parquet",
            "s3_path": target_s3,
            "column_name": col_to_drop
        }, None)
        print(json.dumps(res, indent=2))
    else:
        print("Usage: python3 lambda_function.py <s3_path> [column_name]")
        print("Example: python3 lambda_function.py s3://my-bucket/bronze/data/moveworks/interactions/ _ingested_at")
