"""
AWS Glue PySpark ETL Script: Dynamic Multi-Table Silver Layer Apache Iceberg Transformation

Features:
- Dynamic Deduplication Keys & Order-By Columns from silver_config.json:
  - Reads explicit `deduplication_keys` (single key or composite list) and `deduplication_order_by` from table config.
- Merge Strategy Support:
  - SCD Type 1 (UPSERT via Spark SQL MERGE INTO) - Overwrites modified records to keep current state.
  - SCD Type 2 (Slowly Changing Dimension Type 2) - Maintains full historical version audit trail with `_valid_from`, `_valid_to`, and `_is_current` columns.
  - APPEND & OVERWRITE modes.
- Integrated SilverTransformer engine (declarative casts, renames, filters, custom PySpark script hooks).
"""

import sys
import os
import json
import logging
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import traceback
from datetime import datetime, timezone
from typing import Optional
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, coalesce, lit, current_timestamp, cast,
    to_timestamp, sha2, concat_ws, when, upper, max as spark_max
)

# Ensure script directories are on sys.path for Silver & Gold imports
script_dir = os.path.dirname(os.path.abspath(__file__))
gold_dir = os.path.join(os.path.dirname(script_dir), "gold", "script")
for path in [script_dir, gold_dir, os.getcwd(), "/tmp/extraPython"]:
    if os.path.exists(path) and path not in sys.path:
        sys.path.insert(0, path)

from silver_config_loader import SilverConfigLoader
from transformer import SilverTransformer
from gold_layer_manager import GoldLayerManager


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
    - Standardized timestamp [YYYY-MM-DD HH:MM:SS UTC] and 5-char aligned severity.
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


root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for h in list(root_logger.handlers):
    root_logger.removeHandler(h)

stdout_handler = FlushStreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(CleanLogFormatter())
root_logger.addHandler(stdout_handler)

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

logger = logging.getLogger("DynamicSilverIcebergETL")
logger.setLevel(logging.INFO)
logger.propagate = True


def parse_spark_arguments() -> dict:
    """
    Parses CLI arguments passed by Step Functions or AWS Glue Job Run dynamically.
    Enforces 3-tier precedence: 1. Glue CLI Argument -> 2. Config File -> 3. Code Default
    """
    arg_dict = {}
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg.startswith('--'):
            arg_content = arg[2:]
            if '=' in arg_content:
                key, val = arg_content.split('=', 1)
                arg_dict[key.strip()] = val.strip()
                i += 1
            elif i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                arg_dict[arg_content.strip()] = sys.argv[i + 1].strip()
                i += 2
            else:
                arg_dict[arg_content.strip()] = ''
                i += 1
        else:
            i += 1

    def get_cli_arg(*names, default=None):
        for name in names:
            if name in arg_dict and arg_dict[name] is not None and str(arg_dict[name]).strip() != '':
                return str(arg_dict[name]).strip()
            for k, v in arg_dict.items():
                if k.lower() == name.lower() and v is not None and str(v).strip() != '':
                    return str(v).strip()
        return default

    config_s3_path = get_cli_arg('SILVER_CONFIG_S3_PATH', 'silver_config_s3_path', 'CONFIG_S3_PATH', 'config_s3_path')

    # Load Silver centralized configuration file
    s3_client = boto3.client('s3') if config_s3_path else None
    silver_full_config = SilverConfigLoader.load_config(config_s3_path=config_s3_path, s3_client=s3_client)
    defaults_cfg = SilverConfigLoader.get_defaults(silver_full_config)

    # Process Layer Routing: Supports 'silver', 'gold', or 'both'
    process_layer = (get_cli_arg('PROCESS_LAYER', 'process_layer', default='silver')).lower().strip()
    if process_layer not in ('silver', 'gold', 'both'):
        raise ValueError(
            f"Invalid --PROCESS_LAYER value '{process_layer}'. "
            f"Strictly 2 options are supported: '--PROCESS_LAYER=silver' or '--PROCESS_LAYER=gold' (or '--PROCESS_LAYER=both')."
        )

    source_system = get_cli_arg('SOURCE_SYSTEM', 'source_system')
    if not source_system or not str(source_system).strip():
        logger.error("Missing required parameter '--SOURCE_SYSTEM'. Example: --SOURCE_SYSTEM servicenow")
        raise ValueError(
            "CRITICAL CONFIG ERROR: Missing required parameter '--SOURCE_SYSTEM'.\n"
            "The ultimate Gold query path is 's3://<bucket>/gold/query/<source>/<table_name>.sql'.\n"
            "Please explicitly specify the source system (e.g. --SOURCE_SYSTEM servicenow)."
        )

    source_system_clean = source_system.strip().lower()

    # Data Lake Bucket: CLI > Config > Env > Default
    data_lake_bucket = get_cli_arg(
        'DATA_LAKE_BUCKET', 'data_lake_bucket',
        'BRONZE_BUCKET', 'bronze_bucket',
        default=defaults_cfg.get('data_lake_bucket') or os.environ.get('DATA_LAKE_BUCKET', 'uax-datalake-dev-bucket')
    )

    # Glue Database: CLI > Config (Required)
    glue_database = (
        get_cli_arg('GLUE_DATABASE', 'glue_database', 'GLUE_DB_NAME', 'glue_db_name')
        or defaults_cfg.get('glue_database')
        or defaults_cfg.get('glue_catalog', {}).get('database_name')
    )
    if not glue_database or not str(glue_database).strip():
        raise ValueError(
            "CRITICAL CONFIG ERROR: 'glue_database' is missing or empty in silver_config.json "
            "(pipeline_defaults.glue_database or glue_catalog.database_name) and was not provided via CLI. "
            "Please configure 'glue_database' (e.g. 'uax_datalake_db_dev')."
        )
    glue_database = str(glue_database).strip()

    # Bronze Data Prefix: CLI > Config > Default ('bronze/data')
    bronze_data_prefix = (
        get_cli_arg('BRONZE_DATA_PREFIX', 'bronze_data_prefix', 'BRONZE_PREFIX', 'bronze_prefix')
        or defaults_cfg.get('bronze_data_prefix')
        or defaults_cfg.get('bronze_prefix', 'bronze/data')
    ).strip('/')

    # Silver Data Prefix: CLI > Config > Default ('silver/data')
    silver_data_prefix = (
        get_cli_arg('SILVER_DATA_PREFIX', 'silver_data_prefix', 'SILVER_PREFIX', 'silver_prefix')
        or defaults_cfg.get('silver_data_prefix')
        or defaults_cfg.get('silver_prefix', 'silver/data')
    ).strip('/')

    # Table Prefix: CLI > Config (Required)
    table_prefix = (
        get_cli_arg('TABLE_PREFIX', 'table_prefix', 'SILVER_TABLE_PREFIX', 'silver_table_prefix')
        or defaults_cfg.get('table_prefix')
        or defaults_cfg.get('glue_catalog', {}).get('table_prefix')
    )
    if not table_prefix or not str(table_prefix).strip():
        raise ValueError(
            "CRITICAL CONFIG ERROR: 'table_prefix' is missing or empty in silver_config.json "
            "(pipeline_defaults.table_prefix or glue_catalog.table_prefix) and was not provided via CLI. "
            "Please configure 'table_prefix' (e.g. 'tbl_')."
        )
    table_prefix = str(table_prefix).strip()

    # Resolve dynamic table list: CLI overrides config default tables
    raw_tables = get_cli_arg(
        'SOURCE_TABLE_NAME', 'source_table_name',
        'TABLE_NAME', 'table_name',
        'TABLES', 'tables',
        'TABLE_NAMES', 'table_names'
    )
    if raw_tables:
        table_list = [t.strip() for t in raw_tables.split(',') if t.strip()]
        logger.info(f"Using CLI parameter table override: {table_list}")
    elif process_layer == 'gold':
        table_list = []
        logger.info("Process layer is 'gold'. Bypassing Silver table_list resolution.")
    else:
        table_list = SilverConfigLoader.get_default_tables(source_system_clean, silver_full_config)
        if not table_list:
            raise ValueError(
                f"CRITICAL CONFIG ERROR: 'default_tables' is missing or empty for source system '{source_system_clean}' "
                f"in silver_config.json (source_systems.{source_system_clean}.default_tables) and was not provided via CLI (--SOURCE_TABLE_NAME). "
                f"Please configure at least one Bronze source table (e.g. 'raw_tbl_incident') in silver_config.json."
            )
        logger.info(f"Using config default tables: {table_list}")

    job_name = get_cli_arg('JOB_NAME', 'job_name', default=f"glue-silver-etl-{source_system_clean}")

    watermark_cfg = defaults_cfg.get('watermark', {})

    # Watermark Enabled: CLI > Config > True
    cli_wm_enabled = get_cli_arg('WATERMARK_ENABLED', 'watermark_enabled')
    if cli_wm_enabled is not None:
        watermark_enabled = str(cli_wm_enabled).strip().lower() in ('true', '1', 'yes')
    else:
        watermark_enabled = bool(watermark_cfg.get('enabled', True))

    # Full Refresh Toggle: CLI > Config > False (if True, ignores watermark and scans all Bronze)
    cli_full_refresh = get_cli_arg('FULL_REFRESH', 'full_refresh')
    if cli_full_refresh is not None:
        full_refresh = str(cli_full_refresh).strip().lower() in ('true', '1', 'yes')
    else:
        full_refresh = bool(watermark_cfg.get('full_refresh', False))

    # Watermark Column: CLI > Config > '_ingested_at'
    watermark_column = (
        get_cli_arg('WATERMARK_COLUMN', 'watermark_column')
        or watermark_cfg.get('watermark_column', '_ingested_at')
    )

    # Sync Watermark Table: CLI > Config > True
    cli_sync_watermark = get_cli_arg('SYNC_WATERMARK_TABLE', 'sync_watermark_table')
    if cli_sync_watermark is not None:
        sync_watermark_table = str(cli_sync_watermark).strip().lower() in ('true', '1', 'yes')
    else:
        sync_watermark_table = bool(watermark_cfg.get('sync_watermark_table', True))

    # Watermark Table Name: CLI > Config
    watermark_table_name = (
        get_cli_arg('WATERMARK_TABLE_NAME', 'watermark_table_name')
        or watermark_cfg.get('watermark_table_name')
    )
    if sync_watermark_table and (not watermark_table_name or not str(watermark_table_name).strip()):
        raise ValueError(
            "CRITICAL CONFIG ERROR: 'watermark_table_name' is missing or empty in silver_config.json "
            "(silver_defaults.watermark.watermark_table_name) and was not provided via CLI. "
            "Please configure 'watermark_table_name' (e.g. 'tbl_watermarks')."
        )
    if watermark_table_name:
        watermark_table_name = str(watermark_table_name).strip()

    # Metadata Prefix: Config > 'metadata/silver'
    metadata_prefix = watermark_cfg.get('metadata_prefix', 'metadata/silver').strip('/')

    # Crawler Parameters: CLI > Config
    crawler_name = (
        get_cli_arg('CRAWLER_NAME', 'crawler_name', 'SILVER_CRAWLER_NAME', 'silver_crawler_name')
        or defaults_cfg.get('crawler_name')
        or defaults_cfg.get('silver_crawler_name')
    )
    if crawler_name:
        crawler_name = str(crawler_name).strip()

    cli_trigger_crawler = get_cli_arg('TRIGGER_CRAWLER', 'trigger_crawler')
    if cli_trigger_crawler is not None:
        trigger_crawler = str(cli_trigger_crawler).strip().lower() in ('true', '1', 'yes')
    else:
        # Defaults to False for Silver Iceberg: Spark natively updates the Glue Data Catalog.
        glue_cat_cfg = defaults_cfg.get('glue_catalog', {})
        trigger_crawler = bool(glue_cat_cfg.get('trigger_crawler', False))

    # Gold Serving Layer Parameters
    gold_targets_raw = get_cli_arg('GOLD_TARGETS', 'gold_targets', 'GOLD_TARGET', 'gold_target', default='aurora')
    gold_targets = [t.strip().lower() for t in str(gold_targets_raw).split(',') if t.strip()]
    gold_target = gold_targets[0] if gold_targets else 'aurora'

    gold_schema = get_cli_arg('GOLD_SCHEMA', 'gold_schema', 'RDS_SCHEMA', 'rds_schema')
    needs_mysql = any(t in gold_targets for t in ('aurora', 'rds', 'mysql'))
    if process_layer in ('gold', 'both') and needs_mysql and (not gold_schema or not str(gold_schema).strip()):
        raise ValueError(
            "CRITICAL CONFIG ERROR: Missing required parameter '--GOLD_SCHEMA'.\n"
            "Downstream target includes Aurora/MySQL, where no fallback schema is permitted.\n"
            "Please explicitly specify the target MySQL schema name (e.g. --GOLD_SCHEMA enterprise_reporting)."
        )
    if gold_schema:
        gold_schema = str(gold_schema).strip()

    default_gold_query = f"s3://{data_lake_bucket}/gold/query/{source_system_clean}"
    default_gold_data = f"s3://{data_lake_bucket}/gold/data/{source_system_clean}"
    gold_query_s3_path = get_cli_arg('GOLD_QUERY_S3_PATH', 'gold_query_s3_path', default=default_gold_query)
    gold_data_s3_path = get_cli_arg('GOLD_DATA_S3_PATH', 'gold_data_s3_path', default=default_gold_data)
    connection_name = get_cli_arg('CONNECTION_NAME', 'connection_name', 'GLUE_CONNECTION_NAME', 'glue_connection_name')
    rds_secret_name = get_cli_arg('RDS_SECRET_NAME', 'rds_secret_name', 'SECRET_NAME', 'secret_name', 'DB_SECRET_NAME', 'db_secret_name', 'DB_SECRET', 'db_secret')
    rds_host = get_cli_arg('RDS_HOST', 'rds_host', 'RDS_URL', 'rds_url', 'HOST', 'host')
    rds_port = get_cli_arg('RDS_PORT', 'rds_port', 'PORT', 'port', default='3306')
    rds_user = get_cli_arg('RDS_USER', 'rds_user', 'RDS_USERNAME', 'rds_username', 'USER', 'user')
    rds_password = get_cli_arg(
        'RDS_PASSWORD', 'rds_password',
        'RDS_PASSWORDS', 'rds_passwords',
        'PASSWORD', 'password',
        'PASSWORDS', 'passwords',
        'DB_PASSWORD', 'db_password',
        'DB_PASSWORDS', 'db_passwords',
        'RDS_PWD', 'rds_pwd',
        'PWD', 'pwd'
    )

    # Athena Workgroup: CLI > Env > Default (uax-datalake-workgroup-{env})
    env = 'dev'
    if glue_database:
        parts = glue_database.split('_')
        if len(parts) > 1 and parts[-1] in ('dev', 'qa', 'staging', 'prod', 'test'):
            env = parts[-1]
    default_athena_wg = os.environ.get('ATHENA_WORKGROUP') or f"uax-datalake-workgroup-{env}"
    athena_workgroup = get_cli_arg(
        'ATHENA_WORKGROUP', 'athena_workgroup',
        'WORKGROUP', 'workgroup',
        default=default_athena_wg
    )
    if athena_workgroup and str(athena_workgroup).strip().lower() == 'primary':
        athena_workgroup = default_athena_wg

    # External DW: Redshift & Snowflake parameters
    redshift_schema = get_cli_arg('REDSHIFT_SCHEMA', 'redshift_schema', default='gold_spectrum_schema')
    redshift_iam_role = get_cli_arg('REDSHIFT_IAM_ROLE', 'redshift_iam_role')
    redshift_secret_name = get_cli_arg('REDSHIFT_SECRET_NAME', 'redshift_secret_name')
    snowflake_database = get_cli_arg('SNOWFLAKE_DATABASE', 'snowflake_database', default='UAX_ANALYTICS_DB')
    snowflake_schema = get_cli_arg('SNOWFLAKE_SCHEMA', 'snowflake_schema', default='GOLD_MARTS')
    snowflake_external_volume = get_cli_arg('SNOWFLAKE_EXTERNAL_VOLUME', 'snowflake_external_volume', default='UAX_S3_ICEBERG_VOLUME')
    snowflake_secret_name = get_cli_arg('SNOWFLAKE_SECRET_NAME', 'snowflake_secret_name')

    return {
        'PROCESS_LAYER': process_layer,
        'JOB_NAME': job_name,
        'SOURCE_SYSTEM': source_system_clean,
        'TABLE_LIST': table_list,
        'DATA_LAKE_BUCKET': data_lake_bucket,
        'GLUE_DATABASE': glue_database,
        'TABLE_PREFIX': table_prefix,
        'BRONZE_DATA_PREFIX': bronze_data_prefix,
        'SILVER_DATA_PREFIX': silver_data_prefix,
        'WATERMARK_ENABLED': watermark_enabled,
        'FULL_REFRESH': full_refresh,
        'WATERMARK_COLUMN': watermark_column,
        'SYNC_WATERMARK_TABLE': sync_watermark_table,
        'WATERMARK_TABLE_NAME': watermark_table_name,
        'METADATA_PREFIX': metadata_prefix,
        'CRAWLER_NAME': crawler_name,
        'TRIGGER_CRAWLER': trigger_crawler,
        'SILVER_FULL_CONFIG': silver_full_config,
        'GOLD_SCHEMA': gold_schema,
        'GOLD_TARGET': gold_target,
        'GOLD_TARGETS': ','.join(gold_targets),
        'GOLD_QUERY_S3_PATH': gold_query_s3_path,
        'GOLD_DATA_S3_PATH': gold_data_s3_path,
        'CONNECTION_NAME': connection_name,
        'RDS_SECRET_NAME': rds_secret_name,
        'RDS_HOST': rds_host,
        'RDS_PORT': rds_port,
        'RDS_USER': rds_user,
        'RDS_PASSWORD': rds_password,
        'ATHENA_WORKGROUP': athena_workgroup,
        'WORKGROUP': athena_workgroup,
        'REDSHIFT_SCHEMA': redshift_schema,
        'REDSHIFT_IAM_ROLE': redshift_iam_role,
        'REDSHIFT_SECRET_NAME': redshift_secret_name,
        'SNOWFLAKE_DATABASE': snowflake_database,
        'SNOWFLAKE_SCHEMA': snowflake_schema,
        'SNOWFLAKE_EXTERNAL_VOLUME': snowflake_external_volume,
        'SNOWFLAKE_SECRET_NAME': snowflake_secret_name,
        'ARG_DICT': arg_dict
    }



def get_silver_watermark_key(source_system: str, table_clean: str, metadata_prefix: str = "metadata/silver") -> str:
    """
    Returns the S3 metadata key for a Silver table watermark state file.
    Example: metadata/silver/servicenow/incident/watermark.json
    """
    clean_prefix = metadata_prefix.strip('/')
    return f"{clean_prefix}/{source_system}/{table_clean}/watermark.json"


def get_silver_last_load_date(
    s3_client,
    bucket: str,
    state_key: str,
    table_display: str,
    full_refresh: bool = False
) -> Optional[str]:
    """
    Retrieves the last processed watermark timestamp from S3 metadata JSON.
    Returns None if full_refresh is requested or if the watermark state file does not exist.
    """
    if full_refresh:
        logger.info(f"FULL REFRESH requested for '{table_display}'. Bypassing watermark state.")
        return None

    if not s3_client:
        return None

    s3_path = f"s3://{bucket}/{state_key}"
    try:
        logger.info(f"Checking for Silver High-Water Mark state file at '{s3_path}'...")
        response = s3_client.get_object(Bucket=bucket, Key=state_key)
        state_content = response['Body'].read().decode('utf-8')
        state_data = json.loads(state_content)
        last_load_date = state_data.get('last_load_date')
        if last_load_date and str(last_load_date).strip():
            logger.info(f"SILVER HIGH-WATER MARK FOUND ({s3_path}): '{last_load_date}' for table '{table_display}'.")
            return str(last_load_date).strip()
    except ClientError as err:
        code = err.response.get('Error', {}).get('Code')
        if code in ('NoSuchKey', '404'):
            logger.info(f"Silver watermark state file NOT present in S3 at '{s3_path}'. Performing initial full load...")
        else:
            logger.warning(f"Error reading Silver watermark from '{s3_path}': {err}. Defaulting to full load.")
    except Exception as err:
        logger.warning(f"Unexpected error checking watermark for '{table_display}': {err}. Defaulting to full load.")

    return None


def update_silver_watermark(
    s3_client,
    bucket: str,
    state_key: str,
    source_system: str,
    table_name: str,
    new_watermark: str,
    total_records: int,
    current_run_time: str
) -> None:
    """
    Writes/Updates the Silver High-Water Mark JSON metadata file in S3 upon successful Iceberg write.
    The 'table_name' field is formatted as 'tbl_<tablename>' (e.g. tbl_incident).
    """
    if not table_name or not str(table_name).strip():
        raise ValueError("CRITICAL ERROR: 'table_name' must be provided and non-empty when updating Silver watermark.")

    if not s3_client:
        logger.warning("s3_client not available. Skipping Silver watermark update.")
        return

    s3_path = f"s3://{bucket}/{state_key}"
    state_payload = {
        "source_system": source_system,
        "table_name": table_name,
        "last_load_date": new_watermark,
        "last_status": "SUCCESS",
        "records_processed": total_records,
        "updated_at": current_run_time
    }

    try:
        logger.info(f"Updating Silver watermark at '{s3_path}' with payload: {state_payload}")
        # Athena OpenX JsonSerDe requires single-line JSON (JSON Lines / NDJSON).
        # Multi-line pretty-printed JSON causes TextInputFormat to parse line-by-line and return NULL for all columns.
        single_line_payload = json.dumps(state_payload) + '\n'
        s3_client.put_object(
            Bucket=bucket,
            Key=state_key,
            Body=single_line_payload.encode('utf-8'),
            ContentType="application/json"
        )
        logger.info(f"Successfully updated Silver S3 watermark at '{s3_path}'")
    except Exception as err:
        logger.warning(f"Failed to update Silver watermark at '{s3_path}': {err}")


def sanitize_watermark_files(s3_cli, bucket: str, prefix: str) -> int:
    """
    Scans all watermark.json files under s3://{bucket}/{prefix}/.
    If any file is formatted as multi-line JSON (pretty-printed), rewrites it as single-line JSON Lines (NDJSON).
    Athena OpenX JsonSerDe reads files line-by-line; pretty-printed JSON results in NULL values for all columns.
    """
    if not s3_cli or not bucket or not prefix:
        return 0
    clean_prefix = prefix.strip('/')
    fixed_count = 0
    try:
        paginator = s3_cli.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=clean_prefix):
            for obj in page.get('Contents', []):
                key = obj.get('Key', '')
                if key.endswith('watermark.json'):
                    try:
                        resp = s3_cli.get_object(Bucket=bucket, Key=key)
                        raw_body = resp['Body'].read().decode('utf-8')
                        if '\n' in raw_body.strip():
                            parsed = json.loads(raw_body)
                            single_line = json.dumps(parsed) + '\n'
                            s3_cli.put_object(
                                Bucket=bucket,
                                Key=key,
                                Body=single_line.encode('utf-8'),
                                ContentType="application/json"
                            )
                            fixed_count += 1
                            logger.info(f"[SILVER WATERMARK SANITIZER] Re-saved multi-line watermark as single-line JSON: s3://{bucket}/{key}")
                    except Exception as parse_err:
                        logger.warning(f"Could not sanitize watermark file s3://{bucket}/{key}: {parse_err}")
    except Exception as list_err:
        logger.warning(f"Could not list watermark files for sanitization under s3://{bucket}/{clean_prefix}: {list_err}")
    return fixed_count


def sync_silver_watermark_catalog_table(
    glue_client,
    database_name: str,
    watermark_table_name: str,
    bucket: str,
    metadata_prefix: str = "metadata/silver",
    s3_client=None
) -> str:
    """
    Creates or ensures an Athena-queryable AWS Glue Catalog external table for all Silver High-Water Mark state files.
    Location: s3://{bucket}/{metadata_prefix}/
    Using recursive directory scanning so all watermark.json files across sources and tables can be queried in Athena:
    SELECT * FROM <database_name>.<watermark_table_name>;
    """
    if not glue_client:
        return f"{database_name}.{watermark_table_name}"

    clean_prefix = metadata_prefix.strip('/')
    watermark_location = f"s3://{bucket}/{clean_prefix}/"

    # Automatically sanitize any legacy multi-line watermark.json files
    if s3_client:
        sanitize_watermark_files(s3_client, bucket, clean_prefix)

    columns = [
        {'Name': 'source_system', 'Type': 'string'},
        {'Name': 'table_name', 'Type': 'string'},
        {'Name': 'last_load_date', 'Type': 'string'},
        {'Name': 'last_status', 'Type': 'string'},
        {'Name': 'records_processed', 'Type': 'bigint'},
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
                'case.insensitive': 'true',
                'dots.in.keys': 'false',
                'mapping.source_system': 'source_system',
                'mapping.table_name': 'table_name',
                'mapping.last_load_date': 'last_load_date',
                'mapping.last_status': 'last_status',
                'mapping.records_processed': 'records_processed',
                'mapping.records_ingested': 'records_processed',
                'mapping.updated_at': 'updated_at'
            }
        }
    }

    table_input = {
        'Name': watermark_table_name,
        'Description': 'Athena queryable table for all Silver High-Water Mark state files',
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'classification': 'json',
            'recursive.directories': 'true'
        },
        'StorageDescriptor': storage_desc
    }

    try:
        glue_client.get_table(DatabaseName=database_name, Name=watermark_table_name)
        logger.info(f"Silver Watermark Catalog Table verified: {database_name}.{watermark_table_name}")
        try:
            glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
            logger.info(f"Updated Silver Watermark Catalog Table definition for {database_name}.{watermark_table_name}")
        except Exception as update_err:
            logger.warning(f"Could not update existing Silver Watermark table definition: {update_err}")
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('EntityNotFoundException', 'NoSuchEntityException'):
            try:
                glue_client.create_table(
                    DatabaseName=database_name,
                    TableInput=table_input
                )
                logger.info(f"Created Athena Silver Watermark Catalog Table: {database_name}.{watermark_table_name} at '{watermark_location}'")
            except ClientError as ce:
                if ce.response.get('Error', {}).get('Code') != 'AlreadyExistsException':
                    logger.warning(f"Failed to create Silver Watermark Catalog table '{watermark_table_name}': {ce}")
        else:
            logger.warning(f"Error checking Silver Watermark table '{watermark_table_name}': {e}")
    except Exception as err:
        logger.warning(f"Unexpected error syncing Silver Watermark table '{watermark_table_name}': {err}")

    return f"{database_name}.{watermark_table_name}"


def quote_iceberg_table(table_name: str) -> str:
    """
    Escapes database and table identifiers with backticks for Spark SQL syntax safety (e.g. `glue_catalog`.`db-name`.`tbl-name`).
    """
    parts = table_name.split('.')
    return '.'.join([f"`{p.strip('`')}`" for p in parts])


def purge_s3_table_location(s3_client, s3_uri: str):
    """
    Safely deletes any orphaned or corrupted files at an S3 URI when an Iceberg table is confirmed corrupted.
    Protects against deleting root or short prefixes by requiring at least 3 path segments.
    """
    if not s3_client or not s3_uri or not s3_uri.startswith("s3://"):
        return
    try:
        parsed = urlparse(s3_uri)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')
        if not prefix.endswith('/'):
            prefix += '/'
        parts = [p for p in prefix.strip('/').split('/') if p]
        if len(parts) < 3:
            logger.warning(f"Refusing to purge suspiciously shallow S3 prefix: '{prefix}'")
            return

        paginator = s3_client.get_paginator('list_objects_v2')
        total_deleted = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = page.get('Contents', [])
            if objects:
                delete_keys = [{'Key': obj['Key']} for obj in objects]
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': delete_keys})
                total_deleted += len(delete_keys)
        if total_deleted > 0:
            logger.info(f"Purged {total_deleted} corrupted/orphaned S3 object(s) under '{s3_uri}'.")
    except Exception as e:
        logger.warning(f"Failed to clean up S3 table location '{s3_uri}': {e}")


def check_iceberg_table_exists(spark, glue_client, database_name: str, table_name: str, s3_client=None, silver_location: str = None) -> bool:
    """
    Safely checks whether an Iceberg table exists and is valid/accessible.
    1. Checks AWS Glue Data Catalog for table registration.
    2. If found, inspects its metadata_location parameter and verifies the metadata file exists in S3.
       If the S3 metadata file does not exist (404 NoSuchKey), this identifies a 'ghost' catalog entry
       (common when S3 test buckets are purged without dropping the Glue Catalog table).
       In this case, the stale Glue Catalog entry is automatically dropped so the table can be cleanly recreated.
    3. Probes the table using Spark SQL DESCRIBE TABLE. If S3 NoSuchKeyException occurs, automatically
       drops the stale catalog entry and returns False to allow clean recreation.
    """
    if s3_client is None:
        try:
            s3_client = boto3.client('s3')
        except Exception:
            pass

    table_found_in_glue = False
    metadata_location = None
    table_location = silver_location

    if glue_client:
        try:
            resp = glue_client.get_table(DatabaseName=database_name, Name=table_name)
            table_meta = resp.get('Table', {})
            table_found_in_glue = True
            params = table_meta.get('Parameters', {})
            metadata_location = params.get('metadata_location')
            table_type = params.get('table_type', '').upper()
            glue_table_type = table_meta.get('TableType', '').upper()
            table_location = table_meta.get('StorageDescriptor', {}).get('Location') or silver_location

            # If the Glue Catalog table was created by a Crawler or legacy job as a standard Hive table
            # (not an Iceberg table, or missing metadata_location), Iceberg SparkCatalog cannot operate on it.
            # Drop the stale non-Iceberg registration so Iceberg can create the real table cleanly.
            if table_type != 'ICEBERG' or not metadata_location:
                logger.warning(
                    f"[NON-ICEBERG CATALOG TABLE DETECTED] Glue Catalog table `{database_name}`.`{table_name}` "
                    f"is registered as a standard Hive table ({glue_table_type}, table_type='{table_type}', metadata_location={metadata_location}). "
                    f"This was likely created by an AWS Glue Crawler. Automatically dropping stale catalog registration "
                    f"so Iceberg can create the table cleanly..."
                )
                try:
                    glue_client.delete_table(DatabaseName=database_name, Name=table_name)
                    logger.info(f"Successfully dropped non-Iceberg catalog table `{database_name}`.`{table_name}`.")
                except Exception as del_err:
                    logger.warning(f"Failed to delete stale table `{database_name}`.`{table_name}` from Glue Catalog: {del_err}")
                return False

            # Validate whether the metadata_location file actually exists in S3
            if metadata_location and s3_client and metadata_location.startswith('s3://'):
                parsed = urlparse(metadata_location)
                b = parsed.netloc
                k = parsed.path.lstrip('/')
                try:
                    s3_client.head_object(Bucket=b, Key=k)
                except ClientError as ce:
                    err_code = ce.response.get('Error', {}).get('Code', '')
                    http_status = ce.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
                    if err_code in ('404', 'NoSuchKey', 'NotFound') or http_status == 404:
                        logger.warning(
                            f"[GHOST TABLE DETECTED] Glue Catalog table `{database_name}`.`{table_name}` points to "
                            f"non-existent S3 metadata '{metadata_location}' (404 NoSuchKey). "
                            f"Dropping stale Glue Catalog registration and cleaning up S3 path to allow clean recreation..."
                        )
                        try:
                            glue_client.delete_table(DatabaseName=database_name, Name=table_name)
                            logger.info(f"Successfully dropped stale Glue Catalog table `{database_name}`.`{table_name}`.")
                        except Exception as del_err:
                            logger.warning(f"Failed to delete stale table `{database_name}`.`{table_name}` from Glue Catalog: {del_err}")
                        if table_location and s3_client:
                            purge_s3_table_location(s3_client, table_location)
                        return False
                    else:
                        logger.warning(f"Could not verify S3 metadata_location '{metadata_location}': {ce}")
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') in ('EntityNotFoundException', 'NoSuchEntityException'):
                return False
            logger.warning(f"Error checking Glue catalog for table {database_name}.{table_name}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error checking Glue catalog for table {database_name}.{table_name}: {e}")

    # Verify table accessibility via Spark probe
    quoted = quote_iceberg_table(f"glue_catalog.{database_name}.{table_name}")
    try:
        spark.sql(f"DESCRIBE TABLE {quoted}")
        return True
    except Exception as probe_err:
        err_str = str(probe_err)
        if any(x in err_str for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "NotFoundException", "404", "NoSuchTableException", "Table or view not found", "Path does not exist", "not an Iceberg table", "ValidationException")):
            if table_found_in_glue:
                logger.warning(
                    f"[CORRUPTED/NON-ICEBERG TABLE DETECTED] Spark probe for Iceberg table '{quoted}' failed ({probe_err}). "
                    f"Automatically dropping stale catalog registration to allow clean Iceberg creation..."
                )
                if glue_client:
                    try:
                        glue_client.delete_table(DatabaseName=database_name, Name=table_name)
                        logger.info(f"Successfully dropped stale Glue Catalog table `{database_name}`.`{table_name}`.")
                    except Exception as del_err:
                        logger.warning(f"Failed to delete stale table from Glue Catalog: {del_err}")
                if table_location and s3_client and any(x in err_str for x in ("NoSuchKey", "NotFound", "404")):
                    purge_s3_table_location(s3_client, table_location)
            return False
        return False


def trigger_silver_iceberg_crawler(glue_client, crawler_name: str):
    """
    Triggers the Silver Iceberg Glue Crawler to sync the Glue Data Catalog with newly created/updated Iceberg tables.
    Safely handles running crawlers or non-existent crawler names without failing the ETL job.
    """
    if not glue_client or not crawler_name:
        return
    try:
        crawler = glue_client.get_crawler(Name=crawler_name)
        status = crawler.get('Crawler', {}).get('State')
        if status in ('READY', 'STOPPED'):
            glue_client.start_crawler(Name=crawler_name)
            logger.info(f"Successfully triggered Silver Iceberg Crawler: '{crawler_name}'")
        else:
            logger.info(f"Silver Iceberg Crawler '{crawler_name}' is currently in state '{status}'. Skipping trigger.")
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code == 'CrawlerRunningException':
            logger.info(f"Silver Iceberg Crawler '{crawler_name}' is already running.")
        elif code == 'EntityNotFoundException':
            logger.warning(f"Silver Iceberg Crawler '{crawler_name}' does not exist. Skipping.")
        else:
            logger.warning(f"Failed to start Silver Iceberg Crawler '{crawler_name}': {e}")
    except Exception as e:
        logger.warning(f"Unexpected error triggering crawler '{crawler_name}': {e}")



def get_payload_columns(all_columns: list, nkeys, config_dict: Optional[dict] = None) -> list:
    """
    Returns sorted list of non-key, non-technical business payload columns used for runtime change detection.
    Technical audit columns, system columns, and natural keys are excluded so changes to timestamps do not trigger false diffs.
    """
    technical_cols = set(SilverConfigLoader.get_bronze_technical_columns(config_dict)) | {
        '_is_deleted', '_inserted_at', '_updated_at',
        '_valid_from', '_valid_to', '_is_current',
        '_ingested_at', '_source_system', '_table_name', '_execution_id',
        '_batch_id', '_raw_payload', '_transformed_at', 'row_num', '__runtime_hash'
    }
    nkey_set = set(nkeys if isinstance(nkeys, list) else [nkeys])
    return sorted([c for c in all_columns if c not in nkey_set and c not in technical_cols and not c.startswith('_')])


def build_runtime_hash_expr(payload_cols: list, prefix: str = "") -> str:
    """
    Builds a Spark SQL string expression for runtime SHA-256 hashing across payload columns.
    Example: sha2(concat_ws('||', coalesce(cast(prefix.`col1` as string), '<NULL>'), ...), 256)
    Calculated strictly in runtime memory/temp views and never stored in the Iceberg table.
    """
    if not payload_cols:
        return "sha2('', 256)"
    pfx = f"{prefix}." if prefix else ""
    coalesced = [f"coalesce(cast({pfx}`{c}` as string), '<NULL>')" for c in payload_cols]
    return f"sha2(concat_ws('||', {', '.join(coalesced)}), 256)"


def perform_deduplication(df, nkeys, order_cols, strategy='latest_by_order_column'):
    """
    Performs windowed in-batch deduplication supporting single or composite natural keys and multi-column ordering.
    Drops exact full-row duplicate records and keeps the latest state per natural key.
    """
    # 1. Drop exact duplicate rows in the incoming batch
    initial_count = df.count()
    df = df.dropDuplicates()
    after_drop_dup = df.count()
    if initial_count != after_drop_dup:
        logger.info(f"Dropped {initial_count - after_drop_dup} exact duplicate row(s) from incoming batch.")

    # 2. Window by Natural Key (Nkey) ordered by business update timestamp DESC
    nkey_cols = [col(k) for k in (nkeys if isinstance(nkeys, list) else [nkeys])]
    order_col_list = order_cols if isinstance(order_cols, list) else [order_cols]

    if strategy == 'earliest_by_order_column':
        order_directions = [col(c).asc() for c in order_col_list]
    else:
        order_directions = [col(c).desc() for c in order_col_list]

    window_spec = Window.partitionBy(*nkey_cols).orderBy(*order_directions)
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")


def sync_iceberg_table_schema(spark, quoted_table: str, incoming_df) -> bool:
    """
    Compares incoming DataFrame schema with existing target Iceberg table schema.
    If incoming batch has new columns, dynamically executes 'ALTER TABLE <tbl> ADD COLUMNS (...)'
    to capture schema changes and automatically update the Iceberg table definition.
    Emits [SCHEMA EVOLUTION DETECTED] card and [DDL AUDIT - ALTER TABLE] logs.
    Returns True if schema was evolved, False otherwise.
    """
    try:
        target_df = spark.table(quoted_table)
        target_field_names = {f.name.lower() for f in target_df.schema.fields}
        new_columns = []
        new_column_details = []
        for field in incoming_df.schema.fields:
            if field.name.lower() not in target_field_names:
                new_columns.append(f"`{field.name}` {field.dataType.simpleString()}")
                new_column_details.append((field.name, field.dataType.simpleString()))

        if new_columns:
            alter_sql = f"ALTER TABLE {quoted_table} ADD COLUMNS ({', '.join(new_columns)})"
            diff_lines = [
                "+--------------------------------------------------------------------------------+",
                f"| [SCHEMA EVOLUTION DETECTED] Target Silver Iceberg Table: {quoted_table}",
                "+--------------------------------------------------------------------------------+",
                f"| Detected {len(new_columns)} new column(s) to add to Iceberg schema:"
            ]
            for col_name, col_type in new_column_details:
                diff_lines.append(f"|   ├── Added: '{col_name}' ({col_type})")
            diff_lines.append(f"| Executing DDL: {alter_sql}")
            diff_lines.append("+--------------------------------------------------------------------------------+")
            logger.info("\n".join(diff_lines))

            logger.info(f"[DDL AUDIT - ALTER TABLE] Altering Iceberg table '{quoted_table}' to add columns: {[c[0] for c in new_column_details]}")
            spark.sql(alter_sql)
            logger.info(f"[SCHEMA EVOLUTION] Successfully updated table schema for '{quoted_table}'.")
            return True
        return False
    except Exception as e:
        err_str = str(e)
        if any(x in err_str for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "NotFoundException", "404")):
            raise
        logger.warning(f"[SCHEMA EVOLUTION] Schema sync check for '{quoted_table}' encountered: {e}. Proceeding with write...")
        return False


def execute_iceberg_scd1_upsert(spark, glue_client, database_name: str, target_table_name: str, df, silver_table_name: str, silver_location: str, nkeys, order_cols, s3_client=None):
    """
    Executes SCD Type 1 (UPSERT via Spark SQL MERGE INTO) on Apache Iceberg table.
    - Compares runtime payload hash to detect genuine business changes.
    - Exact duplicate records are ignored (no redundant rewrite).
    - Backdated loads (source.order_col < target.order_col) are ignored to protect newer data.
    - Preserves target._inserted_at while updating target._updated_at = current_timestamp().
    Returns tuple: (is_new_table: bool, schema_evolved: bool)
    """
    quoted_table = quote_iceberg_table(silver_table_name)
    nkey_list = nkeys if isinstance(nkeys, list) else [nkeys]
    order_col_name = order_cols[0] if isinstance(order_cols, list) else order_cols

    table_exists = check_iceberg_table_exists(spark, glue_client, database_name, target_table_name, s3_client=s3_client, silver_location=silver_location)

    if not table_exists:
        logger.info(f"Target Iceberg table '{quoted_table}' does not exist. Creating table with initial data...")
        temp_view = f"init_{target_table_name.replace('.', '_').replace('-', '_')}"
        df.createOrReplaceTempView(temp_view)
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {quoted_table}
                USING iceberg
                LOCATION '{silver_location}'
                AS SELECT * FROM {temp_view}
            """)
            logger.info(f"Successfully created initial Iceberg table '{quoted_table}' via Spark SQL CTAS.")
        except Exception as ctas_err:
            logger.warning(f"Spark SQL CTAS failed ({ctas_err}), falling back to DataFrameWriter...")
            df.write \
              .format("iceberg") \
              .mode("append") \
              .option("path", silver_location) \
              .saveAsTable(quoted_table)
        return True, False
    else:
        try:
            schema_evolved = sync_iceberg_table_schema(spark, quoted_table, df)
            logger.info(f"Executing SCD Type 1 Runtime-Hash MERGE INTO (UPSERT) on '{quoted_table}'...")
            payload_cols = get_payload_columns(df.columns, nkey_list)
            logger.info(f"Payload columns for runtime change detection ({len(payload_cols)}): {payload_cols}")

            temp_view = f"incoming_scd1_{target_table_name.replace('.', '_').replace('-', '_')}"
            df.createOrReplaceTempView(temp_view)

            join_conditions = [f"target.`{k}` = source.`{k}`" for k in nkey_list]
            join_condition = " AND ".join(join_conditions)

            source_hash_expr = build_runtime_hash_expr(payload_cols, prefix="source")
            target_hash_expr = build_runtime_hash_expr(payload_cols, prefix="target")

            # Non-key, non-technical business columns to update
            silver_tech_cols = {'_valid_from', '_valid_to', '_is_current', '_is_deleted', '_inserted_at', '_updated_at'}
            nkey_set = set(nkey_list)
            business_update_cols = [c for c in df.columns if c not in silver_tech_cols and c not in nkey_set]
            update_set_items = [f"target.`{c}` = source.`{c}`" for c in business_update_cols]
            # In SCD1: update in place without maintaining history; update validity window and ensure _is_current remains 'Y'
            update_set_items.append("target.`_valid_from` = coalesce(source.`_valid_from`, current_timestamp())")
            update_set_items.append("target.`_valid_to` = source.`_valid_to`")
            update_set_items.append("target.`_is_current` = 'Y'")
            update_set_items.append("target.`_is_deleted` = source.`_is_deleted`")
            update_set_items.append("target.`_updated_at` = current_timestamp()")
            # target._inserted_at is preserved from initial insert
            update_set_clause = ",\n              ".join(update_set_items)

            # Columns to insert for new records
            insert_cols = [c for c in df.columns if c not in ('_inserted_at', '_updated_at')] + ['_inserted_at', '_updated_at']
            insert_cols_str = ", ".join([f"`{c}`" for c in insert_cols])
            insert_vals_str = ", ".join([f"source.`{c}`" if c not in ('_inserted_at', '_updated_at') else "current_timestamp()" for c in insert_cols])

            # Backdate protection: only update if source order_col >= target order_col (or target order_col is null)
            order_col_check = ""
            if order_col_name in df.columns:
                order_col_check = f"AND (source.`{order_col_name}` >= target.`{order_col_name}` OR target.`{order_col_name}` IS NULL)"

            merge_sql = f"""
            MERGE INTO {quoted_table} AS target
            USING {temp_view} AS source
            ON {join_condition}
            WHEN MATCHED AND (
                target.`_is_deleted` != source.`_is_deleted` OR
                {target_hash_expr} != {source_hash_expr}
            ) {order_col_check} THEN UPDATE SET
              {update_set_clause}
            WHEN NOT MATCHED THEN INSERT
              ({insert_cols_str})
            VALUES
              ({insert_vals_str})
            """
            logger.info(f"Running Spark SQL SCD1 MERGE INTO Query on {quoted_table}...")
            spark.sql(merge_sql)
            return False, schema_evolved
        except Exception as merge_err:
            err_str = str(merge_err)
            if any(x in err_str for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "NotFoundException", "404")):
                logger.warning(
                    f"Iceberg MERGE INTO on '{quoted_table}' failed due to missing S3 metadata/data file: {merge_err}. "
                    f"Target table is corrupted. Recovering: dropping corrupted catalog registration and recreating table via CTAS fallback..."
                )
                if glue_client:
                    try:
                        glue_client.delete_table(DatabaseName=database_name, Name=target_table_name)
                        logger.info(f"Dropped corrupted Glue Catalog table '{database_name}.{target_table_name}'.")
                    except Exception as del_err:
                        logger.warning(f"Failed to delete corrupted catalog table: {del_err}")
                if s3_client and silver_location:
                    purge_s3_table_location(s3_client, silver_location)
                temp_view = f"init_{target_table_name.replace('.', '_').replace('-', '_')}"
                df.createOrReplaceTempView(temp_view)
                try:
                    spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {quoted_table}
                        USING iceberg
                        LOCATION '{silver_location}'
                        AS SELECT * FROM {temp_view}
                    """)
                    logger.info(f"Successfully recovered and recreated Iceberg table '{quoted_table}' via Spark SQL CTAS.")
                except Exception as ctas_err:
                    logger.warning(f"Recovery CTAS failed ({ctas_err}), falling back to DataFrameWriter...")
                    df.write \
                      .format("iceberg") \
                      .mode("append") \
                      .option("path", silver_location) \
                      .saveAsTable(quoted_table)
                return True, False
            else:
                raise


def execute_iceberg_scd2(spark, glue_client, database_name: str, target_table_name: str, df, silver_table_name: str, silver_location: str, nkeys, order_cols, scd2_cfg, s3_client=None):
    """
    Executes SCD Type 2 (Slowly Changing Dimension Type 2) on Apache Iceberg table.
    Tracks historical change history with _valid_from, _valid_to, _is_current ('Y'/'N'), and _is_deleted ('Y'/'N').
    - Uses high-date ('9999-01-01 00:00:00') as default _valid_to for active records.
    - Runtime change detection ensures duplicate records NEVER spawn false versions.
    - Backdated loads cannot expire active records if they are older than the active version.
    """
    quoted_table = quote_iceberg_table(silver_table_name)
    nkey_list = nkeys if isinstance(nkeys, list) else [nkeys]
    order_col_name = order_cols[0] if isinstance(order_cols, list) else order_cols

    valid_from_col = scd2_cfg.get('valid_from_column', '_valid_from')
    valid_to_col = scd2_cfg.get('valid_to_column', '_valid_to')
    is_current_col = scd2_cfg.get('is_current_column', '_is_current')
    high_date_val = scd2_cfg.get('high_date_value', '9999-01-01 00:00:00')

    table_exists = check_iceberg_table_exists(spark, glue_client, database_name, target_table_name, s3_client=s3_client, silver_location=silver_location)

    # Base SCD2 columns for incoming batch ('Y' for active record)
    incoming_df = df.withColumn(valid_from_col, coalesce(col(order_col_name).cast("timestamp"), col(valid_from_col), current_timestamp())) \
                    .withColumn(valid_to_col, to_timestamp(lit(high_date_val))) \
                    .withColumn(is_current_col, lit('Y'))

    # Guarantee all business columns come first, and system audit columns are at the very end
    silver_tech_cols = [valid_from_col, valid_to_col, is_current_col, '_is_deleted', '_inserted_at', '_updated_at']
    business_cols = [c for c in incoming_df.columns if c not in silver_tech_cols]
    incoming_df = incoming_df.select(*(business_cols + silver_tech_cols))

    if not table_exists:
        logger.info(f"SCD Type 2: Target table '{quoted_table}' does not exist. Creating initial table with high-date '{high_date_val}'...")
        temp_view = f"scd2_init_{target_table_name.replace('.', '_').replace('-', '_')}"
        incoming_df.createOrReplaceTempView(temp_view)
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {quoted_table}
                USING iceberg
                LOCATION '{silver_location}'
                AS SELECT * FROM {temp_view}
            """)
            logger.info(f"Successfully created initial SCD2 Iceberg table '{quoted_table}' via Spark SQL CTAS.")
        except Exception as ctas_err:
            logger.warning(f"Spark SQL CTAS failed ({ctas_err}), falling back to DataFrameWriter...")
            incoming_df.write \
                       .format("iceberg") \
                       .mode("append") \
                       .option("path", silver_location) \
                       .saveAsTable(quoted_table)
        return True, False
    else:
        try:
            schema_evolved = sync_iceberg_table_schema(spark, quoted_table, incoming_df)
            logger.info(f"Executing SCD Type 2 Runtime-Hash Change Detection on '{quoted_table}'...")
            payload_cols = get_payload_columns(incoming_df.columns, nkey_list)
            logger.info(f"SCD2 Payload columns for runtime change detection ({len(payload_cols)}): {payload_cols}")

            temp_view = f"scd2_incoming_{target_table_name.replace('.', '_').replace('-', '_')}"
            incoming_df.createOrReplaceTempView(temp_view)

            join_conditions = [f"target.`{k}` = source.`{k}`" for k in nkey_list]
            join_condition = " AND ".join(join_conditions)

            source_hash_expr = build_runtime_hash_expr(payload_cols, prefix="source")
            target_hash_expr = build_runtime_hash_expr(payload_cols, prefix="target")

            # 1. Expire existing active target records ONLY when:
            #    a) Record exists and target._is_current is active ('Y')
            #    b) Payload hash differs OR _is_deleted differs (data actually changed)
            #    c) Incoming record timestamp is >= target._valid_from (not backdated)
            expire_sql = f"""
            MERGE INTO {quoted_table} AS target
            USING {temp_view} AS source
            ON {join_condition} AND (target.`{is_current_col}` = 'Y' OR cast(target.`{is_current_col}` as string) IN ('true', 'TRUE'))
            WHEN MATCHED AND (
                target.`_is_deleted` != source.`_is_deleted` OR
                {target_hash_expr} != {source_hash_expr}
            ) AND (
                source.`{valid_from_col}` >= target.`{valid_from_col}` OR target.`{valid_from_col}` IS NULL
            ) THEN UPDATE SET
              target.`{is_current_col}` = 'N',
              target.`{valid_to_col}` = source.`{valid_from_col}`,
              target.`_updated_at` = current_timestamp()
            """
            logger.info(f"Executing SCD Type 2 Target Expiration MERGE INTO on {quoted_table}...")
            spark.sql(expire_sql)

            # 2. Append new incoming records ONLY if:
            #    a) They are completely new (do not exist in active target at all), OR
            #    b) They represent a genuine change over the prior active version (and are newer than prior version)
            active_target_view = f"scd2_active_{target_table_name.replace('.', '_').replace('-', '_')}"
            spark.sql(f"SELECT * FROM {quoted_table} WHERE `{is_current_col}` = 'Y' OR cast(`{is_current_col}` as string) IN ('true', 'TRUE')").createOrReplaceTempView(active_target_view)

            target_active_hash = build_runtime_hash_expr(payload_cols, prefix="act")
            source_inc_hash = build_runtime_hash_expr(payload_cols, prefix="inc")
            active_join_conditions = [f"act.`{k}` = inc.`{k}`" for k in nkey_list]
            active_join_cond = " AND ".join(active_join_conditions)

            changed_and_new_sql = f"""
            SELECT inc.*
            FROM {temp_view} AS inc
            LEFT JOIN {active_target_view} AS act
              ON {active_join_cond}
            WHERE act.`{nkey_list[0]}` IS NULL
               OR (
                   (act.`_is_deleted` != inc.`_is_deleted` OR {target_active_hash} != {source_inc_hash})
                   AND (inc.`{valid_from_col}` >= act.`{valid_from_col}` OR act.`{valid_from_col}` IS NULL)
               )
            """
            changed_or_new_df = spark.sql(changed_and_new_sql)
            num_new_versions = changed_or_new_df.count()
            logger.info(f"SCD Type 2: Appending {num_new_versions} genuine new/updated version(s) to '{quoted_table}'...")

            if num_new_versions > 0:
                temp_append_view = f"scd2_append_{target_table_name.replace('.', '_').replace('-', '_')}"
                changed_or_new_df.createOrReplaceTempView(temp_append_view)
                try:
                    spark.sql(f"INSERT INTO {quoted_table} SELECT * FROM {temp_append_view}")
                except Exception as ins_err:
                    logger.warning(f"Spark SQL INSERT INTO failed ({ins_err}), falling back to DataFrameWriter...")
                    changed_or_new_df.write \
                                     .format("iceberg") \
                                     .mode("append") \
                                     .option("path", silver_location) \
                                     .saveAsTable(quoted_table)
            else:
                logger.info(f"SCD Type 2: 0 changed records found in incoming batch. No duplicate versions appended.")
            return False, schema_evolved
        except Exception as scd2_err:
            err_str = str(scd2_err)
            if any(x in err_str for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "NotFoundException", "404")):
                logger.warning(
                    f"Iceberg SCD2 on '{quoted_table}' failed due to missing S3 metadata/data file: {scd2_err}. "
                    f"Target table is corrupted. Recovering: dropping corrupted catalog registration and recreating table via CTAS fallback..."
                )
                if glue_client:
                    try:
                        glue_client.delete_table(DatabaseName=database_name, Name=target_table_name)
                        logger.info(f"Dropped corrupted Glue Catalog table '{database_name}.{target_table_name}'.")
                    except Exception as del_err:
                        logger.warning(f"Failed to delete corrupted catalog table: {del_err}")
                if s3_client and silver_location:
                    purge_s3_table_location(s3_client, silver_location)
                temp_view = f"scd2_init_{target_table_name.replace('.', '_').replace('-', '_')}"
                incoming_df.createOrReplaceTempView(temp_view)
                try:
                    spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {quoted_table}
                        USING iceberg
                        LOCATION '{silver_location}'
                        AS SELECT * FROM {temp_view}
                    """)
                    logger.info(f"Successfully recovered and recreated SCD2 Iceberg table '{quoted_table}' via Spark SQL CTAS.")
                except Exception as ctas_err:
                    logger.warning(f"Recovery CTAS failed ({ctas_err}), falling back to DataFrameWriter...")
                    incoming_df.write \
                               .format("iceberg") \
                               .mode("append") \
                               .option("path", silver_location) \
                               .saveAsTable(quoted_table)
                return True, False
            else:
                raise


def main():
    params = parse_spark_arguments()
    job_name = params['JOB_NAME']
    source_system = params['SOURCE_SYSTEM']
    table_list = params['TABLE_LIST']
    bucket_name = params['DATA_LAKE_BUCKET']
    glue_database = params['GLUE_DATABASE']
    table_prefix = params['TABLE_PREFIX']
    bronze_data_prefix = params.get('BRONZE_DATA_PREFIX', 'bronze/data')
    silver_data_prefix = params.get('SILVER_DATA_PREFIX', 'silver/data')
    silver_full_config = params['SILVER_FULL_CONFIG']

    # Watermark Parameters
    watermark_enabled = params.get('WATERMARK_ENABLED', True)
    full_refresh = params.get('FULL_REFRESH', False)
    watermark_column = params.get('WATERMARK_COLUMN', '_ingested_at')
    sync_watermark_table = params.get('SYNC_WATERMARK_TABLE', True)
    watermark_table_name = params.get('WATERMARK_TABLE_NAME')
    metadata_prefix = params.get('METADATA_PREFIX', 'metadata/silver')

    # Initialize Spark, Glue, and Boto3 Clients
    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{bucket_name}/{silver_data_prefix}/")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true")
    conf.set("spark.sql.catalog.glue_catalog.skip-name-validation", "true")
    conf.set("spark.sql.catalog.glue_catalog.aws.glue.skip-name-validation", "true")
    conf.set("spark.sql.defaultCatalog", "glue_catalog")
    conf.set("spark.sql.iceberg.schema-evolution", "true")
    conf.set("spark.sql.iceberg.check-nullability", "false")
    # Parquet reader settings: Disable Vectorized Reader to support mixed/evolving schemas across files
    # (e.g. DOUBLE in one file and string in another file / catalog definition)
    conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    conf.set("spark.sql.parquet.recordLevelFilter.enabled", "false")
    conf.set("spark.sql.parquet.mergeSchema", "false")

    sc = SparkContext.getOrCreate(conf=conf)
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Ensure runtime SparkSession also bypasses Glue catalog name validation for database names with hyphens
    spark.conf.set("spark.sql.catalog.glue_catalog.glue.skip-name-validation", "true")
    spark.conf.set("spark.sql.catalog.glue_catalog.skip-name-validation", "true")
    spark.conf.set("spark.sql.catalog.glue_catalog.aws.glue.skip-name-validation", "true")
    spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")
    spark.conf.set("spark.sql.iceberg.schema-evolution", "true")
    spark.conf.set("spark.sql.iceberg.check-nullability", "false")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.recordLevelFilter.enabled", "false")
    spark.conf.set("spark.sql.parquet.mergeSchema", "false")
    job = Job(glueContext)
    job.init(job_name, params['ARG_DICT'])

    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')

    execution_start_utc = datetime.now(timezone.utc)
    current_run_time = execution_start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

    process_layer = params.get('PROCESS_LAYER', 'silver')
    if process_layer == 'gold':
        logger.info(
            f"\n+================================================================================+\n"
            f"|  ROUTING TO GOLD SERVING ENGINE: --PROCESS_LAYER=gold                           |\n"
            f"+================================================================================+"
        )
        GoldLayerManager.run_gold_pipeline(
            spark=spark,
            params=params,
            glue_client=glue_client,
            s3_client=s3_client
        )
        job.commit()
        logger.info("All Gold serving layer operations completed successfully.")
        return

    start_banner = (
        f"[JOB START] SILVER ICEBERG ETL | Source: {source_system.upper()} | Tables: {', '.join(table_list)}\n"
        "+================================================================================+\n"
        "|                 UAX DATA LAKE - SILVER ICEBERG ETL ENGINE                      |\n"
        "+================================================================================+\n"
        f"|  Job Name           : {job_name:<57}|\n"
        f"|  Source System      : {source_system.upper():<57}|\n"
        f"|  Target Tables      : {', '.join(table_list):<57}|\n"
        f"|  Data Lake Bucket   : {f's3://{bucket_name}/':<57}|\n"
        f"|  Glue Database      : {glue_database:<57}|\n"
        f"|  Table Prefix       : {table_prefix:<57}|\n"
        f"|  Bronze Data Prefix : {bronze_data_prefix:<57}|\n"
        f"|  Silver Data Prefix : {silver_data_prefix:<57}|\n"
        f"|  Watermark Enabled  : {str(watermark_enabled):<57}|\n"
        f"|  Watermark Column   : {watermark_column:<57}|\n"
        f"|  Watermark Athena   : {watermark_table_name:<57}|\n"
        f"|  Full Refresh       : {str(full_refresh):<57}|\n"
        f"|  Start Time (UTC)   : {current_run_time:<57}|\n"
        "+================================================================================+"
    )
    logger.info(start_banner)

    failed_tables = []
    table_stats = []
    tables_created = 0
    schemas_evolved = 0

    for table_idx, table_name in enumerate(table_list, start=1):
        table_clean = table_name.strip().lower()
        raw_base_name = table_clean
        if raw_base_name.startswith("raw_tbl_"):
            raw_base_name = raw_base_name[len("raw_tbl_"):]
        elif raw_base_name.startswith("tbl_"):
            raw_base_name = raw_base_name[len("tbl_"):]
        base_table_name = raw_base_name.replace("-", "_")

        table_cfg = SilverConfigLoader.get_table_config(source_system, table_clean, silver_full_config)
        bronze_table_name = table_cfg.get('source_table_name') or f"raw_tbl_{base_table_name}"
        table_start_time = datetime.now(timezone.utc)

        defaults_cfg = SilverConfigLoader.get_defaults(silver_full_config)
        scd2_cfg = defaults_cfg.get('scd_type2_config', {})

        target_table_name = (table_cfg.get('target_table_name') or f"{table_prefix}{base_table_name}").replace("-", "_")
        silver_table_name = f"glue_catalog.{glue_database}.{target_table_name}"
        bronze_path = f"s3://{bucket_name}/{bronze_data_prefix}/{source_system}/{base_table_name}/"
        silver_location = f"s3://{bucket_name}/{silver_data_prefix}/{source_system}/{base_table_name}/"
        if glue_client:
            try:
                tbl_desc = glue_client.get_table(DatabaseName=glue_database, Name=target_table_name).get('Table', {})
                existing_loc = tbl_desc.get('StorageDescriptor', {}).get('Location')
                if existing_loc and existing_loc.startswith('s3://'):
                    silver_location = existing_loc if existing_loc.endswith('/') else f"{existing_loc}/"
            except Exception:
                pass

        # Resolve High-Water Mark state key & last load date
        state_key = get_silver_watermark_key(source_system, base_table_name, metadata_prefix)
        last_load_date = None
        if watermark_enabled and not full_refresh:
            last_load_date = get_silver_last_load_date(
                s3_client=s3_client,
                bucket=bucket_name,
                state_key=state_key,
                table_display=target_table_name,
                full_refresh=full_refresh
            )

        table_header = (
            f"[TABLE START] {bronze_table_name} -> {target_table_name} [{table_idx}/{len(table_list)}] | Source: {source_system} | Database: {glue_database}\n"
            "+--------------------------------------------------------------------------------+\n"
            f"| >>> [{table_idx}/{len(table_list)}] SILVER PROCESSING: {bronze_table_name.upper()} -> {target_table_name.upper()} (Source: {source_system})\n"
            f"|     Bronze Source Table: {glue_database}.{bronze_table_name}\n"
            f"|     Target Silver Table: {silver_table_name}\n"
            f"|     Silver Location    : {silver_location}\n"
            f"|     Watermark State Key: {state_key if watermark_enabled else 'DISABLED'}\n"
            f"|     Last Watermark     : {last_load_date or ('INITIAL_FULL_LOAD' if watermark_enabled else 'DISABLED')}\n"
            f"|     Table Start (UTC)  : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            "+--------------------------------------------------------------------------------+"
        )
        logger.info(table_header)

        # Resolve Merge Strategy, SCD Type & Deduplication settings
        scd_type = (table_cfg.get('scd_type') or defaults_cfg.get('scd_type', 'scd1')).lower()
        merge_strategy = (table_cfg.get('merge_strategy') or defaults_cfg.get('merge_strategy', 'upsert')).lower()
        dedup_strategy = table_cfg.get('deduplication_strategy') or defaults_cfg.get('deduplication', {}).get('strategy', 'latest_by_order_column')

        try:
            logger.info(
                f"\n================================================================================\n"
                f"[SILVER STEP 1/6] Ingesting Bronze Data & Evaluating Watermark for '{bronze_table_name}'\n"
                f"--------------------------------------------------------------------------------"
            )
            logger.info(f"Reading raw Bronze data for '{bronze_table_name}' from Glue Catalog (`{glue_database}`.`{bronze_table_name}`) or S3 ('{bronze_path}')...")

            df_bronze = None
            batch_count = 0

            # 1. Primary: Read Bronze Hive/Parquet table via GlueContext native catalog reader with resolveChoice
            # (bypasses Iceberg SparkCatalog so it never raises 'Input Glue table is not an Iceberg table',
            # and resolves any type conflicts across Parquet parts like DOUBLE vs String to string)
            if glueContext:
                try:
                    logger.info(f"Attempting to load Bronze data from Glue Catalog table `{glue_database}`.`{bronze_table_name}` via GlueContext...")
                    dyn_frame = glueContext.create_dynamic_frame.from_catalog(
                        database=glue_database,
                        table_name=bronze_table_name
                    )
                    dyn_frame = dyn_frame.resolveChoice(choice='cast:string')
                    temp_df = dyn_frame.toDF()

                    if last_load_date and watermark_enabled and not full_refresh and watermark_column in temp_df.columns:
                        logger.info(f"Applying Silver Watermark filter: `{watermark_column}` > '{last_load_date}' for table '{target_table_name}'...")
                        temp_df = temp_df.filter(col(watermark_column) > lit(last_load_date))

                    batch_count = temp_df.count()
                    df_bronze = temp_df
                    logger.info(f"Successfully loaded {batch_count} Bronze record(s) from Glue Catalog table `{glue_database}`.`{bronze_table_name}`.")
                except Exception as cat_err:
                    logger.warning(
                        f"Glue Catalog table `{glue_database}`.`{bronze_table_name}` read/count failed ({cat_err}). "
                        f"Falling back to direct S3 DynamicFrame reader from '{bronze_path}' with resolveChoice..."
                    )
                    df_bronze = None

            # 2. Fallback: Read directly from S3 using Glue DynamicFrame with resolveChoice(choice='cast:string')
            # Handles heterogeneous Parquet files across partitions where types differ (e.g. DOUBLE vs String)
            if df_bronze is None and glueContext:
                try:
                    logger.info(f"Attempting direct S3 DynamicFrame load from '{bronze_path}' with resolveChoice...")
                    dyn_frame = glueContext.create_dynamic_frame.from_options(
                        connection_type="s3",
                        connection_options={"paths": [bronze_path], "recurse": True},
                        format="parquet"
                    )
                    dyn_frame = dyn_frame.resolveChoice(choice='cast:string')
                    temp_df = dyn_frame.toDF()

                    if last_load_date and watermark_enabled and not full_refresh and watermark_column in temp_df.columns:
                        logger.info(f"Applying Silver Watermark filter: `{watermark_column}` > '{last_load_date}' for table '{target_table_name}'...")
                        temp_df = temp_df.filter(col(watermark_column) > lit(last_load_date))

                    batch_count = temp_df.count()
                    df_bronze = temp_df
                    logger.info(f"Successfully loaded {batch_count} Bronze record(s) from S3 via DynamicFrame.")
                except Exception as s3_dyn_err:
                    logger.warning(f"S3 DynamicFrame read from '{bronze_path}' failed ({s3_dyn_err}). Trying Spark DataFrame reader...")
                    df_bronze = None

            # 3. Fallback: Standard Spark DataFrame Parquet read
            if df_bronze is None:
                try:
                    logger.info(f"Attempting Spark Parquet read from '{bronze_path}'...")
                    temp_df = spark.read.option("mergeSchema", "false").parquet(bronze_path)
                    if last_load_date and watermark_enabled and not full_refresh and watermark_column in temp_df.columns:
                        logger.info(f"Applying Silver Watermark filter: `{watermark_column}` > '{last_load_date}' for table '{target_table_name}'...")
                        temp_df = temp_df.filter(col(watermark_column) > lit(last_load_date))
                    batch_count = temp_df.count()
                    df_bronze = temp_df
                    logger.info(f"Successfully loaded {batch_count} Bronze record(s) via Spark Parquet reader.")
                except Exception as spark_parquet_err:
                    logger.warning(f"Spark Parquet read from '{bronze_path}' failed ({spark_parquet_err}). Trying alternate paths / formats...")

            # 4. Fallback: Alternate directory paths & JSON fallback
            if df_bronze is None:
                alt_bronze_path = f"s3://{bucket_name}/{bronze_data_prefix}/{source_system}/{base_table_name}/"
                for candidate_path in [bronze_path, alt_bronze_path]:
                    if df_bronze is not None:
                        break
                    for read_fn in [
                        lambda p: spark.read.option("mergeSchema", "true").parquet(p),
                        lambda p: spark.read.json(p)
                    ]:
                        try:
                            temp_df = read_fn(candidate_path)
                            if last_load_date and watermark_enabled and not full_refresh and watermark_column in temp_df.columns:
                                temp_df = temp_df.filter(col(watermark_column) > lit(last_load_date))
                            batch_count = temp_df.count()
                            df_bronze = temp_df
                            logger.info(f"Successfully loaded {batch_count} Bronze record(s) from '{candidate_path}'.")
                            break
                        except Exception:
                            continue

            # If all readers fail or directory is empty, skip table
            if df_bronze is None:
                table_duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()
                table_stats.append({
                    "table_name": target_table_name,
                    "status": "SKIPPED_NO_BRONZE_DATA",
                    "scd_type": scd_type.upper(),
                    "merge_strategy": merge_strategy.upper(),
                    "duration_seconds": round(table_duration, 2),
                    "records_processed": 0,
                    "error_message": f"Bronze source data not found or unreadable from Glue Catalog nor S3 path '{bronze_path}'."
                })
                logger.warning(
                    f"[TABLE SKIPPED] Bronze data not found for '{bronze_table_name}' at '{bronze_path}'. "
                    f"Bronze ingestion may not have run yet or 0 records were returned by source API. "
                    f"Skipping Silver table creation until Bronze data is available."
                )
                continue

            logger.info(f"Incoming Bronze records to process for '{target_table_name}': {batch_count}")

            if batch_count == 0:
                table_duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()
                table_end_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                table_stats.append({
                    "table_name": target_table_name,
                    "status": "SKIPPED_UP_TO_DATE",
                    "scd_type": scd_type.upper(),
                    "merge_strategy": merge_strategy.upper(),
                    "duration_seconds": round(table_duration, 2),
                    "records_processed": 0,
                    "error_message": None
                })
                skipped_card = (
                    f"[TABLE SKIPPED] {table_clean} | UP TO DATE | Duration: {table_duration:.2f}s\n"
                    "+================================================================================+\n"
                    f"|  SILVER TABLE SKIPPED: {table_clean} [UP TO DATE]\n"
                    "+--------------------------------------------------------------------------------+\n"
                    f"|  * Source System    : {source_system}\n"
                    f"|  * Table Name       : {table_clean} ({target_table_name})\n"
                    f"|  * Target Iceberg   : {silver_table_name}\n"
                    f"|  * Last Watermark   : {last_load_date}\n"
                    f"|  * New Records      : 0\n"
                    f"|  * Status           : Table is already up to date. Skipping Iceberg write.\n"
                    f"|  * Table Duration   : {table_duration:.2f}s\n"
                    "+================================================================================+"
                )
                logger.info(skipped_card)
                continue

            # Allow CLI Arguments to dynamically override silver_config.json settings for manual testing / Step Functions
            cli_args = params.get('ARG_DICT', {})
            merge_strategy = (cli_args.get('MERGE_STRATEGY') or table_cfg.get('merge_strategy') or defaults_cfg.get('merge_strategy', 'upsert')).lower()
            scd_type = (cli_args.get('SCD_TYPE') or table_cfg.get('scd_type') or defaults_cfg.get('scd_type', 'scd1')).lower()
            scd2_cfg = table_cfg.get('scd2_config') or defaults_cfg.get('scd2_defaults', {})
            dedup_strategy = (cli_args.get('DEDUPLICATION_STRATEGY') or table_cfg.get('deduplication_strategy') or defaults_cfg.get('deduplication', {}).get('strategy', 'latest_by_order_column')).lower()

            columns = df_bronze.columns

            # Dynamically resolve Natural Key (Nkey) from CLI override -> config (fallback to primary_key)
            cli_nkey = cli_args.get('NKEY') or cli_args.get('NKEYS') or cli_args.get('DEDUPLICATION_KEYS') or cli_args.get('PRIMARY_KEY')
            if cli_nkey:
                raw_nkeys = [k.strip() for k in cli_nkey.split(',')] if ',' in cli_nkey else [cli_nkey.strip()]
                nkeys = [k for k in raw_nkeys if k in columns]
                if not nkeys:
                    raise ValueError(
                        f"CRITICAL CONFIG ERROR: CLI parameter override for nkey '{cli_nkey}' does not match any column in table '{bronze_table_name}'. "
                        f"Available columns: {columns}"
                    )
                logger.info(f"CLI Parameter Override: 'nkey' -> {nkeys}")
            else:
                cfg_nkey = table_cfg.get('nkey') or table_cfg.get('deduplication_keys') or table_cfg.get('primary_key')
                if not cfg_nkey:
                    raise ValueError(
                        f"CRITICAL CONFIG ERROR: 'nkey' (natural/deduplication key) is missing for table '{bronze_table_name}' in silver_config.json "
                        f"(source_systems.{source_system}.table_configs.{table_clean}.nkey). "
                        f"Please specify the key column in silver_config.json or pass --NKEY via CLI."
                    )
                if isinstance(cfg_nkey, list):
                    raw_nkeys = [str(k).strip() for k in cfg_nkey if str(k).strip()]
                elif isinstance(cfg_nkey, str) and ',' in cfg_nkey:
                    raw_nkeys = [k.strip() for k in cfg_nkey.split(',') if k.strip()]
                else:
                    raw_nkeys = [str(cfg_nkey).strip()]
                nkeys = [k for k in raw_nkeys if k in columns]
                if not nkeys:
                    raise ValueError(
                        f"CRITICAL CONFIG ERROR: Configured nkey '{raw_nkeys}' does not match any column in Bronze table '{bronze_table_name}'. "
                        f"Check silver_config.json (source_systems.{source_system}.table_configs.{table_clean}.nkey). "
                        f"Available columns: {columns}"
                    )

            # Dynamically resolve Deduplication Order-By Columns from CLI override -> config
            cli_order = cli_args.get('DEDUPLICATION_ORDER_BY') or cli_args.get('ORDER_BY')
            if cli_order:
                raw_orders = [c.strip() for c in cli_order.split(',')] if ',' in cli_order else [cli_order.strip()]
                order_cols = [c for c in raw_orders if c in columns]
                if not order_cols:
                    raise ValueError(
                        f"CRITICAL CONFIG ERROR: CLI parameter override for deduplication_order_by '{cli_order}' does not match any column in table '{bronze_table_name}'. "
                        f"Available columns: {columns}"
                    )
                logger.info(f"CLI Parameter Override: 'deduplication_order_by' -> {order_cols}")
            else:
                cfg_order = table_cfg.get('deduplication_order_by') or table_cfg.get('order_by')
                if not cfg_order:
                    raise ValueError(
                        f"CRITICAL CONFIG ERROR: 'deduplication_order_by' is missing for table '{bronze_table_name}' in silver_config.json "
                        f"(source_systems.{source_system}.table_configs.{table_clean}.deduplication_order_by). "
                        f"Please specify the ordering column in silver_config.json or pass --DEDUPLICATION_ORDER_BY via CLI."
                    )
                raw_orders = cfg_order if isinstance(cfg_order, list) else [cfg_order]
                order_cols = [c for c in raw_orders if c in columns]
                if not order_cols:
                    raise ValueError(
                        f"CRITICAL CONFIG ERROR: Configured deduplication_order_by '{raw_orders}' does not match any column in Bronze table '{bronze_table_name}'. "
                        f"Check silver_config.json (source_systems.{source_system}.table_configs.{table_clean}.deduplication_order_by). "
                        f"Available columns: {columns}"
                    )

            logger.info(f"Deduplicating table '{table_clean}': Nkey={nkeys}, OrderBy={order_cols}, Strategy='{dedup_strategy}'")

            logger.info(
                f"\n================================================================================\n"
                f"[SILVER STEP 2/6] Deduplicating Records for '{table_clean}' (Nkey: {nkeys}, Strategy: '{dedup_strategy}')\n"
                f"--------------------------------------------------------------------------------"
            )
            # 1. Perform in-batch deduplication (drops exact duplicates & keeps latest per Nkey)
            df_dedup = perform_deduplication(df_bronze, nkeys, order_cols, dedup_strategy)

            # 2. Strip Bronze layer system metadata columns so they NEVER pass into Silver tables
            bronze_system_cols = set(SilverConfigLoader.get_bronze_technical_columns(silver_full_config))
            bronze_cols_to_drop = [c for c in df_dedup.columns if c in bronze_system_cols]
            if bronze_cols_to_drop:
                logger.info(f"Stripping Bronze technical columns before Silver processing: {bronze_cols_to_drop}")
                df_dedup = df_dedup.drop(*bronze_cols_to_drop)

            logger.info(
                f"\n================================================================================\n"
                f"[SILVER STEP 3/6] Applying Transformations & API Enrichment Hook for '{table_clean}'\n"
                f"--------------------------------------------------------------------------------"
            )
            primary_order_col = order_cols[0] if isinstance(order_cols, list) and order_cols else order_cols
            context = {
                "glue_database": glue_database,
                "source_system": source_system,
                "table_name": table_clean,
                "base_table_name": base_table_name,
                "data_lake_bucket": bucket_name,
                "silver_data_prefix": silver_data_prefix,
                "table_cfg": table_cfg,
                "cli_args": cli_args,
                "job_name": job_name
            }
            df_transformed = SilverTransformer.apply_transformations(
                df=df_dedup,
                source_system=source_system,
                table_name=table_clean,
                table_cfg=table_cfg,
                order_col_name=primary_order_col,
                nkeys=nkeys,
                spark=spark,
                context=context
            )

            logger.info(
                f"\n================================================================================\n"
                f"[SILVER STEP 4/6 & 5/6] Checking Schema Evolution & Executing Iceberg Write for '{target_table_name}'\n"
                f"--------------------------------------------------------------------------------\n"
                f"Target Iceberg Table: '{silver_table_name}', SCD Type: '{scd_type.upper()}', Merge Strategy: '{merge_strategy.upper()}'"
            )

            # 3. Execute SCD Type 2 or SCD Type 1 / Append / Overwrite
            quoted_iceberg_table = quote_iceberg_table(silver_table_name)
            table_is_new = False
            schema_evolved = False
            if scd_type == 'scd2':
                table_is_new, schema_evolved = execute_iceberg_scd2(
                    spark=spark,
                    glue_client=glue_client,
                    database_name=glue_database,
                    target_table_name=target_table_name,
                    df=df_transformed,
                    silver_table_name=silver_table_name,
                    silver_location=silver_location,
                    nkeys=nkeys,
                    order_cols=order_cols,
                    scd2_cfg=scd2_cfg,
                    s3_client=s3_client
                )
            elif merge_strategy in ('upsert', 'merge_into'):
                table_is_new, schema_evolved = execute_iceberg_scd1_upsert(
                    spark=spark,
                    glue_client=glue_client,
                    database_name=glue_database,
                    target_table_name=target_table_name,
                    df=df_transformed,
                    silver_table_name=silver_table_name,
                    silver_location=silver_location,
                    nkeys=nkeys,
                    order_cols=order_cols,
                    s3_client=s3_client
                )
            elif merge_strategy == 'overwrite':
                table_exists = check_iceberg_table_exists(spark, glue_client, glue_database, target_table_name, s3_client=s3_client, silver_location=silver_location)
                if not table_exists:
                    logger.info(f"Target Iceberg table '{quoted_iceberg_table}' does not exist. Initializing table via Spark SQL CTAS...")
                    temp_view = f"init_{target_table_name.replace('.', '_').replace('-', '_')}"
                    df_transformed.createOrReplaceTempView(temp_view)
                    try:
                        spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {quoted_iceberg_table}
                            USING iceberg
                            LOCATION '{silver_location}'
                            AS SELECT * FROM {temp_view}
                        """)
                    except Exception as ctas_err:
                        logger.warning(f"Spark SQL CTAS failed ({ctas_err}), falling back to DataFrameWriter...")
                        df_transformed.write \
                            .format("iceberg") \
                            .mode("append") \
                            .option("path", silver_location) \
                            .saveAsTable(quoted_iceberg_table)
                    table_is_new = True
                else:
                    schema_evolved = sync_iceberg_table_schema(spark, quoted_iceberg_table, df_transformed)
                    try:
                        df_transformed.write \
                            .format("iceberg") \
                            .mode("overwrite") \
                            .option("path", silver_location) \
                            .saveAsTable(quoted_iceberg_table)
                    except Exception as ow_err:
                        err_str = str(ow_err)
                        if any(x in err_str for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "NotFoundException", "404")):
                            logger.warning(f"Iceberg overwrite on '{quoted_iceberg_table}' failed due to missing S3 file ({ow_err}). Recreating...")
                            if glue_client:
                                try:
                                    glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                                except Exception:
                                    pass
                            if s3_client and silver_location:
                                purge_s3_table_location(s3_client, silver_location)
                            temp_view = f"init_{target_table_name.replace('.', '_').replace('-', '_')}"
                            df_transformed.createOrReplaceTempView(temp_view)
                            spark.sql(f"""
                                CREATE TABLE IF NOT EXISTS {quoted_iceberg_table}
                                USING iceberg
                                LOCATION '{silver_location}'
                                AS SELECT * FROM {temp_view}
                            """)
                            table_is_new = True
                        else:
                            raise
            else:
                table_exists = check_iceberg_table_exists(spark, glue_client, glue_database, target_table_name, s3_client=s3_client, silver_location=silver_location)
                if not table_exists:
                    logger.info(f"Target Iceberg table '{quoted_iceberg_table}' does not exist. Initializing table via Spark SQL CTAS...")
                    temp_view = f"init_{target_table_name.replace('.', '_').replace('-', '_')}"
                    df_transformed.createOrReplaceTempView(temp_view)
                    try:
                        spark.sql(f"""
                            CREATE TABLE IF NOT EXISTS {quoted_iceberg_table}
                            USING iceberg
                            LOCATION '{silver_location}'
                            AS SELECT * FROM {temp_view}
                        """)
                    except Exception as ctas_err:
                        logger.warning(f"Spark SQL CTAS failed ({ctas_err}), falling back to DataFrameWriter...")
                        df_transformed.write \
                            .format("iceberg") \
                            .mode("append") \
                            .option("path", silver_location) \
                            .saveAsTable(quoted_iceberg_table)
                    table_is_new = True
                else:
                    schema_evolved = sync_iceberg_table_schema(spark, quoted_iceberg_table, df_transformed)
                    try:
                        df_transformed.write \
                            .format("iceberg") \
                            .mode("append") \
                            .option("path", silver_location) \
                            .saveAsTable(quoted_iceberg_table)
                    except Exception as app_err:
                        err_str = str(app_err)
                        if any(x in err_str for x in ("NoSuchKey", "The specified key does not exist", "NoSuchKeyException", "NotFoundException", "404")):
                            logger.warning(f"Iceberg append on '{quoted_iceberg_table}' failed due to missing S3 file ({app_err}). Recreating...")
                            if glue_client:
                                try:
                                    glue_client.delete_table(DatabaseName=glue_database, Name=target_table_name)
                                except Exception:
                                    pass
                            if s3_client and silver_location:
                                purge_s3_table_location(s3_client, silver_location)
                            temp_view = f"init_{target_table_name.replace('.', '_').replace('-', '_')}"
                            df_transformed.createOrReplaceTempView(temp_view)
                            spark.sql(f"""
                                CREATE TABLE IF NOT EXISTS {quoted_iceberg_table}
                                USING iceberg
                                LOCATION '{silver_location}'
                                AS SELECT * FROM {temp_view}
                            """)
                            table_is_new = True
                        else:
                            raise

            if table_is_new:
                tables_created += 1
            if schema_evolved:
                schemas_evolved += 1

            # Determine new watermark timestamp from processed Bronze records
            if watermark_column in df_bronze.columns:
                max_val = df_bronze.select(spark_max(col(watermark_column))).collect()[0][0]
                new_watermark = str(max_val) if max_val is not None else current_run_time
            else:
                new_watermark = current_run_time

            # Update High-Water Mark state in S3 (table_name='tbl_<base_table_name>')
            if watermark_enabled:
                logger.info(
                    f"\n================================================================================\n"
                    f"[SILVER STEP 6/6] Updating Watermark State & Catalog for '{target_table_name}'\n"
                    f"--------------------------------------------------------------------------------"
                )
                update_silver_watermark(
                    s3_client=s3_client,
                    bucket=bucket_name,
                    state_key=state_key,
                    source_system=source_system,
                    table_name=target_table_name,
                    new_watermark=new_watermark,
                    total_records=batch_count,
                    current_run_time=current_run_time
                )

                if sync_watermark_table:
                    sync_silver_watermark_catalog_table(
                        glue_client=glue_client,
                        database_name=glue_database,
                        watermark_table_name=watermark_table_name,
                        bucket=bucket_name,
                        metadata_prefix=metadata_prefix,
                        s3_client=s3_client
                    )

            table_duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()
            table_end_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            table_stats.append({
                "table_name": target_table_name,
                "status": "SUCCESS",
                "scd_type": scd_type.upper(),
                "merge_strategy": merge_strategy.upper(),
                "duration_seconds": round(table_duration, 2),
                "records_processed": batch_count,
                "error_message": None
            })

            summary_card = (
                f"[TABLE SUMMARY] {bronze_table_name} -> {target_table_name} | SUCCESS | SCD: {scd_type.upper()} | Merge: {merge_strategy.upper()} | Duration: {table_duration:.2f}s\n"
                "+================================================================================+\n"
                f"|  SILVER TABLE COMPLETED: {target_table_name} [SUCCESS]\n"
                "+--------------------------------------------------------------------------------+\n"
                f"|  * Source System    : {source_system}\n"
                f"|  * Bronze Table     : {glue_database}.{bronze_table_name}\n"
                f"|  * Target Silver    : {silver_table_name}\n"
                f"|  * SCD Type         : {scd_type.upper()}\n"
                f"|  * Merge Strategy   : {merge_strategy.upper()}\n"
                f"|  * Natural Key Nkey : {nkeys}\n"
                f"|  * Order By Columns : {order_cols}\n"
                f"|  * Records Ingested : {batch_count}\n"
                f"|  * Last Watermark   : {last_load_date or ('INITIAL_FULL_LOAD' if watermark_enabled else 'DISABLED')}\n"
                f"|  * New Watermark    : {new_watermark if watermark_enabled else 'DISABLED'}\n"
                f"|  * Watermark S3 Key : {state_key if watermark_enabled else 'DISABLED'}\n"
                f"|  * Table Duration   : {table_duration:.2f}s\n"
                f"|  * Start Time (UTC) : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
                f"|  * End Time (UTC)   : {table_end_time_str}\n"
                f"|  * Silver Location  : {silver_location}\n"
                f"+================================================================================+"
            )
            logger.info(summary_card)

        except Exception as err:
            table_duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()
            failed_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            tb = traceback.format_exc()
            table_stats.append({
                "table_name": target_table_name,
                "status": "FAILED",
                "scd_type": scd_type.upper(),
                "merge_strategy": merge_strategy.upper(),
                "duration_seconds": round(table_duration, 2),
                "records_processed": 0,
                "error_message": str(err)
            })
            failed_card = (
                f"\n+================================================================================+\n"
                f"|  ERROR DIAGNOSTIC CARD: SILVER TABLE FAILURE [FAILED]\n"
                f"+================================================================================+\n"
                f"|  * Execution Layer    : SILVER\n"
                f"|  * Failed Entity      : {target_table_name}\n"
                f"|  * Source System      : {source_system}\n"
                f"|  * Bronze Table       : {glue_database}.{bronze_table_name}\n"
                f"|  * Target Silver      : {silver_table_name}\n"
                f"|  * SCD / Merge Mode   : SCD: {scd_type.upper()} | Merge: {merge_strategy.upper()}\n"
                f"|  * S3 Silver Location : {silver_location}\n"
                f"|  * Table Duration     : {table_duration:.2f}s\n"
                f"|  * Failed At (UTC)    : {failed_time_str}\n"
                f"|  * Exception Type     : {err.__class__.__name__}\n"
                f"|  * Exception Message  : {str(err)}\n"
                f"+--------------------------------------------------------------------------------+\n"
                f"|  * Full Python / Spark Stack Trace:\n"
                f"{tb.strip()}\n"
                f"+================================================================================+"
            )
            logger.error(failed_card)
            failed_tables.append((target_table_name, str(err)))

    # Trigger Silver Iceberg Crawler if:
    # 1. Explicitly requested via --TRIGGER_CRAWLER / TRIGGER_CRAWLER = true
    # 2. Explicitly provided --CRAWLER_NAME parameter (and TRIGGER_CRAWLER is not explicitly false)
    # 3. Any new Iceberg table was created for the first time (tables_created > 0)
    # 4. Any table schema was evolved with new columns (schemas_evolved > 0)
    # Silver Iceberg tables natively register and evolve schemas in Glue Catalog via Spark SQL.
    # Crawlers should ONLY trigger if explicitly enabled (TRIGGER_CRAWLER = True) to prevent
    # standard crawlers from traversing /data and /metadata and creating duplicate junk tables.
    cli_trigger_crawler = params.get('TRIGGER_CRAWLER')
    cli_crawler_name = params.get('CRAWLER_NAME') or params.get('ARG_DICT', {}).get('CRAWLER_NAME') or params.get('ARG_DICT', {}).get('SILVER_CRAWLER_NAME')

    should_trigger_crawler = bool(cli_trigger_crawler) if cli_trigger_crawler is not None else False
    if not should_trigger_crawler:
        logger.info("Silver Iceberg Crawler trigger skipped (trigger_crawler is false/disabled; Iceberg tables commit directly to Glue Catalog).")

    if should_trigger_crawler:
        crawler_name = (
            cli_crawler_name
            or glue_database.replace('_db_', '-silver-iceberg-crawler-').replace('-db-', '-silver-iceberg-crawler-').replace('_', '-')
        )
        logger.info(
            f"Triggering Silver Iceberg Crawler '{crawler_name}' "
            f"(tables_created: {tables_created}, schemas_evolved: {schemas_evolved}, "
            f"cli_crawler: {cli_crawler_name}, trigger_param: {cli_trigger_crawler})..."
        )
        trigger_silver_iceberg_crawler(glue_client, crawler_name)

    job.commit()

    execution_end_utc = datetime.now(timezone.utc)
    total_job_duration = (execution_end_utc - execution_start_utc).total_seconds()
    overall_status = "FAILED" if failed_tables else "SUCCESS"

    breakdown_lines = []
    for t in table_stats:
        status_tag = "[OK]  " if t['status'] == 'SUCCESS' else ("[SKIP]" if t['status'] == 'SKIPPED_UP_TO_DATE' else "[FAIL]")
        records_str = f"Records: {t.get('records_processed', 0):>5}"
        breakdown_lines.append(
            f"|  {status_tag} {t['table_name']:<20} | SCD: {t['scd_type']:<5} | Merge: {t['merge_strategy']:<10} | {records_str} | Time: {t['duration_seconds']:>6.2f}s | Status: {t['status']}"
        )
        if t.get('error_message'):
            breakdown_lines.append(f"|         └── Error: {t['error_message']}")

    breakdown_str = "\n".join(breakdown_lines)

    overall_card = (
        f"[JOB REPORT] SILVER ICEBERG ETL | Status: {overall_status} | Tables: {len(table_list) - len(failed_tables)}/{len(table_list)} | Duration: {total_job_duration:.2f}s\n"
        "+================================================================================+\n"
        "|                  SILVER ICEBERG ETL FINAL EXECUTION REPORT                     |\n"
        "+================================================================================+\n"
        f"|  Job Name              : {job_name}\n"
        f"|  Source System         : {source_system.upper()}\n"
        f"|  Overall Job Status    : {overall_status}\n"
        f"|  Total Tables          : {len(table_list)} (Succeeded: {len(table_list) - len(failed_tables)}, Failed: {len(failed_tables)})\n"
        f"|  Glue Database         : {glue_database}\n"
        f"|  Start Time (UTC)      : {current_run_time}\n"
        f"|  End Time (UTC)        : {execution_end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        f"|  Total Job Duration    : {total_job_duration:.2f}s\n"
        "+--------------------------------------------------------------------------------+\n"
        "|  TABLE EXECUTION BREAKDOWN:\n"
        f"{breakdown_str}\n"
        "+--------------------------------------------------------------------------------+\n"
        "|  S3 PERSISTENCE LOCATIONS:\n"
        f"|  * Silver Tables Path  : s3://{bucket_name}/{silver_data_prefix}/{source_system}/\n"
        f"|  * Watermark State     : s3://{bucket_name}/{metadata_prefix}/{source_system}/\n"
        f"|  * Watermark Athena Tbl: {glue_database}.{watermark_table_name}\n"
        "+================================================================================+"
    )
    logger.info(overall_card)

    if failed_tables:
        err_details = "\n".join([f"  * Table '{t[0]}': {t[1]}" for t in failed_tables])
        err_summary = (
            f"Silver Iceberg ETL completed with failures in {len(failed_tables)} table(s):\n"
            f"{err_details}"
        )
        logger.error(err_summary)
        raise RuntimeError(err_summary)

    logger.info("All Silver Apache Iceberg ETL transformations completed successfully.")

    # Process Layer Chaining: When --PROCESS_LAYER=both, seamlessly execute Gold after successful Silver
    if process_layer == 'both':
        logger.info(
            f"\n+================================================================================+\n"
            f"|  CHAINING EXECUTION: SILVER SUCCEEDED -> NOW RUNNING GOLD SERVING ENGINE       |\n"
            f"+================================================================================+"
        )
        GoldLayerManager.run_gold_pipeline(
            spark=spark,
            params=params,
            glue_client=glue_client,
            s3_client=s3_client
        )
        logger.info("All Gold serving layer operations completed successfully.")


if __name__ == "__main__":
    main()
