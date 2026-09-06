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
from datetime import datetime, timezone
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, coalesce, lit, current_timestamp, cast

# Ensure script directory is on sys.path for SilverConfigLoader & SilverTransformer imports
script_dir = os.path.dirname(os.path.abspath(__file__))
for path in [script_dir, os.getcwd(), "/tmp/extraPython"]:
    if os.path.exists(path) and path not in sys.path:
        sys.path.insert(0, path)

from silver_config_loader import SilverConfigLoader
from transformer import SilverTransformer


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
    defaults_cfg = silver_full_config.get('silver_defaults', {})

    source_system = get_cli_arg('SOURCE_SYSTEM', 'source_system')
    if not source_system:
        logger.error("Missing required parameter '--SOURCE_SYSTEM'. Example: --SOURCE_SYSTEM servicenow")
        raise ValueError("Missing required parameter '--SOURCE_SYSTEM'.")

    source_system_clean = source_system.strip().lower()

    # Data Lake Bucket: CLI > Config > Env > Default
    data_lake_bucket = get_cli_arg(
        'DATA_LAKE_BUCKET', 'data_lake_bucket',
        'BRONZE_BUCKET', 'bronze_bucket',
        default=defaults_cfg.get('data_lake_bucket') or os.environ.get('DATA_LAKE_BUCKET', 'uax-datalake-dev-bucket')
    )

    # Glue Database: CLI > Config > Default
    glue_database = get_cli_arg(
        'GLUE_DATABASE', 'glue_database',
        default=defaults_cfg.get('glue_database', 'uax-datalake-db-dev')
    )

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

    # Resolve dynamic table list: CLI overrides config default tables
    raw_tables = get_cli_arg('TABLE_NAME', 'table_name', 'TABLES', 'tables', 'TABLE_NAMES', 'table_names')
    if raw_tables:
        table_list = [t.strip() for t in raw_tables.split(',') if t.strip()]
        logger.info(f"Using CLI parameter table override: {table_list}")
    else:
        table_list = SilverConfigLoader.get_default_tables(source_system_clean, silver_full_config)
        if not table_list:
            table_list = ['incident']
        logger.info(f"Using config default tables: {table_list}")

    job_name = get_cli_arg('JOB_NAME', 'job_name', default=f"glue-silver-iceberg-etl-{source_system_clean}")

    return {
        'JOB_NAME': job_name,
        'SOURCE_SYSTEM': source_system_clean,
        'TABLE_LIST': table_list,
        'DATA_LAKE_BUCKET': data_lake_bucket,
        'GLUE_DATABASE': glue_database,
        'BRONZE_DATA_PREFIX': bronze_data_prefix,
        'SILVER_DATA_PREFIX': silver_data_prefix,
        'SILVER_FULL_CONFIG': silver_full_config,
        'ARG_DICT': arg_dict
    }


def perform_deduplication(df, pk_keys, order_cols, strategy='latest_by_order_column'):
    """
    Performs windowed deduplication supporting single or composite primary keys and multi-column ordering.
    """
    pk_cols = [col(k) for k in (pk_keys if isinstance(pk_keys, list) else [pk_keys])]
    order_col_list = order_cols if isinstance(order_cols, list) else [order_cols]

    if strategy == 'earliest_by_order_column':
        order_directions = [col(c).asc() for c in order_col_list]
    else:
        order_directions = [col(c).desc() for c in order_col_list]

    window_spec = Window.partitionBy(*pk_cols).orderBy(*order_directions)
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")


def execute_iceberg_scd1_upsert(spark, df, silver_table_name, silver_location, pk_keys):
    """
    Executes SCD Type 1 (UPSERT via Spark SQL MERGE INTO) on Apache Iceberg table.
    Overwrites modified records to maintain current state.
    """
    temp_view = f"incoming_batch_{silver_table_name.replace('.', '_')}"
    df.createOrReplaceTempView(temp_view)

    table_exists = spark.catalog.tableExists(silver_table_name)

    if not table_exists:
        logger.info(f"Target Iceberg table '{silver_table_name}' does not exist. Creating table...")
        df.write \
          .format("iceberg") \
          .mode("append") \
          .option("path", silver_location) \
          .saveAsTable(silver_table_name)
    else:
        logger.info(f"Executing SCD Type 1 MERGE INTO (UPSERT) on '{silver_table_name}'...")
        if isinstance(pk_keys, list):
            join_condition = " AND ".join([f"target.{k} = source.{k}" for k in pk_keys])
        else:
            join_condition = f"target.{pk_keys} = source.{pk_keys}"

        merge_sql = f"""
        MERGE INTO {silver_table_name} AS target
        USING {temp_view} AS source
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        logger.info(f"Running Spark SQL MERGE INTO Query:\n{merge_sql}")
        spark.sql(merge_sql)


def execute_iceberg_scd2(spark, df, silver_table_name, silver_location, pk_keys, order_cols, scd2_cfg):
    """
    Executes SCD Type 2 (Slowly Changing Dimension Type 2) on Apache Iceberg table.
    Tracks historical change history with _valid_from, _valid_to, and _is_current.
    """
    valid_from_col = scd2_cfg.get('valid_from_column', '_valid_from')
    valid_to_col = scd2_cfg.get('valid_to_column', '_valid_to')
    is_current_col = scd2_cfg.get('is_current_column', '_is_current')

    order_col_name = order_cols[0] if isinstance(order_cols, list) else order_cols

    # Enrich incoming batch with SCD Type 2 tracking columns
    incoming_df = df.withColumn(valid_from_col, col(order_col_name)) \
                    .withColumn(valid_to_col, lit(None).cast("timestamp")) \
                    .withColumn(is_current_col, lit(True))

    table_exists = spark.catalog.tableExists(silver_table_name)

    if not table_exists:
        logger.info(f"SCD Type 2: Target table '{silver_table_name}' does not exist. Creating initial table...")
        incoming_df.write \
                   .format("iceberg") \
                   .mode("append") \
                   .option("path", silver_location) \
                   .saveAsTable(silver_table_name)
    else:
        logger.info(f"SCD Type 2: Expiring matching target rows and inserting new versions for '{silver_table_name}'...")
        temp_view = f"scd2_incoming_{silver_table_name.replace('.', '_')}"
        incoming_df.createOrReplaceTempView(temp_view)

        if isinstance(pk_keys, list):
            join_condition = " AND ".join([f"target.{k} = source.{k}" for k in pk_keys])
        else:
            join_condition = f"target.{pk_keys} = source.{pk_keys}"

        # 1. Expire existing active target records matching incoming primary keys
        expire_sql = f"""
        MERGE INTO {silver_table_name} AS target
        USING {temp_view} AS source
        ON {join_condition} AND target.{is_current_col} = true
        WHEN MATCHED THEN UPDATE SET
          target.{is_current_col} = false,
          target.{valid_to_col} = source.{valid_from_col}
        """
        logger.info(f"Executing SCD Type 2 Target Expiration MERGE INTO:\n{expire_sql}")
        spark.sql(expire_sql)

        # 2. Append new incoming records as current active versions
        logger.info(f"Appending new active SCD Type 2 versions to '{silver_table_name}'...")
        incoming_df.write \
                   .format("iceberg") \
                   .mode("append") \
                   .option("path", silver_location) \
                   .saveAsTable(silver_table_name)


def main():
    params = parse_spark_arguments()
    job_name = params['JOB_NAME']
    source_system = params['SOURCE_SYSTEM']
    table_list = params['TABLE_LIST']
    bucket_name = params['DATA_LAKE_BUCKET']
    glue_database = params['GLUE_DATABASE']
    bronze_data_prefix = params.get('BRONZE_DATA_PREFIX', 'bronze/data')
    silver_data_prefix = params.get('SILVER_DATA_PREFIX', 'silver/data')
    silver_full_config = params['SILVER_FULL_CONFIG']

    # Initialize Spark & Glue Contexts configured for Apache Iceberg
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, params['ARG_DICT'])

    execution_start_utc = datetime.now(timezone.utc)
    current_run_time = execution_start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

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
        f"|  Bronze Data Prefix : {bronze_data_prefix:<57}|\n"
        f"|  Silver Data Prefix : {silver_data_prefix:<57}|\n"
        f"|  Start Time (UTC)   : {current_run_time:<57}|\n"
        "+================================================================================+"
    )
    logger.info(start_banner)

    failed_tables = []
    table_stats = []

    for table_idx, table_name in enumerate(table_list, start=1):
        table_clean = table_name.strip().lower()
        table_start_time = datetime.now(timezone.utc)

        bronze_path = f"s3://{bucket_name}/{bronze_data_prefix}/{source_system}/{table_clean}/"
        silver_table_name = f"{glue_database}.silver_{source_system}_{table_clean}"
        silver_location = f"s3://{bucket_name}/{silver_data_prefix}/{source_system}/{table_clean}/"

        table_header = (
            f"[TABLE START] {table_clean} [{table_idx}/{len(table_list)}] | Source: {source_system} | Database: {glue_database}\n"
            "+--------------------------------------------------------------------------------+\n"
            f"| >>> [{table_idx}/{len(table_list)}] SILVER PROCESSING: {table_clean.upper()} (Source: {source_system})\n"
            f"|     Target Table       : {silver_table_name}\n"
            f"|     Silver Location    : {silver_location}\n"
            f"|     Table Start (UTC)  : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            "+--------------------------------------------------------------------------------+"
        )
        logger.info(table_header)

        table_cfg = SilverConfigLoader.get_table_config(source_system, table_clean, silver_full_config)
        defaults_cfg = silver_full_config.get('silver_defaults', {})
        scd2_cfg = defaults_cfg.get('scd_type2_config', {})

        # Resolve Merge Strategy, SCD Type & Deduplication settings
        scd_type = (table_cfg.get('scd_type') or defaults_cfg.get('scd_type', 'scd1')).lower()
        merge_strategy = (table_cfg.get('merge_strategy') or defaults_cfg.get('merge_strategy', 'upsert')).lower()
        dedup_strategy = table_cfg.get('deduplication_strategy') or defaults_cfg.get('deduplication', {}).get('strategy', 'latest_by_order_column')

        try:
            logger.info(f"Reading raw Bronze data from: '{bronze_path}'")

            try:
                df_bronze = spark.read.option("mergeSchema", "true").parquet(bronze_path)
            except Exception as read_err:
                logger.warning(f"Could not read Parquet at '{bronze_path}' ({read_err}). Trying JSON or legacy path...")
                try:
                    df_bronze = spark.read.json(bronze_path)
                except Exception:
                    # Fallback check for legacy bronze path without /data/
                    legacy_bronze_path = f"s3://{bucket_name}/bronze/{source_system}/{table_clean}/"
                    logger.info(f"Checking legacy bronze path: '{legacy_bronze_path}'")
                    try:
                        df_bronze = spark.read.option("mergeSchema", "true").parquet(legacy_bronze_path)
                    except Exception:
                        df_bronze = spark.read.json(legacy_bronze_path)

            # Allow CLI Arguments to dynamically override silver_config.json settings for manual testing / Step Functions
            cli_args = params.get('ARG_DICT', {})
            merge_strategy = (cli_args.get('MERGE_STRATEGY') or table_cfg.get('merge_strategy') or defaults_cfg.get('merge_strategy', 'upsert')).lower()
            scd_type = (cli_args.get('SCD_TYPE') or table_cfg.get('scd_type') or defaults_cfg.get('scd_type', 'scd1')).lower()
            scd2_cfg = table_cfg.get('scd2_config') or defaults_cfg.get('scd2_defaults', {})
            dedup_strategy = (cli_args.get('DEDUPLICATION_STRATEGY') or table_cfg.get('deduplication_strategy') or defaults_cfg.get('deduplication', {}).get('strategy', 'latest_by_order_column')).lower()

            if df_bronze.rdd.isEmpty():
                legacy_bronze_path = f"s3://{bucket_name}/bronze/{source_system}/{table_clean}/"
                try:
                    df_legacy = spark.read.option("mergeSchema", "true").parquet(legacy_bronze_path)
                    if not df_legacy.rdd.isEmpty():
                        logger.info(f"Found records in legacy Bronze path: '{legacy_bronze_path}'")
                        df_bronze = df_legacy
                    else:
                        logger.warning(f"No records found in Bronze layer at '{bronze_path}'. Skipping Silver table write.")
                        continue
                except Exception:
                    logger.warning(f"No records found in Bronze layer at '{bronze_path}'. Skipping Silver table write.")
                    continue

            columns = df_bronze.columns

            # Dynamically resolve Deduplication Keys from CLI override -> config (fallback to primary_key)
            cli_pk = cli_args.get('DEDUPLICATION_KEYS') or cli_args.get('PRIMARY_KEY')
            if cli_pk:
                pk_keys = [k.strip() for k in cli_pk.split(',')] if ',' in cli_pk else cli_pk.strip()
                logger.info(f"CLI Parameter Override: 'deduplication_keys' -> {pk_keys}")
            else:
                pk_keys = table_cfg.get('deduplication_keys') or table_cfg.get('primary_key') or ("sys_id" if "sys_id" in columns else ("id" if "id" in columns else columns[0]))
            
            # Dynamically resolve Deduplication Order-By Columns from CLI override -> config
            cli_order = cli_args.get('DEDUPLICATION_ORDER_BY') or cli_args.get('ORDER_BY')
            if cli_order:
                order_cols = [c.strip() for c in cli_order.split(',')] if ',' in cli_order else cli_order.strip()
                logger.info(f"CLI Parameter Override: 'deduplication_order_by' -> {order_cols}")
            else:
                order_cols = table_cfg.get('deduplication_order_by') or table_cfg.get('order_by') or ("_ingested_at" if "_ingested_at" in columns else ("sys_updated_on" if "sys_updated_on" in columns else columns[0]))

            logger.info(f"Deduplicating table '{table_clean}': PK={pk_keys}, OrderBy={order_cols}, Strategy='{dedup_strategy}'")

            # Perform Deduplication
            df_dedup = perform_deduplication(df_bronze, pk_keys, order_cols, dedup_strategy)

            # Apply Declarative and Custom File Transformations
            df_transformed = SilverTransformer.apply_transformations(
                df=df_dedup,
                source_system=source_system,
                table_name=table_clean,
                table_cfg=table_cfg,
                spark=spark
            )

            logger.info(f"Target Iceberg Table: '{silver_table_name}', SCD Type: '{scd_type.upper()}', Merge Strategy: '{merge_strategy.upper()}'")

            # Execute SCD Type 2 or SCD Type 1 / Append / Overwrite
            if scd_type == 'scd2':
                execute_iceberg_scd2(spark, df_transformed, silver_table_name, silver_location, pk_keys, order_cols, scd2_cfg)
            elif merge_strategy in ('upsert', 'merge_into'):
                execute_iceberg_scd1_upsert(spark, df_transformed, silver_table_name, silver_location, pk_keys)
            elif merge_strategy == 'overwrite':
                df_transformed.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("path", silver_location) \
                    .saveAsTable(silver_table_name)
            else:
                df_transformed.write \
                    .format("iceberg") \
                    .mode("append") \
                    .option("path", silver_location) \
                    .saveAsTable(silver_table_name)

            table_duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()
            table_end_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            table_stats.append({
                "table_name": table_clean,
                "status": "SUCCESS",
                "scd_type": scd_type.upper(),
                "merge_strategy": merge_strategy.upper(),
                "duration_seconds": round(table_duration, 2),
                "error_message": None
            })

            summary_card = (
                f"[TABLE SUMMARY] {table_clean} | SUCCESS | SCD: {scd_type.upper()} | Merge: {merge_strategy.upper()} | Duration: {table_duration:.2f}s\n"
                "+================================================================================+\n"
                f"|  SILVER TABLE COMPLETED: {table_clean} [SUCCESS]\n"
                "+--------------------------------------------------------------------------------+\n"
                f"|  * Source System    : {source_system}\n"
                f"|  * Table Name       : {table_clean}\n"
                f"|  * Target Iceberg   : {silver_table_name}\n"
                f"|  * SCD Type         : {scd_type.upper()}\n"
                f"|  * Merge Strategy   : {merge_strategy.upper()}\n"
                f"|  * Deduplication PK : {pk_keys}\n"
                f"|  * Order By Columns : {order_cols}\n"
                f"|  * Table Duration   : {table_duration:.2f}s\n"
                f"|  * Start Time (UTC) : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
                f"|  * End Time (UTC)   : {table_end_time_str}\n"
                f"|  * Silver Location  : {silver_location}\n"
                "+================================================================================+"
            )
            logger.info(summary_card)

        except Exception as err:
            table_duration = (datetime.now(timezone.utc) - table_start_time).total_seconds()
            failed_time_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            table_stats.append({
                "table_name": table_clean,
                "status": "FAILED",
                "scd_type": scd_type.upper(),
                "merge_strategy": merge_strategy.upper(),
                "duration_seconds": round(table_duration, 2),
                "error_message": str(err)
            })
            failed_card = (
                f"[TABLE FAILED] {table_clean} | FAILED | Duration: {table_duration:.2f}s | Error: {str(err)[:60]}\n"
                "+================================================================================+\n"
                f"|  SILVER TABLE FAILED: {table_clean} [FAILED]\n"
                "+--------------------------------------------------------------------------------+\n"
                f"|  * Source System    : {source_system}\n"
                f"|  * Table Name       : {table_clean}\n"
                f"|  * Status           : FAILED\n"
                f"|  * Error Details    : {err}\n"
                f"|  * Table Duration   : {table_duration:.2f}s\n"
                f"|  * Start Time (UTC) : {table_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
                f"|  * Failed At (UTC)  : {failed_time_str}\n"
                "+================================================================================+"
            )
            logger.error(failed_card)
            failed_tables.append((table_clean, str(err)))

    job.commit()

    execution_end_utc = datetime.now(timezone.utc)
    total_job_duration = (execution_end_utc - execution_start_utc).total_seconds()
    overall_status = "FAILED" if failed_tables else "SUCCESS"

    breakdown_lines = []
    for t in table_stats:
        status_tag = "[OK]  " if t['status'] == 'SUCCESS' else "[FAIL]"
        breakdown_lines.append(
            f"|  {status_tag} {t['table_name']:<20} | SCD: {t['scd_type']:<5} | Merge: {t['merge_strategy']:<10} | Time: {t['duration_seconds']:>6.2f}s | Status: {t['status']}"
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
        "+================================================================================+"
    )
    logger.info(overall_card)

    if failed_tables:
        err_summary = f"Silver Iceberg ETL completed with failures in {len(failed_tables)} table(s): {[t[0] for t in failed_tables]}"
        logger.error(err_summary)
        raise RuntimeError(err_summary)

    logger.info("All Silver Apache Iceberg ETL transformations completed successfully.")


if __name__ == "__main__":
    main()
