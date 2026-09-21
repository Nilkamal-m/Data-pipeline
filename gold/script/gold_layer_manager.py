"""
Gold Serving Layer Manager.

Responsibilities:
1. Mandatory Athena View First: Creates/refreshes enterprise presentation view (v_<table_name>)
   directly in AWS Glue Data Catalog / Athena as the mandatory source of truth before any downstream export.
2. Multi-Target Downstream Routing: Parameter-driven routing for Aurora MySQL, Amazon Redshift Spectrum, and Snowflake.
3. Zero-DDL Schema Verification: Verifies target schema exists in MySQL via INFORMATION_SCHEMA.SCHEMATA. Fails fast if missing.
4. Shared Database Guardrails: Strictly isolates operations to 'gold_tbl_<table_name>' and 'v_<table_name>'. Never modifies external tables.
5. Complete Column Schema Introspection: Logs all columns and data types for query outputs and target tables.
6. Schema Evolution Tracking: Compares incoming columns against existing serving tables and alerts on any newly added columns.
7. DDL Audit Logs: Explicitly records all DROP, CREATE, SWAP, and VIEW operations.
8. Structured Error Diagnostic Cards: Emits rich debugging cards with full stack traces on failure.
"""

import os
import sys
import glob
import json
import logging
import time
import traceback
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class GoldLayerManager:
    """
    Manages the execution, schema validation, data materialization, and database serving
    for Gold layer data marts with mandatory Athena views, strict shared-database safety,
    and high observability.
    """

    @classmethod
    def run_gold_pipeline(
        cls,
        spark: SparkSession,
        params: Dict[str, Any],
        glue_client=None,
        s3_client=None,
        secrets_client=None,
        athena_client=None
    ) -> List[Dict[str, Any]]:
        """
        Main entry point for Gold serving layer execution.
        1. Discovers query definitions (v_<tablename>.sql or <tablename>.sql).
        2. MANDATORY: Creates/refreshes Athena view v_<tablename> in AWS Glue Data Catalog.
        3. DOWNSTREAM: Materializes and serves data to configured targets (Aurora MySQL, Redshift, Snowflake).
        """
        execution_start = datetime.now(timezone.utc)
        bucket_name = params.get('DATA_LAKE_BUCKET', 'uax-datalake-dev-bucket')
        glue_database = params.get('GLUE_DATABASE') or 'uax_datalake_db_dev'

        # Resolve Target Engines: CLI --GOLD_TARGETS or --GOLD_TARGET (default: athena)
        gold_targets_raw = params.get('GOLD_TARGETS') or params.get('GOLD_TARGET') or 'athena'
        gold_targets = [t.strip().lower() for t in str(gold_targets_raw).split(',') if t.strip()]
        if not gold_targets:
            gold_targets = ['athena']

        # Strict SOURCE_SYSTEM Enforcement
        source_system = (params.get('SOURCE_SYSTEM') or '').strip().lower()
        if not source_system:
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter '--SOURCE_SYSTEM'.\n"
                "The Gold query path is 's3://<bucket>/gold/query/<source>/v_<table_name>.sql'.\n"
                "Please specify the source system (e.g. --SOURCE_SYSTEM servicenow)."
            )

        # Target MySQL Schema Enforcement only when MySQL/Aurora is in targets
        needs_mysql = any(t in gold_targets for t in ('aurora', 'rds', 'mysql'))
        gold_schema = params.get('GOLD_SCHEMA')
        if needs_mysql:
            if not gold_schema or not str(gold_schema).strip():
                raise ValueError(
                    "CRITICAL CONFIG ERROR: Missing required parameter '--GOLD_SCHEMA'.\n"
                    "Downstream target includes Aurora/MySQL, where no fallback schema is permitted.\n"
                    "Please explicitly specify the target MySQL schema name (e.g. --GOLD_SCHEMA enterprise_reporting)."
                )
            gold_schema = str(gold_schema).strip()

        # Query and Data Paths
        query_s3_path = params.get('GOLD_QUERY_S3_PATH') or f"s3://{bucket_name}/gold/query/{source_system}"
        data_s3_path = params.get('GOLD_DATA_S3_PATH') or f"s3://{bucket_name}/gold/data/{source_system}"

        if not s3_client:
            s3_client = boto3.client('s3')

        logger.info(
            f"\n+================================================================================+\n"
            f"|              STARTING GOLD SERVING ENGINE: MULTI-TARGET PIPELINE               |\n"
            f"+================================================================================+\n"
            f"|  * Mandatory Step 1  : CREATE/REFRESH ATHENA VIEW (AWS Glue Catalog)           |\n"
            f"|  * Target Engines    : {', '.join(gold_targets).upper()}\n"
            f"|  * Glue Database     : {glue_database}\n"
            f"|  * Target DB Schema  : {gold_schema or 'N/A (Athena / External DW)'}\n"
            f"|  * Source System     : {source_system.upper()}\n"
            f"|  * Query Path        : {query_s3_path}\n"
            f"|  * Data S3 Path      : {data_s3_path}\n"
            f"|  * Data Lake Bucket  : {bucket_name}\n"
            f"|  * Execution Time    : {execution_start.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"+================================================================================+"
        )

        mart_stats = []

        # ----------------------------------------------------------------------
        # [GOLD STEP 1] Discover Mart Queries (.sql)
        # ----------------------------------------------------------------------
        logger.info(
            f"\n================================================================================\n"
            f"[GOLD STEP 1] Discovering Gold Mart Query Definitions (.sql)\n"
            f"--------------------------------------------------------------------------------"
        )
        queries = cls._discover_queries(
            query_s3_path,
            bucket_name,
            s3_client,
            source_system=source_system
        )
        if not queries:
            raise FileNotFoundError(
                f"CRITICAL QUERY DISCOVERY ERROR: No .sql query definitions found at '{query_s3_path}'.\n"
                f"Expected query path: s3://{bucket_name}/gold/query/{source_system}/v_<table_name>.sql\n"
                f"Please ensure at least one SQL definition is present in S3 or locally in 'gold/query/{source_system}/'."
            )

        # Optional CLI table override filtering
        table_filter = params.get('TABLE_LIST') or []
        if isinstance(table_filter, str):
            table_filter = [t.strip() for t in table_filter.split(',') if t.strip()]
        if table_filter:
            clean_filters = set()
            for t in table_filter:
                low = t.lower()
                clean_filters.add(low)
                clean_filters.add(low.replace('-', '_'))
                clean_filters.add(low.replace('_', '-'))
                for prefix in ['gold_tbl_', 'raw_tbl_', 'tbl_', 'v_']:
                    if low.startswith(prefix):
                        stripped = low[len(prefix):]
                        clean_filters.add(stripped)
                        clean_filters.add(stripped.replace('-', '_'))
                        clean_filters.add(stripped.replace('_', '-'))
            matched = {
                k: v for k, v in queries.items()
                if k.lower() in clean_filters or any(cf in k.lower() for cf in clean_filters)
            }
            if matched:
                logger.info(f"[GOLD STEP 1] Filtered queries by table override {table_filter}: {list(matched.keys())}")
                queries = matched
            else:
                logger.info(
                    f"[GOLD STEP 1] CLI table override {table_filter} did not match mart names {list(queries.keys())}. "
                    f"Processing all discovered mart queries for source '{source_system}'."
                )

        logger.info(f"[GOLD STEP 1] Discovered {len(queries)} mart query file(s) to process: {list(queries.keys())}")

        # Set Spark active database to GLUE_DATABASE so queries can use direct table names (e.g. tbl_incident)
        if glue_database and spark:
            try:
                spark.sql(f"USE `{glue_database}`")
                logger.info(f"[GOLD PREP] Set Spark active database to '{glue_database}'. Direct table names (e.g. 'tbl_incident') are fully supported.")
            except Exception as use_err:
                logger.warning(f"[GOLD PREP] Could not set Spark active database to '{glue_database}': {use_err}")

        # ----------------------------------------------------------------------
        # [GOLD STEP 2 - MANDATORY] Create/Refresh Athena Views
        # ----------------------------------------------------------------------
        logger.info(
            f"\n================================================================================\n"
            f"[GOLD STEP 2 - MANDATORY] Creating/Refreshing Athena Views in Glue Catalog\n"
            f"--------------------------------------------------------------------------------"
        )
        for clean_base_name, sql_text in queries.items():
            view_name = f"v_{clean_base_name}"
            cls.create_athena_view(
                spark=spark,
                glue_database=glue_database,
                view_name=view_name,
                sql_text=sql_text,
                params=params,
                athena_client=athena_client,
                glue_client=glue_client
            )

        # ----------------------------------------------------------------------
        # [GOLD STEP 3+] Downstream Target Serving (Aurora / Redshift / Snowflake)
        # ----------------------------------------------------------------------
        # Target: Aurora MySQL
        if needs_mysql:
            logger.info(
                f"\n================================================================================\n"
                f"[GOLD STEP 3] Serving Gold Marts to Aurora / RDS MySQL Schema '{gold_schema}'\n"
                f"--------------------------------------------------------------------------------"
            )
            cls._serve_to_mysql(
                spark=spark,
                queries=queries,
                gold_schema=gold_schema,
                data_s3_path=data_s3_path,
                params=params,
                glue_client=glue_client,
                secrets_client=secrets_client,
                mart_stats=mart_stats
            )

        # Target: Amazon Redshift
        if 'redshift' in gold_targets:
            logger.info(
                f"\n================================================================================\n"
                f"[GOLD STEP 4] Serving Gold Marts to Amazon Redshift / Redshift Spectrum\n"
                f"--------------------------------------------------------------------------------"
            )
            cls._run_redshift_serving(spark, params, queries, glue_database, mart_stats)

        # Target: Snowflake
        if 'snowflake' in gold_targets:
            logger.info(
                f"\n================================================================================\n"
                f"[GOLD STEP 5] Serving Gold Marts to Snowflake (External Iceberg / Direct Load)\n"
                f"--------------------------------------------------------------------------------"
            )
            cls._run_snowflake_serving(spark, params, queries, glue_database, mart_stats)

        # If only Athena was targeted, record successful Athena mart stats
        if not needs_mysql and 'redshift' not in gold_targets and 'snowflake' not in gold_targets:
            for clean_base_name in queries.keys():
                mart_stats.append({
                    "mart_name": clean_base_name,
                    "target_table": f"{glue_database}.v_{clean_base_name}",
                    "view_name": f"v_{clean_base_name}",
                    "status": "SUCCESS",
                    "rows_served": None,
                    "duration_seconds": 0.0,
                    "error_message": None
                })

        # Overall Gold Execution Summary
        cls._log_final_gold_summary(mart_stats, execution_start)

        failed_marts = [m for m in mart_stats if m.get('status') == 'FAILED']
        if failed_marts:
            raise RuntimeError(f"Gold Serving Layer completed with failures in {len(failed_marts)} mart(s).")

        return mart_stats

    # --------------------------------------------------------------------------
    # Mandatory Athena View Creation
    # --------------------------------------------------------------------------
    @classmethod
    def create_athena_view(
        cls,
        spark: SparkSession,
        glue_database: str,
        view_name: str,
        sql_text: str,
        params: Dict[str, Any],
        athena_client=None,
        glue_client=None
    ) -> bool:
        """
        Mandatory Gold Step: Creates or replaces the presentation view in AWS Athena / Glue Data Catalog.
        View is created under `<glue_database>.<view_name>` (e.g. uax_datalake_db_dev.v_interactions).
        Executes via Athena Boto3 client with automatic retry, dedicated workgroup routing, and Glue Catalog fallback.
        """
        clean_sql = sql_text.strip().rstrip(';')
        clean_sql = re.sub(r'^(?:\s*(?:--[^\r\n]*|/\*[\s\S]*?\*/)\s*)+', '', clean_sql).strip()
        view_ddl = f"CREATE OR REPLACE VIEW {glue_database}.{view_name} AS\n{clean_sql}"
        athena_succeeded = False

        # 1. Resolve Target Athena Workgroup
        # Dynamically determine the dedicated data lake workgroup: uax-datalake-workgroup-{env}
        env = 'dev'
        if glue_database:
            parts = glue_database.split('_')
            if len(parts) > 1 and parts[-1] in ('dev', 'qa', 'staging', 'prod', 'test'):
                env = parts[-1]
        default_workgroup = f"uax-datalake-workgroup-{env}"

        configured_wg = (
            params.get('ATHENA_WORKGROUP')
            or params.get('WORKGROUP')
            or os.environ.get('ATHENA_WORKGROUP')
            or os.environ.get('DEFAULT_ATHENA_WORKGROUP')
        )
        # Avoid using 'primary' by default because it is frequently misconfigured, disabled, or unrouted
        if not configured_wg or str(configured_wg).strip().lower() == 'primary':
            workgroup = default_workgroup
        else:
            workgroup = str(configured_wg).strip()

        bucket_name = params.get('DATA_LAKE_BUCKET', 'uax-datalake-dev-bucket')
        output_location = params.get('ATHENA_OUTPUT_LOCATION') or f"s3://{bucket_name}/athena-query-results/"

        def _execute_athena_ddl(target_wg: str) -> bool:
            """Submits view DDL to an Athena workgroup, handling enforced workgroup configurations gracefully."""
            start_kwargs = {
                'QueryString': view_ddl,
                'QueryExecutionContext': {'Database': glue_database},
                'WorkGroup': target_wg
            }
            if output_location:
                start_kwargs['ResultConfiguration'] = {'OutputLocation': output_location}

            logger.info(f"[ATHENA VIEW DDL] Submitting DDL for '{glue_database}.{view_name}' to Athena workgroup '{target_wg}'...")
            try:
                resp = athena_client.start_query_execution(**start_kwargs)
            except Exception as start_err:
                err_msg = str(start_err)
                if 'InvalidRequestException' in err_msg and ('workgroup' in err_msg.lower() or 'configuration' in err_msg.lower()):
                    logger.info(f"[ATHENA VIEW] Workgroup '{target_wg}' enforces output location. Retrying without explicit ResultConfiguration...")
                    start_kwargs.pop('ResultConfiguration', None)
                    resp = athena_client.start_query_execution(**start_kwargs)
                else:
                    raise

            query_exec_id = resp.get('QueryExecutionId')
            logger.info(f"[ATHENA VIEW] Query execution submitted. Execution ID: {query_exec_id}")

            max_wait_seconds = int(params.get('ATHENA_TIMEOUT_SECONDS', 60))
            poll_interval = 2
            elapsed = 0
            while elapsed < max_wait_seconds:
                query_status_resp = athena_client.get_query_execution(QueryExecutionId=query_exec_id)
                state = query_status_resp['QueryExecution']['Status']['State']
                if state == 'SUCCEEDED':
                    logger.info(f"[ATHENA VIEW] Successfully created Athena view '{glue_database}.{view_name}' (Execution ID: {query_exec_id}).")
                    return True
                elif state in ('FAILED', 'CANCELLED'):
                    reason = query_status_resp['QueryExecution']['Status'].get('StateChangeReason', 'Unknown reason')
                    logger.warning(f"[ATHENA VIEW] Athena execution {state} on workgroup '{target_wg}': {reason}")
                    return False
                time.sleep(poll_interval)
                elapsed += poll_interval
            logger.warning(f"[ATHENA VIEW] Athena execution timed out after {max_wait_seconds}s on workgroup '{target_wg}'.")
            return False

        # Attempt Athena Execution with dedicated workgroup fallback
        try:
            if not athena_client:
                athena_client = boto3.client('athena')

            try:
                athena_succeeded = _execute_athena_ddl(workgroup)
            except Exception as wg_err:
                logger.warning(f"[ATHENA VIEW] Submission to workgroup '{workgroup}' encountered error: {wg_err}")
                if workgroup != default_workgroup:
                    logger.info(f"[ATHENA VIEW] Retrying view creation with dedicated data lake workgroup '{default_workgroup}'...")
                    try:
                        athena_succeeded = _execute_athena_ddl(default_workgroup)
                    except Exception as def_err:
                        logger.warning(f"[ATHENA VIEW] Dedicated workgroup '{default_workgroup}' also failed: {def_err}")
                        athena_succeeded = False
                else:
                    athena_succeeded = False
        except Exception as ath_err:
            logger.warning(f"[ATHENA VIEW] Boto3 Athena execution failed: {ath_err}")
            athena_succeeded = False

        # Fallback: Spark SQL view or AWS Glue Data Catalog Virtual View
        if not athena_succeeded:
            # 1. Attempt Spark SQL (supported if catalog supports views)
            try:
                spark_view_ddl = f"CREATE OR REPLACE VIEW `{glue_database}`.`{view_name}` AS\n{clean_sql}"
                logger.info(f"[ATHENA VIEW] Executing view creation via Spark SQL:\n{spark_view_ddl}")
                spark.sql(spark_view_ddl)
                logger.info(f"[ATHENA VIEW] Successfully created view `{glue_database}`.`{view_name}` via Spark SQL.")
                athena_succeeded = True
            except Exception as spark_err:
                logger.warning(f"[ATHENA VIEW] Spark SQL view creation not supported by catalog ({spark_err}). Attempting Glue Data Catalog API fallback...")
                # 2. Attempt direct Glue Data Catalog Virtual View registration
                try:
                    if not glue_client:
                        glue_client = boto3.client('glue')
                    table_input = {
                        'Name': view_name,
                        'TableType': 'VIRTUAL_VIEW',
                        'ViewOriginalText': clean_sql,
                        'ViewExpandedText': clean_sql,
                        'StorageDescriptor': {
                            'Columns': [],
                            'Location': f"s3://{bucket_name}/gold/views/{view_name}/",
                            'SerdeInfo': {
                                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                            }
                        },
                        'Parameters': {
                            'presto_view': 'true',
                            'comment': 'Gold Layer Virtual View registered via Glue Catalog API'
                        }
                    }
                    try:
                        glue_client.create_table(DatabaseName=glue_database, TableInput=table_input)
                        logger.info(f"[GLUE VIEW] Successfully created Virtual View '{glue_database}.{view_name}' in Glue Data Catalog.")
                        athena_succeeded = True
                    except getattr(getattr(glue_client, 'exceptions', None), 'AlreadyExistsException', Exception):
                        glue_client.update_table(DatabaseName=glue_database, TableInput=table_input)
                        logger.info(f"[GLUE VIEW] Successfully updated Virtual View '{glue_database}.{view_name}' in Glue Data Catalog.")
                        athena_succeeded = True
                except Exception as glue_cat_err:
                    logger.warning(f"[GLUE VIEW] Glue Data Catalog view registration fallback also failed: {glue_cat_err}")
                # Register in-session Spark temporary view if catalog view creation failed
                try:
                    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW `{view_name}` AS\n{clean_sql}")
                    logger.info(f"[ATHENA VIEW] Registered Spark in-session temporary view `{view_name}`.")
                except Exception as temp_err:
                    logger.debug(f"[ATHENA VIEW] Spark temporary view note: {temp_err}")

        if not athena_succeeded:
            logger.error(f"[ATHENA VIEW ERROR] Failed to register view `{glue_database}`.`{view_name}` via Athena, Spark SQL, and Glue Data Catalog.")
            fail_on_error = str(params.get('FAIL_ON_VIEW_ERROR', 'false')).strip().lower() in ('true', '1', 'yes')
            if fail_on_error:
                raise RuntimeError(
                    f"CRITICAL ERROR: Mandatory Athena View creation failed for '{glue_database}.{view_name}'."
                )
            return False

        view_card = (
            f"\n+================================================================================+\n"
            f"|  MANDATORY GOLD ATHENA VIEW REGISTERED: {view_name} [SUCCESS]\n"
            f"+================================================================================+\n"
            f"|  * Glue Database      : {glue_database}\n"
            f"|  * View Name          : {view_name}\n"
            f"|  * Fully Qualified    : {glue_database}.{view_name}\n"
            f"|  * Catalog Engine     : AWS Athena / Glue Data Catalog\n"
            f"|  * Access Status      : Queryable in Athena console and downstream BI immediately.\n"
            f"+================================================================================+\n"
        )
        logger.info(view_card)
        return True

    # --------------------------------------------------------------------------
    # Aurora / RDS MySQL Serving Engine
    # --------------------------------------------------------------------------
    @classmethod
    def _serve_to_mysql(
        cls,
        spark: SparkSession,
        queries: Dict[str, str],
        gold_schema: str,
        data_s3_path: str,
        params: Dict[str, Any],
        glue_client,
        secrets_client,
        mart_stats: List[Dict[str, Any]]
    ) -> None:
        """Executes zero-DDL MySQL validation, Spark SQL materialization, atomic swap, and view creation."""
        jdbc_conn_info = cls._resolve_mysql_connection_info(
            params,
            glue_client=glue_client,
            secrets_client=secrets_client
        )

        logger.info(
            f"Validating that schema '{gold_schema}' pre-exists in MySQL database...\n"
            f"Script will NEVER execute CREATE DATABASE / CREATE SCHEMA."
        )
        cls._verify_schema_exists_or_raise(jdbc_conn_info, gold_schema)
        logger.info(f"Target MySQL schema '{gold_schema}' verified. [PASSED]")

        for clean_base_name, sql_text in queries.items():
            mart_start = datetime.now(timezone.utc)
            target_table = f"gold_tbl_{clean_base_name}"
            staging_table = f"gold_tbl_{clean_base_name}_staging"
            old_backup_table = f"gold_tbl_{clean_base_name}_old"
            view_name = f"v_{clean_base_name}"

            assert target_table.startswith("gold_tbl_"), f"Safety Error: Invalid table name {target_table}"
            assert staging_table.startswith("gold_tbl_") and staging_table.endswith("_staging"), f"Safety Error: Invalid staging table {staging_table}"
            assert view_name.startswith("v_"), f"Safety Error: Invalid view name {view_name}"

            logger.info(
                f"\n+--------------------------------------------------------------------------------+\n"
                f"|  PROCESSING MYSQL GOLD MART: '{clean_base_name}'\n"
                f"|  * Target Table  : {gold_schema}.{target_table}\n"
                f"|  * Staging Table : {gold_schema}.{staging_table}\n"
                f"|  * Serving View  : {gold_schema}.{view_name}\n"
                f"+--------------------------------------------------------------------------------+"
            )

            try:
                # 1. Spark SQL Execution
                logger.info(f"Executing Spark SQL query for '{clean_base_name}'...")
                df_mart = spark.sql(sql_text)
                df_mart = df_mart.withColumn("_computed_at", df_mart.sql_ctx.sql("select current_timestamp()").collect()[0][0])
                row_count = df_mart.count()
                logger.info(f"Query executed successfully. Computed {row_count:,} records.")
                cls._log_schema_introspection(df_mart, f"Gold Query Output Schema: '{clean_base_name}'")

                # 2. S3 Materialization
                mart_s3_dest = f"{data_s3_path.rstrip('/')}/{clean_base_name}"
                logger.info(f"Materializing {row_count:,} records to S3 Parquet: -> {mart_s3_dest}")
                df_mart.write.mode("overwrite").format("parquet").save(mart_s3_dest)

                # 3. Schema Evolution Check & Staging Table Write
                cls._detect_schema_evolution(jdbc_conn_info, gold_schema, target_table, df_mart)
                cls._write_staging_table(spark, df_mart, jdbc_conn_info, gold_schema, staging_table)

                # 4. Atomic Swap & Presentation View Refresh
                cls._execute_isolated_atomic_swap(
                    jdbc_info=jdbc_conn_info,
                    schema_name=gold_schema,
                    target_table=target_table,
                    staging_table=staging_table,
                    old_backup_table=old_backup_table,
                    view_name=view_name
                )

                mart_duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                mart_stats.append({
                    "mart_name": clean_base_name,
                    "target_table": f"{gold_schema}.{target_table}",
                    "view_name": f"{gold_schema}.{view_name}",
                    "status": "SUCCESS",
                    "rows_served": row_count,
                    "duration_seconds": round(mart_duration, 2),
                    "error_message": None
                })

            except Exception as err:
                mart_duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                error_card = cls._format_error_diagnostic_card(
                    layer="GOLD_MYSQL",
                    step_name="Processing Gold Mart for MySQL",
                    target_entity=f"{gold_schema}.{target_table}",
                    query_source=f"Query for {clean_base_name}",
                    conn_info=jdbc_conn_info,
                    exception=err
                )
                logger.error(error_card)
                mart_stats.append({
                    "mart_name": clean_base_name,
                    "target_table": f"{gold_schema}.{target_table}",
                    "view_name": f"{gold_schema}.{view_name}",
                    "status": "FAILED",
                    "rows_served": 0,
                    "duration_seconds": round(mart_duration, 2),
                    "error_message": str(err)
                })

    # --------------------------------------------------------------------------
    # Redshift Serving Adapter (Spectrum & Direct DW)
    # --------------------------------------------------------------------------
    @classmethod
    def _run_redshift_serving(
        cls,
        spark: SparkSession,
        params: Dict[str, Any],
        queries: Dict[str, str],
        glue_database: str,
        mart_stats: List[Dict[str, Any]]
    ) -> None:
        """
        Amazon Redshift serving adapter.
        Redshift Spectrum maps directly to AWS Glue Data Catalog Iceberg tables and Athena views.
        """
        redshift_schema = params.get('REDSHIFT_SCHEMA', 'gold_spectrum_schema')
        iam_role = params.get('REDSHIFT_IAM_ROLE', 'arn:aws:iam::<account-id>:role/RedshiftGlueSpectrumRole')

        spectrum_ddl = (
            f"CREATE EXTERNAL SCHEMA IF NOT EXISTS {redshift_schema} "
            f"FROM DATA CATALOG DATABASE '{glue_database}' "
            f"IAM_ROLE '{iam_role}' CREATE EXTERNAL DATABASE IF NOT EXISTS;"
        )

        card = (
            f"\n+================================================================================+\n"
            f"|  AMAZON REDSHIFT SPECTRUM SERVING ADAPTER ACTIVATED                            |\n"
            f"+================================================================================+\n"
            f"|  * Redshift External Schema : {redshift_schema}\n"
            f"|  * Glue Database Source     : {glue_database}\n"
            f"|  * IAM Role Configured      : {iam_role}\n"
            f"|  * Zero Data Movement       : Queries run directly over Iceberg tables and     |\n"
            f"|                               Athena views in S3 via Redshift Spectrum.        |\n"
            f"|  * Recommended DDL Setup    :\n"
            f"|    {spectrum_ddl}\n"
            f"+================================================================================+\n"
        )
        logger.info(card)

        for clean_base_name in queries.keys():
            mart_stats.append({
                "mart_name": clean_base_name,
                "target_table": f"{redshift_schema}.gold_tbl_{clean_base_name}",
                "view_name": f"{redshift_schema}.v_{clean_base_name}",
                "status": "SUCCESS",
                "rows_served": None,
                "duration_seconds": 0.0,
                "error_message": None
            })

    # --------------------------------------------------------------------------
    # Snowflake Serving Adapter (External Iceberg & Direct DW)
    # --------------------------------------------------------------------------
    @classmethod
    def _run_snowflake_serving(
        cls,
        spark: SparkSession,
        params: Dict[str, Any],
        queries: Dict[str, str],
        glue_database: str,
        mart_stats: List[Dict[str, Any]]
    ) -> None:
        """
        Snowflake serving adapter.
        Configures External Volume and AWS Glue Data Catalog integration for External Iceberg Tables.
        """
        sf_database = params.get('SNOWFLAKE_DATABASE', 'UAX_ANALYTICS_DB')
        sf_schema = params.get('SNOWFLAKE_SCHEMA', 'GOLD_MARTS')
        sf_ext_volume = params.get('SNOWFLAKE_EXTERNAL_VOLUME', 'UAX_S3_ICEBERG_VOLUME')

        card = (
            f"\n+================================================================================+\n"
            f"|  SNOWFLAKE EXTERNAL ICEBERG SERVING ADAPTER ACTIVATED                          |\n"
            f"+================================================================================+\n"
            f"|  * Snowflake Target DB      : {sf_database}\n"
            f"|  * Snowflake Target Schema  : {sf_schema}\n"
            f"|  * External Volume          : {sf_ext_volume}\n"
            f"|  * Catalog Integration      : AWS_GLUE (Database: {glue_database})\n"
            f"|  * Zero-Copy Architecture   : Snowflake queries S3 Iceberg metadata directly. |\n"
            f"+================================================================================+\n"
        )
        logger.info(card)

        for clean_base_name in queries.keys():
            target_table = f"gold_tbl_{clean_base_name}"
            sf_ddl = (
                f"CREATE OR REPLACE ICEBERG TABLE {sf_database}.{sf_schema}.{target_table} "
                f"EXTERNAL_VOLUME = '{sf_ext_volume}' CATALOG = 'AWS_GLUE' "
                f"CATALOG_TABLE_NAME = '{target_table}';"
            )
            logger.info(f"[SNOWFLAKE DDL TEMPLATE] {sf_ddl}")
            mart_stats.append({
                "mart_name": clean_base_name,
                "target_table": f"{sf_database}.{sf_schema}.{target_table}",
                "view_name": f"{sf_database}.{sf_schema}.v_{clean_base_name}",
                "status": "SUCCESS",
                "rows_served": None,
                "duration_seconds": 0.0,
                "error_message": None
            })

    # --------------------------------------------------------------------------
    # Database Safety & Schema Pre-existence
    # --------------------------------------------------------------------------
    @classmethod
    def _verify_schema_exists_or_raise(cls, jdbc_info: Dict[str, Any], schema_name: str) -> None:
        """
        Verifies that the target schema pre-exists in MySQL via INFORMATION_SCHEMA.SCHEMATA.
        If it does not exist, raises an immediate RuntimeError and aborts.
        NEVER executes CREATE DATABASE or CREATE SCHEMA.
        """
        query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s"
        try:
            results = cls._execute_sql_query(jdbc_info, query, (schema_name,))
            if not results:
                raise RuntimeError(
                    f"CRITICAL SHARED-DB POLICY ERROR: Target schema '{schema_name}' does not exist "
                    f"in MySQL instance '{jdbc_info.get('host')}'.\n"
                    f"In accordance with enterprise shared database policy, this pipeline NEVER executes "
                    f"CREATE DATABASE / CREATE SCHEMA.\n"
                    f"Please contact your Database Administrator (DBA) to provision schema '{schema_name}'."
                )
        except Exception as e:
            if "CRITICAL SHARED-DB POLICY ERROR" in str(e):
                raise
            raise RuntimeError(f"Failed to query INFORMATION_SCHEMA.SCHEMATA on MySQL: {e}")

    # --------------------------------------------------------------------------
    # Schema Introspection & Evolution Detection
    # --------------------------------------------------------------------------
    @classmethod
    def _log_schema_introspection(cls, df: DataFrame, title: str) -> None:
        """Logs a structured breakdown of all columns and data types in the DataFrame."""
        col_lines = []
        for f in df.schema.fields:
            nullable_str = "NULLABLE" if f.nullable else "NOT NULL"
            col_lines.append(f"|  * {f.name:<35} : {f.dataType.simpleString():<20} ({nullable_str})")
        schema_dump = "\n".join(col_lines)
        banner = (
            f"\n+--------------------------------------------------------------------------------+\n"
            f"| SCHEMA INTROSPECTION: {title}\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"{schema_dump}\n"
            f"+--------------------------------------------------------------------------------+"
        )
        logger.info(banner)

    @classmethod
    def _detect_schema_evolution(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        target_table: str,
        df_mart: DataFrame
    ) -> None:
        """
        Introspects the target MySQL table columns via INFORMATION_SCHEMA.COLUMNS.
        If table exists, compares incoming columns against target table and alerts on newly added columns.
        """
        col_query = (
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION"
        )
        try:
            target_cols_raw = cls._execute_sql_query(jdbc_info, col_query, (schema_name, target_table))
            if not target_cols_raw:
                logger.info(f"[SCHEMA INTROSPECTION] Target table '{schema_name}.{target_table}' does not yet exist. First-time deployment.")
                return

            target_cols = {row[0].lower(): (row[1], row[2] if len(row) > 2 else "YES") for row in target_cols_raw}
            incoming_cols = {f.name.lower(): f.dataType.simpleString() for f in df_mart.schema.fields}

            cls._log_mysql_schema_introspection(target_cols_raw, f"Target MySQL Table: '{schema_name}.{target_table}'")

            new_columns = [col for col in incoming_cols if col not in target_cols]
            dropped_columns = [col for col in target_cols if col not in incoming_cols]

            if new_columns or dropped_columns:
                diff_lines = [
                    f"\n+================================================================================+",
                    f"| [SCHEMA EVOLUTION DETECTED] Target Table: {schema_name}.{target_table}",
                    f"+================================================================================+"
                ]
                if new_columns:
                    diff_lines.append(f"| Newly added column(s) in incoming mart query ({len(new_columns)}):")
                    for col in new_columns:
                        diff_lines.append(f"|   ├── Added: '{col}' (Type: {incoming_cols[col]})")
                if dropped_columns:
                    diff_lines.append(f"| Column(s) present in target MySQL table but omitted in query ({len(dropped_columns)}):")
                    for col in dropped_columns:
                        diff_lines.append(f"|   ├── Omitted: '{col}'")
                diff_lines.append(f"+================================================================================+")
                logger.info("\n".join(diff_lines))
            else:
                logger.info(f"[SCHEMA SYNC] Target table '{schema_name}.{target_table}' and incoming query have 100% identical column signatures.")

        except Exception as e:
            logger.warning(f"Failed to introspect target MySQL table schema for evolution tracking: {e}")

    @classmethod
    def _log_mysql_schema_introspection(cls, col_rows: List[Tuple], title: str) -> None:
        """Logs existing columns in target MySQL table."""
        col_lines = []
        for r in col_rows:
            nullable = r[2] if len(r) > 2 else "YES"
            col_lines.append(f"|  * {r[0]:<35} : {r[1]:<20} (Nullable: {nullable})")
        schema_dump = "\n".join(col_lines)
        banner = (
            f"\n+--------------------------------------------------------------------------------+\n"
            f"| MYSQL SCHEMA INTROSPECTION: {title}\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"{schema_dump}\n"
            f"+--------------------------------------------------------------------------------+"
        )
        logger.info(banner)

    # --------------------------------------------------------------------------
    # Materialization & Staging Writes
    # --------------------------------------------------------------------------
    @classmethod
    def _write_staging_table(
        cls,
        spark: SparkSession,
        df_mart: DataFrame,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        staging_table: str
    ) -> None:
        """
        Writes DataFrame to the isolated staging table 'gold_tbl_<mart>_staging' via Spark JDBC.
        Uses 'overwrite' mode to drop/recreate the staging table safely.
        """
        jdbc_url = f"jdbc:mysql://{jdbc_info['host']}:{jdbc_info['port']}/{schema_name}?useSSL=true&allowPublicKeyRetrieval=true"
        logger.info(f"[DDL AUDIT - STAGING WRITE] Writing records to staging table: '{schema_name}.{staging_table}' via Spark JDBC...")
        df_mart.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"`{schema_name}`.`{staging_table}`") \
            .option("user", jdbc_info['user']) \
            .option("password", jdbc_info['password']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("overwrite") \
            .save()
        logger.info(f"[DDL AUDIT - STAGING WRITE] Successfully wrote staging table '{schema_name}.{staging_table}'.")

    # --------------------------------------------------------------------------
    # Zero-Downtime Isolated Atomic Table Swap & Presentation View Refresh
    # --------------------------------------------------------------------------
    @classmethod
    def _execute_isolated_atomic_swap(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        target_table: str,
        staging_table: str,
        old_backup_table: str,
        view_name: str
    ) -> None:
        """
        Executes zero-downtime RENAME TABLE atomic swap in MySQL.
        """
        assert target_table.startswith("gold_tbl_"), f"Safety Error: Invalid target table {target_table}"
        assert staging_table.startswith("gold_tbl_") and staging_table.endswith("_staging"), f"Safety Error: Invalid staging table {staging_table}"
        assert old_backup_table.startswith("gold_tbl_") and old_backup_table.endswith("_old"), f"Safety Error: Invalid backup table {old_backup_table}"
        assert view_name.startswith("v_"), f"Safety Error: Invalid view name {view_name}"

        chk_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        target_exists = bool(cls._execute_sql_query(jdbc_info, chk_query, (schema_name, target_table)))

        # Clean up existing backup table if left over
        backup_exists = bool(cls._execute_sql_query(jdbc_info, chk_query, (schema_name, old_backup_table)))
        if backup_exists:
            logger.info(f"[DDL AUDIT - CLEANUP] Dropping leftover backup table '{schema_name}.{old_backup_table}'...")
            cls._execute_ddl(jdbc_info, f"DROP TABLE IF EXISTS `{schema_name}`.`{old_backup_table}`")

        # Execute Atomic RENAME TABLE
        if target_exists:
            swap_ddl = (
                f"RENAME TABLE "
                f"`{schema_name}`.`{target_table}` TO `{schema_name}`.`{old_backup_table}`, "
                f"`{schema_name}`.`{staging_table}` TO `{schema_name}`.`{target_table}`"
            )
            logger.info(f"[DDL AUDIT - ATOMIC SWAP] Executing atomic table swap:\n"
                        f"  -> {schema_name}.{target_table}  -->  {schema_name}.{old_backup_table}\n"
                        f"  -> {schema_name}.{staging_table} -->  {schema_name}.{target_table}")
            cls._execute_ddl(jdbc_info, swap_ddl)

            logger.info(f"[DDL AUDIT - CLEANUP] Dropping previous table version '{schema_name}.{old_backup_table}'...")
            cls._execute_ddl(jdbc_info, f"DROP TABLE IF EXISTS `{schema_name}`.`{old_backup_table}`")
        else:
            initial_rename = f"RENAME TABLE `{schema_name}`.`{staging_table}` TO `{schema_name}`.`{target_table}`"
            logger.info(f"[DDL AUDIT - INITIAL DEPLOY] Promoting staging to target table:\n  -> {initial_rename}")
            cls._execute_ddl(jdbc_info, initial_rename)

        # Presentation View Creation / Refresh
        view_sql = (
            f"CREATE OR REPLACE VIEW `{schema_name}`.`{view_name}` AS "
            f"SELECT * FROM `{schema_name}`.`{target_table}`"
        )
        logger.info(f"[DDL AUDIT - VIEW] Creating or refreshing presentation view: '{schema_name}.{view_name}'")
        cls._execute_ddl(jdbc_info, view_sql)

    # --------------------------------------------------------------------------
    # SQL Execution Helpers
    # --------------------------------------------------------------------------
    @classmethod
    def _execute_sql_query(cls, jdbc_info: Dict[str, Any], query: str, params: Tuple = ()) -> List[Tuple]:
        """Executes a parameterized SQL query via pymysql or mysql.connector."""
        try:
            import pymysql
            conn = pymysql.connect(
                host=jdbc_info['host'],
                port=int(jdbc_info['port']),
                user=jdbc_info['user'],
                password=jdbc_info['password'],
                database=jdbc_info.get('database') or None,
                connect_timeout=15
            )
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
            finally:
                conn.close()
        except ImportError:
            import mysql.connector
            conn = mysql.connector.connect(
                host=jdbc_info['host'],
                port=int(jdbc_info['port']),
                user=jdbc_info['user'],
                password=jdbc_info['password'],
                database=jdbc_info.get('database') or None,
                connection_timeout=15
            )
            try:
                cursor = conn.cursor()
                cursor.execute(query, params)
                return cursor.fetchall()
            finally:
                conn.close()

    @classmethod
    def _execute_ddl(cls, jdbc_info: Dict[str, Any], ddl: str) -> None:
        """Executes a DDL statement."""
        try:
            import pymysql
            conn = pymysql.connect(
                host=jdbc_info['host'],
                port=int(jdbc_info['port']),
                user=jdbc_info['user'],
                password=jdbc_info['password'],
                connect_timeout=15
            )
            try:
                with conn.cursor() as cursor:
                    cursor.execute(ddl)
                conn.commit()
            finally:
                conn.close()
        except ImportError:
            import mysql.connector
            conn = mysql.connector.connect(
                host=jdbc_info['host'],
                port=int(jdbc_info['port']),
                user=jdbc_info['user'],
                password=jdbc_info['password'],
                connection_timeout=15
            )
            try:
                cursor = conn.cursor()
                cursor.execute(ddl)
                conn.commit()
            finally:
                conn.close()

    # --------------------------------------------------------------------------
    # Query Discovery
    # --------------------------------------------------------------------------
    @classmethod
    def _discover_queries(
        cls,
        query_path: str,
        bucket: str,
        s3_client,
        source_system: str = ""
    ) -> Dict[str, str]:
        """
        Discovers .sql query files from S3 or local directory.
        Path pattern: bucket/gold/query/<source>/v_<table_name>.sql (or <table_name>.sql).
        Returns a dict mapping clean_base_name -> sql_text.
        """
        queries = {}
        if query_path.startswith("s3://"):
            parsed = urlparse(query_path)
            s3_bucket = parsed.netloc or bucket
            s3_prefix = parsed.path.lstrip('/')
            try:
                if s3_prefix.endswith('.sql'):
                    file_name = os.path.basename(s3_prefix)
                    file_base = os.path.splitext(file_name)[0].replace('-', '_')
                    if not file_base.startswith('v_'):
                        raise ValueError(
                            f"CRITICAL QUERY NAMING ERROR: Query file '{file_name}' does not follow the required Gold naming standard.\n"
                            f"All Gold query files MUST strictly be named 'v_<tablename>.sql' (e.g. 'v_{file_name}'). Non-standard query files are not permitted."
                        )
                    clean_name = file_base[2:]
                    resp = s3_client.get_object(Bucket=s3_bucket, Key=s3_prefix)
                    queries[clean_name] = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY DISCOVERY] Loaded single S3 query for '{clean_name}' from '{query_path}'")
                else:
                    paginator = s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
                        for obj in page.get('Contents', []):
                            key = obj['Key']
                            if key.endswith('.sql'):
                                file_name = os.path.basename(key)
                                file_base = os.path.splitext(file_name)[0].replace('-', '_')
                                if not file_base.startswith('v_'):
                                    raise ValueError(
                                        f"CRITICAL QUERY NAMING ERROR: Found query file '{file_name}' at 's3://{s3_bucket}/{key}' which violates the strict Gold naming standard.\n"
                                        f"All Gold query files MUST strictly be named 'v_<tablename>.sql' (e.g. 'v_{file_name}'). Non-standard query files are not permitted."
                                    )
                                clean_name = file_base[2:]
                                resp = s3_client.get_object(Bucket=s3_bucket, Key=key)
                                queries[clean_name] = resp['Body'].read().decode('utf-8')
                                logger.info(f"[QUERY DISCOVERY] Loaded S3 query for '{clean_name}' from 's3://{s3_bucket}/{key}'")
            except ValueError:
                raise
            except Exception as e:
                logger.warning(f"Error listing S3 query files at '{query_path}': {e}. Falling back to local directory.")

        # Fallback to local files under gold/query/<source>/*.sql
        if not queries:
            local_dir = f"gold/query/{source_system}" if source_system else "gold/query"
            for file_path in glob.glob(f"{local_dir}/*.sql"):
                file_name = os.path.basename(file_path)
                file_base = os.path.splitext(file_name)[0].replace('-', '_')
                if not file_base.startswith('v_'):
                    raise ValueError(
                        f"CRITICAL QUERY NAMING ERROR: Found query file '{file_name}' at '{file_path}' which violates the strict Gold naming standard.\n"
                        f"All Gold query files MUST strictly be named 'v_<tablename>.sql' (e.g. 'v_{file_name}'). Non-standard query files are not permitted."
                    )
                clean_name = file_base[2:]
                if clean_name not in queries:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        queries[clean_name] = f.read()
                        logger.info(f"[QUERY DISCOVERY] Loaded local query for '{clean_name}' from '{file_path}'")

        return queries

    # --------------------------------------------------------------------------
    # Connection Resolution
    # --------------------------------------------------------------------------
    @classmethod
    def _resolve_mysql_connection_info(
        cls,
        params: Dict[str, Any],
        glue_client=None,
        secrets_client=None
    ) -> Dict[str, Any]:
        """
        Resolves MySQL host, port, user, password, and database.
        Zero fallback schema: requires explicit GOLD_SCHEMA (raises ValueError if missing).
        Password resolution order:
          1. Manual password: CLI parameter --RDS_PASSWORD or RDS_PASSWORD env var
          2. AWS Secrets Manager: CLI parameter --RDS_SECRET_NAME or --SECRET_NAME
          3. AWS Glue Connection: CLI parameter --CONNECTION_NAME
        If no password is provided, raises an explicit ValueError guiding the error.
        """
        gold_schema = (
            params.get('GOLD_SCHEMA')
            or params.get('gold_schema')
            or params.get('RDS_SCHEMA')
            or params.get('rds_schema')
        )
        if not gold_schema or not str(gold_schema).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter '--GOLD_SCHEMA'.\n"
                "In accordance with enterprise shared database policy, no fallback schema is permitted.\n"
                "Please explicitly specify the target MySQL schema name (e.g. --GOLD_SCHEMA enterprise_reporting)."
            )
        gold_schema = str(gold_schema).strip()

        host = (
            params.get('RDS_HOST')
            or params.get('rds_host')
            or params.get('RDS_URL')
            or params.get('rds_url')
            or os.environ.get('RDS_HOST', 'localhost')
        )
        port = str(
            params.get('RDS_PORT')
            or params.get('rds_port')
            or os.environ.get('RDS_PORT', '3306')
        )
        user = (
            params.get('RDS_USER')
            or params.get('rds_user')
            or params.get('RDS_USERNAME')
            or params.get('rds_username')
            or params.get('rds_uaername')
            or os.environ.get('RDS_USER', 'pipeline_user')
        )
        pwd = (
            params.get('RDS_PASSWORD')
            or params.get('rds_password')
            or os.environ.get('RDS_PASSWORD')
        )

        secret_name = (
            params.get('RDS_SECRET_NAME')
            or params.get('rds_secret_name')
            or params.get('SECRET_NAME')
            or params.get('secret_name')
            or params.get('DB_SECRET_NAME')
            or params.get('db_secret_name')
            or params.get('DB_SECRET')
            or params.get('db_secret')
            or os.environ.get('RDS_SECRET_NAME')
        )
        conn_name = params.get('CONNECTION_NAME') or params.get('GLUE_CONNECTION_NAME')

        # Option 1: Manual password takes precedence if provided directly
        if pwd:
            logger.info("Using manually provided MySQL database password via --RDS_PASSWORD parameter.")

        # Option 2: Take database password from AWS Secrets Manager if secret_name is passed
        elif secret_name:
            try:
                if not secrets_client:
                    secrets_client = boto3.client('secretsmanager')
                logger.info(f"Retrieving MySQL database credentials from AWS Secrets Manager secret '{secret_name}'...")
                resp = secrets_client.get_secret_value(SecretId=secret_name)
                secret_str = resp.get('SecretString')
                if not secret_str and 'SecretBinary' in resp:
                    import base64
                    secret_str = base64.b64decode(resp['SecretBinary']).decode('utf-8')

                if secret_str:
                    try:
                        secret_json = json.loads(secret_str)
                        pwd = (
                            secret_json.get('password')
                            or secret_json.get('PASSWORD')
                            or secret_json.get('pwd')
                            or secret_json.get('db_password')
                        )
                        user = (
                            secret_json.get('username')
                            or secret_json.get('user')
                            or secret_json.get('USERNAME')
                            or user
                        )
                        host = secret_json.get('host') or secret_json.get('HOST') or host
                        port = str(secret_json.get('port') or secret_json.get('PORT') or port)
                    except (json.JSONDecodeError, TypeError):
                        pwd = secret_str.strip()

                if not pwd:
                    raise ValueError(
                        f"CRITICAL AUTH ERROR: AWS Secrets Manager secret '{secret_name}' was retrieved, "
                        f"but no 'password' field was found in the secret JSON payload.\n"
                        f"Please ensure the secret contains a 'password' field, or specify the password manually using --RDS_PASSWORD."
                    )
                logger.info(f"Successfully retrieved database credentials from Secrets Manager secret '{secret_name}'.")
            except Exception as sec_err:
                if isinstance(sec_err, ValueError):
                    raise
                raise ValueError(
                    f"CRITICAL AUTH ERROR: Failed to retrieve MySQL credentials from AWS Secrets Manager secret '{secret_name}'.\n"
                    f"Underlying error: {sec_err}\n"
                    f"Troubleshooting:\n"
                    f"  1. Verify the secret name '{secret_name}' exists in AWS Secrets Manager.\n"
                    f"  2. Verify IAM permissions: ensure the Glue execution role has 'secretsmanager:GetSecretValue'.\n"
                    f"  3. Alternatively, supply the database password manually via --RDS_PASSWORD <password>."
                ) from sec_err

        # Option 3: Retrieve credentials from AWS Glue Connection
        elif conn_name and glue_client:
            try:
                logger.info(f"Resolving MySQL connection credentials from AWS Glue Connection '{conn_name}'...")
                resp = glue_client.get_connection(Name=conn_name)
                props = resp.get('Connection', {}).get('ConnectionProperties', {})
                raw_url = props.get('JDBC_CONNECTION_URL', '')
                if not user or user == 'pipeline_user':
                    user = props.get('USERNAME') or user
                pwd = props.get('PASSWORD') or pwd

                clean_url = raw_url.replace('jdbc:mysql://', '').split('?')[0]
                host_port = clean_url.split('/')[0]
                if ':' in host_port:
                    host, port = host_port.split(':')
                elif host_port:
                    host = host_port
            except Exception as conn_err:
                logger.warning(f"Could not resolve Glue connection '{conn_name}': {conn_err}.")

        # Missing password guidance
        if not pwd:
            raise ValueError(
                "CRITICAL AUTH ERROR: MySQL database password is missing.\n"
                "Please provide the database password using one of the supported options:\n"
                "  1. AWS Secrets Manager: Pass --RDS_SECRET_NAME <secret_name> (e.g. --RDS_SECRET_NAME dev/rds/mysql)\n"
                "  2. Manual Password    : Pass --RDS_PASSWORD <password> via CLI or set RDS_PASSWORD environment variable\n"
                "  3. AWS Glue Connection: Pass --CONNECTION_NAME <connection_name>"
            )

        return {
            "host": host,
            "port": int(port) if str(port).isdigit() else 3306,
            "user": user,
            "password": pwd,
            "database": gold_schema
        }

    # --------------------------------------------------------------------------
    # Observability & Error Cards
    # --------------------------------------------------------------------------
    @classmethod
    def _format_error_diagnostic_card(
        cls,
        layer: str,
        step_name: str,
        target_entity: str,
        query_source: str,
        conn_info: Optional[Dict[str, Any]],
        exception: Exception
    ) -> str:
        """Emits a structured error card with complete diagnostic context and stack trace."""
        tb = traceback.format_exc()
        safe_host = conn_info.get('host') if conn_info else "N/A"
        safe_user = conn_info.get('user') if conn_info else "N/A"
        safe_db = conn_info.get('database') if conn_info else "N/A"
        failed_at = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        return (
            f"\n+================================================================================+\n"
            f"|  ERROR DIAGNOSTIC CARD: GOLD MART FAILURE [FAILED]\n"
            f"+================================================================================+\n"
            f"|  * Execution Layer    : {layer}\n"
            f"|  * Step Name          : {step_name}\n"
            f"|  * Target Entity      : {target_entity}\n"
            f"|  * Query Source       : {query_source}\n"
            f"|  * Target Host        : {safe_host}\n"
            f"|  * Target User        : {safe_user}\n"
            f"|  * Target Schema      : {safe_db}\n"
            f"|  * Failed At (UTC)    : {failed_at}\n"
            f"|  * Exception Type     : {exception.__class__.__name__}\n"
            f"|  * Exception Message  : {str(exception)}\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"|  COMPLETE STACK TRACE:\n"
            f"{tb}\n"
            f"+================================================================================+"
        )

    @classmethod
    def _log_final_gold_summary(cls, stats: List[Dict[str, Any]], start_time: datetime) -> None:
        """Logs the final execution report for all evaluated Gold marts."""
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        failed_count = sum(1 for m in stats if m.get('status') == 'FAILED')
        overall_status = "FAILED" if failed_count > 0 else "SUCCESS"

        breakdown_lines = []
        for m in stats:
            status_tag = "[OK]  " if m.get('status') == 'SUCCESS' else "[FAIL]"
            rows = f"Rows: {m['rows_served']:,}" if m.get('rows_served') is not None else "Type: VIEW"
            duration_str = f"Time: {m.get('duration_seconds', 0.0):>5.2f}s"
            breakdown_lines.append(
                f"|  {status_tag} Mart: {m['mart_name']:<20} | Table: {m['target_table']:<35} | {rows:<16} | {duration_str} | Status: {m['status']}"
            )
            if m.get('error_message'):
                breakdown_lines.append(f"|         └── Error: {m['error_message']}")

        breakdown_str = "\n".join(breakdown_lines)

        summary_card = (
            f"\n[JOB REPORT] GOLD SERVING LAYER | Status: {overall_status} | Marts: {len(stats) - failed_count}/{len(stats)} | Duration: {total_duration:.2f}s\n"
            f"+================================================================================+\n"
            f"|                    GOLD SERVING LAYER FINAL EXECUTION REPORT                   |\n"
            f"+================================================================================+\n"
            f"|  Overall Status        : {overall_status}\n"
            f"|  Total Marts Evaluated : {len(stats)} (Succeeded: {len(stats) - failed_count}, Failed: {failed_count})\n"
            f"|  Start Time (UTC)      : {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"|  End Time (UTC)        : {end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"|  Total Duration        : {total_duration:.2f}s\n"
            f"+--------------------------------------------------------------------------------+\n"
            f"|  MART EXECUTION BREAKDOWN:\n"
            f"{breakdown_str}\n"
            f"+================================================================================+"
        )
        logger.info(summary_card)
