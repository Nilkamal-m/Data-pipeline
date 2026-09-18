"""
Gold Serving Layer Manager.

Responsibilities:
1. Zero-DDL Schema Verification: Verifies --GOLD_SCHEMA exists in MySQL via INFORMATION_SCHEMA.SCHEMATA. Fails fast if missing.
2. Shared Database Guardrails: Strictly isolates operations to 'gold_tbl_<table_name>' and 'v_<table_name>'. Never modifies external tables.
3. Step-by-Step Lifecycle Logs: Logs explicit progress from [GOLD STEP 1/6] to [GOLD STEP 6/6].
4. Complete Column Schema Introspection: Logs all columns and data types for query outputs and existing MySQL target tables.
5. Schema Evolution Tracking: Compares incoming columns against existing MySQL tables and alerts on any newly added columns.
6. DDL Audit Logs: Explicitly records all DROP, CREATE, SWAP, and VIEW operations.
7. Multi-Target DW Stubs: Clean routing for Aurora MySQL, Redshift, and Snowflake.
8. Structured Error Diagnostic Cards: Emits rich debugging cards with full stack traces on failure.
"""

import os
import sys
import glob
import json
import logging
import traceback
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
    for Gold layer data marts with strict shared-database safety and high observability.
    """

    @classmethod
    def run_gold_pipeline(
        cls,
        spark: SparkSession,
        params: Dict[str, Any],
        glue_client=None,
        s3_client=None,
        secrets_client=None
    ) -> List[Dict[str, Any]]:
        """
        Main entry point for Gold serving layer execution.
        Discovers query definitions, runs Spark SQL transformations, checks target schemas,
        materializes Parquet datasets to S3, and loads target serving tables.
        """
        execution_start = datetime.now(timezone.utc)
        bucket_name = params.get('DATA_LAKE_BUCKET', 'uax-datalake-dev-bucket')
        gold_target = (params.get('GOLD_TARGET') or 'aurora').lower()

        # 1. Strict GOLD_SCHEMA Enforcement (Zero Fallback Policy)
        gold_schema = params.get('GOLD_SCHEMA')
        if not gold_schema or not str(gold_schema).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter '--GOLD_SCHEMA'.\n"
                "In accordance with enterprise shared database policy, no fallback schema is permitted.\n"
                "Please explicitly specify the target MySQL schema name (e.g. --GOLD_SCHEMA enterprise_reporting)."
            )
        gold_schema = str(gold_schema).strip()

        # 2. Strict SOURCE_SYSTEM Enforcement for the ultimate query path: bucket/gold/query/<source>/<table_name>.sql
        source_system = (params.get('SOURCE_SYSTEM') or '').strip().lower()
        if not source_system:
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter '--SOURCE_SYSTEM'.\n"
                "The ultimate Gold query path is 's3://<bucket>/gold/query/<source>/<table_name>.sql'.\n"
                "Please specify the source system (e.g. --SOURCE_SYSTEM servicenow)."
            )

        # Simplified ultimate query and data paths
        query_s3_path = params.get('GOLD_QUERY_S3_PATH') or f"s3://{bucket_name}/gold/query/{source_system}"
        data_s3_path = params.get('GOLD_DATA_S3_PATH') or f"s3://{bucket_name}/gold/data/{source_system}"

        if not s3_client:
            s3_client = boto3.client('s3')

        logger.info(
            f"\n+================================================================================+\n"
            f"|              STARTING GOLD SERVING ENGINE: MULTI-TARGET PIPELINE               |\n"
            f"+================================================================================+\n"
            f"|  * Target Engine     : {gold_target.upper()}\n"
            f"|  * Target Schema     : {gold_schema}\n"
            f"|  * Source System     : {source_system.upper() if source_system else 'ALL'}\n"
            f"|  * Query S3 Path     : {query_s3_path}\n"
            f"|  * Data S3 Path      : {data_s3_path}\n"
            f"|  * Data Lake Bucket  : {bucket_name}\n"
            f"|  * Execution Time    : {execution_start.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"+================================================================================+"
        )

        mart_stats = []

        # ----------------------------------------------------------------------
        # Target DW Routing: Aurora MySQL, Redshift, Snowflake
        # ----------------------------------------------------------------------
        if gold_target == 'redshift':
            logger.info("[GOLD TARGET] Routing to Amazon Redshift serving adapter...")
            return cls._run_redshift_stub(spark, params, query_s3_path, data_s3_path)
        elif gold_target == 'snowflake':
            logger.info("[GOLD TARGET] Routing to Snowflake serving adapter...")
            return cls._run_snowflake_stub(spark, params, query_s3_path, data_s3_path)
        elif gold_target != 'aurora':
            raise ValueError(
                f"Unsupported --GOLD_TARGET '{gold_target}'. Supported options: 'aurora', 'redshift', 'snowflake'."
            )

        # ----------------------------------------------------------------------
        # Aurora MySQL Pipeline
        # ----------------------------------------------------------------------
        # Resolve JDBC Connection Parameters
        jdbc_conn_info = cls._resolve_mysql_connection_info(
            params,
            glue_client=glue_client,
            secrets_client=secrets_client
        )

        # [GOLD STEP 1/6] Strict Schema Pre-Existence Check (Zero DDL Policy)
        logger.info(
            f"\n================================================================================\n"
            f"[GOLD STEP 1/6] Validating Target Database Schema (Zero DDL Policy)\n"
            f"--------------------------------------------------------------------------------\n"
            f"Verifying that schema '{gold_schema}' pre-exists in MySQL database...\n"
            f"Script will NEVER execute CREATE DATABASE / CREATE SCHEMA."
        )
        cls._verify_schema_exists_or_raise(jdbc_conn_info, gold_schema)
        logger.info(f"[GOLD STEP 1/6] Schema '{gold_schema}' pre-existence verified in MySQL. [PASSED]")

        # [GOLD STEP 2/6] Query Discovery
        logger.info(
            f"\n================================================================================\n"
            f"[GOLD STEP 2/6] Discovering Gold Mart Query Definitions (.sql)\n"
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
                f"The ultimate query path is: s3://{bucket_name}/gold/query/{source_system}/<table_name>.sql\n"
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
                logger.info(f"[GOLD STEP 2/6] Filtered queries by table override {table_filter}: {list(matched.keys())}")
                queries = matched
            else:
                logger.info(
                    f"[GOLD STEP 2/6] CLI table override {table_filter} did not match mart names {list(queries.keys())}. "
                    f"Processing all discovered mart queries for source '{source_system}'."
                )

        logger.info(f"[GOLD STEP 2/6] Discovered {len(queries)} mart query file(s) to process: {list(queries.keys())}")

        # Process each discovered mart query
        for table_base_name, sql_text in queries.items():
            clean_base_name = table_base_name.strip().replace('-', '_')
            mart_start = datetime.now(timezone.utc)
            target_table = f"gold_tbl_{clean_base_name}"
            staging_table = f"gold_tbl_{clean_base_name}_staging"
            old_backup_table = f"gold_tbl_{clean_base_name}_old"
            view_name = f"v_{clean_base_name}"

            # Shared Database Safety Assertion
            assert target_table.startswith("gold_tbl_"), f"Safety Error: Invalid table name {target_table}"
            assert staging_table.startswith("gold_tbl_") and staging_table.endswith("_staging"), f"Safety Error: Invalid staging table {staging_table}"
            assert view_name.startswith("v_"), f"Safety Error: Invalid view name {view_name}"

            logger.info(
                f"\n+--------------------------------------------------------------------------------+\n"
                f"|  PROCESSING GOLD MART: '{clean_base_name}'\n"
                f"|  * Target Table  : {gold_schema}.{target_table}\n"
                f"|  * Staging Table : {gold_schema}.{staging_table}\n"
                f"|  * Serving View  : {gold_schema}.{view_name}\n"
                f"+--------------------------------------------------------------------------------+"
            )

            try:
                # [GOLD STEP 3/6] Spark SQL Execution & Result Introspection
                logger.info(
                    f"\n[GOLD STEP 3/6] Executing Spark SQL query for '{clean_base_name}'..."
                )
                df_mart = spark.sql(sql_text)

                # Attach Gold Layer Metadata
                df_mart = df_mart.withColumn("_computed_at", df_mart.sql_ctx.sql("select current_timestamp()").collect()[0][0])

                row_count = df_mart.count()
                logger.info(f"[GOLD STEP 3/6] Query executed successfully. Computed {row_count:,} records.")

                # Log full column schema introspection
                cls._log_schema_introspection(df_mart, f"Gold Query Output Schema: '{clean_base_name}'")

                # [GOLD STEP 4/6] S3 Materialization
                mart_s3_dest = f"{data_s3_path.rstrip('/')}/{clean_base_name}"
                logger.info(
                    f"\n[GOLD STEP 4/6] Materializing {row_count:,} records to S3 Parquet:\n"
                    f"                -> Location: {mart_s3_dest}"
                )
                df_mart.write.mode("overwrite").format("parquet").save(mart_s3_dest)
                logger.info(f"[GOLD STEP 4/6] S3 Parquet materialization completed.")

                # [GOLD STEP 5/6] Schema Evolution Check & Staging Table Write
                logger.info(
                    f"\n[GOLD STEP 5/6] Introspecting target MySQL table & writing staging table via Spark JDBC..."
                )
                cls._detect_schema_evolution(jdbc_conn_info, gold_schema, target_table, df_mart)

                # Write to staging table
                cls._write_staging_table(spark, df_mart, jdbc_conn_info, gold_schema, staging_table)
                logger.info(f"[GOLD STEP 5/6] Staging table write complete. [PASSED]")

                # [GOLD STEP 6/6] Zero-Downtime Atomic Swap & Presentation View Refresh
                logger.info(
                    f"\n[GOLD STEP 6/6] Performing isolated atomic swap and presentation view refresh..."
                )
                cls._execute_isolated_atomic_swap(
                    jdbc_info=jdbc_conn_info,
                    schema_name=gold_schema,
                    target_table=target_table,
                    staging_table=staging_table,
                    old_backup_table=old_backup_table,
                    view_name=view_name
                )
                logger.info(f"[GOLD STEP 6/6] Atomic swap and view refresh complete. [PASSED]")

                mart_duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                mart_stats.append({
                    "mart_name": table_base_name,
                    "target_table": target_table,
                    "view_name": view_name,
                    "status": "SUCCESS",
                    "rows_served": row_count,
                    "duration_seconds": round(mart_duration, 2),
                    "error_message": None
                })

                logger.info(
                    f"\n+================================================================================+\n"
                    f"|  GOLD MART COMPLETED: {table_base_name} [SUCCESS]\n"
                    f"+--------------------------------------------------------------------------------+\n"
                    f"|  * Physical Table : {gold_schema}.{target_table}\n"
                    f"|  * Reporting View : {gold_schema}.{view_name}\n"
                    f"|  * Records Loaded : {row_count:,}\n"
                    f"|  * S3 Data Path   : {mart_s3_dest}\n"
                    f"|  * Mart Duration  : {mart_duration:.2f}s\n"
                    f"+================================================================================+"
                )

            except Exception as err:
                mart_duration = (datetime.now(timezone.utc) - mart_start).total_seconds()
                error_card = cls._format_error_diagnostic_card(
                    layer="GOLD",
                    step_name="Processing Gold Mart",
                    target_entity=f"{gold_schema}.{target_table}",
                    query_source=f"Query for {table_base_name}",
                    conn_info=jdbc_conn_info,
                    exception=err
                )
                logger.error(error_card)
                mart_stats.append({
                    "mart_name": table_base_name,
                    "target_table": target_table,
                    "view_name": view_name,
                    "status": "FAILED",
                    "rows_served": 0,
                    "duration_seconds": round(mart_duration, 2),
                    "error_message": str(err)
                })

        # Overall Gold Execution Summary
        cls._log_final_gold_summary(mart_stats, execution_start)

        failed_marts = [m for m in mart_stats if m['status'] == 'FAILED']
        if failed_marts:
            raise RuntimeError(f"Gold Serving Layer completed with failures in {len(failed_marts)} mart(s).")

        return mart_stats

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
        """
        Logs a structured breakdown of all columns and data types in the DataFrame.
        """
        fields = df.schema.fields
        lines = [
            "+--------------------------------------------------------------------------------+",
            f"| [SCHEMA INTROSPECTION] {title[:66]}",
            "+--------------------------------------------------------------------------------+",
            f"| Total Column Count: {len(fields)}",
            "|"
        ]
        for f in fields:
            lines.append(f"|   |-- {f.name:<32} : {f.dataType.simpleString()}")
        lines.append("+--------------------------------------------------------------------------------+")
        logger.info("\n".join(lines))

    @classmethod
    def _detect_schema_evolution(
        cls,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        table_name: str,
        incoming_df: DataFrame
    ) -> None:
        """
        Inspects existing MySQL target table schema via INFORMATION_SCHEMA.COLUMNS.
        Compares against incoming DataFrame columns and alerts developers if new columns are added.
        """
        query = (
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION"
        )
        try:
            existing_cols = cls._execute_sql_query(jdbc_info, query, (schema_name, table_name))
            if not existing_cols:
                logger.info(
                    f"[INITIAL LOAD] Target table '{schema_name}.{table_name}' does not exist yet in MySQL.\n"
                    f"               First-time initialization will create table with {len(incoming_df.columns)} column(s)."
                )
                return

            existing_col_names = {row[0].lower(): row[1] for row in existing_cols}
            incoming_fields = incoming_df.schema.fields

            new_columns = [
                f for f in incoming_fields if f.name.lower() not in existing_col_names
            ]

            if new_columns:
                diff_lines = [
                    "+--------------------------------------------------------------------------------+",
                    f"| [SCHEMA EVOLUTION DETECTED] New column(s) detected for '{schema_name}.{table_name}'",
                    "+--------------------------------------------------------------------------------+",
                    f"| Previous Active Table Columns : {len(existing_col_names)}",
                    f"| Incoming New Query Columns    : {len(incoming_fields)}",
                    "|",
                    f"| Newly Added Column(s) ({len(new_columns)}):"
                ]
                for nf in new_columns:
                    diff_lines.append(f"|   ├── Added: '{nf.name}' ({nf.dataType.simpleString()})")
                diff_lines.append("+--------------------------------------------------------------------------------+")
                logger.info("\n".join(diff_lines))
            else:
                logger.info(f"[SCHEMA SYNC] Target table '{table_name}' schema matches incoming columns (Count: {len(incoming_fields)}).")

        except Exception as e:
            logger.warning(f"[SCHEMA SYNC] Could not verify schema evolution for '{table_name}': {e}. Continuing write.")

    # --------------------------------------------------------------------------
    # Database Writes & Atomic Blue/Green Swap
    # --------------------------------------------------------------------------
    @classmethod
    def _write_staging_table(
        cls,
        spark: SparkSession,
        df: DataFrame,
        jdbc_info: Dict[str, Any],
        schema_name: str,
        staging_table: str
    ) -> None:
        """
        Writes data to the staging table using Spark JDBC with 'overwrite' mode.
        Emits DDL audit logs for table creation/write.
        """
        jdbc_url = f"jdbc:mysql://{jdbc_info['host']}:{jdbc_info['port']}/{schema_name}?useSSL=true&serverTimezone=UTC"

        logger.info(f"[DDL AUDIT - DROP] Dropping pre-existing staging table if present: '{schema_name}.{staging_table}'")
        cls._execute_ddl(jdbc_info, f"DROP TABLE IF EXISTS `{schema_name}`.`{staging_table}`")

        logger.info(f"[DDL AUDIT - CREATE] Creating staging table via Spark JDBC: '{schema_name}.{staging_table}'")
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"`{schema_name}`.`{staging_table}`") \
            .option("user", jdbc_info['user']) \
            .option("password", jdbc_info['password']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("batchsize", 5000) \
            .mode("overwrite") \
            .save()

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
        Performs an atomic swap strictly between target_table and staging_table with zero downtime.
        Strictly guards that only gold_tbl_* tables and v_* views are ever manipulated.
        """
        # 1. Clean up old backup table if lingering
        logger.info(f"[DDL AUDIT - DROP] Dropping obsolete backup table if present: '{schema_name}.{old_backup_table}'")
        cls._execute_ddl(jdbc_info, f"DROP TABLE IF EXISTS `{schema_name}`.`{old_backup_table}`")

        # 2. Check if target physical table already exists
        check_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        exists = cls._execute_sql_query(jdbc_info, check_query, (schema_name, target_table))

        if exists:
            # Atomic RENAME swap: target -> old, staging -> target
            swap_sql = (
                f"RENAME TABLE `{schema_name}`.`{target_table}` TO `{schema_name}`.`{old_backup_table}`, "
                f"`{schema_name}`.`{staging_table}` TO `{schema_name}`.`{target_table}`"
            )
            logger.info(
                f"[DDL AUDIT - SWAP] Executing atomic table rename in MySQL:\n"
                f"                  - `{target_table}` -> `{old_backup_table}`\n"
                f"                  - `{staging_table}` -> `{target_table}`"
            )
            cls._execute_ddl(jdbc_info, swap_sql)

            # Drop old backup table
            logger.info(f"[DDL AUDIT - DROP] Dropping old backup table: '{schema_name}.{old_backup_table}'")
            cls._execute_ddl(jdbc_info, f"DROP TABLE IF EXISTS `{schema_name}`.`{old_backup_table}`")
        else:
            # Initial Load: simple rename staging -> target
            logger.info(f"[DDL AUDIT - RENAME] Initializing active table: `{staging_table}` -> `{target_table}`")
            cls._execute_ddl(
                jdbc_info,
                f"RENAME TABLE `{schema_name}`.`{staging_table}` TO `{schema_name}`.`{target_table}`"
            )

        # 3. Create or Replace Presentation View
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
        """Executes a parameterized SQL query via pymysql or mysql-connector."""
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
            # Fallback to pure java/spark or mysql.connector if installed
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
    # Query Discovery & Loading
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
        Ultimate path structure is: bucket/gold/query/<source>/<table_name>.sql.
        Returns a dict mapping table_base_name -> sql_text.
        """
        queries = {}
        if query_path.startswith("s3://"):
            parsed = urlparse(query_path)
            s3_bucket = parsed.netloc or bucket
            s3_prefix = parsed.path.lstrip('/')
            try:
                if s3_prefix.endswith('.sql'):
                    base_name = os.path.splitext(os.path.basename(s3_prefix))[0].replace('-', '_')
                    resp = s3_client.get_object(Bucket=s3_bucket, Key=s3_prefix)
                    queries[base_name] = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY DISCOVERY] Loaded single S3 query for '{base_name}' from '{query_path}'")
                else:
                    paginator = s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
                        for obj in page.get('Contents', []):
                            key = obj['Key']
                            if key.endswith('.sql'):
                                base_name = os.path.splitext(os.path.basename(key))[0].replace('-', '_')
                                resp = s3_client.get_object(Bucket=s3_bucket, Key=key)
                                queries[base_name] = resp['Body'].read().decode('utf-8')
                                logger.info(f"[QUERY DISCOVERY] Loaded S3 query for '{base_name}' from 's3://{s3_bucket}/{key}'")
            except Exception as e:
                logger.warning(f"Error listing S3 query files at '{query_path}': {e}. Falling back to local directory.")

        # Fallback to local files under gold/query/<source>/*.sql
        if not queries:
            local_dir = f"gold/query/{source_system}" if source_system else "gold/query"
            for file_path in glob.glob(f"{local_dir}/*.sql"):
                base_name = os.path.splitext(os.path.basename(file_path))[0].replace('-', '_')
                if base_name not in queries:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        queries[base_name] = f.read()
                        logger.info(f"[QUERY DISCOVERY] Loaded local query for '{base_name}' from '{file_path}'")

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
        gold_schema = params.get('GOLD_SCHEMA')
        if not gold_schema or not str(gold_schema).strip():
            raise ValueError(
                "CRITICAL CONFIG ERROR: Missing required parameter '--GOLD_SCHEMA'.\n"
                "In accordance with enterprise shared database policy, no fallback schema is permitted.\n"
                "Please explicitly specify the target MySQL schema name (e.g. --GOLD_SCHEMA enterprise_reporting)."
            )
        gold_schema = str(gold_schema).strip()

        host = params.get('RDS_HOST', os.environ.get('RDS_HOST', 'localhost'))
        port = str(params.get('RDS_PORT', os.environ.get('RDS_PORT', '3306')))
        user = params.get('RDS_USER', os.environ.get('RDS_USER', 'pipeline_user'))
        pwd = params.get('RDS_PASSWORD') or os.environ.get('RDS_PASSWORD')

        secret_name = (
            params.get('RDS_SECRET_NAME')
            or params.get('SECRET_NAME')
            or params.get('DB_SECRET_NAME')
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
                    except json.JSONDecodeError:
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
            "port": port,
            "user": user,
            "password": pwd,
            "database": gold_schema
        }

    # --------------------------------------------------------------------------
    # Error Diagnostics & Logging Helpers
    # --------------------------------------------------------------------------
    @classmethod
    def _format_error_diagnostic_card(
        cls,
        layer: str,
        step_name: str,
        target_entity: str,
        query_source: str,
        conn_info: Dict[str, Any],
        exception: Exception
    ) -> str:
        """
        Formats a comprehensive error diagnostic card with full stack traces.
        """
        safe_host = conn_info.get('host', 'unknown')
        safe_user = conn_info.get('user', 'unknown')
        safe_db = conn_info.get('database', 'unknown')
        tb = traceback.format_exc()

        card = [
            "+================================================================================+",
            f"|  ERROR DIAGNOSTIC CARD: {layer} PIPELINE EXECUTION FAILURE [FAILED]",
            "+================================================================================+",
            f"|  * Execution Layer    : {layer}",
            f"|  * Failed Step        : {step_name}",
            f"|  * Target Entity      : {target_entity}",
            f"|  * Query / SQL Source : {query_source}",
            f"|  * Database Host      : {safe_host} (User: {safe_user}, Database: {safe_db})",
            f"|  * Exception Type     : {exception.__class__.__name__}",
            f"|  * Exception Message  : {str(exception)}",
            "+--------------------------------------------------------------------------------+",
            "|  * Full Python / Spark Stack Trace:",
            tb.strip(),
            "+================================================================================+"
        ]
        return "\n".join(card)

    @classmethod
    def _log_final_gold_summary(cls, stats: List[Dict[str, Any]], start_time: datetime) -> None:
        """Logs the final execution summary card for Gold layer marts."""
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        failed_count = len([s for s in stats if s['status'] == 'FAILED'])
        overall_status = "FAILED" if failed_count > 0 else "SUCCESS"

        breakdown_lines = []
        for s in stats:
            tag = "[OK]  " if s['status'] == 'SUCCESS' else "[FAIL]"
            breakdown_lines.append(
                f"|  {tag} Mart: {s['mart_name']:<22} | Table: {s['target_table']:<25} | Rows: {s['rows_served']:>6,} | Time: {s['duration_seconds']:>6.2f}s | Status: {s['status']}"
            )
            if s.get('error_message'):
                breakdown_lines.append(f"|         └── Error: {s['error_message']}")

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

    # --------------------------------------------------------------------------
    # Multi-Target Stubs: Redshift & Snowflake
    # --------------------------------------------------------------------------
    @classmethod
    def _run_redshift_stub(
        cls,
        spark: SparkSession,
        params: Dict[str, Any],
        query_path: str,
        data_path: str
    ) -> List[Dict[str, Any]]:
        """
        Modular stub for Amazon Redshift serving.
        Future teams can activate Redshift Spectrum external tables or Redshift connector.
        """
        logger.info(
            "+================================================================================+\n"
            "|  REDSHIFT SERVING ADAPTER (FUTURE DW OPTION)                                    |\n"
            "+================================================================================+\n"
            "|  * Parquet datasets persisted to S3 are queryable via Redshift Spectrum.       |\n"
            "|  * Ready for AWS Redshift spark connector integration.                         |\n"
            "+================================================================================+"
        )
        return [{"status": "STUB_SUCCESS", "target": "redshift"}]

    @classmethod
    def _run_snowflake_stub(
        cls,
        spark: SparkSession,
        params: Dict[str, Any],
        query_path: str,
        data_path: str
    ) -> List[Dict[str, Any]]:
        """
        Modular stub for Snowflake serving.
        Future teams can activate Snowflake external Iceberg tables or Snowflake spark connector.
        """
        logger.info(
            "+================================================================================+\n"
            "|  SNOWFLAKE SERVING ADAPTER (FUTURE DW OPTION)                                  |\n"
            "+================================================================================+\n"
            "|  * Parquet / Iceberg datasets are queryable via Snowflake External Iceberg.    |\n"
            "|  * Ready for net.snowflake.spark.snowflake integration.                        |\n"
            "+================================================================================+"
        )
        return [{"status": "STUB_SUCCESS", "target": "snowflake"}]
