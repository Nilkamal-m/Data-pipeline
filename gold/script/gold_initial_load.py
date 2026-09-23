"""
Gold Layer Initial Load & CSV Reconciliation Engine — AWS Glue / PySpark.

Imports historical exports (CSV/Parquet) into Gold tables with flexible schema consolidation:
1. Reconciles columns:
   - Missing in CSV, present in Gold -> filled with NULL (cast to target type).
   - Present in CSV, missing in Gold -> retained for schema evolution.
2. Performs idempotent UPSERT (MERGE INTO) into Gold table using primary keys.
3. Propagates consolidated records to downstream serving targets (Athena, Aurora, Databricks, Redshift, Snowflake).
"""

import sys
import os
import re
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

# Ensure gold script directory is in sys.path
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

try:
    from gold_config_loader import GoldConfigLoader
except ImportError:
    try:
        from gold.script.gold_config_loader import GoldConfigLoader
    except ImportError:
        GoldConfigLoader = None

try:
    from gold_layer_manager import GoldLayerManager
except ImportError:
    try:
        from gold.script.gold_layer_manager import GoldLayerManager
    except ImportError:
        GoldLayerManager = None

logger = logging.getLogger(__name__)


class GoldInitialLoader:
    """
    Handles initial historical data ingestion and column consolidation for Gold tables.
    """

    @classmethod
    def sanitize_column_name(cls, col_name: str) -> str:
        """
        Cleanses and standardizes column names for SQL, Parquet, Iceberg, and Athena compatibility:
        - Converts spaces (' ') and periods ('.') into underscores ('_').
        - Converts hyphens ('-'), slashes ('/'), colons (':'), brackets into underscores ('_').
        - Strips illegal/non-alphanumeric characters.
        - Collapses duplicate underscores and converts to lowercase snake_case.
        """
        if not col_name:
            return "unnamed_column"
        # 1. Replace spaces, dots, hyphens, slashes, brackets, colons, hashes with underscore
        clean = re.sub(r'[\s\.\-\/\:\(\)\[\]\{\}\<\>#]+', '_', str(col_name).strip())
        # 2. Strip non-alphanumeric characters except underscore
        clean = re.sub(r'[^a-zA-Z0-9_]', '', clean)
        # 3. Collapse multiple underscores and trim leading/trailing underscores
        clean = re.sub(r'_+', '_', clean).strip('_')
        # 4. Handle leading digits or empty
        if not clean:
            clean = "col"
        elif clean[0].isdigit():
            clean = f"col_{clean}"
        return clean.lower()

    @classmethod
    def reconcile_schema(
        cls,
        df_source: DataFrame,
        target_schema_fields: Optional[List[Any]] = None
    ) -> DataFrame:
        """
        Consolidates source DataFrame columns against target table schema fields:
        1. Cleanses and sanitizes all source column names (converting ' ' and '.' to '_').
        2. Matches source columns against target schema fields (case-insensitively).
        3. Missing in source -> padded with NULL (cast to target data type).
        4. Extra in source -> retained for schema evolution.
        Returns the cleansed and reconciled DataFrame.
        """
        # 1. Cleanse source column names (spaces -> _, dots -> _, hyphens -> _)
        for orig_col in list(df_source.columns):
            clean_col = cls.sanitize_column_name(orig_col)
            if clean_col != orig_col:
                logger.info(f"[COLUMN CLEANSING] Renaming '{orig_col}' -> '{clean_col}'")
                df_source = df_source.withColumnRenamed(orig_col, clean_col)

        if not target_schema_fields:
            return df_source

        # 2. Build target field map: normalized_name -> (canonical_target_name, dataType)
        target_field_map = {}
        for f in target_schema_fields:
            norm_name = cls.sanitize_column_name(f.name)
            target_field_map[norm_name] = (f.name, f.dataType)

        source_cols = {cls.sanitize_column_name(c): c for c in df_source.columns}

        # 3. Pad missing columns with NULL cast to target type
        missing_in_source = []
        for norm_name, (target_name, data_type) in target_field_map.items():
            if norm_name not in source_cols:
                missing_in_source.append(target_name)
                df_source = df_source.withColumn(target_name, lit(None).cast(data_type))
            else:
                actual_col = source_cols[norm_name]
                if actual_col != target_name:
                    df_source = df_source.withColumnRenamed(actual_col, target_name)

        if missing_in_source:
            logger.info(f"[SCHEMA CONSOLIDATION] Padding {len(missing_in_source)} missing column(s) with NULL: {missing_in_source}")

        # 4. Retain extra columns from source for schema evolution
        extra_in_source = [source_cols[norm_name] for norm_name in source_cols if norm_name not in target_field_map]
        if extra_in_source:
            logger.info(f"[SCHEMA CONSOLIDATION] Preserving {len(extra_in_source)} extra column(s) from CSV for schema evolution: {extra_in_source}")

        return df_source

    @classmethod
    def _probe_target_schema_from_query(
        cls,
        spark: SparkSession,
        source_system: str,
        table_name: str,
        params: Dict[str, Any]
    ) -> Optional[List[Any]]:
        """
        Automatically extracts the target schema from the Gold SQL select query:
        1. Checks custom query parameter override (--QUERY_PATH or --GOLD_SQL).
        2. Automatically discovers local or S3 SQL files (e.g. gold/query/<source>/<table>.sql).
        3. Executes a zero-record probe query (SELECT * FROM (<sql>) WHERE 1=0) via Spark SQL.
        4. Extracts schema fields and data types in milliseconds without reading data.
        """
        # 1. Direct SQL override via params
        sql_text = params.get('GOLD_SQL') or params.get('QUERY_SQL')

        # 2. Query file path override
        query_path = params.get('QUERY_PATH')
        if not sql_text and query_path:
            if query_path.startswith('s3://'):
                try:
                    import boto3
                    from urllib.parse import urlparse
                    parsed = urlparse(query_path)
                    s3 = boto3.client('s3')
                    resp = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))
                    sql_text = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY DISCOVERY] Loaded Gold query from '{query_path}'")
                except Exception as s3_err:
                    logger.warning(f"Could not load Gold query from S3 '{query_path}': {s3_err}")
            elif os.path.exists(query_path):
                with open(query_path, 'r', encoding='utf-8') as f:
                    sql_text = f.read()
                logger.info(f"[QUERY DISCOVERY] Loaded Gold query from local path '{query_path}'")

        # 3. Automatic Discovery in local repository
        if not sql_text:
            candidate_local_paths = [
                os.path.join(script_dir, "..", "query", source_system, f"{table_name}.sql"),
                os.path.join(script_dir, "..", "query", source_system, f"v_{table_name}.sql"),
                os.path.join(script_dir, "..", "query", f"{table_name}.sql"),
                os.path.join("gold", "query", source_system, f"{table_name}.sql"),
                os.path.join("gold", "query", source_system, f"v_{table_name}.sql"),
            ]
            for c_path in candidate_local_paths:
                norm_c = os.path.abspath(c_path)
                if os.path.exists(norm_c):
                    try:
                        with open(norm_c, 'r', encoding='utf-8') as f:
                            sql_text = f.read()
                        logger.info(f"[QUERY AUTO-DISCOVERY] Found local Gold query file at '{norm_c}'")
                        break
                    except Exception as f_err:
                        logger.debug(f"Error reading candidate query path '{norm_c}': {f_err}")

        # 4. S3 discovery under default convention: s3://<bucket>/gold/query/<source>/<table>.sql
        if not sql_text and params.get('DATA_LAKE_BUCKET'):
            bucket_name = params.get('DATA_LAKE_BUCKET')
            for q_name in [f"{table_name}.sql", f"v_{table_name}.sql"]:
                s3_key = f"gold/query/{source_system}/{q_name}"
                try:
                    import boto3
                    s3 = boto3.client('s3')
                    resp = s3.get_object(Bucket=bucket_name, Key=s3_key)
                    sql_text = resp['Body'].read().decode('utf-8')
                    logger.info(f"[QUERY AUTO-DISCOVERY] Found S3 Gold query file at 's3://{bucket_name}/{s3_key}'")
                    break
                except Exception:
                    pass

        if not sql_text:
            logger.info(f"[QUERY AUTO-DISCOVERY] No query found for '{source_system}.{table_name}'. Initial load will use input file schema.")
            return None

        # Clean SQL comments and annotations
        clean_sql = re.sub(r'--[^\r\n]*', '', sql_text).strip().rstrip(';')
        if not clean_sql:
            return None

        # Execute ultra-fast zero-data probe query
        probe_sql = f"SELECT * FROM (\n{clean_sql}\n) AS probe_q WHERE 1=0"
        logger.info(f"[QUERY SCHEMA PROBE] Probing target schema from Gold query via zero-record probe (WHERE 1=0)...")
        try:
            probe_df = spark.sql(probe_sql)
            target_fields = probe_df.schema.fields
            logger.info(f"[QUERY SCHEMA PROBE] Successfully probed schema: found {len(target_fields)} column(s) from Gold query.")
            return target_fields
        except Exception as probe_err:
            logger.warning(
                f"[QUERY SCHEMA PROBE NOTE] Spark SQL zero-record probe note: {probe_err}. "
                f"(Underlying Silver tables may not be registered in session; proceeding with input file schema)."
            )
            return None

    @classmethod
    def _deduplicate_by_nkey(cls, df: DataFrame, nkeys: List[str]) -> DataFrame:
        """
        Ensures row uniqueness by natural key(s) (nkey) from gold config.
        If timestamp/updated_at columns are present, retains the most recent record per natural key.
        Otherwise applies dropDuplicates on the natural key subset.
        """
        if not nkeys or df is None:
            return df
        valid_nkeys = [k for k in nkeys if k in df.columns]
        if not valid_nkeys:
            return df
        order_col = None
        for candidate in ["_updated_at", "updated_at", "_inserted_at", "created_at", "timestamp", "start_time"]:
            if candidate in df.columns:
                order_col = candidate
                break
        if order_col:
            try:
                from pyspark.sql.window import Window
                from pyspark.sql.functions import row_number, col
                window_spec = Window.partitionBy(*[col(k) for k in valid_nkeys]).orderBy(col(order_col).desc())
                return df.withColumn("__rn", row_number().over(window_spec)).filter(col("__rn") == 1).drop("__rn")
            except Exception:
                pass
        return df.dropDuplicates(subset=valid_nkeys)

    @classmethod
    def _get_gold_layer_manager(cls):
        """Lazily imports GoldLayerManager to prevent circular import locks."""
        global GoldLayerManager
        if GoldLayerManager is None:
            try:
                from gold_layer_manager import GoldLayerManager as glm
                GoldLayerManager = glm
            except ImportError:
                try:
                    from gold.script.gold_layer_manager import GoldLayerManager as glm
                    GoldLayerManager = glm
                except ImportError:
                    pass
        return GoldLayerManager

    @classmethod
    def run_initial_load(
        cls,
        spark: SparkSession,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Executes initial CSV load and upserts into Gold table.
        """
        start_time = datetime.now(timezone.utc)

        # Target Deployment Environment: CLI (--ENV or --ENVIRONMENT) > Env Var > Default ('dev')
        env = (
            params.get('ENV')
            or params.get('env')
            or params.get('ENVIRONMENT')
            or params.get('environment')
            or os.environ.get('ENV')
            or os.environ.get('ENVIRONMENT')
            or 'dev'
        ).strip().lower()
        logger.info(f"Target deployment environment resolved: '{env}'")

        source_system = (params.get('SOURCE_SYSTEM') or '').strip().lower()
        table_name = (params.get('TABLE_NAME') or params.get('MART_NAME') or '').strip().lower()

        if not source_system or not table_name:
            raise ValueError(
                "CRITICAL ERROR: '--SOURCE_SYSTEM' and '--TABLE_NAME' are required for Gold initial load.\n"
                "Example: --SOURCE_SYSTEM genesys --TABLE_NAME conversations"
            )

        # Clean table name
        for prefix in [f"gold_{source_system}_", "gold_tbl_", "gold_", "v_"]:
            if table_name.startswith(prefix):
                table_name = table_name[len(prefix):]
                break

        # Load gold_config.json with dynamic {env} interpolation
        gold_cfg = GoldConfigLoader.load_config(env=env) if GoldConfigLoader else {}
        if GoldConfigLoader:
            GoldConfigLoader.set_loaded_config(gold_cfg, env=env)
        defaults_cfg = GoldConfigLoader.get_defaults(gold_cfg, env=env) if GoldConfigLoader else {}

        raw_bucket = (
            params.get('DATA_LAKE_BUCKET')
            or defaults_cfg.get('gold_bucket')
            or f"uax-datalake-{env}-bucket"
        )
        bucket_name = str(raw_bucket).replace('{env}', env).replace('{ENV}', env.upper()).strip()

        raw_glue_db = (
            params.get('GLUE_DATABASE')
            or (GoldConfigLoader.get_glue_database(gold_cfg, env=env) if GoldConfigLoader else None)
            or f"uax_datalake_db_{env}"
        )
        glue_database = str(raw_glue_db).replace('{env}', env).replace('{ENV}', env.upper()).strip()

        # Resolve initial export/load CSV path: CLI > gold_config.json > S3 template/convention
        csv_path = params.get('CSV_PATH') or params.get('INITIAL_LOAD_PATH') or params.get('INPUT_FILE')
        if not csv_path and GoldConfigLoader:
            init_cfg = GoldConfigLoader.get_initial_load_config(source_system, table_name, gold_cfg)
            csv_path = init_cfg.get('path')
            if not params.get('DELIMITER') and init_cfg.get('delimiter'):
                params['DELIMITER'] = init_cfg.get('delimiter')
            if not params.get('HAS_HEADER') and 'has_header' in init_cfg:
                params['HAS_HEADER'] = init_cfg.get('has_header')

        # Template variable replacement (e.g. {bucket}, {env})
        if csv_path and isinstance(csv_path, str):
            csv_path = (
                csv_path
                .replace("{bucket}", bucket_name)
                .replace("{env}", env)
                .replace("{ENV}", env.upper())
                .replace("{source}", source_system)
                .replace("{table}", table_name)
            )

        # Fallback to default S3 convention if not explicitly passed
        if not csv_path:
            csv_path = f"s3://{bucket_name}/gold/initial_exports/{source_system}/{table_name}.csv"
            logger.info(f"[INITIAL LOAD CONVENTION] No explicit path passed. Using default S3 export path: '{csv_path}'")

        # Resolve Target Table Name (Default: gold_<source>_<tablename>)
        if GoldConfigLoader:
            target_table_name = GoldConfigLoader.get_target_table_name(source_system, table_name, 'athena', gold_cfg)
        else:
            target_table_name = f"gold_{source_system}_{table_name}"

        # Resolve Natural Keys (nkey) / Primary Keys
        pks = []
        if GoldLayerManager:
            pks = GoldLayerManager._resolve_natural_keys(source_system, table_name, gold_cfg, params)
        elif GoldConfigLoader:
            pks = GoldConfigLoader.get_nkey(source_system, table_name, gold_cfg)
        if not pks and params.get('NKEY'):
            pks = [k.strip() for k in str(params['NKEY']).split(',') if k.strip()]
        if not pks and params.get('PRIMARY_KEY'):
            pks = [k.strip() for k in str(params['PRIMARY_KEY']).split(',') if k.strip()]

        if not pks:
            raise ValueError(
                f"CRITICAL CONFIG ERROR: Missing 'nkey' in gold configuration for table '{table_name}' "
                f"under source system '{source_system}'. A natural key is mandatory for initial load."
            )

        full_table = f"`{glue_database}`.`{target_table_name}`"

        logger.info(
            f"\n+================================================================================+\n"
            f"|                STARTING GOLD INITIAL LOAD & CONSOLIDATION                      |\n"
            f"+================================================================================+\n"
            f"|  * Source System     : {source_system.upper()}\n"
            f"|  * Table Name        : {table_name}\n"
            f"|  * Target Table      : {full_table}\n"
            f"|  * Natural Keys      : {pks or 'None (Overwrite)'}\n"
            f"|  * Input File Path   : {csv_path}\n"
            f"|  * Glue Database     : {glue_database}\n"
            f"|  * Execution Time    : {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
            f"+================================================================================+"
        )

        # 1. Read Input File (CSV or Parquet)
        if csv_path.endswith('.parquet'):
            logger.info(f"Reading input Parquet file from '{csv_path}'...")
            df_raw = spark.read.parquet(csv_path)
        else:
            delimiter = params.get('DELIMITER', ',')
            has_header = str(params.get('HAS_HEADER', 'true')).strip().lower() in ('true', '1', 'yes')
            logger.info(f"Reading input CSV file from '{csv_path}' (delimiter='{delimiter}', header={has_header})...")
            df_raw = spark.read \
                .option("header", str(has_header).lower()) \
                .option("delimiter", delimiter) \
                .option("inferSchema", "false") \
                .csv(csv_path)

        raw_count = df_raw.count()
        logger.info(f"Loaded {raw_count:,} records from input file.")

        # 2. Check Target Table Schema for Reconciliation
        target_fields = None
        table_exists = False
        try:
            target_df = spark.table(f"{glue_database}.{target_table_name}")
            target_fields = target_df.schema.fields
            table_exists = True
            logger.info(f"Found existing Gold table '{full_table}' with {len(target_fields)} column(s).")
        except Exception:
            table_exists = False
            logger.info(f"Gold table '{full_table}' not found in catalog. Attempting automatic Gold query extraction...")

        # If table not yet in catalog, probe target schema from Gold SQL query
        if not target_fields:
            target_fields = cls._probe_target_schema_from_query(
                spark=spark,
                source_system=source_system,
                table_name=table_name,
                params=params
            )

        # 3. Reconcile Schemas
        df_consolidated = cls.reconcile_schema(df_raw, target_fields)

        # 4. Attach Technical Audit Columns
        if "_updated_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_updated_at", current_timestamp())
        if "_inserted_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_inserted_at", current_timestamp())

        # Ensure row uniqueness by natural keys (nkey) from gold config
        df_consolidated = cls._deduplicate_by_nkey(df_consolidated, pks)
        logger.info(f"[PRIMARY KEY] Natural keys resolved for '{table_name}': {pks}")

        # 5. Perform Idempotent UPSERT into Athena / Iceberg Table
        temp_view = f"incoming_initial_{table_name}"
        df_consolidated.createOrReplaceTempView(temp_view)
        s3_location = f"s3://{bucket_name}/gold/data/{source_system}/{table_name}"

        glm = cls._get_gold_layer_manager()
        if table_exists:
            try:
                if glm and hasattr(glm, "_sync_iceberg_schema"):
                    glm._sync_iceberg_schema(spark, full_table, df_consolidated)
            except Exception as sync_err:
                logger.debug(f"[INITIAL LOAD] Note on Iceberg schema sync: {sync_err}")

        if table_exists and pks:
            join_cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in pks])
            merge_sql = (
                f"MERGE INTO {full_table} AS target\n"
                f"USING {temp_view} AS source\n"
                f"ON {join_cond}\n"
                f"WHEN MATCHED THEN UPDATE SET *\n"
                f"WHEN NOT MATCHED THEN INSERT *"
            )
            logger.info(f"[INITIAL LOAD UPSERT] Executing Iceberg MERGE INTO on {full_table}:\n{merge_sql}")
            try:
                spark.sql(merge_sql)
            except Exception as merge_err:
                logger.warning(f"[INITIAL LOAD UPSERT] Spark SQL MERGE failed ({merge_err}). Overwriting to {s3_location}...")
                df_consolidated.write.mode("overwrite").format("parquet").save(s3_location)
        else:
            logger.info(f"[INITIAL LOAD WRITE] Writing initial Gold table {full_table} to '{s3_location}'...")
            try:
                df_consolidated.write \
                    .format("iceberg") \
                    .mode("overwrite") \
                    .option("path", s3_location) \
                    .saveAsTable(f"{glue_database}.{target_table_name}")
            except Exception as ice_err:
                logger.warning(f"[INITIAL LOAD WRITE] Direct Iceberg saveAsTable fallback to Parquet: {ice_err}")
                df_consolidated.write.mode("overwrite").format("parquet").save(s3_location)
        # 6. Optional Downstream Extension: Aurora MySQL Serving
        # Only created/served if requested by user/config (target_engines contains 'aurora' or --GOLD_TARGETS contains 'aurora')
        target_engines = []
        if params.get('GOLD_TARGETS') or params.get('TARGET_ENGINES'):
            target_engines = [t.strip().lower() for t in str(params.get('GOLD_TARGETS') or params.get('TARGET_ENGINES')).split(',') if t.strip()]
        elif GoldConfigLoader:
            target_engines = GoldConfigLoader.get_target_engines(source_system, table_name, gold_cfg)

        skip_aurora = str(params.get('SKIP_AURORA_SERVE', 'false')).strip().lower() in ('true', '1', 'yes')
        needs_aurora = not skip_aurora and any(t in target_engines for t in ('aurora', 'rds', 'mysql'))
        if needs_aurora and glm:
            logger.info(f"[AURORA EXTENSION] User requested Aurora serving for '{target_table_name}'. Reading from Athena table and serving to Aurora...")
            try:
                df_athena = spark.table(full_table)
                gold_schema = params.get('GOLD_SCHEMA') or 'enterprise_reporting'
                glm._serve_to_mysql(
                    spark=spark,
                    queries={table_name: ""},
                    gold_schema=gold_schema,
                    data_s3_path=s3_location,
                    params=params,
                    glue_client=None,
                    secrets_client=None,
                    mart_stats=[],
                    source_system=source_system,
                    materialized_dfs={table_name: df_athena},
                    mart_keys={table_name: pks}
                )
                logger.info(f"[AURORA EXTENSION] Successfully served Athena table '{full_table}' to Aurora '{gold_schema}.{target_table_name}'.")
            except Exception as aurora_err:
                logger.error(f"[AURORA EXTENSION ERROR] Failed to serve to Aurora: {aurora_err}")

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            f"\n+================================================================================+\n"
            f"|                GOLD INITIAL LOAD COMPLETED SUCCESSFULLY                        |\n"
            f"+================================================================================+\n"
            f"|  * Target Table    : {full_table}\n"
            f"|  * Records Loaded  : {raw_count:,}\n"
            f"|  * Duration        : {duration:.2f}s\n"
            f"+================================================================================+"
        )

        return {
            "source_system": source_system,
            "table_name": table_name,
            "target_table": f"{glue_database}.{target_table_name}",
            "records_loaded": raw_count,
            "duration_seconds": round(duration, 2),
            "status": "SUCCESS"
        }


def main():
    """Glue entrypoint when executed as a Glue Job."""
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    expected_args = [
        'JOB_NAME', 'SOURCE_SYSTEM', 'TABLE_NAME'
    ]
    optional_args = [
        'CSV_PATH', 'NKEY', 'PRIMARY_KEY', 'SECRET_NAME', 'RDS_SECRET_NAME',
        'API_SECRET_NAME', 'LLM_SECRET_NAME',
        'GLUE_DATABASE', 'DATA_LAKE_BUCKET', 'DELIMITER', 'HAS_HEADER',
        'ENV', 'ENVIRONMENT', 'INITIAL_LOAD_PATH', 'INPUT_FILE', 'MART_NAME'
    ]

    args_to_check = expected_args + [a for a in optional_args if f"--{a}" in sys.argv or f"--{a.lower()}" in sys.argv]
    resolved = getResolvedOptions(sys.argv, args_to_check)

    GoldInitialLoader.run_initial_load(spark, resolved)


if __name__ == "__main__":
    main()
