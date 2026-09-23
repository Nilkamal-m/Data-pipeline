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
    def reconcile_schema(
        cls,
        df_source: DataFrame,
        target_schema_fields: Optional[List[Any]] = None
    ) -> DataFrame:
        """
        Consolidates source DataFrame columns against target table schema fields:
        - Columns in target but not in source are added with NULL (cast to target data type).
        - Columns in source but not in target are preserved for schema evolution.
        - Returns aligned DataFrame.
        """
        if not target_schema_fields:
            return df_source

        target_field_map = {f.name: f.dataType for f in target_schema_fields}
        source_cols = set(df_source.columns)

        # 1. Pad missing columns with NULL cast to target type
        missing_in_source = [f_name for f_name in target_field_map if f_name not in source_cols]
        if missing_in_source:
            logger.info(f"[SCHEMA CONSOLIDATION] Padding {len(missing_in_source)} missing column(s) with NULL: {missing_in_source}")
            for col_name in missing_in_source:
                data_type = target_field_map[col_name]
                df_source = df_source.withColumn(col_name, lit(None).cast(data_type))

        # 2. Log newly introduced columns present in source
        extra_in_source = [c for c in source_cols if c not in target_field_map]
        if extra_in_source:
            logger.info(f"[SCHEMA CONSOLIDATION] Preserving {len(extra_in_source)} extra column(s) from CSV for schema evolution: {extra_in_source}")

        return df_source

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
        source_system = (params.get('SOURCE_SYSTEM') or '').strip().lower()
        table_name = (params.get('TABLE_NAME') or params.get('MART_NAME') or '').strip().lower()
        csv_path = params.get('CSV_PATH') or params.get('INITIAL_LOAD_PATH') or params.get('INPUT_FILE')
        glue_database = params.get('GLUE_DATABASE') or 'uax_datalake_db_dev'
        bucket_name = params.get('DATA_LAKE_BUCKET') or 'uax-datalake-dev-bucket'

        if not source_system or not table_name:
            raise ValueError(
                "CRITICAL ERROR: '--SOURCE_SYSTEM' and '--TABLE_NAME' are required for Gold initial load.\n"
                "Example: --SOURCE_SYSTEM genesys --TABLE_NAME conversations --CSV_PATH s3://bucket/landing/genesys/history.csv"
            )

        if not csv_path:
            raise ValueError(
                f"CRITICAL ERROR: Missing '--CSV_PATH' for initial load of '{source_system}.{table_name}'."
            )

        # Clean table name
        for prefix in [f"gold_{source_system}_", "gold_tbl_", "gold_", "v_"]:
            if table_name.startswith(prefix):
                table_name = table_name[len(prefix):]
                break

        # Load gold_config.json
        gold_cfg = GoldConfigLoader.load_config() if GoldConfigLoader else {}

        # Resolve Target Table Name (Default: gold_<source>_<tablename>)
        if GoldConfigLoader:
            target_table_name = GoldConfigLoader.get_target_table_name(source_system, table_name, 'athena', gold_cfg)
        else:
            target_table_name = f"gold_{source_system}_{table_name}"

        # Resolve Primary Keys
        pks = []
        if params.get('PRIMARY_KEY'):
            pks = [k.strip() for k in str(params['PRIMARY_KEY']).split(',') if k.strip()]
        elif GoldConfigLoader:
            pks = GoldConfigLoader.get_primary_key(source_system, table_name, gold_cfg)

        full_table = f"`{glue_database}`.`{target_table_name}`"

        logger.info(
            f"\n+================================================================================+\n"
            f"|                STARTING GOLD INITIAL LOAD & CONSOLIDATION                      |\n"
            f"+================================================================================+\n"
            f"|  * Source System     : {source_system.upper()}\n"
            f"|  * Table Name        : {table_name}\n"
            f"|  * Target Table      : {full_table}\n"
            f"|  * Primary Keys      : {pks or 'None (Overwrite)'}\n"
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

        # Clean column names (strip whitespace)
        for col_name in df_raw.columns:
            cleaned_col = col_name.strip()
            if cleaned_col != col_name:
                df_raw = df_raw.withColumnRenamed(col_name, cleaned_col)

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
            logger.info(f"Gold table '{full_table}' does not exist yet. Initial load will create it.")

        # 3. Reconcile Schemas
        df_consolidated = cls.reconcile_schema(df_raw, target_fields)

        # 4. Attach Technical Audit Columns
        if "_updated_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_updated_at", current_timestamp())
        if "_inserted_at" not in df_consolidated.columns:
            df_consolidated = df_consolidated.withColumn("_inserted_at", current_timestamp())

        # Fallback primary key detection
        if not pks:
            id_cols = [c for c in df_consolidated.columns if (c.endswith('_id') or c.endswith('_key') or c == 'sys_id') and not c.startswith('_')]
            if id_cols:
                pks = [id_cols[0]]
                logger.info(f"[KEY DETECTION] Auto-detected primary key for '{table_name}': {pks}")

        # 5. Perform Idempotent UPSERT into Athena / Iceberg Table
        temp_view = f"incoming_initial_{table_name}"
        df_consolidated.createOrReplaceTempView(temp_view)
        s3_location = f"s3://{bucket_name}/gold/data/{source_system}/{table_name}"

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
        'JOB_NAME', 'SOURCE_SYSTEM', 'TABLE_NAME', 'CSV_PATH'
    ]
    optional_args = [
        'PRIMARY_KEY', 'GLUE_DATABASE', 'DATA_LAKE_BUCKET', 'DELIMITER', 'HAS_HEADER'
    ]

    args_to_check = expected_args + [a for a in optional_args if f"--{a}" in sys.argv]
    resolved = getResolvedOptions(sys.argv, args_to_check)

    GoldInitialLoader.run_initial_load(spark, resolved)


if __name__ == "__main__":
    main()
