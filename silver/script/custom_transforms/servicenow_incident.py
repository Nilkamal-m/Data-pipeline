"""
Sample Custom Transformation Script for ServiceNow Incident Table.

Demonstrates:
1. Inter-table lookup / join: Calling another Silver or Bronze table via `spark` and `context`.
2. Extensible API enrichment hook: Populating new column(s) with a NULL string default value (lit(None).cast("string")).
3. High-visibility logging of external tables called and columns added.

Custom transformation logic can define any of:
- `transform(df, spark, context)` -> Full access to SparkSession and runtime execution metadata.
- `transform(df, spark)`
- `transform(df)`
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, current_timestamp, lit

logger = logging.getLogger(__name__)


def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    """
    Custom transformation entry point invoked by Silver Iceberg ETL engine.

    Args:
        df (DataFrame): PySpark DataFrame after deduplication and declarative transforms.
        spark (SparkSession, optional): Active SparkSession with Glue / Iceberg catalog connectivity.
        context (dict, optional): Runtime execution metadata containing:
            - 'glue_database': Target Glue database name
            - 'source_system': e.g. 'servicenow'
            - 'table_name': e.g. 'incident'
            - 'data_lake_bucket': S3 bucket name
            - 'silver_data_prefix': Silver prefix in S3

    Returns:
        DataFrame: Transformed PySpark DataFrame.
    """
    initial_cols = set(df.columns)
    logger.info("[CUSTOM TRANSFORM] Starting custom transformation logic for ServiceNow incident...")

    # --------------------------------------------------------------------------
    # 1. Inter-Table Querying Example:
    # If this table needs data from another table (e.g. user lookup / department lookup),
    # the script can query that table directly using `spark.table(...)` or `spark.read`:
    # --------------------------------------------------------------------------
    if spark is not None and context is not None:
        glue_db = context.get('glue_database', 'silver_db')
        user_table_name = f"glue_catalog.{glue_db}.tbl_sys_user"

        try:
            # Check if reference table is registered and queryable
            if spark.catalog.tableExists(user_table_name):
                logger.info(f"[CUSTOM TRANSFORM - TABLE CALL] Querying reference table '{user_table_name}'...")
                df_users = spark.table(user_table_name).select(
                    col("sys_id").alias("user_ref_sys_id"),
                    col("department").alias("caller_department_name"),
                    col("email").alias("caller_email_address")
                )
                if "caller_id" in df.columns:
                    df = df.join(df_users, df["caller_id"] == df_users["user_ref_sys_id"], "left").drop("user_ref_sys_id")
                    logger.info(f"[CUSTOM TRANSFORM - TABLE JOIN] Successfully joined with '{user_table_name}' on caller_id.")
            else:
                logger.info(f"[CUSTOM TRANSFORM - TABLE CALL] Reference table '{user_table_name}' not yet created in catalog. Skipping join.")
        except Exception as e:
            logger.warning(f"[CUSTOM TRANSFORM - TABLE CALL] Optional reference table lookup encountered: {e}. Skipping join.")

    # --------------------------------------------------------------------------
    # 2. Field Cleansing & Derived Columns:
    # Uppercase urgency code and business derivations
    # --------------------------------------------------------------------------
    if "urgency" in df.columns:
        df = df.withColumn("urgency_code", upper(col("urgency")))

    # --------------------------------------------------------------------------
    # 3. Future API Enrichment Hook:
    # A future team can call an external API by passing designated source columns
    # (e.g. caller_id, category, description) and storing the returned value.
    # As of now, initialize the column with a NULL string default value.
    # --------------------------------------------------------------------------
    if "api_enrichment_field" not in df.columns:
        logger.info("[CUSTOM TRANSFORM - API HOOK] Initializing 'api_enrichment_field' with NULL string default value.")
        df = df.withColumn("api_enrichment_field", lit(None).cast("string"))

    # Technical transform timestamp
    df = df.withColumn("_transformed_at", current_timestamp())

    new_cols = [c for c in df.columns if c not in initial_cols]
    logger.info(f"[CUSTOM TRANSFORM] Completed. New column(s) added by custom script: {new_cols}")

    return df
