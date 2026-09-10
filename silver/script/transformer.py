"""
Silver Layer Transformation Engine.

Applies declarative transformations configured in silver_config.json:
- filter_expression (SQL filter clause)
- column_casts (data type casting e.g. integer, double, timestamp)
- column_renames (renaming dictionary)
- custom_expressions (PySpark SQL expressions)
- drop_columns (list of columns to exclude)
- custom_transform_script (dynamic external PySpark transformation module invocation)
"""

import os
import sys
import logging
import importlib.util
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, when, current_timestamp, upper, coalesce, to_timestamp

logger = logging.getLogger(__name__)


class SilverTransformer:
    """
    Applies declarative transformations, technical audit columns (_valid_from, _valid_to, _is_current,
    _is_deleted, _inserted_at, _updated_at), and custom Python transformation files to Silver DataFrames.
    Guarantees:
    1. Bronze layer system metadata columns are stripped and never passed into Silver tables.
    2. All Silver system-generated audit columns are appended at the very end of the tables.
    """

    @classmethod
    def apply_transformations(
        cls,
        df: DataFrame,
        source_system: str,
        table_name: str,
        table_cfg: Dict[str, Any],
        order_col_name: Optional[str] = None,
        nkeys: Optional[Any] = None,
        spark=None
    ) -> DataFrame:
        """
        Executes all configured declarative and custom file transformations for a table.
        Strips Bronze system columns and enriches DataFrame with Silver technical audit columns.
        """
        logger.info(f"Applying transformations for '{source_system}.{table_name}'...")

        # 1. Resolve primary ordering column if not explicitly supplied
        if not order_col_name:
            cfg_order = table_cfg.get('deduplication_order_by') or table_cfg.get('order_by')
            if cfg_order:
                order_col_name = cfg_order[0] if isinstance(cfg_order, list) else cfg_order

        # 2. Strip Bronze system-generated columns so they NEVER pass into Silver
        bronze_system_cols = {'_ingested_at', '_source_system', '_table_name', '_execution_id', '_batch_id', '_raw_payload'}
        bronze_drops = [c for c in df.columns if c in bronze_system_cols]
        if bronze_drops:
            logger.info(f"Removing Bronze system metadata columns from Silver processing: {bronze_drops}")
            df = df.drop(*bronze_drops)

        # 3. Apply Filter Expression if specified
        filter_expr = table_cfg.get('filter_expression')
        if filter_expr and isinstance(filter_expr, str) and filter_expr.strip():
            logger.info(f"Applying filter expression: '{filter_expr}'")
            df = df.filter(expr(filter_expr))

        # 4. Apply Column Casts
        column_casts = table_cfg.get('column_casts', {})
        for column_name, target_type in column_casts.items():
            if column_name in df.columns:
                logger.info(f"Casting column '{column_name}' -> '{target_type}'")
                df = df.withColumn(column_name, col(column_name).cast(target_type))

        # 5. Apply Custom SQL Expressions
        custom_expressions = table_cfg.get('custom_expressions', {})
        for new_col, sql_expr in custom_expressions.items():
            logger.info(f"Adding derived column '{new_col}' = expr('{sql_expr}')")
            df = df.withColumn(new_col, expr(sql_expr))

        # 6. Apply Column Renames
        column_renames = table_cfg.get('column_renames', {})
        for old_name, new_name in column_renames.items():
            if old_name in df.columns:
                logger.info(f"Renaming column '{old_name}' -> '{new_name}'")
                df = df.withColumnRenamed(old_name, new_name)

        # 7. Exclude Columns if configured in silver_config.json (protecting key columns and technical audit columns)
        exclude_cfg = table_cfg.get('exclude_columns') or table_cfg.get('drop_columns') or []
        if isinstance(exclude_cfg, str):
            exclude_cfg = [c.strip() for c in exclude_cfg.split(',') if c.strip()]
        elif not isinstance(exclude_cfg, list):
            exclude_cfg = []

        nkey_list = nkeys if isinstance(nkeys, list) else ([nkeys] if nkeys else [])
        if not nkey_list:
            cfg_nkey = table_cfg.get('nkey') or table_cfg.get('deduplication_keys') or table_cfg.get('primary_key')
            if cfg_nkey:
                nkey_list = cfg_nkey if isinstance(cfg_nkey, list) else [cfg_nkey]

        protected_cols = set(nkey_list)
        if order_col_name:
            protected_cols.add(order_col_name)

        cols_to_exclude = [c for c in exclude_cfg if c in df.columns and c not in protected_cols]
        if cols_to_exclude:
            logger.info(f"Excluding configured columns from Silver table '{table_name}': {cols_to_exclude}")
            df = df.drop(*cols_to_exclude)

        attempted_key_drops = [c for c in exclude_cfg if c in protected_cols]
        if attempted_key_drops:
            logger.warning(
                f"Protected key/order column(s) {attempted_key_drops} cannot be excluded from Silver table '{table_name}'. "
                f"These columns are required for natural key identification and deduplication."
            )

        # 8. Apply Custom External Transformation File if configured
        custom_script_path = table_cfg.get('custom_transform_script') or table_cfg.get('custom_transform_file')
        if custom_script_path:
            df = cls._apply_custom_script(df, custom_script_path, spark)

        # 9. Enrich Technical Audit Columns & Place them at the very end
        high_date_val = table_cfg.get('high_date_value', '9999-01-01 00:00:00')
        df = cls._enrich_technical_columns(df, order_col_name=order_col_name, high_date_val=high_date_val)

        return df

    @classmethod
    def _enrich_technical_columns(
        cls,
        df: DataFrame,
        order_col_name: Optional[str] = None,
        high_date_val: str = '9999-01-01 00:00:00'
    ) -> DataFrame:
        """
        Guarantees that standard Silver technical audit columns are consistently attached:
        - _valid_from: Effective start timestamp (order column or current_timestamp()).
        - _valid_to: Effective end timestamp (high-date '9999-01-01 00:00:00').
        - _is_current: 'Y' if active/current, 'N' if expired/historical.
        - _is_deleted: 'Y' if soft-deleted / inactive, otherwise 'N'.
        - _inserted_at: Initialized with current_timestamp().
        - _updated_at: Initialized with current_timestamp().

        CRITICAL: All system-generated audit columns are appended at the very LAST of the table.
        """
        cols = df.columns

        # 1. Effective Validity Window (_valid_from, _valid_to) & Current Flag (_is_current: 'Y'/'N')
        if order_col_name and order_col_name in cols:
            df = df.withColumn("_valid_from", coalesce(col(order_col_name).cast("timestamp"), current_timestamp()))
        elif "_valid_from" not in cols:
            df = df.withColumn("_valid_from", current_timestamp())

        if "_valid_to" not in cols:
            df = df.withColumn("_valid_to", to_timestamp(lit(high_date_val)))

        # Current flag: 'Y' for active/current record, 'N' for expired/historical
        if "_is_current" not in cols:
            df = df.withColumn("_is_current", lit("Y"))
        else:
            df = df.withColumn(
                "_is_current",
                when(upper(col("_is_current").cast("string")).isin("Y", "TRUE", "1"), lit("Y")).otherwise(lit("N"))
            )

        # 2. Soft-deletion status (_is_deleted: 'Y'/'N')
        if "_is_deleted" in cols:
            df = df.withColumn(
                "_is_deleted",
                when(upper(col("_is_deleted").cast("string")).isin("Y", "TRUE", "1"), lit("Y")).otherwise(lit("N"))
            )
        elif "sys_is_deleted" in cols:
            df = df.withColumn(
                "_is_deleted",
                when(upper(col("sys_is_deleted").cast("string")).isin("Y", "TRUE", "1"), lit("Y")).otherwise(lit("N"))
            )
        elif "is_deleted" in cols:
            df = df.withColumn(
                "_is_deleted",
                when(upper(col("is_deleted").cast("string")).isin("Y", "TRUE", "1"), lit("Y")).otherwise(lit("N"))
            )
        elif "deleted" in cols:
            df = df.withColumn(
                "_is_deleted",
                when(upper(col("deleted").cast("string")).isin("Y", "TRUE", "1"), lit("Y")).otherwise(lit("N"))
            )
        else:
            df = df.withColumn("_is_deleted", lit("N"))

        # 3. Technical timestamps (_inserted_at, _updated_at)
        if "_inserted_at" not in cols:
            df = df.withColumn("_inserted_at", current_timestamp())

        df = df.withColumn("_updated_at", current_timestamp())

        # 4. Strip any Bronze metadata columns so they never enter Silver tables
        bronze_system_cols = {'_ingested_at', '_source_system', '_table_name', '_execution_id', '_batch_id', '_raw_payload'}
        remaining_bronze = [c for c in df.columns if c in bronze_system_cols]
        if remaining_bronze:
            df = df.drop(*remaining_bronze)

        # 5. Column Ordering: All business payload columns FIRST, system generated audit columns LAST
        silver_tech_cols = ['_valid_from', '_valid_to', '_is_current', '_is_deleted', '_inserted_at', '_updated_at']
        business_cols = [c for c in df.columns if c not in silver_tech_cols]
        df = df.select(*(business_cols + silver_tech_cols))

        return df

    @classmethod
    def _apply_custom_script(cls, df: DataFrame, script_path: str, spark=None) -> DataFrame:
        """
        Dynamically loads and calls the transform(df, spark) function from a custom Python file.
        """
        logger.info(f"Loading custom transform script: '{script_path}'...")
        
        # Resolve absolute path
        abs_script_path = script_path
        if not os.path.isabs(script_path):
            current_dir = os.path.dirname(os.path.abspath(__file__))
            abs_script_path = os.path.join(current_dir, script_path)

        if not os.path.exists(abs_script_path):
            logger.warning(f"Custom transform script file not found at '{abs_script_path}'. Skipping custom file transform.")
            return df

        try:
            module_name = f"custom_transform_{os.path.splitext(os.path.basename(script_path))[0]}"
            spec = importlib.util.spec_from_file_location(module_name, abs_script_path)
            custom_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(custom_module)

            if hasattr(custom_module, 'transform'):
                logger.info(f"Invoking custom transform() function in '{script_path}'...")
                df = custom_module.transform(df, spark)
            else:
                logger.warning(f"Custom script '{script_path}' does not define a 'transform(df, spark)' function.")

        except Exception as err:
            logger.error(f"Error executing custom transform script '{script_path}': {err}")
            raise

        return df
