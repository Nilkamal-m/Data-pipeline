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
import inspect
import importlib.util
import re
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, when, current_timestamp, upper, coalesce, to_timestamp, regexp_replace

logger = logging.getLogger(__name__)


class SilverTransformer:
    """
    Applies declarative transformations, technical audit columns (_valid_from, _valid_to, _is_current,
    _is_deleted, _inserted_at, _updated_at), API enrichment hooks, and custom Python transformation
    files to Silver DataFrames.
    Guarantees:
    1. Bronze layer system metadata columns are stripped and never passed into Silver tables.
    2. Comprehensive column schema introspection is logged before and after transformations.
    3. Custom transformation scripts receive the active SparkSession and runtime execution context to query other tables.
    4. Extensible API enrichment hook attaches configured columns with a NULL string default value.
    5. All Silver system-generated audit columns are appended at the very end of the tables.
    """

    @classmethod
    def log_schema_introspection(cls, df: DataFrame, title: str) -> None:
        """
        Logs a clean, structured schema breakdown of all columns and data types in the DataFrame.
        Enables developers to quickly audit column presence, data types, and debug schema issues.
        """
        fields = df.schema.fields
        tech_cols = {'_valid_from', '_valid_to', '_is_current', '_is_deleted', '_inserted_at', '_updated_at'}
        payload_fields = [f for f in fields if f.name not in tech_cols]
        audit_fields = [f for f in fields if f.name in tech_cols]

        lines = [
            "+--------------------------------------------------------------------------------+",
            f"| [SCHEMA INTROSPECTION] {title[:66]}",
            "+--------------------------------------------------------------------------------+",
            f"| Total Column Count: {len(fields)} (Payload: {len(payload_fields)}, System Audit: {len(audit_fields)})",
            "|"
        ]

        if payload_fields:
            lines.append(f"| Business / Payload Columns ({len(payload_fields)}):")
            for f in payload_fields:
                lines.append(f"|   |-- {f.name:<32} : {f.dataType.simpleString()}")

        if audit_fields:
            lines.append("|")
            lines.append(f"| Silver Technical Audit Columns ({len(audit_fields)} - Placed Last):")
            for f in audit_fields:
                lines.append(f"|   |-- {f.name:<32} : {f.dataType.simpleString()}")

        lines.append("+--------------------------------------------------------------------------------+")
        logger.info("\n".join(lines))

    @classmethod
    def clean_illegal_chars(cls, df: Any, pattern: Optional[str] = None) -> Any:
        """
        Cleans illegal characters using the expression provided from configuration.
        If no expression/pattern is provided, ignores and returns DataFrame unchanged.

        Supports both pandas DataFrames (via applymap/map) and PySpark DataFrames (via dynamic regexp_replace).
        """
        if not pattern or not isinstance(pattern, str) or not pattern.strip():
            return df

        def remove_illegal_chars(value):
            if isinstance(value, str):
                return re.sub(pattern, '', value)
            return value

        # Pandas DataFrame support (via applymap or map)
        if hasattr(df, 'applymap'):
            try:
                return df.applymap(remove_illegal_chars)
            except Exception:
                pass
        if hasattr(df, 'map') and not hasattr(df, '_jdf'):
            try:
                return df.map(remove_illegal_chars)
            except Exception:
                pass

        # PySpark DataFrame support (native distributed regex across all columns dynamically)
        if hasattr(df, 'columns') and hasattr(df, 'withColumn'):
            for col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    regexp_replace(col(col_name).cast("string"), pattern, '')
                )
            return df

        return df

    @classmethod
    def apply_transformations(
        cls,
        df: DataFrame,
        table_cfg: Dict[str, Any],
        source_system: str,
        table_name: str,
        spark=None,
        nkeys=None,
        bronze_metadata_cols=None,
        order_col_name: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Applies declarative transformations in strict deterministic order:
        1. Custom Transform Script (table-specific Python script)
        2. Bronze System Metadata Exclusions (drops ingest runtime columns)
        3. Default String Column Casting (casts all incoming business payload columns to string)
        4. Clean Illegal Characters (dynamically applied if expression is configured for this table)
        5. Filter Expression
        6. Column Casts
        7. Custom SQL Expressions
        8. Column Renames
        9. Exclude Columns (drops unneeded columns, protecting keys & audit cols)
        10. Append Silver Technical Audit Metadata
        """
        logger.info(f"Applying Silver transformations for '{source_system}.{table_name}'...")

        # 1. Log incoming schema introspection
        cls.log_schema_introspection(df, f"Incoming Schema: '{source_system}.{table_name}'")

        if not order_col_name:
            cfg_order = table_cfg.get('deduplication_order_by') or table_cfg.get('order_by')
            if cfg_order:
                order_col_name = cfg_order[0] if isinstance(cfg_order, list) else cfg_order

        # 1. Drop Bronze System Metadata Columns
        drops = bronze_metadata_cols or [
            '_raw_data', '_ingest_timestamp', '_extracted_at', '_batch_id',
            '_source_system', '_source_table', '_file_name', '_file_path', '_row_num',
            '_ingested_at', '_table_name', '_execution_id', '_raw_payload'
        ]
        bronze_drops = [c for c in drops if c in df.columns]
        if bronze_drops:
            logger.info(f"Removing Bronze system metadata columns from Silver processing: {bronze_drops}")
            df = df.drop(*bronze_drops)

        # 3. Make all columns string by default (unless explicitly cast in column_casts)
        # Guarantees schema consistency across batches and prevents type conflicts with nested structs/primitives
        cast_all_str = table_cfg.get('cast_all_columns_to_string', True)
        if cast_all_str:
            logger.info(f"Casting all payload columns to string by default for '{source_system}.{table_name}'...")
            silver_tech = {'_valid_from', '_valid_to', '_is_current', '_is_deleted', '_inserted_at', '_updated_at'}
            for c in df.columns:
                if c not in silver_tech:
                    df = df.withColumn(c, col(c).cast("string"))

        # 4. Clean illegal characters dynamically if expression is configured for this table
        illegal_expr = table_cfg.get('clean_illegal_chars_expression') or table_cfg.get('clean_illegal_chars_pattern')
        if illegal_expr and isinstance(illegal_expr, str) and illegal_expr.strip():
            logger.info(f"Dynamically cleaning illegal characters for '{source_system}.{table_name}' using config expression: '{illegal_expr}'...")
            df = cls.clean_illegal_chars(df, pattern=illegal_expr)

        # 5. Apply Filter Expression if specified
        filter_expr = table_cfg.get('filter_expression')
        if filter_expr and isinstance(filter_expr, str) and filter_expr.strip():
            logger.info(f"Applying filter expression: '{filter_expr}'")
            df = df.filter(expr(filter_expr))

        # 7. Apply Column Casts
        column_casts = table_cfg.get('column_casts', {})
        for column_name, target_type in column_casts.items():
            if column_name in df.columns:
                logger.info(f"Casting column '{column_name}' -> '{target_type}'")
                df = df.withColumn(column_name, col(column_name).cast(target_type))

        # 6. Apply Custom SQL Expressions
        custom_expressions = table_cfg.get('custom_expressions', {})
        for new_col, sql_expr in custom_expressions.items():
            logger.info(f"Adding derived column '{new_col}' = expr('{sql_expr}')")
            df = df.withColumn(new_col, expr(sql_expr))

        # 7. Apply Column Renames
        column_renames = table_cfg.get('column_renames', {})
        for old_name, new_name in column_renames.items():
            if old_name in df.columns:
                logger.info(f"Renaming column '{old_name}' -> '{new_name}'")
                df = df.withColumnRenamed(old_name, new_name)

        # 8. Exclude Columns if configured in silver_config.json (protecting key columns and technical audit columns)
        exclude_cfg = table_cfg.get('exclude_columns') or table_cfg.get('drop_columns') or []
        if isinstance(exclude_cfg, str):
            exclude_cfg = [c.strip() for c in exclude_cfg.split(',') if c.strip()]
        elif not isinstance(exclude_cfg, list):
            exclude_cfg = []

        nkey_list = nkeys if isinstance(nkeys, list) else ([nkeys] if nkeys else [])
        if not nkey_list:
            cfg_nkey = table_cfg.get('nkey') or table_cfg.get('deduplication_keys') or table_cfg.get('primary_key')
            if cfg_nkey:
                if isinstance(cfg_nkey, list):
                    nkey_list = [str(k).strip() for k in cfg_nkey if str(k).strip()]
                elif isinstance(cfg_nkey, str) and ',' in cfg_nkey:
                    nkey_list = [k.strip() for k in cfg_nkey.split(',') if k.strip()]
                else:
                    nkey_list = [str(cfg_nkey).strip()]

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

        # 9. Apply Custom External Transformation File if configured (can query other tables via spark & context)
        custom_script_path = table_cfg.get('custom_transform_script') or table_cfg.get('custom_transform_file')
        if custom_script_path:
            df = cls._apply_custom_script(df, custom_script_path, spark=spark, context=context)

        # 10. Apply Extensible External Columns Hook (initializes configured external_columns to NULL string default)
        df = cls._apply_external_columns(df, table_cfg)

        # 11. Enrich Technical Audit Columns & Place them at the very end
        high_date_val = table_cfg.get('high_date_value', '9999-01-01 00:00:00')
        df = cls._enrich_technical_columns(df, order_col_name=order_col_name, high_date_val=high_date_val)

        # 12. Log final transformed schema introspection
        cls.log_schema_introspection(df, f"Final Transformed Silver Schema: '{source_system}.{table_name}'")

        return df

    @classmethod
    def _apply_external_columns(cls, df: DataFrame, table_cfg: Dict[str, Any]) -> DataFrame:
        """
        Extensible hook for external columns / external API enrichment.
        Attaches configured external columns initialized with a NULL string default value (lit(None).cast("string")).
        Future development teams can leverage this hook or a custom transform script to call an external API
        passing designated source columns and populating the returned values into these external columns.
        """
        external_cols = (
            table_cfg.get('external_columns')
            or table_cfg.get('api_enrichment_columns')
            or table_cfg.get('api_columns')
            or []
        )
        if isinstance(external_cols, str):
            external_cols = [c.strip() for c in external_cols.split(',') if c.strip()]

        if external_cols:
            for col_name in external_cols:
                if col_name not in df.columns:
                    logger.info(f"[EXTERNAL COLUMNS] Attaching external column '{col_name}' initialized to NULL string default.")
                    df = df.withColumn(col_name, lit(None).cast("string"))
                else:
                    logger.info(f"[EXTERNAL COLUMNS] External column '{col_name}' already present in DataFrame.")
        return df

    # Backward compatibility alias
    _apply_api_enrichment = _apply_external_columns

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
    def _apply_custom_script(
        cls,
        df: DataFrame,
        script_path: str,
        spark=None,
        context: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Dynamically loads and calls the transform function from a custom Python file.
        Supports flexible function signatures:
        - transform(df, spark, context): full access to active SparkSession and metadata (e.g. glue_database) to query other tables
        - transform(df, spark): standard transformation with SparkSession
        - transform(df): simple DataFrame transform
        """
        if not script_path or not str(script_path).strip():
            return df

        logger.info(f"Loading custom transform script: '{script_path}'...")

        # Resolve absolute path across local workspace and AWS Glue runtime environments (/tmp)
        abs_script_path = script_path
        if not os.path.isabs(script_path):
            current_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = [
                os.path.join(current_dir, script_path),
                os.path.join(os.getcwd(), script_path),
                os.path.join('/tmp', script_path),
                os.path.join('/tmp', os.path.basename(script_path))
            ]
            for cand in candidates:
                if os.path.exists(cand):
                    abs_script_path = cand
                    break

        if not os.path.exists(abs_script_path):
            logger.warning(f"Custom transform script file not found at '{abs_script_path}'. Skipping custom file transform.")
            return df

        try:
            module_name = f"custom_transform_{os.path.splitext(os.path.basename(script_path))[0]}"
            spec = importlib.util.spec_from_file_location(module_name, abs_script_path)
            custom_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(custom_module)

            if hasattr(custom_module, 'transform'):
                transform_func = getattr(custom_module, 'transform')
                sig = inspect.signature(transform_func)
                param_names = list(sig.parameters.keys())
                num_params = len(param_names)

                logger.info(
                    f"[CUSTOM TRANSFORM] Invoking transform() in '{script_path}' "
                    f"with signature ({', '.join(param_names)})..."
                )

                if "context" in param_names:
                    df = transform_func(df, spark=spark, context=context)
                elif num_params >= 3:
                    df = transform_func(df, spark, context)
                elif num_params == 2:
                    df = transform_func(df, spark)
                else:
                    df = transform_func(df)

                logger.info(f"[CUSTOM TRANSFORM] Completed transform() execution from '{script_path}'.")
            else:
                logger.warning(f"Custom script '{script_path}' does not define a 'transform()' function.")

        except Exception as err:
            logger.error(f"Error executing custom transform script '{script_path}': {err}", exc_info=True)
            raise

        return df

    @classmethod
    def _execute_custom_script(
        cls,
        df: DataFrame,
        script_path: str,
        spark=None,
        *args,
        **kwargs
    ) -> DataFrame:
        """Compatibility wrapper delegating to _apply_custom_script."""
        return cls._apply_custom_script(df, script_path, spark=spark)


# Module-level alias for direct invocation
clean_illegal_chars = SilverTransformer.clean_illegal_chars
