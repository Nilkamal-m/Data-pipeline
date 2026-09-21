"""
Custom Transformation Script for Moveworks Users Table.
"""

import logging
import re
from pyspark.sql.functions import col, regexp_replace, current_timestamp

logger = logging.getLogger(__name__)


def clean_illegal_chars(df, pattern=None):
    """
    Cleans illegal characters from DataFrame using the expression provided from configuration.
    If no expression/pattern is provided, ignores and returns DataFrame unchanged.
    Supports both pandas (applymap/map) and PySpark (regexp_replace).
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

    # PySpark DataFrame support:
    # 1. Cast all columns to string by default
    # 2. Apply regexp_replace to strip illegal characters using pattern from config
    if hasattr(df, 'columns') and hasattr(df, 'withColumn'):
        for col_name in df.columns:
            df = df.withColumn(
                col_name,
                regexp_replace(col(col_name).cast("string"), pattern, '')
            )
        return df

    return df


def transform(df, spark=None, context: dict = None):
    """
    Custom transformation entry point invoked by Silver Iceberg ETL engine.
    Casts all columns to string by default, cleans illegal control characters using
    the expression configured in silver_config.json, and appends technical audit metadata.
    """
    logger.info("[CUSTOM TRANSFORM] Casting columns to string for Moveworks users...")
    if hasattr(df, 'columns') and hasattr(df, 'withColumn'):
        for c in df.columns:
            df = df.withColumn(c, col(c).cast("string"))

    # Resolve illegal char expression dynamically from table configuration
    table_cfg = context.get('table_cfg', {}) if isinstance(context, dict) else {}
    illegal_expr = table_cfg.get('clean_illegal_chars_expression') or table_cfg.get('clean_illegal_chars_pattern')
    if illegal_expr:
        logger.info(f"[CUSTOM TRANSFORM] Applying illegal char expression from config: {illegal_expr}")
        df = clean_illegal_chars(df, pattern=illegal_expr)

    if hasattr(df, 'withColumn') and "_transformed_at" not in getattr(df, 'columns', []):
        df = df.withColumn("_transformed_at", current_timestamp())

    return df
