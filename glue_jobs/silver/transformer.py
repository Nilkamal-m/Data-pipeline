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
from pyspark.sql.functions import col, expr

logger = logging.getLogger(__name__)


class SilverTransformer:
    """
    Applies both declarative transformations and custom Python transformation files to Silver DataFrames.
    """

    @classmethod
    def apply_transformations(
        cls,
        df: DataFrame,
        source_system: str,
        table_name: str,
        table_cfg: Dict[str, Any],
        spark=None
    ) -> DataFrame:
        """
        Executes all configured declarative and custom file transformations for a table.
        """
        logger.info(f"Applying transformations for '{source_system}.{table_name}'...")

        # 1. Apply Filter Expression if specified
        filter_expr = table_cfg.get('filter_expression')
        if filter_expr and isinstance(filter_expr, str) and filter_expr.strip():
            logger.info(f"Applying filter expression: '{filter_expr}'")
            df = df.filter(expr(filter_expr))

        # 2. Apply Column Casts
        column_casts = table_cfg.get('column_casts', {})
        for column_name, target_type in column_casts.items():
            if column_name in df.columns:
                logger.info(f"Casting column '{column_name}' -> '{target_type}'")
                df = df.withColumn(column_name, col(column_name).cast(target_type))

        # 3. Apply Custom SQL Expressions
        custom_expressions = table_cfg.get('custom_expressions', {})
        for new_col, sql_expr in custom_expressions.items():
            logger.info(f"Adding derived column '{new_col}' = expr('{sql_expr}')")
            df = df.withColumn(new_col, expr(sql_expr))

        # 4. Apply Column Renames
        column_renames = table_cfg.get('column_renames', {})
        for old_name, new_name in column_renames.items():
            if old_name in df.columns:
                logger.info(f"Renaming column '{old_name}' -> '{new_name}'")
                df = df.withColumnRenamed(old_name, new_name)

        # 5. Drop Unwanted Columns
        drop_columns = table_cfg.get('drop_columns', [])
        if drop_columns:
            existing_drops = [c for c in drop_columns if c in df.columns]
            if existing_drops:
                logger.info(f"Dropping columns: {existing_drops}")
                df = df.drop(*existing_drops)

        # 6. Apply Custom External Transformation File if configured
        custom_script_path = table_cfg.get('custom_transform_script') or table_cfg.get('custom_transform_file')
        if custom_script_path:
            df = cls._apply_custom_script(df, custom_script_path, spark)

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
