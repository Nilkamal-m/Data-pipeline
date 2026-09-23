"""
Custom Transformation Script for Genesys Conversations Gold Mart.

Pattern: gold/script/custom_transforms/<source>_<table>.py
Signature: transform(df, spark=None, context=None) -> DataFrame
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

logger = logging.getLogger(__name__)


def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    """
    Custom transformation entry point invoked by Gold serving engine.

    Args:
        df (DataFrame): PySpark DataFrame produced by the Gold SQL query.
        spark (SparkSession, optional): Active SparkSession.
        context (dict, optional): Runtime execution metadata containing table_name,
                                  source_system, glue_database, primary_key, etc.

    Returns:
        DataFrame: Transformed PySpark DataFrame ready for Gold upsert.
    """
    logger.info("[CUSTOM TRANSFORM] Custom transform hook invoked for Genesys Gold conversations.")

    # Attach/verify technical audit column if not already present
    if "_updated_at" not in df.columns:
        df = df.withColumn("_updated_at", current_timestamp())

    return df
