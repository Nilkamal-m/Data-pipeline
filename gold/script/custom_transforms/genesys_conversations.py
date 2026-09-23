"""
Custom Transformation Script for Genesys Conversations Gold Mart.

Pattern: gold/script/custom_transforms/<source>_<table>.py
Signature: transform(df, spark=None, context=None) -> DataFrame

Incremental / LLM Processing Behavior:
- When 'incremental: true' is configured in gold_config.json, GoldLayerManager automatically
  filters `df` to ONLY the new or modified records (the delta) before calling this transform!
- Therefore, any downstream LLM enrichment (e.g. Bedrock Claude, OpenAI, Comprehend) runs
  ONLY on the delta records, completely eliminating exponential cost growth and rate limit exhaustion.
- The returned DataFrame is then merged idempotently into Athena (Iceberg MERGE INTO)
  and Aurora MySQL (ON DUPLICATE KEY UPDATE) based on the natural key ('nkey').
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

logger = logging.getLogger(__name__)


def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    """
    Custom transformation entry point invoked by Gold serving engine.

    Args:
        df (DataFrame): PySpark DataFrame containing ONLY the new/changed delta records
                        when incremental mode is active, or the full dataset on initial run.
        spark (SparkSession, optional): Active SparkSession.
        context (dict, optional): Runtime execution metadata containing table_name,
                                  source_system, glue_database, primary_keys, is_incremental, etc.

    Returns:
        DataFrame: Transformed/enriched PySpark DataFrame ready for Gold upsert.
    """
    context = context or {}
    table_name = context.get("table_name", "conversations")
    is_incremental = context.get("is_incremental", False)

    logger.info(
        f"[CUSTOM TRANSFORM] Invoking transform for '{table_name}' "
        f"(incremental_mode={is_incremental})..."
    )

    # Attach/verify technical audit column if not already present
    if "_updated_at" not in df.columns:
        df = df.withColumn("_updated_at", current_timestamp())

    # Example LLM Enrichment:
    # Because GoldLayerManager isolates the delta, df only contains new/changed rows.
    # LLM invocations (e.g. AWS Bedrock, OpenAI) can be applied here safely.

    return df
