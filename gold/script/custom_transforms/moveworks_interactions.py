"""
Custom Transformation Script for Moveworks Interactions Gold Mart.

Pattern: gold/script/custom_transforms/<source>_<table>.py
Signature: transform(df, spark=None, context=None) -> DataFrame

Developer Notes:
- Developers have full control over custom business logic, multi-column enrichment,
  external API / LLM calls, micro-batching, and rate limiting directly within this script.
- Strict Precedence Hierarchy:
    Custom Transform (df.withColumn) > SQL Query Column > Config Default (external_columns NULL)
- Initial Load Awareness: When context.get('is_initial_load') is True, historical pre-enriched
  records can be preserved directly using PySpark when()/otherwise() expressions.
"""

import logging
from typing import Optional, Dict, Any, List

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


# ==============================================================================
# SAMPLE DEVELOPER IMPLEMENTATION TEMPLATE (COMMENTED OUT)
# ==============================================================================
# The code below is a reference guide for developers implementing custom
# multi-column enrichment, micro-batching, rate-limiting, or external API calls
# (e.g. Moveworks NLP intent classification, resolution summarization).
#
# import json
# import time
# import random
# from pyspark.sql.functions import col, lit, current_timestamp, when
#
# def get_moveworks_credentials(secret_name: str, region_name: str = "us-east-1") -> Dict[str, Any]:
#     """
#     Retrieves Moveworks API credentials or LLM secrets from AWS Secrets Manager.
#     """
#     try:
#         import boto3
#         client = boto3.client("secretsmanager", region_name=region_name)
#         response = client.get_secret_value(SecretId=secret_name)
#         return json.loads(response.get("SecretString", "{}"))
#     except Exception as err:
#         logger.warning(f"[CUSTOM TRANSFORM] Could not retrieve secret '{secret_name}': {err}")
#         return {}
#
#
# def enrich_interaction_sample(text_payload: str, max_retries: int = 3) -> Dict[str, str]:
#     """
#     Sample external classification call with developer-managed rate limiting and retry backoff.
#     """
#     for attempt in range(max_retries):
#         try:
#             # time.sleep(0.04)  # Enforce developer rate limit (e.g. 25 RPS)
#             snippet = str(text_payload)[:40] if text_payload else "General Interaction"
#             return {
#                 "resolution_summary": f"Resolved automatically via Moveworks bot: {snippet}",
#                 "bot_intent_label": "IT_SERVICE_REQUEST"
#             }
#         except Exception as err:
#             if "429" in str(err) or "Throttling" in str(err):
#                 time.sleep((2 ** attempt) + random.uniform(0.1, 0.5))
#             else:
#                 raise
#     return {
#         "resolution_summary": "Standard resolution summary (fallback)",
#         "bot_intent_label": "UNKNOWN"
#     }
#
#
# def apply_moveworks_enrichment(df: DataFrame, context: dict) -> DataFrame:
#     """
#     Demonstrates multi-column enrichment for external_columns:
#     ['resolution_summary', 'bot_intent_label']
#     """
#     is_initial_load = context.get("is_initial_load", False)
#     external_cols = context.get("external_columns", ["resolution_summary", "bot_intent_label"])
#
#     # Multi-Column Precedence: df.withColumn overrides SQL or config defaults
#     for ext_col in external_cols:
#         if ext_col not in df.columns:
#             df = df.withColumn(ext_col, lit(None).cast("string"))
#
#     # Preserve existing pre-enriched historical values on initial load:
#     df = df.withColumn(
#         "resolution_summary",
#         when(
#             col("resolution_summary").isNotNull() & (col("resolution_summary") != ""),
#             col("resolution_summary")  # Retain existing historical value
#         ).otherwise(
#             lit("Automated bot resolution (sample)")
#         )
#     ).withColumn(
#         "bot_intent_label",
#         when(
#             col("bot_intent_label").isNotNull() & (col("bot_intent_label") != ""),
#             col("bot_intent_label")  # Retain existing historical value
#         ).otherwise(
#             lit("SERVICE_CATALOG_ACCESS")
#         )
#     )
#     return df
# ==============================================================================


def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    """
    Custom transformation entry point invoked by Gold serving engine during both
    Initial Load (is_initial_load=True) and Incremental Silver Mart execution.

    Args:
        df (DataFrame): PySpark DataFrame to transform.
        spark (SparkSession, optional): Active SparkSession.
        context (dict, optional): Runtime execution metadata containing:
            - source_system: Source system identifier (e.g. 'moveworks')
            - table_name: Clean table base name (e.g. 'interactions')
            - target_table: Target Iceberg table name (e.g. 'gold_moveworks_interactions')
            - is_initial_load: Boolean indicating if this is an initial CSV load
            - is_incremental: Boolean flag indicating delta vs full load
            - external_columns: List of configured external columns (e.g. ['resolution_summary', 'bot_intent_label'])
            - api_secret_name: AWS Secrets Manager secret holding external credentials
            - params: Complete dictionary of CLI / Glue job arguments

    Returns:
        DataFrame: Transformed PySpark DataFrame adhering to Precedence Hierarchy:
                   Custom Transform (df.withColumn) > SQL Query Column > Config Default (NULL).
    """
    context = context or {}
    source_system = context.get("source_system", "moveworks")
    table_name = context.get("table_name", "interactions")
    is_initial_load = context.get("is_initial_load", False)
    external_cols = context.get("external_columns", [])

    # Log message confirming custom script invocation
    logger.info(
        f"[CUSTOM TRANSFORM] Calling custom script for '{source_system}.{table_name}' "
        f"(is_initial_load={is_initial_load}, external_columns={external_cols})."
    )

    # --------------------------------------------------------------------------
    # Custom transformation logic can be implemented here by the developer.
    # See commented-out sample functions above for reference patterns.
    # --------------------------------------------------------------------------

    return df
