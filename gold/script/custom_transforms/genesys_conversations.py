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

import json
import logging
from typing import Optional, Dict, Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp

logger = logging.getLogger(__name__)


def get_api_credentials(secret_name: str, region_name: str = "us-east-1") -> Dict[str, Any]:
    """
    Retrieves external API / LLM credentials from AWS Secrets Manager.
    
    Args:
        secret_name: AWS Secrets Manager secret name or ARN.
        region_name: AWS region name.

    Returns:
        Dict[str, Any]: Parsed secret dictionary containing API keys, endpoints, or tokens.
    """
    try:
        import boto3
        client = boto3.client("secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_str = response.get("SecretString", "{}")
        return json.loads(secret_str)
    except Exception as err:
        logger.error(f"[CUSTOM TRANSFORM] Error retrieving secret '{secret_name}': {err}")
        return {}


def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    """
    Custom transformation entry point invoked by Gold serving engine.

    Args:
        df (DataFrame): PySpark DataFrame containing ONLY the new/changed delta records
                        when incremental mode is active, or the full dataset on initial run.
        spark (SparkSession, optional): Active SparkSession.
        context (dict, optional): Runtime execution metadata containing:
            - source_system: Source system identifier (e.g. 'genesys')
            - table_name: Clean table base name (e.g. 'conversations')
            - api_secret_name: Configured or CLI passed API secret name
            - is_incremental: Boolean flag indicating delta vs full load
            - primary_keys: List of natural keys (nkeys)
            - params: Complete dictionary of CLI / Glue job arguments

    Returns:
        DataFrame: Transformed/enriched PySpark DataFrame ready for Gold upsert.
    """
    context = context or {}
    table_name = context.get("table_name", "conversations")
    is_incremental = context.get("is_incremental", False)
    api_secret_name = context.get("api_secret_name") or context.get("llm_secret_name")

    logger.info(
        f"[CUSTOM TRANSFORM] Invoking transform for '{table_name}' "
        f"(incremental_mode={is_incremental}, api_secret_name='{api_secret_name}')..."
    )

    # 1. Attach/verify technical audit column if not already present
    if "_updated_at" not in df.columns:
        df = df.withColumn("_updated_at", current_timestamp())

    # 2. External API / LLM Enrichment Example:
    # If an API secret is configured or passed via CLI (--API_SECRET_NAME),
    # fetch the credentials and perform enrichment on the delta records:
    if api_secret_name:
        logger.info(f"[CUSTOM TRANSFORM] API secret '{api_secret_name}' resolved for '{table_name}'.")
        # credentials = get_api_credentials(api_secret_name)
        # api_key = credentials.get("api_key") or credentials.get("token")
        # Base URLs, Bedrock model IDs, or OpenAI API keys can be used here.

    return df
