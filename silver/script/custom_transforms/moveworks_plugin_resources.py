"""
Custom Transformation Script for Moveworks Plugin Resources Table.

Note:
Default column string casting and illegal character cleaning are handled directly
and declaratively by SilverTransformer in transformer.py via silver_config.json.
This script serves as an extensible hook for table-specific transformations or external API calls.
"""

import logging
from pyspark.sql import DataFrame
# import requests
# from pyspark.sql.functions import col, udf, lit
# from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    """
    Custom transformation entry point invoked by Silver Iceberg ETL engine.

    Args:
        df (DataFrame): PySpark DataFrame after deduplication and declarative transforms.
        spark (SparkSession, optional): Active SparkSession with Glue / Iceberg catalog connectivity.
        context (dict, optional): Runtime execution metadata containing table_cfg, glue_database, etc.

    Returns:
        DataFrame: Transformed PySpark DataFrame.
    """
    logger.info("[CUSTOM TRANSFORM] Custom transform hook invoked for Moveworks tbl_plugin_resources.")

    # --------------------------------------------------------------------------
    # Optional Example: External API Call / Enrichment Pattern
    # --------------------------------------------------------------------------
    # def fetch_external_enrichment(entity_id):
    #     try:
    #         response = requests.get(
    #             f"https://api.external-service.com/v1/enrichment/{entity_id}",
    #             headers={"Authorization": "Bearer <TOKEN>"},
    #             timeout=5
    #         )
    #         if response.status_code == 200:
    #             return response.json().get("enriched_attribute")
    #     except Exception as exc:
    #         logger.warning(f"External API call failed for entity '{entity_id}': {exc}")
    #     return None
    #
    # enrichment_udf = udf(fetch_external_enrichment, StringType())
    # if "id" in df.columns:
    #     df = df.withColumn("external_enrichment_field", enrichment_udf(col("id")))
    # --------------------------------------------------------------------------

    return df
