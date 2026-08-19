"""
Sample Custom Transformation Script for ServiceNow Incident Table.

Custom transformation logic must define a `transform(df, spark)` function returning the transformed PySpark DataFrame.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, current_timestamp


def transform(df: DataFrame, spark=None) -> DataFrame:
    """
    Custom transformation entry point invoked by Silver Iceberg ETL engine.

    Args:
        df (DataFrame): PySpark DataFrame after deduplication.
        spark (SparkSession, optional): Active SparkSession context.

    Returns:
        DataFrame: Transformed PySpark DataFrame.
    """
    # Example Custom Logic: Uppercase urgency/impact status and add processing timestamp
    if "urgency" in df.columns:
        df = df.withColumn("urgency_code", upper(col("urgency")))

    return df.withColumn("_transformed_at", current_timestamp())
