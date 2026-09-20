"""
Custom Transformation Script for Moveworks Conversations Table.
"""

import logging
import re
from pyspark.sql.functions import col, regexp_replace, current_timestamp

logger = logging.getLogger(__name__)


def clean_illegal_chars(df):
    """Clean illegal characters from DataFrame"""
    def remove_illegal_chars(value):
        if isinstance(value, str):
            return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', value)
        return value

    if hasattr(df, 'applymap'):
        return df.applymap(remove_illegal_chars)
    if hasattr(df, 'map'):
        return df.map(remove_illegal_chars)

    # PySpark DataFrame support
    if hasattr(df, 'schema') and hasattr(df, 'withColumn'):
        for field in df.schema.fields:
            if field.dataType.simpleString() == 'string':
                df = df.withColumn(
                    field.name,
                    regexp_replace(col(field.name), r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '')
                )
        return df

    return df


def transform(df, spark=None, context: dict = None):
    """
    Custom transformation entry point invoked by Silver Iceberg ETL engine.
    Cleans illegal control characters and appends technical audit metadata.
    """
    logger.info("[CUSTOM TRANSFORM] Cleaning illegal control characters for Moveworks conversations...")
    df = clean_illegal_chars(df)

    if hasattr(df, 'withColumn') and "_transformed_at" not in getattr(df, 'columns', []):
        df = df.withColumn("_transformed_at", current_timestamp())

    return df
