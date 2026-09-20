#!/usr/bin/env python3
"""
AWS Glue Data Catalog Table Schema Fixer
=========================================
Removes specified columns (e.g. '_ingested_at') from a table's StorageDescriptor.Columns
so that the column is defined SOLELY as a Partition Key.

Why this is needed:
  When historical data was written to S3 before code sanitization, the Parquet files
  contain '_ingested_at' in their internal file footer. An AWS Glue Crawler scans the files
  and puts '_ingested_at' into BOTH StorageDescriptor.Columns and PartitionKeys, causing
  Athena and Spark to show duplicate column errors (e.g. two '_ingested_at' columns).

  Running this script updates the AWS Glue Data Catalog table definition in-place in 2 seconds:
  - Preserves 100% of data in S3 (zero data rewrites, zero data loss)
  - Removes '_ingested_at' from StorageDescriptor.Columns
  - Leaves PartitionKeys ['_ingested_at'] untouched
  - Athena / Spark will immediately recognize single '_ingested_at' partition column

Usage:
  # Local / CloudShell CLI:
  python bronze/script/fix_catalog_schema.py --database uax_datalake_db_dev --table raw_tbl_interactions

  # Custom column and region:
  python bronze/script/fix_catalog_schema.py --database uax_datalake_db_dev --table raw_tbl_interactions --exclude _ingested_at --region us-east-1
"""

import sys
import os
import argparse
import logging
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("fix_catalog_schema")


def fix_glue_catalog_table(
    database_name: str,
    table_name: str,
    exclude_columns: Optional[List[str]] = None,
    region_name: Optional[str] = None,
    glue_client: Optional[object] = None
) -> dict:
    """
    Updates AWS Glue Data Catalog table to remove specified columns from StorageDescriptor.Columns.

    :param database_name: Target AWS Glue Database (e.g. 'uax_datalake_db_dev')
    :param table_name: Target AWS Glue Table (e.g. 'raw_tbl_interactions')
    :param exclude_columns: List of column names to remove (defaults to ['_ingested_at'])
    :param region_name: AWS Region (e.g. 'us-east-1', optional if configured via environment)
    :param glue_client: Optional pre-configured boto3 glue client (for testing/mocking)
    :return: Dictionary summary of changes made
    """
    if exclude_columns is None:
        exclude_columns = ['_ingested_at']
    exclude_set = {c.strip().lower() for c in exclude_columns if c.strip()}

    if glue_client is None:
        import boto3
        kwargs = {}
        if region_name:
            kwargs['region_name'] = region_name
        glue_client = boto3.client('glue', **kwargs)

    logger.info(f"Fetching table definition for '{database_name}.{table_name}'...")
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    table = response['Table']

    sd = table.get('StorageDescriptor', {})
    original_cols = sd.get('Columns', [])
    partition_keys = [pk.get('Name') for pk in table.get('PartitionKeys', [])]

    logger.info(f"Current columns in StorageDescriptor : {len(original_cols)}")
    logger.info(f"Current partition keys               : {partition_keys}")

    cleaned_cols = [c for c in original_cols if c.get('Name', '').strip().lower() not in exclude_set]
    removed_cols = [c.get('Name') for c in original_cols if c.get('Name', '').strip().lower() in exclude_set]

    if not removed_cols:
        msg = f"No columns matching {list(exclude_set)} found in StorageDescriptor.Columns for {database_name}.{table_name}."
        logger.info(f"✓ {msg}")
        return {
            'status': 'NO_CHANGE',
            'database': database_name,
            'table': table_name,
            'message': msg,
            'columns_count': len(cleaned_cols),
            'partition_keys': partition_keys
        }

    # Clean out read-only fields returned by get_table before submitting to update_table
    read_only_keys = [
        'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
        'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId',
        'FederatedTable', 'Owner'
    ]
    table_input = {k: v for k, v in table.items() if k not in read_only_keys}
    table_input['StorageDescriptor']['Columns'] = cleaned_cols

    logger.info(f"Removing column(s) {removed_cols} from StorageDescriptor.Columns...")
    glue_client.update_table(
        DatabaseName=database_name,
        TableInput=table_input
    )

    success_msg = (
        f"✓ Successfully updated '{database_name}.{table_name}' in AWS Glue Data Catalog! "
        f"Removed {removed_cols} from StorageDescriptor.Columns. "
        f"Remaining columns: {len(cleaned_cols)}. Partition keys: {partition_keys}."
    )
    logger.info(success_msg)
    return {
        'status': 'SUCCEEDED',
        'database': database_name,
        'table': table_name,
        'removed_columns': removed_cols,
        'remaining_columns_count': len(cleaned_cols),
        'partition_keys': partition_keys,
        'message': success_msg
    }


def main():
    parser = argparse.ArgumentParser(
        description="Fix AWS Glue Catalog table schema by removing duplicate partition columns from StorageDescriptor.Columns"
    )
    parser.add_argument(
        "--database", "-d",
        required=True,
        help="AWS Glue database name (e.g. uax_datalake_db_dev)"
    )
    parser.add_argument(
        "--table", "-t",
        required=True,
        help="AWS Glue table name (e.g. raw_tbl_interactions)"
    )
    parser.add_argument(
        "--exclude", "-x",
        nargs="+",
        default=["_ingested_at"],
        help="Column names to remove from StorageDescriptor.Columns (default: _ingested_at)"
    )
    parser.add_argument(
        "--region", "-r",
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        help="AWS Region name (default: from environment or AWS config)"
    )

    args = parser.parse_args()
    try:
        res = fix_glue_catalog_table(
            database_name=args.database,
            table_name=args.table,
            exclude_columns=args.exclude,
            region_name=args.region
        )
        print("\nResult:")
        print(f"  Status          : {res['status']}")
        print(f"  Table           : {res['database']}.{res['table']}")
        if res.get('removed_columns'):
            print(f"  Removed Columns : {res['removed_columns']}")
        print(f"  Partition Keys  : {res['partition_keys']}")
        print(f"  Columns Count   : {res.get('remaining_columns_count', res.get('columns_count'))}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Failed to fix Glue table schema: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
