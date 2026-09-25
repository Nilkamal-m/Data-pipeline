# Silver Layer — Configuration Blueprint Reference

This document provides the complete, production-grade configuration blueprint for `silver_config.json`. Every parameter accepted by the Silver Iceberg ETL transformation engine is documented below with its required/optional status, default value, and exact usage in the code.

---

## 1. Complete Blueprint Template

```json
{
  "_comment": "Silver Layer Transformation Blueprint — UAX Data Lake Pipeline",
  "pipeline_defaults": {
    "silver_bucket": "uax-datalake-silver-{env}",
    "state_bucket": "uax-datalake-state-{env}",
    "target_format": "iceberg",
    "table_prefix": "tbl_",
    "merge_strategy": "upsert",
    "scd_type": "scd1",
    "bronze_data_prefix": "bronze/data",
    "silver_data_prefix": "silver/data",
    "state_prefix": "metadata/silver",
    "bronze_technical_columns": [
      "_ingested_at",
      "_source_system",
      "_table_name",
      "_execution_id"
    ],
    "technical_columns": {
      "is_deleted_column": "_is_deleted",
      "inserted_at_column": "_inserted_at",
      "updated_at_column": "_updated_at",
      "valid_from_column": "_valid_from",
      "valid_to_column": "_valid_to",
      "is_current_column": "_is_current",
      "high_date_value": "9999-01-01 00:00:00"
    },
    "scd_type2_config": {
      "valid_from_column": "_valid_from",
      "valid_to_column": "_valid_to",
      "is_current_column": "_is_current",
      "high_date_value": "9999-01-01 00:00:00"
    },
    "deduplication": {
      "enabled": true,
      "strategy": "latest_by_order_column",
      "default_nkey": "sys_id",
      "default_order_column": "sys_updated_on"
    },
    "watermark": {
      "enabled": true,
      "metadata_prefix": "metadata/silver",
      "watermark_column": "_ingested_at",
      "sync_watermark_table": true,
      "watermark_table_name": "tbl_watermarks",
      "full_refresh": false
    },
    "glue_catalog": {
      "enabled": true,
      "database_name": "uax_datalake_db_{env}",
      "crawler_name": "uax-datalake-silver-crawler-{env}",
      "trigger_crawler": false,
      "sync_watermark_table": true,
      "watermark_table_name": "tbl_watermarks"
    },
    "exclude_columns": [],
    "external_columns": [],
    "cast_all_columns_to_string": true
  },
  "source_systems": {
    "servicenow": {
      "tables": {
        "tbl_incident": {
          "source_table_name": "raw_tbl_incident",
          "nkey": "sys_id",
          "deduplication_keys": [
            "sys_id"
          ],
          "deduplication_order_by": [
            "sys_updated_on",
            "_ingested_at"
          ],
          "merge_strategy": "upsert",
          "scd_type": "scd1",
          "deduplication_strategy": "latest_by_order_column",
          "column_renames": {
            "sys_created_by": "created_by_user"
          },
          "column_casts": {
            "sys_mod_count": "integer"
          },
          "exclude_columns": [],
          "external_columns": [],
          "filter_expression": "",
          "custom_expressions": {},
          "custom_transform_script": "custom_transforms/servicenow_incident.py"
        },
        "tbl_user_history": {
          "source_table_name": "raw_tbl_sys_user",
          "nkey": "sys_id",
          "deduplication_keys": [
            "sys_id"
          ],
          "deduplication_order_by": [
            "sys_updated_on"
          ],
          "merge_strategy": "upsert",
          "scd_type": "scd2"
        }
      }
    }
  }
}
```

---

## 2. Parameter Reference & Line-by-Line Annotations

### 2.1 `pipeline_defaults` Block

| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `silver_bucket` | string | **Required** (Config or CLI) | *Used in `uax_silver_etl.py` S3 path builder*: S3 bucket where conformed Iceberg data and metadata files reside. |
| `state_bucket` | string | **Required** (Config or CLI) | *Used in `get_silver_last_load_date()`*: S3 bucket holding the watermarks JSON files. |
| `target_format` | string | Optional (Default: `iceberg`) | *Used in DDL generation*: Target table storage format (`iceberg`). |
| `table_prefix` | string | Optional (Default: `tbl_`) | *Used in naming resolution*: Standardized prefix prepended to all conformed Silver tables. |
| `merge_strategy` | string | Optional (Default: `upsert`) | *Used in Spark SQL execution*: Default merge mode (`upsert`, `append`, `overwrite`). |
| `scd_type` | string | Optional (Default: `scd1`) | *Used in execution router*: Default Slowly Changing Dimension strategy (`scd1` or `scd2`). |
| `bronze_data_prefix` | string | Optional (Default: `bronze/data`) | *Used in Bronze source reader*: S3 prefix where input Bronze Parquet partitions are discovered. |
| `silver_data_prefix` | string | Optional (Default: `silver/data`) | *Used in Iceberg table location*: S3 prefix where Iceberg table data directories are created. |
| `state_prefix` | string | Optional (Default: `metadata/silver`) | *Used in watermark reader/writer*: S3 prefix where Silver watermark JSON files are stored. |
| `bronze_technical_columns` | array | Optional | *Used in `get_payload_columns()`*: Technical columns present in Bronze Parquet that must be excluded from Silver payload hashing and Iceberg table data. |
| `cast_all_columns_to_string`| boolean | Optional (Default: `true`) | *Used in `SilverTransformer`*: Casts incoming business payload fields to string by default to prevent type mismatch and schema merge exceptions. |

---

### 2.2 Deduplication, Watermarking & SCD Blocks

#### `deduplication` Block
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `enabled` | boolean | Optional (Default: `true`) | *Used in `perform_deduplication()`*: If `true`, runs in-batch deduplication before merging into Iceberg. |
| `strategy` | string | Optional (Default: `latest_by_order_column`) | *Used in `perform_deduplication()`*: Deduplication algorithm (`latest_by_order_column`). |
| `default_nkey` | string | Optional (Default: `sys_id`) | *Used in `perform_deduplication()`*: Fallback primary key column when table config does not declare one. |
| `default_order_column`| string | Optional (Default: `sys_updated_on`) | *Used in `perform_deduplication()`*: Fallback sort column for window ordering. |

#### `watermark` Block
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `enabled` | boolean | Optional (Default: `true`) | *Used in Bronze scan*: If `true`, applies incremental filtering on `_ingested_at > last_watermark`. |
| `watermark_column` | string | Optional (Default: `_ingested_at`) | *Used in Bronze scan*: Column used to evaluate newly arrived data boundaries. |
| `sync_watermark_table` | boolean | Optional (Default: `true`) | *Used in `sync_silver_watermark_catalog_table()`*: Registers watermark status in Glue catalog table for operational reporting. |
| `watermark_table_name` | string | Optional (Default: `tbl_watermarks`) | *Used in Glue sync*: Name of the catalog table tracking pipeline execution watermarks. |

#### `scd_type2_config` Block
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `valid_from_column` | string | Optional (Default: `_valid_from`) | *Used in `execute_iceberg_scd2()`*: Column name storing the timestamp when this version became active. |
| `valid_to_column` | string | Optional (Default: `_valid_to`) | *Used in `execute_iceberg_scd2()`*: Column name storing the timestamp when this version was superseded. Set to `9999-01-01 00:00:00` for active records. |
| `is_current_column` | string | Optional (Default: `_is_current`) | *Used in `execute_iceberg_scd2()`*: String flag — `'Y'` if this is the current active version of the record, `'N'` if it has been superseded by a newer version. |
| `is_deleted_column` | string | Optional (Default: `_is_deleted`) | *Used in soft-delete & CDC resolution*: String flag — `'Y'` if the record is soft-deleted, `'N'` for active records. |
| `high_date_value` | string | Optional (Default: `9999-01-01 00:00:00`) | *Used in `execute_iceberg_scd2()`*: Infinity timestamp assigned to active records' `_valid_to`. |

---

### 2.3 `tables.<table>` Block

| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `source_table_name` | string | **Required** | *Used in Bronze reader*: Name of upstream Bronze table folder to read from. |
| `nkey` / `deduplication_keys` | string / array | **Required** | *Used in `perform_deduplication()` & `MERGE INTO`*: Natural key or list of composite keys uniquely identifying an entity. |
| `deduplication_order_by` | array | **Required** | *Used in `perform_deduplication()`*: List of ordering columns (e.g., `["sys_updated_on", "_ingested_at"]`) to break ties. |
| `merge_strategy` | string | Optional (Default: `upsert`) | *Used in merge router*: Table-specific merge strategy (`upsert`, `append`, `overwrite`). |
| `scd_type` | string | Optional (Default: `scd1`) | *Used in merge router*: Table-specific SCD strategy (`scd1` or `scd2`). |
| `column_renames` | object | Optional | *Used in `SilverTransformer`*: Map of `{old_column_name: new_column_name}` applied during conformed cleansing. |
| `column_casts` | object | Optional | *Used in `SilverTransformer`*: Map of `{column_name: target_data_type}` (e.g., `{"count": "integer"}`). |
| `exclude_columns` | array | Optional | *Used in `SilverTransformer`*: List of unwanted columns pruned before loading into Iceberg. |
| `filter_expression` | string | Optional | *Used in `SilverTransformer`*: PySpark SQL filter expression to discard invalid records (e.g., `status != 'DELETED'`). |
| `custom_transform_script` | string | Optional | *Used in `SilverTransformer`*: Path to external Python script executing custom PySpark DataFrame transformations. |
