# Bronze Layer — Configuration Blueprint Reference

This document provides the definitive configuration blueprint for `bronze_config.json`. Every parameter accepted by the Bronze ingestion framework is documented below with its required/optional status, default value, and exact usage in the code.

---

## 1. Complete Blueprint Template

```json
{
  "_comment": "Bronze Layer Ingestion Configuration Blueprint",
  "pipeline_defaults": {
    "bronze_bucket": "uax-datalake-bronze-{env}",
    "state_bucket": "uax-datalake-state-{env}",
    "batch_size": 1000,
    "s3_chunk_size": 10000,
    "max_retries": 3,
    "state_prefix": "metadata/bronze",
    "bronze_data_prefix": "bronze/data",
    "output_format": "parquet",
    "parquet_compression": "snappy",
    "flatten_nested_json": true,
    "flatten_separator": "_",
    "error_handling_mode": "CONTINUE_ON_ERROR",
    "upper_bound": "",
    "default_initial_load_date": "2024-01-01 00:00:00",
    "glue_catalog": {
      "enabled": true,
      "database_name": "uax_datalake_db_{env}",
      "table_prefix": "raw_tbl_",
      "crawler_name": "uax-datalake-bronze-crawler-{env}",
      "trigger_crawler": true,
      "sync_watermark_table": true,
      "watermark_table_name": "raw_tbl_watermarks"
    }
  },
  "source_systems": {
    "servicenow_example": {
      "base_url": "https://instance.service-now.com",
      "api_endpoint_template": "/api/now/table/{table_name}",
      "default_delta_filter": "sys_updated_on>={last_load_date}",
      "response_records_key": "result",
      "batch_size": 1000,
      "tables": {
        "incident": {
          "initial_load_date": "2024-01-01 00:00:00",
          "upper_bound": "",
          "query_override": "active=true^sys_updated_on>={last_load_date}"
        }
      }
    },
    "genesys_example": {
      "base_url": "https://api.mypurecloud.com",
      "api_endpoint_template": "/api/v2/{table_name}",
      "default_delta_filter": "{last_load_date}",
      "response_records_key": "entities",
      "batch_size": 100,
      "tables": {
        "conversations": {
          "initial_load_date": "2024-01-01 00:00:00",
          "upper_bound": ""
        }
      }
    },
    "moveworks_example": {
      "base_url": "https://api.moveworks.ai",
      "assistant_name": "acmecorp-conversations-rest-api",
      "api_endpoint_template": "/export/v1beta2/records/{table_name}",
      "default_delta_filter": "last_updated_time ge '{last_load_date}' and last_updated_time le '{upper_bound}'",
      "response_records_key": "value",
      "batch_size": 500,
      "orderby": "last_updated_time desc",
      "parallel_processing": {
        "enabled": true,
        "max_workers": 5,
        "shard_window_days": 15
      },
      "tables": {
        "interactions": {
          "initial_load_date": "1900-01-01 00:00:00",
          "upper_bound": "",
          "custom_endpoint": "/export/v1beta2/records/interactions"
        }
      }
    },
    "postgresql_example": {
      "connection_type": "database",
      "db_type": "postgresql",
      "port_reference": 5432,
      "default_delta_filter": "updated_at >= '{last_load_date}'",
      "query_template": "SELECT * FROM {table_name} WHERE {query_filter} ORDER BY updated_at ASC",
      "fetch_size": 10000,
      "tables": {
        "orders": {
          "initial_load_date": "2024-01-01 00:00:00",
          "upper_bound": "",
          "query_override": "status = 'COMPLETED' AND updated_at >= '{last_load_date}'"
        }
      }
    },
    "s3_vendor_example": {
      "type": "s3_file",
      "source_bucket": "vendor-feed-bucket",
      "file_prefix": "drops/",
      "file_format": "csv",
      "delimiter": ",",
      "has_header": true,
      "encoding": "utf-8",
      "fetch_mode": "all",
      "tables": {
        "employee_feed": {
          "initial_load_date": "2024-01-01 00:00:00",
          "upper_bound": "",
          "file_path": "drops/employees/",
          "fetch_mode": "latest"
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
| `bronze_bucket` | string | **Required** (Config or Glue Argument) | *Used in `BronzeLoadManager.run()`*: Target S3 bucket where raw Parquet files are written. Supports `{env}` interpolation. |
| `state_bucket` | string | **Required** (Config or Glue Argument) | *Used in `BronzeLoadManager._get_watermark()` / `_commit_watermark()`*: S3 bucket storing state JSON tracking `last_load_date`. |
| `batch_size` | integer | Optional (Default: `1000`) | *Used in HTTP connectors (`HttpClient`)*: Number of records requested per pagination call. |
| `s3_chunk_size` | integer | Optional (Default: `10000`) | *Used in `BronzeLoadManager._write_parquet_partition()`*: In-memory record batch threshold before flushing a Parquet file to S3. |
| `max_retries` | integer | Optional (Default: `3`) | *Used in `http_client.py` (`Retry`)*: Maximum HTTP retry attempts on transient network or 5xx server errors. |
| `state_prefix` | string | Optional (Default: `metadata/bronze`) | *Used in state file path builder*: S3 folder prefix where per-table state JSON files are saved. |
| `bronze_data_prefix` | string | Optional (Default: `bronze/data`) | *Used in `BronzeLoadManager` write path*: S3 folder prefix for raw partitioned Parquet datasets. |
| `output_format` | string | Optional (Default: `parquet`) | *Used in `BronzeLoadManager` serializer*: Target file format (`parquet`). |
| `parquet_compression` | string | Optional (Default: `snappy`) | *Used in PyArrow / Spark Parquet writer*: Compression codec (`snappy`, `gzip`, `none`). |
| `flatten_nested_json` | boolean | Optional (Default: `true`) | *Used in `BronzeLoadManager._flatten_dict()`*: Recursively flattens nested JSON dictionaries into top-level columns. |
| `flatten_separator` | string | Optional (Default: `_`) | *Used in `BronzeLoadManager._flatten_dict()`*: Delimiter joining nested dictionary keys (e.g., `user_profile_id`). |
| `error_handling_mode` | string | Optional (Default: `CONTINUE_ON_ERROR`) | *Used in multi-table loop*: `CONTINUE_ON_ERROR` (skip failed tables and proceed) or `HALT_ON_ERROR` (fail job immediately). |
| `upper_bound` | string | Optional (Default: `""`) | *Used in watermark calculation*: Timestamp boundary (`YYYY-MM-DD HH:MM:SS`) to prevent reading beyond a set point during backfills. |
| `default_initial_load_date` | string | Optional (Default: `""`) | *Used in `BronzeLoadManager._get_watermark()`*: Global fallback start timestamp if table has no prior state and no table-level load date. |
| `glue_catalog.enabled` | boolean | Optional (Default: `true`) | *Used in post-write step*: Whether to register/synchronize the ingested table with the AWS Glue Data Catalog. |
| `glue_catalog.database_name` | string | Optional | *Used in Glue client*: Glue Database name for raw tables. Interpolates `{env}` (e.g., `uax_datalake_db_{env}`). |
| `glue_catalog.table_prefix` | string | Optional (Default: `raw_tbl_`) | *Used in Glue table naming*: Prefix prepended to source table names in Athena/Glue. |
| `glue_catalog.crawler_name` | string | Optional | *Used in Glue Crawler trigger*: Name of Glue Crawler to trigger. Interpolates `{env}`. |
| `glue_catalog.trigger_crawler`| boolean | Optional (Default: `true`) | *Used in `BronzeLoadManager._trigger_crawler()`*: If `true`, invokes `boto3.client('glue').start_crawler()`. |
| `glue_catalog.sync_watermark_table` | boolean | Optional (Default: `true`) | *Used in Glue metadata sync*: Updates the raw watermarks table in Glue for operational dashboards. |
| `glue_catalog.watermark_table_name` | string | Optional (Default: `raw_tbl_watermarks`) | *Used in Glue metadata sync*: Athena table name storing pipeline watermarks. |

---

### 2.2 `source_systems.<source>` Block

#### Common Source Parameters
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `base_url` | string | **Required** (REST APIs) | *Used in HTTP connectors*: Root URL for the source API (e.g., `https://api.mypurecloud.com`). |
| `api_endpoint_template` | string | **Required** (REST APIs) | *Used in HTTP request URL builder*: Path template with `{table_name}` token (e.g., `/api/v2/{table_name}`). |
| `default_delta_filter` | string | Optional | *Used in request query param builder*: Template string for incremental filtering using `{last_load_date}` and `{upper_bound}`. |
| `response_records_key` | string | Optional (Default: `""`) | *Used in HTTP response parser*: Key in API JSON response holding the record list (`result`, `entities`, `value`). |
| `batch_size` | integer | Optional | *Used in pagination handler*: Overrides default batch size for this specific source. |
| `connection_type` | string | Optional | *Used in `connectors/__init__.py`*: Set to `database` for relational DB sources. |
| `type` | string | Optional | *Used in `connectors/__init__.py`*: Set to `s3_file` for external S3 bucket feeds. |

#### Specific Connector Parameters
* **Moveworks Parallel Sharding (`parallel_processing`)**:
  - `enabled`: boolean (Optional, default `false`) — Enables parallel time-window chunking.
  - `max_workers`: integer (Optional, default `5`) — Thread pool worker count for concurrent API requests.
  - `shard_window_days`: integer (Optional, default `15`) — Days per time slice when partitioning long extraction intervals.
* **Relational Database (`connection_type: "database"`)**:
  - `db_type`: string (**Required**) — Database dialect (`postgresql`, `mysql`).
  - `query_template`: string (Optional) — Custom SQL extraction wrapper (e.g., `SELECT * FROM {table_name} WHERE {query_filter}`).
  - `fetch_size`: integer (Optional, default `10000`) — JDBC cursor streaming batch size.
* **S3 File Feeds (`type: "s3_file"`)**:
  - `source_bucket`: string (**Required**) — External bucket containing incoming data drops.
  - `file_prefix`: string (Optional) — S3 key prefix filter for incoming files.
  - `file_format`: string (**Required**) — Input format (`csv`, `json`, `parquet`).
  - `delimiter`: string (Optional, default `,`) — Delimiter for CSV feeds.
  - `has_header`: boolean (Optional, default `true`) — Header row presence flag.
  - `multiLine`: boolean (Optional, default `true`) — Handles multi-paragraph text fields by parsing embedded newlines inside quoted fields.
  - `escape`: string (Optional, default `\\`) — Escape character for quotes/characters in CSV.
  - `fetch_mode`: string (Optional, default `all`) — `all` (incremental modified files) or `latest` (most recent file only).

---

### 2.3 `tables.<table>` Block

| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `initial_load_date` | string | **Required** (If no default) | *Used in `_get_watermark()`*: Cold-start extraction timestamp (`YYYY-MM-DD HH:MM:SS`) when no S3 state exists. |
| `upper_bound` | string | Optional | *Used in `_get_watermark()`*: Table-specific upper bound cutoff overriding the pipeline default. |
| `query_override` | string | Optional | *Used in connector query builder*: Custom API filter expression or SQL WHERE clause replacing `default_delta_filter`. |
| `custom_endpoint` | string | Optional | *Used in HTTP request URL builder*: Specific API path overriding `api_endpoint_template` for non-standard endpoints. |
| `file_path` | string | Optional (S3 Feeds) | *Used in `s3_connector.py`*: Specific S3 subfolder path for this table's files. |
| `fetch_mode` | string | Optional (S3 Feeds) | *Used in `s3_connector.py`*: Table-level override for `all` vs `latest` file selection. |
