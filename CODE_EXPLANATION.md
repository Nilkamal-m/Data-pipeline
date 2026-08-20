# Deep-Dive Technical Code & Configuration Manual

This document provides a low-level, component-by-component architectural and technical justification for **every script, connector, function, and configuration setting** in the UAX Data Pipeline.

---

## 📌 Table of Contents
1. [Core Design Philosophy: Why This Architecture?](#1-core-design-philosophy-why-this-architecture)
2. [Bronze Ingestion Engine Deep Dive (`bronze/script/`)](#2-bronze-ingestion-engine-deep-dive)
   - [uax_bronze_load.py (Main Execution Driver)](#uax_bronze_loadpy)
   - [config_loader.py (Configuration Engine)](#config_loaderpy)
   - [connectors/__init__.py (Factory Registry)](#connectors__init__py)
   - [connectors/http_client.py (Authentication & HTTP Client)](#connectorshttp_clientpy)
   - [connectors/oauth.py (Token Manager & Expiry Cache)](#connectorsoauthpy)
   - [connectors/s3_file.py (File Ingestion Connector)](#connectorss3_filepy)
   - [connectors/database.py (JDBC Connector)](#connectorsdatabasepy)
3. [Silver Apache Iceberg ETL Deep Dive (`silver/script/`)](#3-silver-apache-iceberg-etl-deep-dive)
   - [silver_iceberg_etl.py (PySpark Driver)](#silver_iceberg_etlpy)
   - [transformer.py (Deduplication & Window Functions)](#transformerpy)
4. [Configuration Schemas Explained Line-by-Line](#4-configuration-schemas-explained-line-by-line)
   - [bronze_config.json](#bronze_configjson)
   - [silver_config.json](#silver_configjson)

---

## 1. Core Design Philosophy: Why This Architecture?

Traditional data ingestion pipelines suffer from three common issues:
1. **Hardcoded Code per Source**: Creating a new python script for every vendor causes massive code duplication and maintenance burden.
2. **Memory Crashes (OOM)**: Pulling entire API responses or large files into Glue RAM crashes jobs when extracting millions of rows.
3. **Data Loss / Partial Ingestion**: If an API call fails mid-way, half-extracted files corrupt S3 partitions, causing duplicate or missing records in downstream tables.

### 💡 How Our Code Solves These Problems:
*   **100% Config-Driven**: All endpoints, tables, formats, buckets, and watermarks are defined in `bronze_config.json`. Adding a new REST API or S3 vendor requires **zero code changes**—only a JSON block update.
*   **Memory-Safe Chunking Callback**: Records are processed in memory chunks (e.g. 10,000 records) and immediately written to S3, releasing RAM immediately.
*   **Atomic Staging Promotion**: Files are written to `_staging/exec_<ID>/` first. If extraction succeeds, files are atomically promoted to `bronze/`. If extraction fails, staging files are purged, keeping S3 100% clean.
*   **Strict High-Water Mark (HWM) Enforcement**: Prevents accidental ingestion of entire historical datasets by throwing an explicit error if initial load dates are missing/null.

---

## 2. Bronze Ingestion Engine Deep Dive

### `uax_bronze_load.py`
The primary driver script executed by AWS Glue Python Shell.

#### Key Functions & Technical Justification:

1. **`parse_arguments()`**:
   *   **Why**: Parses CLI arguments passed by Step Functions (`--SOURCE_SYSTEM`, `--TABLE_NAME`, `--CONFIG_S3_PATH`).
   *   **How it Works**: Iterates through `sys.argv` looking for `--key value` pairs. Merges CLI overrides on top of settings loaded from `bronze_config.json`.

2. **`get_secret(secret_name)`**:
   *   **Why**: Retrieves API credentials securely from AWS Secrets Manager.
   *   **Design Choice**: Returns `{}` if `secret_name` is empty or not found. This allows file-based S3 ingestion (which uses AWS IAM roles) to run without needing an API secret.

3. **`flatten_dict(d, parent_key, sep)`**:
   *   **Why**: REST APIs (like ServiceNow or Moveworks) return heavily nested JSON objects. Parquet requires a flat columnar schema.
   *   **How it Works**: Recursively walks nested dictionaries, building dot/underscore delimited column names (e.g. `assigned_to.link` $\rightarrow$ `assigned_to_link`). Array objects are JSON-stringified to preserve schema stability.

4. **`serialize_chunk_to_bytes(records_chunk, output_format, parquet_compression)`**:
   *   **Why**: Converts python dict records into binary streams for S3 storage.
   *   **How it Works**: Uses Pandas/PyArrow to serialize records to Parquet format with Snappy compression. If Parquet fails due to un-flattenable structures, automatically falls back to JSON bytes.

5. **`get_last_load_date(...)` & `update_last_load_date(...)`**:
   *   **Why**: Implements incremental delta extraction.
   *   **How it Works**: Reads `s3://<bucket>/metadata/<source>/<table_name>/watermark.json`. If missing, calls `ConfigLoader.get_table_initial_load_date()` to resolve the initial load date. `update_last_load_date()` updates the watermark ONLY after staging promotion succeeds.

6. **`promote_staging_to_bronze(...)` & `cleanup_failed_staging(...)`**:
   *   **Why**: Guarantees ACID-like atomic execution on S3.
   *   **How it Works**: Copies files from `_staging/` to `bronze/<source>/<table_name>/year=YYYY/month=MM/day=DD/` using `s3_client.copy_object()`, then deletes staging keys.

---

### `config_loader.py`
The configuration engine of the pipeline.

#### Key Methods & Technical Justification:

1. **`load_config(config_s3_path, s3_client)`**:
   *   **Why**: Allows loading `bronze_config.json` dynamically from S3 or falling back to local files.
   *   **Cache Pattern**: Caches the loaded JSON object in class variable `_config_cache` so multiple table iterations don't re-download the JSON file repeatedly.

2. **`get_table_initial_load_date(...)`**:
   *   **Why**: **Strict Load Date Enforcement**.
   *   **Rationale**: Arbitrary fallback dates (like `1970-01-01`) risk pulling millions of unwanted historical records. If a table has no initial load date in `bronze_config.json` and no CLI date is passed, it raises a `ValueError`, halting execution cleanly.

3. **`get_table_query_filter(...)`**:
   *   **Why**: Combines table-specific query filters with High-Water Mark timestamps.
   *   **How it Works**: Interpolates `{last_load_date}` into strings like `sys_updated_on>={last_load_date}` or `active=true^sys_updated_on>={last_load_date}`.

---

### `connectors/__init__.py`
The factory module that maps source system names to connector classes.

```python
CONNECTOR_MAP = {
    'servicenow': ServiceNowConnector,
    'genesys': GenesysConnector,
    'moveworks': MoveworksConnector,
    'database': DatabaseConnector,
    's3_file': S3FileConnector
}
```

*   **`get_connector(source_system, source_config)`**:
    *   Checks `CONNECTOR_MAP` for direct source system names.
    *   If not found, inspects `"type"` field inside `source_config` (e.g. `"type": "s3_file"`).
    *   Allows adding unlimited S3 file vendors (`vendor_a_s3`, `vendor_b_s3`) without changing python code!

---

### `connectors/http_client.py` & `connectors/oauth.py`
Unified HTTP client and token manager.

*   **Basic Auth**: Detects `"auth_type": "basic"`, encodes `username:password` into Base64, and sets `Authorization: Basic <b64>`.
*   **OAuth 2.0 Token Manager**:
    *   **In-Memory Token Cache**: `OAuth2Client._token_cache` caches access tokens with expiration timestamps (`expires_in - 60s`).
    *   **Automatic 401 Retry**: If an API call returns `401 Unauthorized`, `http_client.py` forces a token refresh from `token_url` and retries the API request automatically.

---

### `connectors/s3_file.py`
File-based ingestion connector for external S3 buckets.

*   **Multi-Format Parsing (`parse_file_bytes`)**:
    *   **`csv` / `flat`**: Uses `csv.DictReader` to convert lines into dictionary rows.
    *   **`json` / `ndjson`**: Parses newline-delimited JSON or JSON arrays into dictionaries.
    *   **`text`**: Converts plain text files into line-by-line structured records (`{"line_number": 1, "line_content": "..."}`).
    *   **`parquet`**: Reads parquet binary files using `pandas.read_parquet()`.
*   **Delta Scanning**: Filters files in `source_bucket` by checking `LastModified > last_load_date`.

---

## 3. Silver Apache Iceberg ETL Deep Dive

### `silver_iceberg_etl.py` & `transformer.py`

1. **Apache Iceberg Integration**:
   - Uses PySpark configured with `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`.
   - Iceberg provides ACID transactions, schema evolution, and fast time-travel analytical queries.

2. **Deduplication (`transformer.py`)**:
   - Uses PySpark Window Functions:
     ```python
     windowSpec = Window.partitionBy(primary_key).orderBy(col(updated_at_col).desc())
     deduped_df = df.withColumn("row_num", row_number().over(windowSpec)).filter("row_num = 1").drop("row_num")
     ```
   - Guarantees that even if raw Bronze contains duplicate extracted records, Silver Iceberg tables only store the latest state of each primary key.

3. **Merge Strategies**:
   - **`upsert`**: Executes Spark SQL `MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`.
   - **`insert_only`**: Appends new rows.
   - **`scd2`**: Maintains historical row versions with `valid_from` and `valid_to` timestamps.

---

## 4. Configuration Schemas Explained Line-by-Line

### `bronze_config.json`

```json
{
  "pipeline_defaults": {
    "batch_size": 1000,                  // Number of records requested per REST API page / SQL fetch
    "s3_chunk_size": 10000,              // Number of records accumulated before flushing chunk to S3
    "output_format": "parquet",          // Bronze storage format (parquet or json)
    "parquet_compression": "snappy",     // Compression algorithm for parquet files
    "flatten_nested_json": true,         // Recursively flattens nested JSON payloads
    "flatten_separator": "_",            // Separator character for flattened column names
    "error_handling_mode": "CONTINUE_ON_ERROR", // CONTINUE_ON_ERROR (process remaining tables) vs HALT_ON_ERROR
    "cloudwatch_namespace": "UAX/DataPipeline/Ingestion" // CloudWatch custom metric namespace
  },
  "source_systems": {
    "servicenow": {
      "base_url": "https://your-instance.service-now.com",
      "api_endpoint_template": "/api/now/table/{table_name}", // Interpolates table_name dynamically
      "query_param_template": "sysparm_query={query_filter}^ORDERBYsys_updated_on&sysparm_limit={limit}&sysparm_offset={offset}",
      "default_delta_filter": "sys_updated_on>={last_load_date}",
      "default_tables": ["incident", "change_request", "problem", "sys_user"],
      "response_records_key": "result",  // JSON key containing record array in API response
      "table_initial_load_dates": {
        "incident": "2024-01-01T00:00:00Z" // Table-specific initial load date for first execution
      }
    },
    "vendor_a_s3": {
      "type": "s3_file",                 // Triggers S3FileConnector
      "source_bucket": "vendor-a-bucket",// External S3 bucket to extract files from
      "file_prefix_template": "raw/{table_name}/", // S3 prefix pattern for table files
      "file_format": "csv",              // File format: csv, flat, text, json, parquet
      "delimiter": ",",                  // CSV delimiter
      "has_header": true,                // Whether CSV includes a header line
      "default_tables": ["employee_feed"]
    }
  }
}
```

---

### `silver_config.json`

```json
{
  "pipeline_defaults": {
    "target_database": "uax-datalake-db-dev",
    "iceberg_catalog": "glue_catalog",
    "default_merge_strategy": "upsert"
  },
  "tables": {
    "incident": {
      "primary_key": "sys_id",
      "updated_at_column": "sys_updated_on",
      "merge_strategy": "upsert",
      "partition_cols": ["category"]
    }
  }
}
```
