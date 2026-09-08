# 🚀 Bronze Layer: Features & Configuration Reference Guide

This guide provides:
1. An exhaustive inventory of all **21 enterprise features** built into the Bronze Ingestion Layer.
2. The **Ideal Canonical Structure** of `bronze_config.json` that must be strictly followed.
3. A complete **Parameter Dictionary** documenting every single parameter (required vs. optional, purpose, default value, and operational impact).

---

## 📊 1. Bronze Feature Inventory (21 Enterprise Features)

| # | Feature Category | Feature Name | Primary Benefit |
| :-: | :--- | :--- | :--- |
| **1** | **Ingestion** | Multi-Source Connector Factory | Plug-and-play ingestion for ServiceNow, Moveworks, Genesys, Relational DBs, and S3 |
| **2** | **Security** | Enterprise OAuth2 Token Caching | High-throughput API calls with automatic token refresh and credential caching |
| **3** | **Security** | Centralized AWS Secrets Manager | Zero hardcoded credentials; dynamic retrieval via IAM roles |
| **4** | **State** | High-Watermark Incremental Tracking | Ingests only new and modified records; avoids redundant data extraction |
| **5** | **State** | Dual-Layer State Persistence | Watermark recorded in S3 JSON state file AND Glue Catalog table `tbl_watermarks` |
| **6** | **State** | Full Refresh Override Switch | On-demand `--FULL_REFRESH true` bypass for historical backfills or data reconciliation |
| **7** | **Performance** | Memory-Safe Streaming Chunking | Extracts & streams in batches (e.g. 5,000 rows); guarantees zero OOM on low DPUs |
| **8** | **Integrity** | Atomic Two-Phase Staging | Writes to ephemeral staging first; zero partial/corrupted files in final partitions |
| **9** | **Integrity** | Automatic Failed Staging Cleanup | Purges partial files on failure; maintains pristine data lake cleanliness |
| **10** | **Partitioning** | Hive-Style Partition Layout | Ingests into `_ingested_at=<TIMESTAMP>` for high-performance Athena pruning |
| **11** | **Lineage** | Technical Audit Column Injection | Automatically stamps `_ingested_at`, `_source_system`, `_source_table`, `_execution_id` |
| **12** | **Storage** | Multi-Format Serialization | Native support for Apache Parquet (Snappy-compressed), NDJSON, and CSV |
| **13** | **Governance** | Dynamic Schema Inference | Automatically detects data types from raw payloads and maps to Glue primitive types |
| **14** | **Governance** | Dynamic JSON Key Flattening | Automatically flattens nested JSON hierarchies (`parent_child` notation) |
| **15** | **Catalog** | Instant Glue Partition Registration | Calls Glue API directly; new partitions are queryable in Athena instantly (0-sec wait) |
| **16** | **Catalog** | Glue Database Auto-Provisioning | Ensures catalog database (`uax_datalake_db_dev`) exists before cataloging tables |
| **17** | **Catalog** | On-Demand Glue Crawler Triggering | Triggers crawler for deep schema reconciliation with non-blocking concurrency handling |
| **18** | **Resilience** | 3-Tier Error Handling Modes | Configurable modes: `FAIL_FAST`, `CONTINUE_ON_ERROR`, and `QUARANTINE` |
| **19** | **Resilience** | Dead-Letter Queue (DLQ) Quarantine | Isolates malformed records to S3 quarantine prefix without aborting the job |
| **20** | **Resilience** | Resilient HTTP Client with Backoff | Exponential backoff retries with jitter for handling rate limits (HTTP 429/503) |
| **21** | **Observability**| CloudWatch Metrics & Execution Auditing | Emits custom CloudWatch metrics + detailed S3 JSON run audit reports |

---

## 🏛️ 2. Ideal Configuration Structure (`bronze_config.json`)

The configuration file must strictly follow this two-tier JSON schema:
- **`pipeline_defaults`**: Global runtime settings, S3 folder layouts, error containment modes, and Glue Catalog definitions.
- **`source_systems`**: Individual connectors with API endpoint templates, query filters, pagination controls, and table-level overrides.

```json
{
  "pipeline_defaults": {
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
    "cloudwatch_namespace": "UAX/DataPipeline/Ingestion",
    "glue_catalog": {
      "enabled": true,
      "database_name": "uax_datalake_db_dev",
      "table_prefix": "raw_tbl_",
      "crawler_name": "uax-datalake-bronze-crawler-dev",
      "trigger_crawler": true,
      "sync_watermark_table": true,
      "watermark_table_name": "raw_tbl_watermarks"
    }
  },
  "source_systems": {
    "servicenow": {
      "base_url": "https://your-instance.service-now.com",
      "api_endpoint_template": "/api/now/table/{table_name}",
      "query_param_template": "sysparm_query={query_filter}^ORDERBYsys_updated_on&sysparm_limit={limit}&sysparm_offset={offset}",
      "default_delta_filter": "sys_updated_on>={last_load_date}",
      "default_tables": ["incident", "change_request", "problem", "sys_user"],
      "response_records_key": "result",
      "table_initial_load_dates": {
        "incident": "2024-01-01T00:00:00Z",
        "change_request": "2024-03-01T00:00:00Z",
        "problem": "2024-06-01T00:00:00Z",
        "sys_user": "2024-01-01T00:00:00Z"
      },
      "table_query_overrides": {
        "incident": "active=true^sys_updated_on>={last_load_date}"
      },
      "custom_table_endpoints": {
        "u_special_report": "/api/now/v1/custom_reports"
      }
    },
    "moveworks": {
      "base_url": "https://api.moveworks.ai",
      "assistant_name": "acmecorp-conversations-rest-api",
      "api_endpoint_template": "/export/v1/records/{table_name}",
      "default_delta_filter": "last_updated_time gt '{last_load_date}'",
      "default_tables": ["conversations", "interactions", "users"],
      "response_records_key": "value",
      "orderby": "last_updated_time desc",
      "table_initial_load_dates": {
        "conversations": "2024-01-01T00:00:00Z",
        "interactions": "2024-01-01T00:00:00Z",
        "users": "2024-01-01T00:00:00Z"
      },
      "table_query_overrides": {},
      "custom_table_endpoints": {}
    },
    "genesys": {
      "base_url": "https://api.mypurecloud.com",
      "api_endpoint_template": "/api/v2/{table_name}",
      "query_param_template": "pageSize={limit}&pageNumber={page_number}&interval={last_load_date}",
      "default_delta_filter": "{last_load_date}",
      "default_tables": ["conversations", "users", "queues"],
      "response_records_key": "entities",
      "table_initial_load_dates": {
        "conversations": "2024-01-01T00:00:00Z",
        "users": "2024-01-01T00:00:00Z",
        "queues": "2024-01-01T00:00:00Z"
      },
      "table_query_overrides": {},
      "custom_table_endpoints": {}
    },
    "postgresql": {
      "connection_type": "database",
      "db_type": "postgresql",
      "driver": "org.postgresql.Driver",
      "port": 5432,
      "default_delta_filter": "updated_at >= '{last_load_date}'",
      "default_tables": ["orders", "order_items", "customers"],
      "query_template": "SELECT * FROM {table_name} WHERE {query_filter} ORDER BY updated_at ASC",
      "fetch_size": 10000,
      "table_initial_load_dates": {
        "orders": "2024-01-01T00:00:00Z",
        "order_items": "2024-01-01T00:00:00Z",
        "customers": "2024-01-01T00:00:00Z"
      },
      "table_query_overrides": {}
    },
    "vendor_a_s3": {
      "type": "s3_file",
      "source_bucket": "vendor-a-incoming-bucket",
      "file_prefix_template": "raw_feed/{table_name}/",
      "file_format": "csv",
      "delimiter": ",",
      "has_header": true,
      "encoding": "utf-8",
      "default_tables": ["employee_feed", "orders"],
      "table_initial_load_dates": {
        "employee_feed": "2024-01-01T00:00:00Z",
        "orders": "2024-01-01T00:00:00Z"
      },
      "table_file_patterns": {
        "employee_feed": ".csv",
        "orders": ".csv"
      }
    }
  }
}
```

---

## 📖 3. Complete Parameter Reference & Impact Analysis

### Group A: Global Runtime Settings (`pipeline_defaults`)

#### 1. `batch_size`
- **Type**: `integer`
- **Requirement**: **Optional** (Default: `1000`, or `500` for Moveworks)
- **CLI Override**: `--BATCH_SIZE <int>`
- **Why we use it**: Defines how many records the connector requests per API page / network round-trip.
- **System Impact**:
  - *Too small (<100)*: Causes excessive HTTP requests, increases total runtime, and triggers API rate limiters.
  - *Too large (>5000)*: Source APIs may time out (HTTP 504) or return 413 Payload Too Large.
  - *Omitted*: Automatically falls back to `1000` (or `500` for Moveworks).

#### 2. `s3_chunk_size`
- **Type**: `integer`
- **Requirement**: **Optional** (Default: `10000`)
- **CLI Override**: `--S3_CHUNK_SIZE <int>`
- **Why we use it**: Controls how many in-memory records are buffered before flushing and writing a new Parquet file (`part-XXXXX.parquet`) to S3 staging.
- **System Impact**:
  - *Memory Protection*: Guarantees memory usage remains bounded and constant. The Glue worker never runs out of memory (OOM), even when extracting 50 million records.
  - *File Sizing*: Setting this between `10000` and `50000` yields optimal 10MB–50MB Parquet part files, perfectly optimized for Amazon Athena and PySpark partition pruning.

#### 3. `max_retries`
- **Type**: `integer`
- **Requirement**: **Optional** (Default: `3`)
- **Why we use it**: Configures how many retry attempts the `ResilientHttpClient` performs upon receiving transient HTTP errors (429, 500, 502, 503, 504).
- **System Impact**:
  - Prevents pipeline aborts from momentary network glitches or upstream throttling. Uses randomized exponential backoff jitter.

#### 4. `state_prefix`
- **Type**: `string`
- **Requirement**: **Optional** (Default: `"metadata/bronze"`)
- **CLI Override**: Derived from bucket configuration
- **Why we use it**: Specifies the S3 folder prefix where table high-watermark JSON files are persisted.
- **System Impact**:
  - Dictates S3 location: `s3://<bucket>/<state_prefix>/<source_system>/<table_name>_watermark.json`.
  - Must remain constant across runs so incremental loads reliably detect the previous watermark.

#### 5. `bronze_data_prefix`
- **Type**: `string`
- **Requirement**: **Optional** (Default: `"bronze/data"`)
- **CLI Override**: `--BRONZE_DATA_PREFIX <path>`
- **Why we use it**: Root folder under which all raw data partitions are stored.
- **System Impact**:
  - Directs S3 physical path: `s3://<bucket>/<bronze_data_prefix>/<source>/<table_name>/_ingested_at=<TIMESTAMP>/`.
  - Silver PySpark ETL and Athena tables point directly to this location. Changing this path requires updating downstream Silver configs.

#### 6. `output_format`
- **Type**: `string` (Allowed values: `"parquet"`, `"json"`, `"csv"`)
- **Requirement**: **Optional** (Default: `"parquet"`)
- **CLI Override**: `--OUTPUT_FORMAT <format>`
- **Why we use it**: Defines the file serialization format for raw Bronze files.
- **System Impact**:
  - `"parquet"`: Recommended. Provides columnar compression, fast Athena queries, and native Glue catalog integration.
  - `"json"` / `"csv"`: Can be chosen if raw payloads cannot be strictly structured into columnar format.

#### 7. `parquet_compression`
- **Type**: `string` (Allowed values: `"snappy"`, `"gzip"`, `"zstd"`, `"none"`)
- **Requirement**: **Optional** (Default: `"snappy"`)
- **CLI Override**: `--PARQUET_COMPRESSION <codec>`
- **Why we use it**: Specifies the compression algorithm applied when writing Parquet chunks.
- **System Impact**:
  - `"snappy"` balances lightning-fast write speed with high compression ratios (~70% space reduction).

#### 8. `flatten_nested_json`
- **Type**: `boolean`
- **Requirement**: **Optional** (Default: `true`)
- **CLI Override**: `--FLATTEN_NESTED_JSON true|false`
- **Why we use it**: Unnests inner JSON dictionaries into flat column names (e.g. `{"assigned_to": {"link": "...", "value": "xyz"}}` -> `assigned_to_link`, `assigned_to_value`).
- **System Impact**:
  - `true`: Simplifies downstream SQL queries and makes data instantly queryable without Athena `JSON_EXTRACT` functions.
  - `false`: Stores complex objects as raw JSON strings.

#### 9. `flatten_separator`
- **Type**: `string`
- **Requirement**: **Optional** (Default: `_`)
- **CLI Override**: `--FLATTEN_SEPARATOR <char>`
- **Why we use it**: Delimiter character used when combining parent and child JSON keys during flattening.
- **System Impact**: Standard `_` ensures column names adhere to Athena/Hive regex: `^[a-z0-9_]+$`.

#### 10. `error_handling_mode`
- **Type**: `string` (Allowed values: `"FAIL_FAST"`, `"CONTINUE_ON_ERROR"`, `"QUARANTINE"`)
- **Requirement**: **Optional** (Default: `"CONTINUE_ON_ERROR"`)
- **CLI Override**: `--ERROR_HANDLING_MODE <mode>`
- **Why we use it**: Governs job behavior when an individual table extraction fails during a multi-table batch run.
- **System Impact**:
  - `FAIL_FAST`: Immediately halts the Glue Job on the first error, rolls back staging, and raises an exception.
  - `CONTINUE_ON_ERROR`: Cleans up the failed table, logs the error, and proceeds to extract the remaining tables in the batch.
  - `QUARANTINE`: Writes corrupted or unparseable records to an S3 DLQ prefix (`quarantine/`) and continues processing.

#### 11. `cloudwatch_namespace`
- **Type**: `string`
- **Requirement**: **Optional** (Default: `"UAX/DataPipeline/Ingestion"`)
- **CLI Override**: `--CLOUDWATCH_NAMESPACE <namespace>`
- **Why we use it**: CloudWatch Metric namespace under which ingestion statistics (`IngestionRecords`, `IngestionDuration`, `IngestionErrors`) are published.
- **System Impact**: Powers real-time CloudWatch dashboards and automated SNS alert alarms.

---

### Group B: Glue Catalog & Metadata Settings (`pipeline_defaults.glue_catalog`)

#### 12. `enabled`
- **Type**: `boolean`
- **Requirement**: **Optional** (Default: `true`)
- **CLI Override**: `--SYNC_GLUE_CATALOG true|false`
- **Why we use it**: Master toggle to enable or disable automatic AWS Glue Data Catalog table and partition management.
- **System Impact**: If set to `false`, data is written to S3, but no Glue Catalog tables or partitions are created or updated.

#### 13. `database_name`
- **Type**: `string`
- **Requirement**: **MANDATORY / STRICTLY REQUIRED**
- **CLI Override**: `--GLUE_DATABASE <name>`
- **Current Value**: `"uax_datalake_db_dev"`
- **Why we use it**: Defines the target Glue Data Catalog database for Bronze tables.
- **System Impact**:
  - **CRITICAL**: If missing or empty, the pipeline immediately raises a `ValueError` and aborts on line 335.
  - Uses underscores (`uax_datalake_db_dev`) to comply with Presto/Hive/Power BI identifier standards.

#### 14. `table_prefix`
- **Type**: `string`
- **Requirement**: **MANDATORY / STRICTLY REQUIRED**
- **CLI Override**: `--GLUE_TABLE_PREFIX <prefix>`
- **Current Value**: `"raw_tbl_"`
- **Why we use it**: Distinguishes raw Bronze catalog tables from refined Silver tables inside the unified database.
- **System Impact**:
  - **CRITICAL**: If missing, raises a `ValueError` on line 348.
  - Generates table name: `uax_datalake_db_dev.raw_tbl_<table_name>` (e.g. `raw_tbl_incident`).

#### 15. `crawler_name`
- **Type**: `string`
- **Requirement**: **Optional** (Default: `""`)
- **CLI Override**: `--BRONZE_CRAWLER_NAME <name>` or `--CRAWLER_NAME <name>`
- **Why we use it**: Specifies the AWS Glue Crawler to optionally trigger after ingestion for deep schema reconciliation.
- **System Impact**: Triggered only if `trigger_crawler` is `true`.

#### 16. `trigger_crawler`
- **Type**: `boolean`
- **Requirement**: **Optional** (Default: `true`)
- **CLI Override**: `--TRIGGER_CRAWLER true|false`
- **Why we use it**: Determines whether to invoke `glue:StartCrawler` after raw ingestion completes.
- **System Impact**: Since `uax_bronze_load.py` calls `create_partition()` directly via API, setting this to `false` saves AWS Crawler DPU costs while keeping data immediately queryable.

#### 17. `sync_watermark_table`
- **Type**: `boolean`
- **Requirement**: **Optional** (Default: `true`)
- **CLI Override**: `--SYNC_WATERMARK_TABLE true|false`
- **Why we use it**: Enables updating the centralized Glue Catalog watermark table upon every successful run.
- **System Impact**: Ensures `tbl_watermarks` reflects the exact state of all tables in the lake.

#### 18. `watermark_table_name`
- **Type**: `string`
- **Requirement**: **MANDATORY if `sync_watermark_table` is `true`**
- **CLI Override**: `--WATERMARK_TABLE_NAME <name>`
- **Current Value**: `"raw_tbl_watermarks"`
- **Why we use it**: Names the Glue Catalog table storing pipeline watermarks.
- **System Impact**: If empty when `sync_watermark_table` is `true`, raises a `ValueError` on line 374.

---

### Group C: Source System Parameters (`source_systems.<source_system>`)

#### 19. `base_url`
- **Type**: `string` (Valid HTTPS URL)
- **Requirement**: **Required for API Connectors** (ServiceNow, Moveworks, Genesys)
- **CLI Override**: `--BASE_URL <url>`
- **Why we use it**: Host URL of the source system instance (e.g. `https://your-instance.service-now.com`).
- **System Impact**: All API endpoint URLs are constructed relative to this base URL.

#### 20. `api_endpoint_template`
- **Type**: `string`
- **Requirement**: **Required for API Connectors**
- **Why we use it**: URI template containing `{table_name}` placeholder (e.g. `/api/now/table/{table_name}`).
- **System Impact**: Resolved dynamically for each table extracted in the run.

#### 21. `query_param_template`
- **Type**: `string`
- **Requirement**: **Required for API Connectors**
- **Why we use it**: Defines the URL query string structure with pagination placeholders (`{limit}`, `{offset}`, `{page_number}`, `{query_filter}`).
- **System Impact**: Enforces compliant pagination for ServiceNow (`sysparm_offset`), Moveworks (`cursor`), and Genesys (`pageNumber`).

#### 22. `default_delta_filter`
- **Type**: `string`
- **Requirement**: **Required for Incremental Connectors**
- **Why we use it**: Baseline delta query filter containing the `{last_load_date}` placeholder.
- **Examples**:
  - ServiceNow: `"sys_updated_on>={last_load_date}"`
  - Moveworks: `"last_updated_time gt '{last_load_date}'"`
  - SQL DB: `"updated_at >= '{last_load_date}'"`
- **System Impact**: Injected into the extraction request so only newly modified records are queried from the source.

#### 23. `default_tables`
- **Type**: `array of strings`
- **Requirement**: **MANDATORY in configuration** (if not provided via CLI)
- **CLI Override**: `--SOURCE_TABLE_NAME <comma_separated_tables>`
- **Why we use it**: List of tables to ingest by default when the job is triggered without explicit `--SOURCE_TABLE_NAME`.
- **System Impact**:
  - **CRITICAL**: If missing from config AND omitted from CLI, raises a `ValueError` on line 228.

#### 24. `response_records_key`
- **Type**: `string`
- **Requirement**: **Required for REST API Connectors**
- **CLI Override**: `--RESPONSE_RECORDS_KEY <key>`
- **Why we use it**: Specifies the JSON root key where the list of records resides in the HTTP response.
- **Examples**:
  - ServiceNow: `"result"`
  - Moveworks: `"value"`
  - Genesys: `"entities"`
- **System Impact**: If misconfigured, the connector finds 0 records in the response and logs a warning.

#### 25. `table_initial_load_dates`
- **Type**: `object / dictionary` mapping `table_name` to ISO-8601 timestamp string (`"YYYY-MM-DDTHH:MM:SSZ"`)
- **Requirement**: **STRICTLY ENFORCED (MANDATORY per table)**
- **CLI Override**: `--INITIAL_LOAD_DATE <timestamp>`
- **Why we use it**: Defines the baseline timestamp for the first-ever run of each table before any watermark exists in S3.
- **System Impact**:
  - **CRITICAL SECURITY / STABILITY RULE**: Line 111 of `config_loader.py` **strictly forbids null or missing initial load dates** to prevent accidentally extracting decades of historical data from source APIs.
  - If a table is listed in `default_tables` but missing from `table_initial_load_dates`, the job **intentionally throws a `ValueError`**!

#### 26. `table_query_overrides`
- **Type**: `object / dictionary` mapping `table_name` to query string
- **Requirement**: **Optional** (Defaults to `{}`)
- **CLI Override**: `--CUSTOM_QUERY <query>`
- **Why we use it**: Allows specific tables to include custom business filters (e.g. `incident` table only extracts `active=true`).
- **System Impact**: Overrides `default_delta_filter` for the specified table while automatically retaining the high-watermark delta condition.

#### 27. `custom_table_endpoints`
- **Type**: `object / dictionary` mapping `table_name` to URI string
- **Requirement**: **Optional** (Defaults to `{}`)
- **Why we use it**: Allows a specific table to query a non-standard API endpoint rather than using `api_endpoint_template`.
- **System Impact**: Solves edge cases where custom tables or reporting views live on distinct API paths.

---

### Group D: Relational Database Specific Parameters

#### 28. `connection_type` & `db_type`
- **Type**: `string` (`"database"`, `"postgresql"`, `"mysql"`, `"oracle"`, `"mssql"`)
- **Requirement**: Required when using the Database Connector.
- **Why we use it**: Tells the connector factory to load JDBC drivers and SQL dialects.

#### 29. `query_template` & `fetch_size`
- **Type**: `string` & `integer` (Default `fetch_size`: `10000`)
- **Requirement**: Optional for Database connector.
- **Why we use it**: Configures cursor streaming from relational databases so multi-gigabyte queries do not crash the Glue execution container.

---

### Group E: S3 External File Ingestion Parameters

#### 30. `source_bucket` & `file_prefix_template`
- **Type**: `string`
- **Requirement**: Required when ingesting from external partner S3 buckets.
- **Why we use it**: Locates incoming vendor feeds (e.g. `raw_feed/{table_name}/`).

#### 31. `file_format`, `delimiter`, `has_header`, `encoding`
- **Type**: `string`, `boolean`
- **Requirement**: Required for S3 File connector.
- **Why we use it**: Parses raw CSV, TSV, or JSON file drops into structured Bronze records.

---

## 🔒 4. Strict Configuration Rules & Invariants

To guarantee 100% pipeline stability, every deployment of `bronze_config.json` must pass these strict validation rules:

1. **No Null Initial Load Dates**:
   Every table listed in `default_tables` **MUST** have an entry in `table_initial_load_dates` with a valid ISO-8601 timestamp (e.g. `"2024-01-01T00:00:00Z"`).
2. **Valid Database Identifier**:
   `pipeline_defaults.glue_catalog.database_name` must strictly match `uax_datalake_db_dev` (only lowercase letters, numbers, and underscores).
3. **Table Prefix Consistency**:
   `table_prefix` must end with an underscore (e.g. `"raw_tbl_"`).
4. **Valid JSON Formatting**:
   No trailing commas; all property keys must be double-quoted.
5. **Secrets Manager Alignment**:
   Every source system must have a corresponding secret in AWS Secrets Manager matching:
   `uax-datalake/<source_system>-credentials-<environment>`
   containing the required authentication keys (`username`/`password` or `client_id`/`client_secret`).
