# Bronze Layer — Component Reference & Code Location Index

This document provides a line-by-line master index and component map for the entire Bronze Layer codebase. It details where every class, function, connector, configuration property, and Glue argument is defined, implemented, and referenced.

---

## 1. Directory & File Inventory

| File Path | Component Role | Key Responsibilities |
| :--- | :--- | :--- |
| [`bronze/script/uax_bronze_load.py`](../../bronze/script/uax_bronze_load.py) | **Main Extraction Engine** | CLI parsing, extraction orchestration, record normalization, Parquet serialization, staging promotion, catalog registration, CloudWatch metrics. |
| [`bronze/script/config_loader.py`](../../bronze/script/config_loader.py) | **Configuration Engine** | Dynamic `{env}` interpolation, S3/local JSON loading, pipeline defaults inheritance, watermark state persistence. |
| [`bronze/script/connectors/__init__.py`](../../bronze/script/connectors/__init__.py) | **Connector Registry** | Router mapping `type` and `connection_type` configurations to specialized connector implementations. |
| [`bronze/script/connectors/http_client.py`](../../bronze/script/connectors/http_client.py) | **HTTP Transport** | `urllib3` retry pooling, exponential backoff, rate limiting (HTTP 429 `Retry-After`), socket timeout management. |
| [`bronze/script/connectors/oauth.py`](../../bronze/script/connectors/oauth.py) | **Authentication Handler** | OAuth2 Client Credentials token negotiation, expiration caching, token reuse across micro-batches. |
| [`bronze/script/connectors/servicenow.py`](../../bronze/script/connectors/servicenow.py) | **ServiceNow Connector** | Table API incremental ingestion, `sysparm_offset` pagination, `sys_updated_on` filtering, `"result"` envelope parsing. |
| [`bronze/script/connectors/genesys.py`](../../bronze/script/connectors/genesys.py) | **Genesys Cloud CX Connector** | Analytics conversations/users ingestion, `pageNumber` pagination, interval filtering, `"entities"` envelope parsing. |
| [`bronze/script/connectors/moveworks.py`](../../bronze/script/connectors/moveworks.py) | **Moveworks Connector** | OData nextLink pagination, cursor traversal, parallel time-window sharding (`ThreadPoolExecutor`), `"value"` envelope parsing. |
| [`bronze/script/connectors/database.py`](../../bronze/script/connectors/database.py) | **RDBMS Connector** | JDBC / relational streaming extraction (`fetch_size: 10000`), SQL dialect templating, incremental timestamp filtering. |
| [`bronze/script/connectors/s3_file.py`](../../bronze/script/connectors/s3_file.py) | **S3 File Drop Connector** | Vendor file ingestion, `fetch_mode` (`all` vs `latest`), pattern matching (`fnmatch`), CSV multi-line quote escape handling. |
| [`bronze/script/config/bronze_config.json`](../../bronze/script/config/bronze_config.json) | **Pipeline Configuration** | Blueprint defining pipeline defaults, S3 storage prefixes, source system endpoints, and table metadata. |
| [`bronze/script/protegrity_encryption.py`](../../bronze/script/protegrity_encryption.py) | **Protegrity PII Tokenizer** | Multi-column 500-record batching, exponential jitter backoff, Protegrity Protector Lambda API invocation (`/v2/protect`). |

---

## 2. Code Component Map: `uax_bronze_load.py`

| Function / Component | Line Range | Description & Architectural Purpose | References / Callers |
| :--- | :--- | :--- | :--- |
| `get_secret(secret_name)` | L129–L163 | Decrypts credentials from AWS Secrets Manager using standard and fallback key lookups. | Called in `main()` at L1320. |
| `parse_arguments()` | L166–L435 | Resolves Glue Job Arguments (`getResolvedOptions`) with priority over S3 configuration defaults. | Called in `main()` at L1251. |
| `flatten_and_expand_record()` | L440–L493 | Recursively flattens nested dicts AND explodes arrays of objects into multiple individual rows (1-to-N). | Called in `chunk_writer_callback()` at L1420. |
| `flatten_dict_single()` | L495–L529 | Recursively flattens nested dictionaries into a single flat row; joins primitive arrays with commas. | Helper for `flatten_and_expand_record()`. |
| `serialize_chunk_to_bytes()` | L532–L585 | Converts record batch into Snappy-compressed Apache Parquet. **Raises `RuntimeError` on failure (no JSON fallback).** | Called in `chunk_writer_callback()` at L1435. |
| `emit_cloudwatch_metrics()` | L587–L628 | Pushes execution telemetry (`RecordCount`, `DurationSeconds`, `Success`) to CloudWatch namespace. | Called in table success loop at L1550. |
| `get_table_state_key()` | L630–L634 | Resolves S3 object key for watermark tracking (`metadata/bronze/{source}/{table}/watermark.json`). | Watermark readers and writers. |
| `get_last_load_date()` | L636–L694 | Retrieves previous high-water mark timestamp from S3; falls back to `initial_load_date`. | Called in table extraction prep at L1352. |
| `update_last_load_date()` | L696–L742 | Atomically commits updated watermark state file to S3 post-extraction. | Called in table commit at L1530. |
| `promote_staging_to_bronze()` | L744–L778 | Moves written chunks from temporary staging (`staging/...`) to final S3 partition folder. | Called in atomic table commit at L1515. |
| `cleanup_failed_staging()` | L780–L800 | Purges uncommitted staging chunks if table extraction raises an exception. | Called in exception handler at L1564. |
| `save_execution_log()` | L802–L841 | Writes detailed JSON run execution summary card to S3 execution log directory. | Called in table completion at L1540. |
| `infer_glue_column_type()` | L843–L854 | Maps Python data types (`int`, `float`, `bool`, `str`) to AWS Glue Data Catalog data types. | Called in catalog table schema builder at L918. |
| `ensure_glue_database()` | L856–L876 | Verifies or creates the target AWS Glue Catalog Database. | Called in `main()` at L1305. |
| `sync_bronze_catalog_table()` | L878–L1096 | Automatically creates/evolves Glue Data Catalog Hive external tables pointing to Bronze S3 storage. | Called post-promotion at L1525. |
| `sanitize_watermark_files()` | L1098–L1133 | Rewrites legacy multi-line JSON watermark files into single-line NDJSON for Athena compatibility. | Called in catalog sync at L1145. |
| `sync_watermark_catalog_table()`| L1135–L1223 | Creates an Athena-queryable table over all Bronze watermark state files. | Called in `main()` at L1310. |
| `trigger_glue_crawler()` | L1225–L1247 | Triggers AWS Glue Crawler if enabled in configuration (`trigger_crawler: true`). | Called in post-job catalog sync at L1650. |
| `main()` | L1249–L1738 | Orchestrates the end-to-end extraction lifecycle across all configured tables. | Main CLI entrypoint. |

---

## 3. Connectors Component Map

| Connector Module | Class Name | File & Line Range | Enforced Response Key | Protocols & Strategies |
| :--- | :--- | :--- | :--- | :--- |
| `connectors/servicenow.py` | `ServiceNowConnector` | L1–L185 | `"result"` (L81–L87) | Basic Auth / OAuth2, `sysparm_offset` limit pagination, `sys_updated_on` delta filtering. |
| `connectors/genesys.py` | `GenesysConnector` | L1–L180 | `"entities"` (L76–L82) | OAuth2 Client Credentials, `pageNumber` pagination, ISO 8601 temporal interval extraction. |
| `connectors/moveworks.py` | `MoveworksConnector` | L1–L450 | `"value"` (L119–L125) | OData `@odata.nextLink` cursor traversal, `parallel_processing` time-window sharding. |
| `connectors/database.py` | `DatabaseConnector` | L1–L220 | N/A (Relational) | JDBC cursor streaming (`fetch_size`), dynamic SQL template generation (`query_template`). |
| `connectors/s3_file.py` | `S3FileConnector` | L1–L260 | N/A (File) | `fetch_mode` (`all` vs `latest`), `fnmatch` wildcard globbing, CSV quote escaping. |
| `connectors/http_client.py`| `HttpClient` | L1–L80 | N/A (Utility) | `urllib3.util.retry.Retry`, backoff multiplier, HTTP 429 `Retry-After` header sleep. |
| `connectors/oauth.py` | `OAuthHandler` | L1–L95 | N/A (Utility) | Token negotiation, TTL validation, thread-safe in-memory bearer token cache. |

---

## 4. Configuration Blueprint Cross-Reference

| `bronze_config.json` Parameter | Source Code Location | Fallback Value | Code Usage Description |
| :--- | :--- | :--- | :--- |
| `pipeline_defaults.bronze_bucket` | `uax_bronze_load.py` L220 | `uax-datalake-bronze-{env}` | Target S3 bucket where landing Parquet partitions are stored. |
| `pipeline_defaults.state_bucket` | `uax_bronze_load.py` L225 | `uax-datalake-state-{env}` | S3 bucket where high-water mark state JSON files reside. |
| `pipeline_defaults.bronze_data_prefix` | `uax_bronze_load.py` L230 | `bronze/data` | S3 prefix under which table data folders are partitioned. |
| `pipeline_defaults.runtime.batch_size` | `uax_bronze_load.py` L245 | `5000` | Number of extracted records accumulated before streaming chunk to S3. |
| `source_systems.<source>.base_url` | `connectors/*.py` L65 | Secrets Manager | Upstream REST API root URL endpoint. |
| `source_systems.<source>.api_endpoint_template` | `connectors/*.py` L70 | `/api/v1/{table_name}` | Endpoint path template with dynamic `{table_name}` token. |
| `source_systems.<source>.response_records_key` | `connectors/*.py` L80 | **MANDATORY** | JSON envelope key holding the record list (`result`, `entities`, `value`). |
| `source_systems.<source>.tables.<table>.initial_load_date` | `uax_bronze_load.py` L645 | `1970-01-01 00:00:00` | Baseline watermark timestamp for cold-start extraction. |
| `source_systems.<source>.tables.<table>.flatten_nested_json` | `uax_bronze_load.py` L260 | `true` | When `true`, invokes `flatten_and_expand_record` prior to serialization. |
