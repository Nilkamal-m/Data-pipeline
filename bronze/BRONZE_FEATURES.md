# Bronze Layer — Feature Catalog

> Each feature describes **what it does**, **which code implements it**, and **how to configure it**.  
> All paths are relative to `bronze/`.

---

## Feature Index

| # | Feature | Config Key | Connector |
|---|---------|-----------|-----------|
| F01 | Strict initial load date enforcement | `table_initial_load_dates` | All |
| F02 | S3 High-Water Mark watermark | Auto — `metadata/bronze/` | All |
| F03 | Incremental delta extraction | `default_delta_filter` | All |
| F04 | Two-phase atomic staging | Auto — `_staging/` prefix | All |
| F05 | Memory-safe chunked streaming | `s3_chunk_size` | All |
| F06 | Recursive JSON flattening | `flatten_nested_json` | All |
| F07 | Audit column injection | Auto | All |
| F08 | Parquet serialization (Snappy) | `output_format`, `parquet_compression` | All |
| F09 | Glue Data Catalog auto-sync | `glue_catalog.enabled` | All |
| F10 | Instant Athena partition registration | Auto | All |
| F11 | Watermark Athena table | `sync_watermark_table` | All |
| F12 | CloudWatch custom metrics | `cloudwatch_namespace` | All |
| F13 | S3 execution audit log | Auto — `metadata/logs/` | All |
| F14 | Error handling modes | `error_handling_mode` | All |
| F15 | Multi-table single job | `default_tables` | All |
| F16 | 3-tier config precedence (CLI > Config > Default) | — | All |
| F17 | OAuth 2.0 with token cache | `auth_type: oauth` | MW, SN, GN |
| F18 | Basic Auth | `auth_type: basic` | SN |
| F19 | API key auth | `auth_type: api_key` | HTTP |
| F20 | Exponential backoff + 429 retry | Auto (3 retries) | HTTP |
| F21 | Per-table query overrides | `table_query_overrides` | API/DB |
| F22 | Per-table custom endpoints | `custom_table_endpoints` | API |
| F23 | Parallel time-window sharding | `parallel_processing` | Moveworks |
| F24 | `@odata.nextLink` cursor pagination | Auto | Moveworks |
| F25 | Moveworks per-endpoint rate limiting | `_PAGE_DELAY` (code constant) | Moveworks |
| F26 | ServiceNow offset pagination | Auto | ServiceNow |
| F27 | Genesys page-number pagination | Auto | Genesys |
| F28 | DB cursor batch fetching | `fetch_size` | Database |
| F29 | DB port mandatory enforcement | Secrets Manager | Database |
| F30 | Multi-engine DB support | `db_type` | Database |
| F31 | S3 fetch mode: all vs latest | `fetch_mode` | S3File |
| F32 | S3 per-table path override | `table_paths` | S3File |
| F33 | S3 per-table fetch_mode override | `table_fetch_modes` | S3File |
| F34 | S3 cross-account access | Secrets Manager | S3File |
| F35 | Multi-format file parsing | `file_format` | S3File |
| F36 | Vendor source type-based routing | `type` in config | __init__ |
| F37 | Glue Crawler trigger | `trigger_crawler` | All |
| F38 | Configurable table prefix | `glue_catalog.table_prefix` | All |
| F39 | S3 staging cleanup on failure | Auto | All |
| F40 | JSON fallback if Parquet fails | Auto | All |
| F41 | Configurable upper bound & backfill mode | `upper_bound` / `--UPPER_BOUND` | All |
| F42 | Pluggable zero-touch source onboarding | Pluggable connectors | All |

---

## Feature Details

### F01 — Strict Initial Load Date Enforcement

**Problem solved**: Without a load date, jobs risk re-ingesting all historical records.  
**Implementation**: `uax_bronze_load.py` calls `get_last_load_date()`. If S3 watermark is missing, it reads `table_initial_load_dates.<table>`. If missing, raises `ValueError` immediately.  
**Configuration**:
```json
"table_initial_load_dates": {
  "conversations": "2024-01-01T00:00:00Z",
  "users": "2024-01-01T00:00:00Z"
}
```

---

### F02 — S3 High-Water Mark (HWM) Watermark

**Problem solved**: Pipeline state loss between ephemeral Glue Shell jobs.  
**Implementation**: Writes `s3://<state_bucket>/metadata/bronze/<source>/<table>/watermark.json` containing `last_load_date`, `records_ingested`, `last_status`.  
**Guarantee**: Written only AFTER staging promotion succeeds.

---

### F03 — Incremental Delta Extraction

**Problem solved**: Full-table re-extraction on every run.  
**Implementation**: Connectors append dynamic filter clauses bounded by `last_load_date` and `upper_bound`:
- ServiceNow: `sys_updated_on>={last_load_date}`
- Moveworks: `last_updated_time ge '{last_load_date}' and last_updated_time le '{upper_bound}'`
- Database: `WHERE updated_at >= '{last_load_date}' and updated_at <= '{upper_bound}'`
- S3File: `LastModified > hwm and LastModified <= ub_dt`

---

### F04 — Two-Phase Atomic Staging Promotion

**Problem solved**: Partial parquet files left in Bronze when jobs crash mid-extraction.  
**Implementation**:
1. Phase 1: Write parts to `_staging/exec_<id>/<source>/<table>/delta_<id>_part_0001.parquet`.
2. Phase 2: If extraction finishes cleanly, copies all parts to `bronze/data/<source>/<table>/_ingested_at=<ts>/` and deletes staging objects. On failure, deletes staging without touching Bronze.

---

### F05 — Memory-Safe Chunked Streaming

**Problem solved**: OOM crashes on large table extracts inside small Glue workers.  
**Implementation**: `on_chunk_callback` flushes batches of records to S3 whenever buffer reaches `s3_chunk_size` (default: 10,000 records).

---

### F06 — Recursive JSON Flattening

**Problem solved**: Deeply nested JSON payloads are difficult to query in SQL/Athena.  
**Implementation**: Recursively unpacks dictionaries using `flatten_separator` (default: `_`), producing clean tabular columns (e.g. `user_profile_email`).

---

### F07 — Standard Audit Column Injection

**Problem solved**: Traceability of data lineage across the lake.  
**Implementation**: Every extracted record receives 4 metadata columns:
- `_ingested_at`: UTC timestamp of pipeline execution.
- `_source_system`: Source name (e.g. `moveworks`).
- `_table_name`: Target lake table name.
- `_execution_id`: Unique run identifier (`YYYYMMDD_HHMMSS`).

---

### F08 — Snappy Parquet Serialization

**Problem solved**: High S3 storage costs and slow Athena scan performance.  
**Implementation**: Serializes arrow tables directly into columnar Parquet with Snappy compression before transmission to S3.

---

### F09 / F10 — Glue Data Catalog Instant Sync

**Problem solved**: Slow crawler latency before newly ingested data is queryable in Athena.  
**Implementation**: `sync_bronze_catalog_table()` executes `glue.create_table` or `glue.create_partition` via Boto3 directly upon extraction completion. Data is immediately queryable.

---

### F11 — Athena Watermark Catalog Table

**Problem solved**: Visibility across pipeline watermarks.  
**Implementation**: Ingestion engine maintains an external Glue table `raw_tbl_watermarks` queryable via standard SQL in Athena.

---

### F12 / F13 — Observability & S3 Execution Logs

**Problem solved**: Centralized monitoring.  
**Implementation**: Emits custom CloudWatch metrics (`RecordsIngested`, `IngestionDurationSeconds`, `TableExtractionSuccess`) and stores structured JSON audit logs in `metadata/logs/bronze/`.

---

### F14 — Error Handling Modes

- `CONTINUE_ON_ERROR`: Logs failure, cleans staging for the failing table, and proceeds with remaining tables.
- `HALT_ON_ERROR`: Immediately terminates execution upon first failure.

---

### F23 — Parallel Time-Window Sharding (Moveworks)

**Problem solved**: Slow sequential extraction of millions of records over multi-month date ranges.  
**Implementation**: Breaks `[lower_bound, upper_bound]` into N-day windows and fetches them concurrently using `ThreadPoolExecutor`.

---

### F41 — Configurable Upper Bound & Backfill Mode

**Problem solved**: Historical backfilling or replaying a specific historical window without advancing watermark to the current date.  
**Implementation**:
- CLI parameter: `--UPPER_BOUND 2024-03-01T00:00:00Z`
- Config setting: `"upper_bound": "2024-03-01T00:00:00Z"`
- Dynamic substitution: `ConfigLoader` replaces `{upper_bound}` with the resolved timestamp.
- Watermark update: Advances S3 watermark to `upper_bound` rather than `current_run_time`. Future runs automatically resume from `upper_bound`.

---

### F42 — Pluggable Zero-Touch Source Onboarding

**Problem solved**: Risk of breaking existing pipelines when adding new source integrations.  
**Implementation**:
- Orchestrator `script/uax_bronze_load.py` is 100% connector-agnostic.
- New sources are added by creating a class in `script/connectors/<source>.py` adhering to `fetch_delta()` and registering it in `script/connectors/__init__.py`.
- Zero lines in `uax_bronze_load.py` are modified when onboarding new sources.
