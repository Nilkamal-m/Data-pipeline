# UAX DataLake — Master Developer Knowledge Base

> **Target Audience:** Incoming and Contributing Data Engineers  
> **Scope:** Complete Data Pipeline Architecture — **Bronze**, **Silver**, **Gold**, and **Documentation**  
> **Platform:** AWS Glue + Apache Iceberg + Amazon Aurora MySQL + Amazon Athena  
> **Pattern:** Medallion (Multi-Hop) Lakehouse Architecture  
> **Last Updated:** October 2026  

---

## Table of Contents

1. [Executive Summary & Handover Baseline](#1-executive-summary--handover-baseline)
2. [Medallion Pipeline Architecture](#2-medallion-pipeline-architecture)
3. [Repository Structure & Component Inventory](#3-repository-structure--component-inventory)
4. [S3 Path Conventions & Data Layout](#4-s3-path-conventions--data-layout)
5. [Bronze Layer — Raw Ingestion Engine](#5-bronze-layer--raw-ingestion-engine)
6. [Silver Layer — Conformed Iceberg Lakehouse](#6-silver-layer--conformed-iceberg-lakehouse)
7. [Gold Layer — Business Marts & Serving](#7-gold-layer--business-marts--serving)
8. [Configuration Schema Blueprints](#8-configuration-schema-blueprints)
9. [Connector Architecture & Adding New Sources](#9-connector-architecture--adding-new-sources)
10. [Custom Transform Extension Framework](#10-custom-transform-extension-framework)
11. [Protegrity PII Encryption System](#11-protegrity-pii-encryption-system)
12. [Documentation Cross-Reference Map](#12-documentation-cross-reference-map)
13. [Glue Job Arguments Quick Reference](#13-glue-job-arguments-quick-reference)
14. [Operational Patterns & Golden Rules](#14-operational-patterns--golden-rules)
15. [Next Developer Roadmap](#15-next-developer-roadmap)

---

## 1. Executive Summary & Handover Baseline

This knowledge base is the authoritative, end-to-end engineering transfer document for the UAX DataLake pipeline. A new developer should be able to read this document and start contributing without needing verbal handover.

### 1.1 What Has Been Built & Verified

The following capabilities are **fully implemented, tested (202 unit tests passing), and production-ready**:

| Capability | Status | Key Files |
| :--- | :--- | :--- |
| Multi-source REST API ingestion (ServiceNow, Genesys, Moveworks) | ✅ Complete | `bronze/script/uax_bronze_load.py`, `bronze/script/connectors/` |
| Relational DB extraction (PostgreSQL, MySQL, MariaDB) | ✅ Complete | `bronze/script/connectors/database.py` |
| S3 file ingestion (CSV, JSON, Parquet) | ✅ Complete | `bronze/script/connectors/s3_file.py` |
| Incremental watermark state management | ✅ Complete | Bronze: `metadata/bronze/`, Silver: `metadata/silver/` |
| Apache Iceberg SCD Type 1 upserts | ✅ Complete | `silver/script/uax_silver_etl.py` |
| Apache Iceberg SCD Type 2 (versioned history) | ✅ Complete | `silver/script/uax_silver_etl.py` |
| Declarative Silver transformations (casts, renames, filters) | ✅ Complete | `silver/script/transformer.py` |
| Gold SQL DAG topological dependency resolution | ✅ Complete | `gold/script/gold_layer_manager.py` |
| Zero-downtime Aurora MySQL atomic table swaps | ✅ Complete | `gold/script/gold_layer_manager.py` |
| Physical Iceberg table materialization for Athena | ✅ Complete | `gold/script/gold_layer_manager.py` |
| Cold-start historical CSV/Parquet bootstrap | ✅ Complete | `gold/script/gold_initial_load.py` |
| Dynamic custom transform plugin framework | ✅ Complete | `gold/script/custom_transforms/` |
| Column conflict resolution & deduplication precedence | ✅ Complete | Both `gold_layer_manager.py` and `gold_initial_load.py` |
| Protegrity PII tokenization (Bronze dict + Gold PySpark) | ✅ Complete | `bronze/script/protegrity_encryption.py`, `gold/script/protegrity_encryption.py` |
| Dynamic MySQL schema evolution (`ALTER TABLE ADD COLUMN`) | ✅ Complete | `gold/script/gold_layer_manager.py` |
| Ghost table auto-detection & remediation | ✅ Complete | `gold/script/gold_layer_manager.py` |
| Cross-layer Spark temp view registration (Bronze/Silver/Gold) | ✅ Complete | `gold/script/gold_layer_manager.py` |

---

## 2. Medallion Pipeline Architecture

Data flows through three decoupled stages, orchestrated sequentially by an AWS Glue Workflow:

```
[Source APIs / Databases / S3 File Drops]
                  │
                  ▼
   ┌──────────────────────────────┐
   │       1. BRONZE LAYER        │  → Raw Landing Zone (Immutable Append-Only)
   │    (uax_bronze_load.py)      │  → Format: Snappy Parquet on Amazon S3
   │                              │  → Partition: _ingested_at=YYYY-MM-DDTHH:MM:SSZ
   └──────────────┬───────────────┘
                  │  Glue Workflow Trigger (On Step 1 Success)
                  ▼
   ┌──────────────────────────────┐
   │       2. SILVER LAYER        │  → Cleaned, Conformed, Deduplicated
   │     (uax_silver_etl.py)      │  → Format: Apache Iceberg ACID Tables
   │                              │  → Merge: SCD Type 1 or SCD Type 2
   └──────────────┬───────────────┘
                  │  Glue Workflow Trigger (On Step 2 Success)
                  ▼
   ┌──────────────────────────────┐
   │        3. GOLD LAYER         │  → Curated Business Marts & Aggregations
   │   (gold_layer_manager.py)    │  → Format: Physical Iceberg Tables
   │                              │  → Serving: Athena + Aurora MySQL Atomic Swaps
   └──────────────┬───────────────┘
                  │
         ┌────────┴────────┐
         ▼                 ▼
   [Amazon Athena]   [Amazon Aurora MySQL]
   (Ad-Hoc Query)    (enterprise_reporting.*)
                          │
                          ▼
                  [Power BI / Tableau / Metabase]
```

**Orchestration:** AWS Glue Workflow (`uax-datalake-{source}-workflow-{env}`) triggers jobs sequentially. On completion, Amazon SNS sends success metrics or failure diagnostic cards.

---

## 3. Repository Structure & Component Inventory

```
Data-pipeline/
├── bronze/
│   └── script/
│       ├── uax_bronze_load.py          # Main Bronze ingestion engine (~1,500 lines)
│       ├── config_loader.py            # ConfigLoader class for bronze_config.json (~660 lines)
│       ├── protegrity_encryption.py    # PII tokenization module (shared with Gold)
│       ├── config/
│       │   └── bronze_config.json      # Source system endpoints, watermarks, encryption config
│       └── connectors/
│           ├── __init__.py             # Connector registry (CONNECTOR_MAP)
│           ├── servicenow.py           # ServiceNow REST API connector
│           ├── genesys.py              # Genesys PureCloud API connector
│           ├── moveworks.py            # Moveworks Export API connector
│           ├── database.py             # JDBC/SQL connector (PostgreSQL, MySQL, MariaDB)
│           ├── s3_file.py              # S3 file reader (CSV, JSON, Parquet)
│           ├── http_client.py          # Base HTTP client with retry logic
│           └── oauth.py                # OAuth2 client credentials flow
│
├── silver/
│   └── script/
│       ├── uax_silver_etl.py           # Main Silver ETL engine (~1,500 lines)
│       ├── transformer.py              # SilverTransformer class (~474 lines)
│       ├── silver_config_loader.py     # SilverConfigLoader class (~404 lines)
│       ├── config/
│       │   └── silver_config.json      # Natural keys, SCD modes, casts, transforms
│       └── custom_transforms/
│           ├── servicenow_incident.py  # Inter-table lookup sample
│           ├── moveworks_conversations.py
│           ├── moveworks_interactions.py
│           ├── moveworks_plugin_calls.py
│           ├── moveworks_plugin_resources.py
│           └── moveworks_users.py
│
├── gold/
│   └── script/
│       ├── gold_layer_manager.py       # Main Gold orchestrator (~4,306 lines)
│       ├── gold_initial_load.py        # Cold-start bootstrap engine (~1,697 lines)
│       ├── gold_config_loader.py       # GoldConfigLoader class (~684 lines)
│       ├── protegrity_encryption.py    # PySpark DataFrame distributed encryption
│       ├── config/
│       │   └── gold_config.json        # Mart definitions, serving engines, external columns
│       └── custom_transforms/
│           ├── genesys_conversations.py    # Sample: Genesys enrichment hook
│           └── moveworks_interactions.py   # Sample: Moveworks enrichment hook
│
├── docs/
│   ├── ARCHITECTURE.md                 # End-to-end architecture & Glue Workflow
│   ├── ONBOARDING_GUIDE.md             # Developer setup & local environment
│   ├── bronze/
│   │   ├── BRONZE_LAYER.md             # Bronze technical deep dive
│   │   ├── CONFIG_BLUEPRINT.md         # bronze_config.json schema reference
│   │   ├── ENHANCEMENT_GUIDE.md        # Bronze extension blueprints
│   │   └── REFERENCE_INDEX.md          # Method-level code map
│   ├── silver/
│   │   ├── SILVER_LAYER.md             # Silver technical deep dive
│   │   ├── CONFIG_BLUEPRINT.md         # silver_config.json schema reference
│   │   ├── ENHANCEMENT_GUIDE.md        # Silver extension blueprints
│   │   └── REFERENCE_INDEX.md          # Method-level code map
│   └── gold/
│       ├── GOLD_LAYER.md               # Gold technical deep dive
│       ├── CONFIG_BLUEPRINT.md         # gold_config.json schema reference
│       ├── ENHANCEMENT_GUIDE.md        # Gold extension blueprints
│       └── REFERENCE_INDEX.md          # Method-level code map (with exact line ranges)
│
├── knowledge.md                        # THIS FILE — Master developer knowledge base
└── README.md                           # Repository overview
```

---

## 4. S3 Path Conventions & Data Layout

All data, metadata, and query files follow strict S3 path conventions:

### 4.1 Bronze Layer Paths

| Path Pattern | Purpose | Example |
| :--- | :--- | :--- |
| `bronze/data/{source}/{table}/_ingested_at=YYYY-MM-DDTHH:MM:SSZ/` | Immutable raw partitioned Parquet files | `bronze/data/servicenow/incident/_ingested_at=2026-10-01T06:00:00Z/` |
| `bronze/staging/{source}/{table}/_staging_{uuid}/` | Transient staging area (promoted or cleaned up) | `bronze/staging/genesys/conversations/_staging_abc123/` |
| `metadata/bronze/{source}/{table}/watermark.json` | Per-table incremental watermark state | `metadata/bronze/moveworks/interactions/watermark.json` |

### 4.2 Silver Layer Paths

| Path Pattern | Purpose | Example |
| :--- | :--- | :--- |
| `silver/data/{source}/{table}/` | Iceberg table data root (managed by Iceberg catalog) | `silver/data/servicenow/incident/` |
| `metadata/silver/{source}_{table}_watermark.json` | Per-table Silver watermark state | `metadata/silver/servicenow_incident_watermark.json` |

### 4.3 Gold Layer Paths

| Path Pattern | Purpose | Example |
| :--- | :--- | :--- |
| `gold/data/{source}/{table}/` | Iceberg table data root for Gold marts | `gold/data/genesys/conversations/` |
| `gold/query/{source}/` | SQL mart definition files | `gold/query/genesys/conversations.sql` |
| `gold/initial_exports/{source}/{table}.csv` | Historical CSV/Parquet for cold-start bootstrap | `gold/initial_exports/moveworks/interactions.csv` |
| `gold/script/custom_transforms/` | Custom Python transform scripts | `gold/script/custom_transforms/genesys_conversations.py` |
| `gold/script/config/gold_config.json` | Gold pipeline configuration | — |

### 4.4 Watermark JSON Format

```json
{
  "last_load_date": "2026-10-01T06:00:00Z",
  "last_record_count": 1523,
  "last_execution_id": "jr_abc123",
  "last_updated": "2026-10-01T06:05:12Z"
}
```

### 4.5 Naming Conventions

| Entity | Convention | Example |
| :--- | :--- | :--- |
| Bronze Glue Catalog Table | `raw_tbl_{table}` | `raw_tbl_incident` |
| Silver Iceberg Table | `tbl_{table}` | `tbl_incident` |
| Gold Iceberg Table | `gold_{source}_{table}` | `gold_genesys_conversations` |
| MySQL Staging Table | `staging_tbl_{table}` | `staging_tbl_gold_genesys_conversations` |
| Glue Database | `uax_datalake_db_{env}` | `uax_datalake_db_dev` |
| Watermark Table | `raw_tbl_watermarks` (Bronze), `tbl_watermarks` (Silver) | — |

---

## 5. Bronze Layer — Raw Ingestion Engine

### 5.1 Core Module: `uax_bronze_load.py`

The Bronze layer is an **immutable, append-only raw landing zone**. It extracts from upstream sources without modifying business meaning and commits to Amazon S3 as Snappy Parquet.

**Key Functions:**

| Function | Purpose |
| :--- | :--- |
| `parse_arguments()` | Resolves CLI/Glue args: `--SOURCE_SYSTEM`, `--TABLE_NAME`, `--ENV`, `--BRONZE_BUCKET`, `--SECRET_NAME`, etc. |
| `flatten_and_expand_record()` | Recursively flattens nested JSON dicts/lists into tabular columns with `_` separator. Unpacks arrays into multiple rows. |
| `flatten_dict_single()` | Non-expanding flatten (no array explosion). Used when `flatten_nested_json=true`. |
| `serialize_chunk_to_bytes()` | Converts Python dicts to Parquet bytes in memory using PyArrow. Supports Snappy compression. |
| `get_last_load_date()` | Reads watermark JSON from S3 to determine incremental extraction start point. |
| `update_last_load_date()` | Writes updated watermark JSON after successful batch write. |
| `promote_staging_to_bronze()` | Atomically moves files from `bronze/staging/` to immutable `bronze/data/` partition. |
| `cleanup_failed_staging()` | Deletes staging objects on extraction failure to prevent orphaned files. |
| `sync_bronze_catalog_table()` | Registers or updates Glue Catalog table definition with partition keys. |
| `sync_watermark_catalog_table()` | Registers or updates Glue Catalog watermark tracking table. |
| `main()` | Entrypoint: resolves connectors via `get_connector()`, iterates tables, extracts, flattens, encrypts PII, writes Parquet, promotes staging. |

### 5.2 Two-Phase Atomic Staging Pattern

```
1. Extract records from source API/DB
2. Flatten nested JSON → tabular records
3. [Optional] Encrypt PII columns via Protegrity Lambda
4. Serialize to Parquet bytes in memory (PyArrow)
5. Write to S3 staging: bronze/staging/{source}/{table}/_staging_{uuid}/part_000.parquet
6. Verify file integrity & row count
7. On Success → promote_staging_to_bronze() → Rename to bronze/data/{source}/{table}/_ingested_at=...
8. On Failure → cleanup_failed_staging() → Delete staging prefix
9. Update watermark JSON
10. Sync Glue Catalog
```

### 5.3 Core Module: `config_loader.py`

Class `ConfigLoader` manages `bronze_config.json`:

| Method | Purpose |
| :--- | :--- |
| `load_config()` | Loads from S3 or local path with `{env}` interpolation and caching. |
| `get_source_config()` | Returns source system block (e.g. `source_systems.servicenow`). |
| `get_table_config()` | Returns table-specific config merged with defaults. |
| `get_source_tables()` | Lists all configured tables for a source system. |
| `get_table_initial_load_date()` | Resolves fallback chain: table → source → pipeline_defaults → CLI. |
| `get_table_upper_bound()` | Resolves extraction end timestamp (for backfill windowing). |
| `get_table_query_filter()` | Resolves query filter string with `{last_load_date}` / `{upper_bound}` token substitution. |
| `get_table_endpoint()` | Resolves REST API endpoint from template + overrides. |
| `get_table_file_path()` | Resolves S3 file path for S3FileConnector tables. |
| `get_table_fetch_mode()` | Returns `'all'` (incremental) or `'latest'` (snapshot/full refresh). |
| `get_glue_catalog_config()` | Returns Glue Catalog registration settings. |

---

## 6. Silver Layer — Conformed Iceberg Lakehouse

### 6.1 Core Module: `uax_silver_etl.py`

The Silver layer reads Bronze Parquet, deduplicates, transforms, and upserts into Apache Iceberg tables using Spark SQL `MERGE INTO`.

**Key Functions:**

| Function | Purpose |
| :--- | :--- |
| `parse_spark_arguments()` | Resolves CLI/Glue args: `--SOURCE_SYSTEM`, `--TABLE_NAME`, `--DATA_LAKE_BUCKET`, etc. |
| `get_silver_last_load_date()` | Reads Silver watermark from `metadata/silver/{source}_{table}_watermark.json`. |
| `update_silver_watermark()` | Writes updated Silver watermark after successful Iceberg merge. |
| `check_iceberg_table_exists()` | Validates Iceberg table existence via Spark catalog + Glue Client + S3 metadata check. Ghost table detection included. |
| `perform_deduplication()` | Windowed `ROW_NUMBER()` partitioned by natural keys, ordered by `_updated_at DESC`. |
| `build_runtime_hash_expr()` | Builds MD5 hash expression across payload columns for change detection. |
| `sync_iceberg_table_schema()` | Proactively issues `ALTER TABLE ADD COLUMNS` for incoming schema evolution. |
| `execute_iceberg_scd1_upsert()` | Iceberg `MERGE INTO` with match-update + not-matched-insert on natural keys. |
| `execute_iceberg_scd2()` | Closes existing active record (`_is_current=false`, `_valid_to=now`), inserts new active row when payload hash changes. |
| `resolve_table_identifiers()` | Maps config table names to Silver table name (`tbl_*`), Iceberg path, and Bronze source table (`raw_tbl_*`). |
| `resolve_key_columns()` | Extracts natural keys and deduplication order columns from config. |
| `main()` | Entrypoint: iterates tables, reads Bronze Parquet, applies transformations, deduplicates, merges into Iceberg. |

### 6.2 Transformation Engine: `transformer.py`

Class `SilverTransformer` applies per-table declarative transformations:

| Method | Purpose |
| :--- | :--- |
| `apply_transformations()` | Master orchestrator: strips Bronze metadata, applies casts/renames/filters/expressions/drops, adds audit columns, runs custom script. |
| `log_schema_introspection()` | Logs structured schema breakdown showing column names, types, and nullability. |
| `_apply_column_casts()` | Applies type casts configured in `column_casts` (e.g. `{"amount": "double"}`). |
| `_apply_column_renames()` | Applies column renames configured in `column_renames`. |
| `_apply_filter_expression()` | Applies Spark SQL `WHERE` filter from `filter_expression`. |
| `_apply_custom_expressions()` | Evaluates computed SQL expressions from `custom_expressions`. |
| `_apply_drop_columns()` | Drops columns listed in `drop_columns`. |
| `_apply_custom_script()` | Dynamically imports and executes `custom_transform_script` Python module. |
| `_add_audit_columns()` | Appends `_valid_from`, `_valid_to`, `_is_current`, `_is_deleted`, `_inserted_at`, `_updated_at`. |

### 6.3 Technical Audit Columns (Silver)

Every Silver Iceberg table includes these system-generated columns:

| Column | Type | Description |
| :--- | :--- | :--- |
| `_valid_from` | Timestamp | When this version of the record became active |
| `_valid_to` | Timestamp | When superseded (`9999-01-01` for current) |
| `_is_current` | Boolean | `true` for active record, `false` for historical (SCD2) |
| `_is_deleted` | Boolean | Soft deletion flag |
| `_inserted_at` | Timestamp | Pipeline execution timestamp (when row was written) |
| `_updated_at` | Timestamp | Source record modification timestamp |

### 6.4 SCD Type 1 vs SCD Type 2

| SCD Mode | Behavior | Use Case |
| :--- | :--- | :--- |
| `scd1` | Match on natural key → update payload in-place. Insert if new. | Current-state dimension tables (e.g. `incident`, `users`). |
| `scd2` | Match on natural key → if payload hash changed, close old record (`_is_current=false`, `_valid_to=now`) and insert new active row. | Full audit history tables (e.g. `change_request`). |

---

## 7. Gold Layer — Business Marts & Serving

### 7.1 Core Module: `gold_layer_manager.py`

Class `GoldLayerManager` is the monolithic orchestration engine. It contains an embedded copy of `GoldInitialLoader` for zero-dependency AWS Glue execution.

**Execution Flow:**

```
1. Load gold_config.json (S3 or local) → _load_gold_config()
2. Discover SQL query files → _discover_queries()
3. Extract SQL annotations (-- @primary_key, -- @materialize) → _extract_sql_annotations()
4. Resolve topological DAG dependencies → _sort_queries_by_dependency()
5. For each query (in dependency order):
   a. Register cross-layer Spark temp views → _ensure_dependency_views_registered()
      - Bronze: raw_tbl_* → reads Parquet from S3
      - Silver: tbl_*     → reads Iceberg tables
      - Gold:   gold_*    → reads prior Gold Iceberg tables
   b. Check if cold-start needed → _check_and_run_initial_load()
      - If Iceberg table missing → trigger GoldInitialLoader.run_initial_load()
   c. Execute Spark SQL query
   d. Apply custom transform hook → _apply_custom_transform()
   e. Resolve column conflicts → _resolve_and_deduplicate_columns()
   f. Reconcile schema (timestamp standardization) → reconcile_schema()
   g. Deduplicate by natural keys → _deduplicate_by_nkey()
   h. Materialize to Iceberg → _materialize_athena_table()
   i. Serve to Aurora MySQL → _serve_to_mysql()
      - Schema evolution → _detect_schema_evolution()
      - Write staging table → _write_staging_table()
      - Atomic swap → _execute_isolated_atomic_swap()
      - Index natural keys → _ensure_mysql_index()
6. Log final summary → _log_final_gold_summary()
```

### 7.2 Cold-Start Bootstrap: `gold_initial_load.py`

Class `GoldInitialLoader` bootstraps Gold tables from historical CSV/Parquet:

```
1. Read CSV/Parquet from S3 (gold/initial_exports/{source}/{table}.csv)
2. Cleanse column names → sanitize_column_name()
3. Probe target schema from SQL query → _probe_target_schema_from_query()
4. [Optional] Encrypt PII → _get_protegrity_encryption_manager()
5. Reconcile schema → reconcile_schema()
6. Apply custom transform (is_initial_load=True) → _apply_custom_transform()
7. Resolve column conflicts → _resolve_and_deduplicate_columns()
8. Deduplicate by natural keys → _deduplicate_by_nkey()
9. Create or replace Iceberg table → _create_iceberg_table_from_df()
10. [Optional] Serve to Aurora MySQL
```

### 7.3 Zero-Downtime MySQL Atomic Swap

```sql
-- Step 1: Write data to staging table
CREATE TABLE staging_tbl_gold_genesys_conversations ...
INSERT INTO staging_tbl_gold_genesys_conversations SELECT * FROM spark_df

-- Step 2: Detect schema evolution & apply
ALTER TABLE gold_genesys_conversations ADD COLUMN new_col VARCHAR(65535)

-- Step 3: Create B-Tree index on natural keys
CREATE INDEX idx_nkey ON staging_tbl_gold_genesys_conversations (conversation_id)

-- Step 4: Atomic rename (zero-downtime swap)
RENAME TABLE gold_genesys_conversations TO gold_genesys_conversations_old,
             staging_tbl_gold_genesys_conversations TO gold_genesys_conversations;
DROP TABLE gold_genesys_conversations_old;
```

### 7.4 Technical Audit Columns (Gold)

| Column | Type | Description |
| :--- | :--- | :--- |
| `_inserted_at` | Timestamp | Pipeline execution timestamp |
| `_updated_at` | Timestamp | Source record modification timestamp |
| `_data_as_of` | Timestamp | Silver watermark high-water mark (optional) |

---

## 8. Configuration Schema Blueprints

### 8.1 Bronze: `bronze_config.json` Structure

```json
{
  "pipeline_defaults": {
    "bronze_bucket": "",           // S3 bucket (or pass --BRONZE_BUCKET)
    "state_bucket": "",            // Watermark S3 bucket
    "batch_size": 1000,            // API pagination batch size
    "s3_chunk_size": 10000,        // Records per Parquet file
    "output_format": "parquet",
    "parquet_compression": "snappy",
    "flatten_nested_json": true,
    "error_handling_mode": "CONTINUE_ON_ERROR",  // or "HALT_ON_ERROR"
    "glue_catalog": {
      "enabled": true,
      "database_name": "uax_datalake_db_{env}",
      "table_prefix": "raw_tbl_"
    },
    "encryption": {
      "enabled": false,
      "lambda_arn": "",            // Protegrity Lambda ARN (mandatory when enabled=true)
      "batch_size": 500
    }
  },
  "source_systems": {
    "<source_name>": {
      "base_url": "https://...",
      "api_endpoint_template": "/api/v2/{table_name}",
      "default_delta_filter": "updated_on>={last_load_date}",
      "response_records_key": "result",
      "tables": {
        "<table_name>": {
          "initial_load_date": "2024-01-01 00:00:00",
          "upper_bound": "",
          "query_override": "",
          "encryption_columns": {"col": "DATA_ELEMENT"}
        }
      }
    }
  }
}
```

**Source System Types:**
- **REST API** (`servicenow`, `genesys`, `moveworks`): Uses `base_url`, `api_endpoint_template`, `default_delta_filter`.
- **Database** (`postgresql`, `mysql`): Uses `query_template`, `fetch_size`.
- **S3 File** (`vendor_a_s3`): Uses `"type": "s3_file"`, `source_bucket`, `file_format`, `fetch_mode` (`all` or `latest`).

### 8.2 Silver: `silver_config.json` Structure

```json
{
  "pipeline_defaults": {
    "target_format": "iceberg",
    "table_prefix": "tbl_",
    "merge_strategy": "upsert",
    "scd_type": "scd1",
    "bronze_technical_columns": ["_ingested_at", "_source_system", "_table_name", "_execution_id"],
    "technical_columns": {
      "is_deleted_column": "_is_deleted",
      "inserted_at_column": "_inserted_at",
      "updated_at_column": "_updated_at",
      "valid_from_column": "_valid_from",
      "valid_to_column": "_valid_to",
      "is_current_column": "_is_current"
    },
    "deduplication": {
      "enabled": true,
      "strategy": "latest_by_order_column"
    },
    "watermark": {
      "enabled": true,
      "watermark_column": "_ingested_at"
    },
    "cast_all_columns_to_string": true
  },
  "source_systems": {
    "<source>": {
      "tables": {
        "tbl_<table>": {
          "source_table_name": "raw_tbl_<table>",
          "nkey": "sys_id",
          "deduplication_keys": ["sys_id"],
          "deduplication_order_by": ["sys_updated_on", "_ingested_at"],
          "scd_type": "scd1",
          "column_renames": {},
          "column_casts": {"field": "integer"},
          "filter_expression": "",
          "custom_expressions": {},
          "drop_columns": [],
          "custom_transform_script": "custom_transforms/script.py"
        }
      }
    }
  }
}
```

### 8.3 Gold: `gold_config.json` Structure

```json
{
  "pipeline_defaults": {
    "gold_data_prefix": "gold/data",
    "target_format": "iceberg",
    "table_prefix": "gold_{source}_",
    "default_target_engines": ["athena"],
    "initial_load": {
      "template": "s3://{bucket}/gold/initial_exports/{source}/{table}.csv",
      "format": "csv",
      "delimiter": ",",
      "has_header": true
    },
    "encryption": { "enabled": false, "lambda_arn": "" }
  },
  "source_systems": {
    "<source>": {
      "target_engines": ["athena", "aurora"],
      "tables": {
        "<table>": {
          "nkey": ["business_key_col"],
          "incremental": true,
          "initial_load": {
            "path": "s3://{bucket}/gold/initial_exports/<source>/<table>.csv"
          },
          "custom_transform_script": "gold/script/custom_transforms/<source>_<table>.py",
          "api_secret_name": "dev/data-pipeline/<source>-api",
          "external_columns": ["enrichment_col_1", "enrichment_col_2"],
          "encryption_columns": {"pii_col": "DATA_ELEMENT"},
          "aurora": {
            "schema": "enterprise_reporting",
            "table_name": "gold_<source>_<table>"
          }
        }
      }
    }
  }
}
```

---

## 9. Connector Architecture & Adding New Sources

### 9.1 Connector Registry

All connectors are registered in `bronze/script/connectors/__init__.py`:

```python
CONNECTOR_MAP = {
    'servicenow':  ServiceNowConnector,
    'genesys':     GenesysConnector,
    'moveworks':   MoveworksConnector,
    'peakon':      PeakonConnector,
    'postgresql':  DatabaseConnector,
    'mysql':       DatabaseConnector,
    'mariadb':     DatabaseConnector,
    'sqlite':      DatabaseConnector,
    's3_file':     S3FileConnector,
}
```

Every connector exposes a single public method: `fetch_delta(table_name, last_load_date, upper_bound, config, ...)`.

### 9.2 Adding a New REST API Source

1. **Create connector file:** `bronze/script/connectors/newsource.py`
2. **Implement class** with `fetch_delta()` → returns `List[Dict]` of raw records.
3. **Register** in `connectors/__init__.py`:
   ```python
   from .newsource import NewSourceConnector
   CONNECTOR_MAP['newsource'] = NewSourceConnector
   ```
4. **Add config** in `bronze_config.json` under `source_systems.newsource`.
5. **Store credentials** in AWS Secrets Manager under the key referenced by `_secrets_required`.

### 9.3 Adding a New S3 File Source

For vendor CSV/JSON/Parquet feeds, use the existing `S3FileConnector` without writing code:

```json
{
  "source_systems": {
    "vendor_c_s3": {
      "type": "s3_file",
      "source_bucket": "vendor-c-bucket",
      "file_format": "csv",
      "delimiter": ",",
      "has_header": true,
      "fetch_mode": "all",
      "tables": {
        "daily_report": {
          "initial_load_date": "2024-01-01 00:00:00",
          "file_path": "reports/daily/"
        }
      }
    }
  }
}
```

---

## 10. Custom Transform Extension Framework

### 10.1 Transform Hook Signature (Gold)

```python
def transform(
    df: DataFrame,
    spark: Optional[SparkSession] = None,
    context: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Context keys:
      - source_system (str): e.g. 'genesys', 'moveworks'
      - table_name (str): e.g. 'conversations', 'interactions'
      - is_initial_load (bool): True if cold-start; False if incremental
      - external_columns (List[str]): Configured enrichment columns
      - llm_column (str): Primary external column (backward compat)
      - env (str): 'dev', 'stage', 'prod'
      - api_secret_name (str): Secrets Manager secret for API keys
      - logger (Logger): Pipeline logger
    """
    return df
```

### 10.2 Transform Hook Signature (Silver)

```python
def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    """
    Context keys:
      - glue_database (str): Glue catalog database name
      - source_system (str): Source system identifier
      - table_name (str): Silver table name
      - data_lake_bucket (str): S3 bucket
      - silver_data_prefix (str): Silver S3 prefix
    """
    return df
```

### 10.3 Four-Tier Gold Script Resolution

```
[1] Explicit config path (gold_config.json → custom_transform_script)
     │ Not found
     ▼
[2] Convention-based local search:
    - gold/script/custom_transforms/<source>_<table>.py
    - gold/script/custom_transform/<source>_<table>.py  (singular/plural tolerance)
    - Also tries: <table>.py, <source_variant>_<table>.py
     │ Not found
     ▼
[3] Python importlib.util.find_spec() across sys.path
    (Locates scripts in Glue --extra-py-files archives)
     │ Not found
     ▼
[4] S3 download fallback:
    Downloads from s3://{bucket}/gold/script/custom_transforms/<filename>
    into /tmp/custom_transforms/ on ephemeral Glue worker
```

### 10.4 Column Conflict Resolution Precedence

$$\mathbf{Custom\ Transform\ (df.withColumn)} > \mathbf{SQL\ Query\ Column} > \mathbf{Config\ Default\ (external\_columns\ NULL)}$$

Implemented in `_resolve_and_deduplicate_columns()` in both `gold_layer_manager.py` (L1880–L1945) and `gold_initial_load.py` (L698–L759).

### 10.5 Sample Scripts

| Script | Source System | External Columns | Architecture |
| :--- | :--- | :--- | :--- |
| `gold/script/custom_transforms/genesys_conversations.py` | Genesys | `conversation_summary` | Batch REST API calls, OAuth token auth, partition iteration |
| `gold/script/custom_transforms/moveworks_interactions.py` | Moveworks | `resolution_summary`, `bot_intent_label` | Micro-batch HTTP, tokenized payloads, error-tolerant enrichment |

---

## 11. Protegrity PII Encryption System

### 11.1 Dual Processing Engines

| Engine | Layer | Module | Method |
| :--- | :--- | :--- | :--- |
| Record-level encryption | Bronze | `bronze/script/protegrity_encryption.py` | `encrypt_record_chunk()` — encrypts Python dictionaries in memory |
| DataFrame-level encryption | Gold | `gold/script/protegrity_encryption.py` | `encrypt_spark_dataframe()` — uses PySpark `mapInPandas` for distributed micro-batch processing |

### 11.2 Configuration

```json
{
  "encryption": {
    "enabled": true,
    "lambda_arn": "arn:aws:lambda:us-east-1:123456789:function:protegrity-protector-{ENV}",
    "batch_size": 500,
    "secret_name": "uax/protegrity-credentials-{env}",
    "user_secret_key": "protegrity_user_secret"
  }
}
```

Per-table column mappings:
```json
{
  "encryption_columns": {
    "first_name": "EMP_MEMBERNAME_ALPHANUM_LP",
    "email": "EMP_MEMBER_EMAIL_LP"
  }
}
```

### 11.3 Retry Strategy

Randomized exponential jitter backoff (up to 5 attempts) matching Protegrity Protector API (`/v2/protect`) requirements.

---

## 12. Documentation Cross-Reference Map

| Document | Scope | When to Read |
| :--- | :--- | :--- |
| `docs/ARCHITECTURE.md` | End-to-end architecture, Mermaid diagrams, orchestration flows | System overview, cross-layer integration |
| `docs/ONBOARDING_GUIDE.md` | Local dev setup, environment config, execution commands | First-time environment setup |
| `docs/bronze/BRONZE_LAYER.md` | Bronze technical specs, staging patterns, connector guides | Modifying Bronze ingestion |
| `docs/bronze/CONFIG_BLUEPRINT.md` | `bronze_config.json` schema reference | Adding tables or endpoints |
| `docs/bronze/ENHANCEMENT_GUIDE.md` | Bronze extension blueprints | Scaling ingestion or new connector types |
| `docs/bronze/REFERENCE_INDEX.md` | Method-level code map for Bronze scripts | Code navigation & debugging |
| `docs/silver/SILVER_LAYER.md` | Silver specs, Iceberg merge, SCD1/SCD2, payload hashing | Modifying Silver transformation |
| `docs/silver/CONFIG_BLUEPRINT.md` | `silver_config.json` schema reference | Configuring natural keys, casts, SCD modes |
| `docs/silver/ENHANCEMENT_GUIDE.md` | Silver extension blueprints | Extending transformation capabilities |
| `docs/silver/REFERENCE_INDEX.md` | Method-level code map for Silver scripts | Code navigation & debugging |
| `docs/gold/GOLD_LAYER.md` | Gold specs, DAG resolution, Iceberg materialization, MySQL swaps | Modifying Gold pipeline |
| `docs/gold/CONFIG_BLUEPRINT.md` | `gold_config.json` schema reference | Adding marts, engines, external columns |
| `docs/gold/ENHANCEMENT_GUIDE.md` | Gold extension blueprints | New serving engines or complex marts |
| `docs/gold/REFERENCE_INDEX.md` | Method-level code map with exact line ranges for Gold scripts | Code navigation & debugging |

---

## 13. Glue Job Arguments Quick Reference

### 13.1 Bronze Job

| Argument | Required | Description |
| :--- | :--- | :--- |
| `--SOURCE_SYSTEM` | Yes | Source identifier (e.g. `servicenow`, `genesys`, `moveworks`) |
| `--TABLE_NAME` | No | Single table filter (default: all source tables) |
| `--ENV` | No | Environment (`dev`, `stage`, `prod`). Default: `dev` |
| `--BRONZE_BUCKET` | No | S3 bucket override |
| `--SECRET_NAME` | No | AWS Secrets Manager secret for API credentials |
| `--INITIAL_LOAD_DATE` | No | Override initial watermark timestamp |
| `--UPPER_BOUND` | No | Extraction end timestamp (for backfill windowing) |

### 13.2 Silver Job

| Argument | Required | Description |
| :--- | :--- | :--- |
| `--SOURCE_SYSTEM` | Yes | Source identifier |
| `--TABLE_NAME` | No | Single table filter (default: all tables for source) |
| `--ENV` | No | Environment. Default: `dev` |
| `--DATA_LAKE_BUCKET` | No | S3 bucket override |
| `--FULL_REFRESH` | No | Force full rebuild (drops + recreates Iceberg table) |

### 13.3 Gold Job

| Argument | Required | Description |
| :--- | :--- | :--- |
| `--SOURCE_SYSTEM` | Yes | Source identifier |
| `--TABLE_NAME` / `--MART_NAME` | No | Single mart filter (default: all discovered queries) |
| `--ENV` | No | Environment. Default: `dev` |
| `--GOLD_SCHEMA` / `--RDS_GOLD_SCHEMA` | Yes (for Aurora) | MySQL target schema. **Zero-fallback policy** — no hardcoded defaults |
| `--RDS_SECRET_NAME` | No | Secrets Manager secret for MySQL credentials |
| `--API_SECRET_NAME` / `--LLM_SECRET_NAME` | No | Secret for external API / LLM credentials |
| `--DATA_LAKE_BUCKET` | No | S3 bucket override |
| `--GOLD_TARGETS` / `--TARGET_ENGINES` | No | Serving engines: `athena`, `aurora`. Default: `aurora` |
| `--FULL_REFRESH` | No | Force full re-aggregation from Silver |
| `--RELOAD_INITIAL` | No | Force re-ingestion of historical CSV |
| `--SKIP_INITIAL_LOAD` | No | Bypass cold-start check |
| `--ENABLE_ENCRYPTION` | No | Toggle Protegrity PII tokenization |

---

## 14. Operational Patterns & Golden Rules

### 14.1 Zero-Fallback Schema Policy
- `--RDS_GOLD_SCHEMA` is **mandatory** for Aurora serving. The pipeline fails immediately if omitted. Never hardcode shared database names.

### 14.2 Universal String Standardization
- All non-audit business columns in Gold are cast to `StringType` to prevent Iceberg type mismatch errors between historical backfills and incremental streaming data.
- Timestamps standardized to `YYYY-MM-DD HH:MM:SS` string format.

### 14.3 Staging Table Safety
- Transient MySQL staging tables **must** use prefix `staging_tbl_*`.
- `_validate_droppable_table_name()` prevents accidental drops of non-staging tables.

### 14.4 Secrets Management
- Never log, commit, or hardcode credentials. Always resolve dynamically via AWS Secrets Manager.
- Bronze secrets: `_secrets_required` array in config documents mandatory keys.
- Gold secrets: `api_secret_name` for external APIs, `secret_name` for MySQL.

### 14.5 Schema Evolution is Automatic
- **Iceberg:** `_sync_iceberg_schema()` / `sync_iceberg_table_schema()` proactively adds new columns before merge.
- **MySQL:** `_detect_schema_evolution()` inspects `information_schema.COLUMNS` and runs `ALTER TABLE ADD COLUMN`.

### 14.6 Documentation Maintenance
- When method signatures or line numbers shift, update the corresponding `REFERENCE_INDEX.md` file in `docs/<layer>/`.

---

## 15. Next Developer Roadmap

### Task 1: Activate Live API Calls in Custom Transforms
1. Open `gold/script/custom_transforms/genesys_conversations.py` (or `moveworks_interactions.py`).
2. The batching, partition processing, secret lookup, and error handling scaffold is already built.
3. Connect the live HTTP call (or AWS Bedrock / OpenAI SDK) to populate the configured `external_columns`.
4. Store API credentials in AWS Secrets Manager under the name matching `api_secret_name` in `gold_config.json`.

### Task 2: Onboard a New Source System End-to-End
**Step 1 — Bronze:**
- Add source config to `bronze/script/config/bronze_config.json`.
- If custom API, create `bronze/script/connectors/newsource.py` and register in `__init__.py`.
- If S3 file feed, add `"type": "s3_file"` to the source config block.

**Step 2 — Silver:**
- Add table configs to `silver/script/config/silver_config.json` with `nkey`, `deduplication_order_by`, and `scd_type`.
- Optionally add `custom_transform_script` in `silver/script/custom_transforms/`.

**Step 3 — Gold:**
- Create SQL mart query at `gold/query/<source>/<mart_name>.sql`.
- Annotate: `-- @primary_key: business_key_col`.
- Register in `gold/script/config/gold_config.json` with `nkey`, `target_engines`, and optional `external_columns`.
- Optionally create `gold/script/custom_transforms/<source>_<mart_name>.py`.

### Task 3: Add a New Column to an Existing Gold Mart
1. Add the column to the SQL query in `gold/query/<source>/<table>.sql`.
2. If it's an external enrichment, add to `external_columns` in `gold_config.json`.
3. If populated by custom transform, update the script in `gold/script/custom_transforms/`.
4. Run the Gold job — schema evolution handles Iceberg (`ALTER TABLE ADD COLUMNS`) and MySQL (`ALTER TABLE ADD COLUMN`) automatically.

### Task 4: Enable PII Encryption on a Table
1. Set `"encryption": { "enabled": true, "lambda_arn": "arn:..." }` in `pipeline_defaults`.
2. Add `encryption_columns` mapping to the table config:
   ```json
   "encryption_columns": {
     "customer_name": "EMP_MEMBERNAME_ALPHANUM_LP",
     "customer_email": "EMP_MEMBER_EMAIL_LP"
   }
   ```
3. Store Protegrity credentials in Secrets Manager under `secret_name`.

### Task 5: Switch a Silver Table from SCD1 to SCD2
1. In `silver_config.json`, change `"scd_type": "scd1"` to `"scd_type": "scd2"`.
2. The Silver engine automatically closes existing active records and inserts new versioned rows with `_valid_from`, `_valid_to`, and `_is_current` columns.
