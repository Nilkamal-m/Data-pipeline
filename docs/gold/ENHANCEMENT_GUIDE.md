# Gold Layer — Enhancement Guide

This guide explains how the Gold layer code is structured and how to safely extend it — adding new marts, custom transforms, new downstream targets, or modifying incremental vs. full-refresh behavior.

---

## 1. Code Structure at a Glance

```
gold/script/
├── gold_layer_manager.py      # Main entry point, standalone Glue job & orchestration engine
├── gold_config_loader.py      # Config reader, {env} interpolation & cache manager
├── gold_initial_load.py       # Historical CSV backfill, schema reconciliation & encryption
├── protegrity_encryption.py   # Distributed PySpark PII tokenization via mapInPandas
├── config/
│   └── gold_config.json       # Mart definitions, nkeys, target engine routing
└── custom_transforms/
    ├── genesys_conversations.py
    └── moveworks_interactions.py

gold/query/
├── genesys/
│   └── conversations.sql      # Mart SQL — joins Silver Iceberg tables
├── moveworks/
│   ├── interactions.sql
│   └── feedbacks.sql          # Dependent mart — queries gold_moveworks_interactions
└── servicenow/
    └── incident_kpi.sql
```

---

## 2. How the Main Job Works (`gold_layer_manager.py`)

```
parse_spark_arguments()
    └─► GoldConfigLoader.load_config()
        └─► [Cold-Start / Recovery Phase]
            _check_and_run_initial_load()
                ├─► Ghost Table Check: boto3 glue.delete_table() if S3 metadata missing
                ├─► Iceberg Incompatibility Check: _check_and_resolve_iceberg_incompatibilities()
                │     └─► Purge & recreate if existing column types conflict with incoming schema
                └─► GoldInitialLoader.run_initial_load() if initial CSV exists in gold/initial_exports/
                      ├─► Protegrity encryption (if encryption_columns configured & lambda_arn available)
                      ├─► reconcile_schema() → timestamp standardization + string enforcement
                      ├─► _apply_custom_transform() (is_initial_load=True, historical data pre-enriched)
                      └─► _resolve_and_deduplicate_columns() (Transform > SQL/CSV > Config)
        └─► [Mart Discovery & DAG Phase]
            _discover_queries()
            _sort_queries_by_dependency()     # Topological sort for inter-mart DAG
            └─► for each query in topological order:
                    _ensure_dependency_views_registered()
                        # Pre-loads raw_tbl_* (Bronze), tbl_* (Silver), gold_* (Gold) as Spark temp views
                    execute_mart_sql(query_file)
                    └─► filter_incremental_delta()    # If incremental=true: delta anti-join (includes unenriched llm_column rows)
                    └─► run_custom_transform()        # If custom_transform_script configured (batched rate-limited loop)
                    └─► _resolve_and_deduplicate_columns() # Transform (df.withColumn) > SQL > Config (NULL)
                    └─► reconcile_schema()          # Timestamp standardization + string enforcement
                    └─► _align_schema()               # Pad missing cols as NULL, reorder to catalog
                    └─► _materialize_athena_table()   # Spark SQL MERGE INTO Gold Iceberg table
                    └─► _serve_to_mysql()             # If 'aurora' in targets: zero-downtime atomic swap
```

---

## 3. Schema Reconciliation & Timestamp Standardization

### How `reconcile_schema()` Works

Both `GoldInitialLoader` and `GoldLayerManager` contain identical `reconcile_schema()` implementations that enforce the Gold layer data contract:

```python
# Step 1: Sanitize column names (spaces, dots, hyphens → underscores, lowercase)
# Step 2: For each column:
#   - Skip _updated_at, _inserted_at, _data_as_of (keep as TimestampType)
#   - If _is_timestamp_column() returns True → _conform_timestamp_to_string()
#   - Otherwise → cast to StringType
# Step 3: Pad missing target columns with NULL
# Step 4: Retain extra columns for schema evolution
```

### Timestamp Detection (`_is_timestamp_column()`)

A column is identified as a timestamp if:
1. Its PySpark data type is `TimestampType`, `DateType`, or `TimestampNTZType`.
2. Its name exactly matches: `timestamp`, `datetime`, `date`, `created_at`, `updated_at`, etc.
3. Its name ends with: `_timestamp`, `_ts`, `_at`, `_time`, `_date`, `_on`, `_start`, `_end`.
4. Its name starts with: `timestamp_`, `datetime_`, `date_`, `time_`.
5. Its name contains: `conversation_start`, `event_time`, `sys_created`, etc.

**Exclusion**: `_updated_at`, `_inserted_at`, `_data_as_of` are always excluded and remain as `TimestampType`.

### Timestamp Conversion (`_conform_timestamp_to_string()`)

Converts timestamps to `YYYY-MM-DD HH:MM:SS` string format. Handles:
- Native PySpark timestamp/date types
- Epoch seconds (10 digits) and epoch milliseconds (13 digits)
- ISO 8601 strings with timezone (`2024-09-24T11:00:00Z`, `+00:00`)
- Space-separated strings (`2024-09-24 11:00:00.123`)
- Slash-delimited strings (`2024/09/24`, `09/24/2024`, `dd/MM/yyyy`)
- Date-only strings (`2024-09-24`)
- Null / empty / `"null"` / `"none"` / `"nan"` → `NULL`

---

## 4. Incremental vs. Full Refresh

### Incremental Mode (`"incremental": true`)

The engine calls `filter_incremental_delta(silver_df, gold_table)`:

1. Reads the current maximum `_updated_at` from the existing Gold Iceberg table.
2. Filters the incoming Silver dataset: `WHERE _updated_at > max_gold_updated_at OR gold.nkey IS NULL`.
3. Only the delta rows are merged into Gold Iceberg.

**Why `IS NULL` check?**  
New records that do not exist in Gold yet will not have a matching `_updated_at` in the Gold table, so the join returns NULL. Including these ensures new entities are always ingested even if their `_updated_at` matches the watermark.

### Full Refresh (`"incremental": false`)

The engine uses the full Silver DataFrame as-is and runs a complete `MERGE INTO`. Use this for:
- Small dimension tables that change frequently and are cheap to reprocess.
- Reference / lookup tables where partial updates are unsafe.

To trigger a one-time full refresh regardless of config:
```bash
aws glue start-job-run \
  --job-name "glue-gold-moveworks-dev" \
  --arguments '{"--FULL_REFRESH": "true", "--SOURCE_SYSTEM": "moveworks"}'
```

---

## 5. Dynamic Schema Alignment (Zero Hardcoding)

Before every Iceberg merge, the engine aligns the incoming DataFrame to the physical Iceberg table schema:

```python
def _align_schema(incoming_df, target_table_fqn, spark):
    target_schema = spark.table(target_table_fqn).schema
    for field in target_schema:
        if field.name not in incoming_df.columns:
            incoming_df = incoming_df.withColumn(field.name, lit(None).cast(field.dataType))
    # Reorder columns to match target ordinal positions
    return incoming_df.select([col(f.name) for f in target_schema])
```

This means:
- Adding a new column to a mart SQL query auto-propagates to the Iceberg table on the next run.
- No manual `ALTER TABLE` or code changes are needed.

---

## 6. Iceberg Schema Incompatibility Auto-Resolution

When the existing Gold Iceberg table has column types that conflict with the incoming DataFrame (e.g., existing `bigint` vs incoming `string` after reconciliation), `_check_and_resolve_iceberg_incompatibilities()`:

1. Compares each incoming column's type against the existing table's type.
2. If mismatches are found, purges the stale Glue Catalog entry and S3 metadata.
3. Returns `True` to signal fresh table creation with the corrected schema.

This happens automatically — no manual intervention is required when upstream schema changes cause type conflicts.

---

## 7. Athena Iceberg Merge (`_materialize_athena_table`)

```sql
MERGE INTO glue_catalog.{database}.{target_table} AS t
USING source_staging AS s
ON t.{nkey} = s.{nkey}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Zero-row optimization:** If the incremental delta produces 0 rows, the merge is skipped entirely and the method returns early with `row_count = 0`. This avoids an unnecessary Iceberg commit and Glue Catalog metadata write.

---

## 8. Aurora MySQL Adapter — Atomic Staging Swap

The Aurora adapter never performs in-place row updates. It uses a full staging-swap to guarantee exact parity with Athena Iceberg:

```
1. Read full conformed dataset from Athena Iceberg (spark.table(gold_table_fqn))
2. Write to Aurora staging table: {table_name}_staging  (JDBC batch insert)
3. Execute zero-downtime DDL swap:
   RENAME TABLE {table} TO {table}_old,
               {table}_staging TO {table};
4. Verify / create performance indexes on natural keys (_ensure_mysql_index)
5. DROP TABLE {table}_old;
```

**Result:** Aurora row count always equals Athena Iceberg row count. Power BI dashboards experience zero downtime or duplicate rows.

---

## 9. Protegrity PII Encryption

### When Encryption is Activated

Encryption is activated for initial load data **only** when:
1. `encryption_columns` is configured for the table in `gold_config.json` (non-empty dict or array).
2. A valid `lambda_arn` is available (from config or CLI).
3. Encryption is not explicitly disabled via `--ENABLE_ENCRYPTION=false`.

If `encryption_columns` is empty or omitted, the entire encryption subsystem is skipped — no `lambda_arn` validation occurs.

### Column Name Resolution

Both Bronze and Gold layers use the same `_clean_key()` method for column matching:
```python
@staticmethod
def _clean_key(k: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_]', '', re.sub(r'[\s\.\-]+', '_', str(k).strip())).lower()
```

This ensures that column names like `Customer Name`, `customer-name`, and `customer_name` all resolve to the same key.

### Configuration Example
```json
"conversations": {
  "encryption_columns": {
    "customer_name": "EMP_MEMBERNAME_ALPHANUM_LP",
    "customer_email": "EMP_MEMBER_EMAIL_LP"
  }
}
```

---

## 10. Historical Initial Load & Ghost Table Auto-Remediation

On the **first run** for a table:
1. **Ghost Table Check**: `GoldLayerManager` checks if the Iceberg table exists in Glue Catalog. If it exists but its underlying S3 metadata was deleted (causing `404 Not Found`), the engine deletes the broken catalog entry via `boto3 glue.delete_table()` and re-provisions cleanly.
2. **Iceberg Incompatibility Check**: If the table exists but column types conflict with incoming data, the table is purged and recreated with the corrected schema.
3. **Initial CSV Ingestion**: If historical CSV exists at `initial_load.path`:
   - `GoldInitialLoader` reads the CSV with `multiLine=true` and `escape="\\"` options.
   - Cleanses column names via `sanitize_column_name()`.
   - Encrypts PII columns via `ProtegrityEncryptionManager` (if `encryption_columns` configured).
   - Reconciles schema with `reconcile_schema()` (timestamp standardization + string enforcement).
   - Deduplicates by `nkey` ordered by `_updated_at DESC`.
   - Writes to the Gold Iceberg table as the baseline.
   - Archives the CSV to `s3://.../gold/initial_exports/{source}/_archived/{table}_{timestamp}.csv`.

Subsequent incremental runs find no CSV at the path (archived) and proceed normally.

**Force re-ingestion of archived history:**
```bash
aws glue start-job-run \
  --job-name "glue-gold-genesys-dev" \
  --arguments '{"--RELOAD_INITIAL": "true", "--SOURCE_SYSTEM": "genesys"}'
```

---

## 11. Custom Transform Hooks & External Column Enrichment

For enrichment logic beyond standard SQL (API calls, LLM summaries, ML scoring, complex parsing):

### 1. Script Location & Naming Convention
```
gold/script/custom_transforms/<source>_<table>.py
```
*Example:* `gold/script/custom_transforms/genesys_conversations.py`

### 2. Standard Interface & Signature
```python
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Dict, Any

def transform(df: DataFrame, spark: Optional[SparkSession] = None, context: Optional[dict] = None) -> DataFrame:
    """
    Invoked during both Initial Load (is_initial_load=True) and Incremental Mart runs.
    """
```

### 3. Context Metadata (`context` dictionary)
The engine automatically passes runtime metadata into `context`:
* `source_system`: Source identifier (e.g. `genesys`).
* `table_name`: Base table name (e.g. `conversations`).
* `target_table`: Target Iceberg table name.
* `is_initial_load`: `True` during historical CSV ingestion; `False` during incremental Silver runs.
* `is_incremental`: Boolean indicating delta mode.
* `external_columns`: List of configured enrichment column names (e.g. `["conversation_summary"]`).
* `api_secret_name`: Secrets Manager secret holding credentials.
* `params`: Complete dictionary of CLI / Glue job arguments.

### 4. Strict Column Precedence Hierarchy
The engine enforces:
$$\mathbf{1.\ Custom\ Transform\ (df.withColumn)} \;\;>\;\; \mathbf{2.\ SQL\ Query\ Column} \;\;>\;\; \mathbf{3.\ Config\ Default\ (external\_columns\ NULL)}$$

Any column added or modified via `df = df.withColumn(col_name, ...)` cleanly overrides SQL query columns and configuration defaults. Duplicate column names are deduplicated case-insensitively.

### 5. Developer Implementation: Rate Limiting, Batching & Sample Template
Developers have complete control to implement rate limiting (e.g. sleep delay or token bucket), micro-batching, and API retry handling directly within their custom script. The pipeline emits structured execution logging and executes the custom transform:

```python
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# ==============================================================================
# SAMPLE DEVELOPER IMPLEMENTATION TEMPLATE (COMMENTED OUT)
# ==============================================================================
# import time, random
# from pyspark.sql.functions import col, lit, when
#
# def call_external_api_with_retry(prompt: str, max_retries: int = 3) -> str:
#     for attempt in range(max_retries):
#         try:
#             # time.sleep(0.05)  # Enforce developer rate limit (e.g. 20 RPS)
#             return f"Enriched insight: {prompt[:50]}"
#         except Exception as err:
#             if "429" in str(err) or "Throttling" in str(err):
#                 time.sleep((2 ** attempt) + random.uniform(0.1, 0.5))
#             else:
#                 raise
#     return "Fallback summary"
# ==============================================================================

def transform(df: DataFrame, spark=None, context: dict = None) -> DataFrame:
    context = context or {}
    source_system = context.get("source_system", "genesys")
    table_name = context.get("table_name", "conversations")
    is_initial_load = context.get("is_initial_load", False)
    external_cols = context.get("external_columns", [])

    # Pipeline logs that custom script is being called
    logger.info(
        f"[CUSTOM TRANSFORM] Calling custom script for '{source_system}.{table_name}' "
        f"(is_initial_load={is_initial_load}, external_columns={external_cols})."
    )

    # Custom transformation logic can be implemented here by the developer.
    return df
```

---

## 12. Adding a New Gold Mart

### Step 1 — Write the mart SQL
File: `gold/query/<source>/<mart_name>.sql`

```sql
SELECT
    i.interaction_id,
    i.subject,
    i.status,
    i.created_at,
    i.last_updated_time AS _updated_at
FROM
    tbl_moveworks_interactions i
WHERE
    i._is_deleted = 'N'
```

The SQL can reference tables across any layer: `raw_tbl_*` (Bronze), `tbl_*` (Silver), or `gold_*` (Gold).

### Step 2 — Add config entry in `gold_config.json`

```json
"source_systems": {
  "moveworks": {
    "target_engines": ["athena", "aurora"],
    "tables": {
      "interactions_summary": {
        "nkey": ["interaction_id"],
        "incremental": true,
        "aurora": {
          "schema": "enterprise_reporting",
          "table_name": "gold_moveworks_interactions_summary"
        }
      }
    }
  }
}
```

### Step 3 — Validate

Athena:
```sql
SELECT COUNT(*), MAX(_updated_at)
FROM uax_datalake_db_dev.gold_moveworks_interactions_summary;
```

Aurora:
```sql
SELECT COUNT(*) FROM enterprise_reporting.gold_moveworks_interactions_summary;
```

Both counts should match.

---

## 13. Adding a New Downstream Target Engine

All downstream serving targets are handled via private `@classmethod` methods on `GoldLayerManager` in [`gold_layer_manager.py`](../../gold/script/gold_layer_manager.py).

To add an additional custom target engine (e.g. a specialized API or document store):

### Step 1 — Add config keys to `gold_config.json`
```json
"interactions": {
  "nkey": ["interaction_id"],
  "incremental": true,
  "my_target": {
    "endpoint": "https://api.internal.corp/v1/sync",
    "table_name": "gold_moveworks_interactions"
  }
}
```

### Step 2 — Implement the serving method in `GoldLayerManager`
```python
@classmethod
def _serve_to_my_target(
    cls,
    spark: SparkSession,
    df_mart: DataFrame,
    source_system: str,
    table_name: str,
    gold_cfg: Dict[str, Any],
    params: Dict[str, Any]
) -> None:
    """Connects to my_target and exports conformed Gold data."""
    logger.info(f"[MY_TARGET] Serving '{table_name}'")
    # Custom push logic here
```

### Step 3 — Wire into the target execution loop in `GoldLayerManager.run()`
```python
if 'my_target' in gold_targets:
    cls._serve_to_my_target(
        spark=spark,
        df_mart=df_mart,
        source_system=source_system,
        table_name=table_name,
        gold_cfg=gold_cfg,
        params=params
    )
```
