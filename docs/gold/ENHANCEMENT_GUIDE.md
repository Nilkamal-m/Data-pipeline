# Gold Layer — Enhancement Guide

This guide explains how the Gold layer code is structured and how to safely extend it — adding new marts, custom transforms, new downstream targets, or modifying incremental vs. full-refresh behavior.

---

## 1. Code Structure at a Glance

```
gold/script/
├── gold_layer_manager.py      # Main entry point, standalone Glue job & orchestration engine
├── gold_config_loader.py      # Config reader, {env} interpolation & cache manager
├── gold_initial_load.py       # Historical CSV backfill & schema reconciliation logic
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
                └─► GoldInitialLoader.load() if initial CSV exists in gold/initial_exports/
        └─► [Mart Discovery & DAG Phase]
            _discover_queries()
            _sort_queries_by_dependency()     # Topological sort for inter-mart DAG
            └─► for each query in topological order:
                    _ensure_dependency_views_registered()
                        # Pre-loads raw_tbl_* (Bronze), tbl_* (Silver), gold_* (Gold) as Spark temp views
                    execute_mart_sql(query_file)
                    └─► run_custom_transform()        # If custom_transform_script configured
                    └─► filter_incremental_delta()    # If incremental=true: delta anti-join
                    └─► _align_schema()               # Pad missing cols as NULL, reorder to catalog
                    └─► _materialize_athena_table()   # Spark SQL MERGE INTO Gold Iceberg table
                    └─► _serve_to_mysql()             # If 'aurora' in targets: zero-downtime atomic swap
```

---

## 3. Incremental vs. Full Refresh

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

## 4. Dynamic Schema Alignment (Zero Hardcoding)

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

## 5. Athena Iceberg Merge (`_materialize_athena_table`)

```sql
MERGE INTO glue_catalog.{database}.{target_table} AS t
USING source_staging AS s
ON t.{nkey} = s.{nkey}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Zero-row optimization:** If the incremental delta produces 0 rows, the merge is skipped entirely and the method returns early with `row_count = 0`. This avoids an unnecessary Iceberg commit and Glue Catalog metadata write.

---

## 6. Aurora MySQL Adapter — Atomic Staging Swap

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

## 7. Historical Initial Load & Ghost Table Auto-Remediation

On the **first run** for a table:
1. **Ghost Table Check**: `GoldLayerManager` checks if the Iceberg table exists in Glue Catalog. If it exists but its underlying S3 metadata was deleted (causing `404 Not Found`), the engine deletes the broken catalog entry via `boto3 glue.delete_table()` and re-provisions cleanly.
2. **Initial CSV Ingestion**: If historical CSV exists at `initial_load.path`:
   - `GoldInitialLoader` reads the CSV with `multiLine=true` and `escape="\"` options.
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

## 8. Custom Transform Hooks

For enrichment logic beyond SQL (API calls, ML scoring, aggregations):

**1. Create the script:**
```
gold/script/custom_transforms/moveworks_interactions.py
```

**2. Implement the interface:**
```python
from pyspark.sql import DataFrame, SparkSession

def transform(df: DataFrame, spark: SparkSession, params: dict) -> DataFrame:
    # params contains config values, secret names, env, etc.
    return df.withColumn("category_label", classify_udf(col("subject")))
```

**3. Reference in `gold_config.json`:**
```json
"interactions": {
  "custom_transform_script": "gold/script/custom_transforms/moveworks_interactions.py"
}
```

The transform runs **after** the mart SQL executes and **before** schema alignment and Iceberg merge.

---

## 9. Adding a New Gold Mart

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

## 10. Adding a New Downstream Target Engine

All downstream serving targets are handled via private `@classmethod` methods on `GoldLayerManager` in [gold_layer_manager.py](file:///Users/nilkamalmahato/Documents/Data-pipeline/gold/script/gold_layer_manager.py).

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
