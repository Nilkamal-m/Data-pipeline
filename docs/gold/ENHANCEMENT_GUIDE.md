# Gold Layer — Enhancement Guide

This guide explains how the Gold layer code is structured and how to safely extend it — adding new marts, custom transforms, new downstream targets, or modifying incremental vs. full-refresh behavior.

---

## 1. Code Structure at a Glance

```
gold/script/
├── gold_layer_manager.py      # Main entry point and orchestration engine
├── gold_config_loader.py      # Config reader, {env} interpolation
├── gold_initial_load.py       # Historical CSV backfill logic
├── config/
│   └── gold_config.json       # Mart definitions, nkeys, target engine routing
└── custom_transforms/
    ├── genesys_conversations.py
    └── moveworks_interactions.py

gold/query/
├── genesys/
│   └── conversations.sql      # Mart SQL — joins Silver Iceberg tables
└── moveworks/
    └── interactions.sql
```

---

## 2. How the Main Job Works (`gold_layer_manager.py`)

```
parse_cli_args()
    └─► GoldConfigLoader.load_config()
        └─► for each table in source_system:

            [Optional: Initial Load]
            check_initial_export_exists()
            └─► GoldInitialLoader.load()      # Parse CSV, dedup, write Iceberg, archive S3

            [Standard Run]
            execute_mart_sql(query_file)       # Run gold/query/{source}/{table}.sql
            └─► filter_incremental_delta()     # If incremental=true: WHERE _updated_at > max(gold._updated_at)
                └─► run_custom_transform()     # If custom_transform_script configured
                    └─► _align_schema()        # Pad missing cols as NULL, reorder to match Iceberg catalog
                        └─► _materialize_athena_table()   # Spark SQL MERGE INTO Gold Iceberg
                            └─► for each target_engine:
                                    # Inline routing block in GoldLayerManager.run()
                                    # if 'aurora' in targets  → _serve_to_mysql()
                                    # if 'databricks' in targets → _run_databricks_serving()
                                    # if 'redshift' in targets   → _run_redshift_serving()
                                    # if 'snowflake' in targets  → _run_snowflake_serving()
```

---

## 3. Incremental vs. Full Refresh

### Incremental Mode (`"incremental": true`)

The engine calls `filter_incremental_delta(silver_df, gold_table)`:

1. Reads the current maximum `_updated_at` from the existing Gold Iceberg table.
2. Filters the incoming Silver dataset: `WHERE _updated_at > max_gold_updated_at OR gold.nkey IS NULL`.
3. Only the delta rows are merged into Gold Iceberg.

**Why `IS NULL` check?**  
New records that don't exist in Gold yet won't have a matching `_updated_at` in the Gold table, so the join returns NULL. Including these ensures new entities are always ingested even if their `_updated_at` matches the watermark.

### Full Refresh (`"incremental": false`)

The engine uses the full Silver DataFrame as-is and runs a complete `MERGE INTO`. Use this for:
- Small dimension tables that change frequently and are cheap to reprocess.
- Reference / lookup tables where partial updates are unsafe.

To trigger a one-time full refresh regardless of config:
```bash
python3 gold/script/gold_layer_manager.py --FULL_REFRESH=true
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
MERGE INTO iceberg_catalog.{database}.{target_table} AS t
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
4. DROP TABLE {table}_old;
```

**Result:** Aurora row count always equals Athena Iceberg row count. Power BI dashboards experience zero downtime or duplicate rows.

---

## 7. Historical Initial Load (`gold_initial_load.py`)

On the **first run** for a table, if a historical CSV exists at the configured `initial_load.path`:

1. `GoldInitialLoader` reads the CSV with `multiLine=true` and `escape="\"` options (handles multi-paragraph text fields).
2. Deduplicates by `nkey` ordered by `_updated_at DESC`.
3. Writes to the Gold Iceberg table as the baseline.
4. Archives the CSV to `s3://.../gold/initial_exports/{source}/_archived/{table}_{timestamp}.csv`.

Subsequent incremental runs find no CSV at the path (archived) and proceed normally.

**Force re-ingestion of archived history:**
```bash
python3 gold/script/gold_layer_manager.py --RELOAD_INITIAL=true
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
    uax_datalake_db_{env}.tbl_moveworks_interactions i
WHERE
    i._is_deleted = 'N'
```

The SQL runs against Silver Iceberg tables registered in Glue. The `{env}` token is interpolated at runtime.

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

All downstream serving targets are handled via **inline routing blocks** inside `GoldLayerManager.run()` in [`gold_layer_manager.py`](../../gold/script/gold_layer_manager.py). There is no separate `adapters/` directory — each target has a dedicated private `@classmethod` on `GoldLayerManager`.

To add a brand new target (e.g., `"my_target"`), follow these four steps:

---

### Step 1 — Add config keys to `gold_config.json`

Under the relevant table, add a block named after your target engine key:

```json
"interactions": {
  "nkey": ["interaction_id"],
  "incremental": true,
  "my_target": {
    "host": "my-target.internal.host",
    "schema": "analytics",
    "table_name": "gold_moveworks_interactions"
  }
}
```

Also add `"my_target"` to the `target_engines` list at the source level (or table level):

```json
"moveworks": {
  "target_engines": ["athena", "aurora", "my_target"],
  ...
}
```

---

### Step 2 — Add the private serving method to `GoldLayerManager`

In `gold/script/gold_layer_manager.py`, add a new `@classmethod` following the same pattern as `_run_redshift_serving` or `_run_databricks_serving`. Add it after the last existing serving method (around line 1460):

```python
# --------------------------------------------------------------------------
# My Target Serving
# --------------------------------------------------------------------------
@classmethod
def _run_my_target_serving(
    cls,
    spark: SparkSession,
    params: Dict[str, Any],
    df_mart: DataFrame,
    clean_base_name: str,
    target_table_name: str,
    primary_keys: List[str],
    mart_stats: List[Dict[str, Any]]
) -> None:
    """
    My Target serving method.
    Connects to my_target and upserts the Gold mart DataFrame.
    """
    host = params.get('MY_TARGET_HOST', 'my-target.internal.host')
    schema = params.get('MY_TARGET_SCHEMA', 'analytics')

    logger.info(f"[MY TARGET] Serving '{target_table_name}' to {host}/{schema}")

    # TODO: implement your actual connection + write logic here
    # Example: JDBC write, REST API push, boto3 call, etc.

    mart_stats.append({
        "mart_name": clean_base_name,
        "target_table": f"{schema}.{target_table_name}",
        "status": "SUCCESS",
        "rows_served": df_mart.count(),
        "duration_seconds": 0.0,
        "error_message": None
    })
```

---

### Step 3 — Wire it into the routing block in `GoldLayerManager.run()`

In `gold/script/gold_layer_manager.py`, find the **downstream target serving block** (look for the comment `# [GOLD STEP 3+] Downstream Target Serving`). After the last existing `if 'snowflake' in gold_targets:` block, add:

```python
# Target: My Target
if 'my_target' in gold_targets:
    logger.info(
        f"\n================================================================================\n"
        f"[GOLD STEP 7] Serving Gold Marts to My Target\n"
        f"--------------------------------------------------------------------------------"
    )
    for clean_base_name, df_mart in materialized_dfs.items():
        my_tbl = GoldConfigLoader.get_target_table_name(
            source_system, clean_base_name, 'my_target', gold_cfg
        ) if GoldConfigLoader else f"gold_{source_system}_{clean_base_name}"
        cls._run_my_target_serving(
            spark=spark,
            params=params,
            df_mart=df_mart,
            clean_base_name=clean_base_name,
            target_table_name=my_tbl,
            primary_keys=mart_keys.get(clean_base_name, []),
            mart_stats=mart_stats
        )
```

> **Where exactly in the file?**  
> Search for `# Target: Snowflake` (line ~916). Your new block goes immediately after the closing of that `if` block.

---

### Step 4 — Validate

Add `"my_target"` to a table's `target_engines` list in `gold_config.json` and run the Gold job with `--SOURCE_SYSTEM=moveworks`. Check the logs for `[MY TARGET]` entries and confirm the mart data reaches the destination.
