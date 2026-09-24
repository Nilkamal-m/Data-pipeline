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
├── adapters/
│   ├── __init__.py            # Adapter registry
│   ├── aurora.py              # Aurora MySQL staging-swap adapter
│   ├── redshift.py            # Redshift Spectrum catalog adapter
│   ├── snowflake.py           # Snowflake external Iceberg table adapter
│   └── databricks.py         # Databricks Unity Catalog adapter
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
                                    adapter.sync(df)      # Aurora, Redshift, Snowflake, Databricks
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
    i._is_deleted = false
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

To add a new serving target (e.g., a custom data store):

1. Create `gold/script/adapters/my_target.py` and implement:
   ```python
   class MyTargetAdapter:
       def sync(self, df, table_config, spark, params):
           ...
   ```

2. Register in `gold/script/adapters/__init__.py`:
   ```python
   ADAPTER_MAP["my_target"] = MyTargetAdapter
   ```

3. Add `"my_target"` to `target_engines` in `gold_config.json` for the relevant table.

4. Add target-specific connection config in the table block:
   ```json
   "my_target": {
     "host": "target.host.internal",
     "schema": "analytics"
   }
   ```
