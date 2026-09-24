# Silver Layer — Enhancement Guide

This guide explains how the Silver layer code is structured and how to safely extend it — adding new tables, custom transforms, or new SCD behaviors.

---

## 1. Code Structure at a Glance

```
silver/script/
├── uax_silver_etl.py          # Main entry point (CLI → orchestration loop)
├── silver_config_loader.py    # Config reader, {env} interpolation, watermark helpers
├── transformer.py             # Core transformation utilities (cast, rename, deduplicate)
├── config/
│   └── silver_config.json     # Table definitions: nkey, SCD type, transforms
└── custom_transforms/
    ├── servicenow_incident.py # Source-specific custom PySpark hook
    └── genesys_conversations.py
```

---

## 2. How the Main Job Works (`uax_silver_etl.py`)

```
parse_cli_args()
    └─► SilverConfigLoader.load_config()
        └─► for each table:
                get_silver_last_load_date()     # Read S3 watermark
                └─► spark.read.parquet(bronze_path)
                    └─► filter(_ingested_at > watermark)
                        └─► SilverTransformer.transform(df, table_cfg)
                            ├─► sanitize_column_names()
                            ├─► exclude bronze technical columns
                            ├─► cast_all_columns_to_string (if enabled)
                            ├─► perform_deduplication()   # Window row_number
                            ├─► run custom_transform_script (if configured)
                            └─► apply column_renames, column_casts
                                └─► check_or_create_iceberg_table()
                                    └─► sync_iceberg_table_schema()  # ALTER TABLE if new cols
                                        └─► execute_iceberg_scd1_upsert() or execute_iceberg_scd2()
                                            └─► update_silver_watermark()
```

---

## 3. Transformer Engine (`transformer.py`)

`SilverTransformer` contains stateless utility methods called by the main job:

| Method | What It Does |
|---|---|
| `sanitize_column_names(df)` | Strips special characters, lowercases column names to prevent Spark SQL parse errors |
| `cast_all_columns_to_string(df)` | Converts all non-technical string columns to `StringType` to prevent type-mismatch Iceberg merge failures |
| `perform_deduplication(df, nkeys, order_cols)` | Uses `Window.partitionBy(*nkeys).orderBy(*order_cols DESC)` + `row_number() == 1` to keep the latest record per key |
| `apply_column_renames(df, renames_dict)` | Renames columns from Bronze names to conformed Silver names |
| `apply_column_casts(df, casts_dict)` | Casts specific columns to declared target types (e.g., `integer`, `decimal(18,2)`) |

---

## 4. Deduplication Deep Dive

Before merging into Iceberg, the incoming Bronze micro-batch is deduplicated in-memory:

```python
window_spec = Window.partitionBy(*nkeys).orderBy(*[col(c).desc() for c in order_cols])
deduped_df = (
    df.withColumn("_row_num", row_number().over(window_spec))
      .filter(col("_row_num") == 1)
      .drop("_row_num")
)
```

- `nkeys` — the natural/composite key columns (e.g., `["sys_id"]`, `["conversation_id"]`)
- `order_cols` — tie-breaking columns (e.g., `["sys_updated_on", "_ingested_at"]`); latest wins

**Why needed?** The same entity can appear multiple times in a Bronze batch if it was updated rapidly within the watermark window. Deduplicating before the merge prevents duplicate keys in the Iceberg table.

---

## 5. SCD Type 1 — In-Place Upsert

Used when only the **latest state** of an entity is needed (most operational tables).

```sql
MERGE INTO iceberg_catalog.{database}.{target_table} AS t
USING source_staging AS s
ON t.{nkey} = s.{nkey}
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
```

The merge runs via `spark.sql(...)`. The `UPDATE SET *` automatically updates all columns matching by name, so new columns (after a schema evolution) are handled without code changes.

---

## 6. SCD Type 2 — Historical Version Tracking

Used when **full change history** must be preserved (e.g., employee department changes, SLA priority shifts).

Technical columns added automatically:

| Column | Purpose |
|---|---|
| `_valid_from` | Timestamp when this version became active |
| `_valid_to` | Timestamp when superseded (`9999-01-01` for the current record) |
| `_is_current` | `true` for active record, `false` for historical |
| `_row_hash` | SHA-256 across all business payload columns — no new version if hash unchanged |

**Execution sequence:**
1. Compute `_row_hash` for incoming records.
2. Join incoming against the current Iceberg table on `nkey`.
3. For changed records (hash differs): update existing row to set `_valid_to = now(), _is_current = false`.
4. Insert the new version with `_valid_from = now(), _valid_to = 9999-01-01, _is_current = true`.

---

## 7. Schema Evolution

When a Bronze source adds new columns, the Silver engine handles this automatically:

```python
def sync_iceberg_table_schema(spark, table_fqn, incoming_df):
    existing_cols = {f.name for f in spark.table(table_fqn).schema}
    new_cols = [f for f in incoming_df.schema if f.name not in existing_cols]
    for col_field in new_cols:
        spark.sql(f"ALTER TABLE {table_fqn} ADD COLUMNS ({col_field.name} {col_field.dataType.simpleString()})")
```

This runs before every merge. Adding a column to the source never requires a manual DDL change.

---

## 8. Custom Transform Hooks

For tables requiring domain-specific logic beyond what the declarative config supports:

**1. Create the script:**
```
silver/script/custom_transforms/genesys_conversations.py
```

**2. Implement the standard interface:**
```python
from pyspark.sql import DataFrame

def transform(df: DataFrame) -> DataFrame:
    # Apply any PySpark transformations
    return df.withColumn("duration_minutes", col("duration_seconds") / 60)
```

**3. Reference it in `silver_config.json`:**
```json
"tbl_conversations": {
  "custom_transform_script": "custom_transforms/genesys_conversations.py"
}
```

The engine dynamically imports and calls `transform(df)` after the standard cleansing steps, before the Iceberg merge.

---

## 9. Adding a New Silver Table

### Step 1 — Add config entry in `silver_config.json`
```json
"source_systems": {
  "genesys": {
    "tables": {
      "tbl_agents": {
        "source_table_name": "raw_tbl_agents",
        "nkey": "agent_id",
        "deduplication_keys": ["agent_id"],
        "deduplication_order_by": ["updated_at", "_ingested_at"],
        "merge_strategy": "upsert",
        "scd_type": "scd1"
      }
    }
  }
}
```

### Step 2 — (Optional) Write a custom transform script
Only if business-specific logic is needed. Otherwise, the declarative config is sufficient.

### Step 3 — Validate via Athena
```sql
SELECT COUNT(*), MAX(_updated_at)
FROM uax_datalake_db_dev.tbl_genesys_agents;
```

---

## 10. Watermark State File

Path: `s3://{state_bucket}/metadata/silver/{source}_{table}_watermark.json`

```json
{
  "source_system": "genesys",
  "table_name": "tbl_conversations",
  "last_load_date": "2026-03-24 10:15:00",
  "records_processed": 3500,
  "status": "SUCCESS"
}
```

The watermark is updated **after** the Iceberg MERGE completes. A failed merge leaves the watermark at the previous value, so the next run re-processes the same window without data loss.
