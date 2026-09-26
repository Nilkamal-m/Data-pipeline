# Silver Layer — Component Reference & Code Location Index

This document provides a line-by-line master index and component map for the entire Silver Layer codebase. It details where every class, function, transformation engine, configuration property, and Glue argument is defined, implemented, and referenced.

---

## 1. Directory & File Inventory

| File Path | Component Role | Key Responsibilities |
| :--- | :--- | :--- |
| [`silver/script/uax_silver_etl.py`](../../silver/script/uax_silver_etl.py) | **Main Iceberg Transformation Engine** | Glue argument resolution, Bronze Hive/S3 ingestion, watermark evaluation, in-batch windowed deduplication, schema evolution, SCD Type 1 UPSERT, SCD Type 2 merge, Glue catalog synchronization. |
| [`silver/script/transformer.py`](../../silver/script/transformer.py) | **Transformation Engine (`SilverTransformer`)** | Column renames, explicit type casting, SQL filter evaluation, technical column enrichment (`_valid_from`, `_valid_to`, `_is_current`, `_is_deleted`), custom external script hooks. |
| [`silver/script/silver_config_loader.py`](../../silver/script/silver_config_loader.py) | **Configuration Engine (`SilverConfigLoader`)** | S3/local JSON loading, `{env}` token interpolation, inheritance of `pipeline_defaults`, technical column exclusion lists, table metadata resolution. |
| [`silver/script/config/silver_config.json`](../../silver/script/config/silver_config.json) | **Pipeline Configuration** | Blueprint defining table prefixes, storage buckets, SCD strategies, deduplication keys/order-by, and custom transform script paths. |
| [`silver/script/custom_transforms/`](../../silver/script/custom_transforms/) | **Custom Script Extensions** | External PySpark transform scripts dynamically loaded per table (e.g. API enrichment, complex regex parsing). |

---

## 2. Code Component Map: `uax_silver_etl.py`

| Function / Component | Line Range | Description & Architectural Purpose | References / Callers |
| :--- | :--- | :--- | :--- |
| `parse_spark_arguments()` | L98–L428 | Resolves Glue Job Arguments (`getResolvedOptions`) with priority over S3 configuration defaults. | Called in `main()` at L1231. |
| `get_silver_watermark_key()` | L431–L438 | Resolves S3 state file path: `metadata/silver/{source}/{table}/watermark.json`. | Watermark readers and writers. |
| `get_silver_last_load_date()`| L440–L478 | Retrieves high-water mark timestamp from S3; returns `None` for cold starts or if `--FULL_REFRESH` is active. | Called in Step 1 extraction at L1370. |
| `update_silver_watermark()` | L480–L525 | Atomically writes updated watermark state JSON to S3 post-successful merge. | Called post-merge at L1845. |
| `sanitize_watermark_files()` | L527–L562 | Converts multi-line JSON watermark files into single-line NDJSON for Athena compatibility. | Called in `sync_silver_watermark_catalog_table()`. |
| `sync_silver_watermark_catalog_table()` | L564–L659 | Registers/evolves an Athena-queryable external catalog table (`tbl_watermarks`) over all Silver watermark files. | Called in `main()` at L1295. |
| `quote_iceberg_table()` | L661–L667 | Quotes table names safely with backticks for Spark SQL execution. | Used across DDL and DML operations. |
| `purge_s3_table_location()` | L669–L699 | Empties S3 table folder during full refresh before rebuilding Iceberg table metadata. | Called in full refresh handler at L765. |
| `check_iceberg_table_exists()` | L701–L806 | Verifies whether the Iceberg table exists in AWS Glue Catalog and checks storage metadata. | Called in Step 4 Iceberg merge at L1690. |
| `trigger_silver_iceberg_crawler()` | L808–L834 | **DISABLED BY POLICY (`trigger_crawler: false`)**: Logs skipped message to protect Iceberg metadata. | Called in job teardown at L1923. |
| `get_payload_columns()` | L836–L849 | Extracts business payload columns while strictly excluding technical/metadata columns (`_*`). | Used for SCD1/2 payload hashing. |
| `build_runtime_hash_expr()` | L851–L862 | Synthesizes SHA-256 hash expression (`sha2(concat_ws('\|\|', ...), 256)`) over payload columns. | Used for SCD Type 2 change detection. |
| `perform_deduplication()` | L864–L889 | Windows records by natural key (`nkey`), sorts by order columns, and filters `row_num == 1`. Supports `latest_by_order_column` (DESC) and `oldest_by_order_column` (ASC). | Called in Step 2 at L1620. |
| `sync_iceberg_table_schema()` | L891–L934 | Compares incoming schema against target Iceberg table; dynamically issues `ALTER TABLE ... ADD COLUMNS`. | Called before merge at L1715. |
| `execute_iceberg_scd1_upsert()` | L936–L1063 | Executes PySpark SQL `MERGE INTO ... WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT`. | Called when `scd_type == 'scd1'` at L1750. |
| `execute_iceberg_scd2()` | L1065–L1227 | Performs 2-step SCD Type 2 merge: expires current record (`_valid_to=now()`, `_is_current='N'`) and inserts new version (`_valid_from=now()`, `_valid_to=9999-01-01`, `_is_current='Y'`). | Called when `scd_type == 'scd2'` at L1780. |
| `main()` | L1229–L1995 | Orchestrates Bronze reading, deduplication, transformations, schema evolution, and Iceberg merge. | Main Glue PySpark entrypoint. |

---

## 3. Transformation & Configuration Modules

| Module & Class | Line Range | Description & Architectural Purpose |
| :--- | :--- | :--- |
| `transformer.py` &rarr; `SilverTransformer` | L1–L350 | Declarative transformation engine executing: <br>1. Technical column injection (`_inserted_at`, `_updated_at`). <br>2. Column renaming (`column_renames`). <br>3. Type casting (`column_casts`). <br>4. SQL row filtering (`filter_expression`). <br>5. Custom script execution hook (`custom_transform_script`). |
| `silver_config_loader.py` &rarr; `SilverConfigLoader` | L1–L280 | Reads JSON config from S3, recursively interpolates `{env}` tokens, merges table configs with `pipeline_defaults`, and extracts technical column exclusion sets. |

---

## 4. Configuration Blueprint Cross-Reference

| `silver_config.json` Parameter | Source Code Location | Fallback Value | Code Usage Description |
| :--- | :--- | :--- | :--- |
| `pipeline_defaults.silver_bucket` | `uax_silver_etl.py` L245 | `uax-datalake-silver-{env}` | Target S3 bucket where conformed Iceberg data and metadata files reside. |
| `pipeline_defaults.state_bucket` | `uax_silver_etl.py` L250 | `uax-datalake-state-{env}` | S3 bucket holding the watermarks JSON files. |
| `pipeline_defaults.target_format` | `uax_silver_etl.py` L255 | `iceberg` | Storage format for conformed tables (`iceberg`). |
| `pipeline_defaults.table_prefix` | `uax_silver_etl.py` L260 | `tbl_` | Standard prefix prepended to all conformed Silver Iceberg tables. |
| `pipeline_defaults.merge_strategy` | `uax_silver_etl.py` L265 | `upsert` | Merge behavior (`upsert`, `append`, `overwrite`). |
| `pipeline_defaults.scd_type` | `uax_silver_etl.py` L270 | `scd1` | Default dimension tracking type (`scd1` or `scd2`). |
| `pipeline_defaults.deduplication.strategy` | `uax_silver_etl.py` L864 | `latest_by_order_column` | Window sort direction: `latest_by_order_column` (.desc) vs `oldest_by_order_column` (.asc). |
| `pipeline_defaults.deduplication.default_nkey` | `uax_silver_etl.py` L1563 | **DO NOT RELY ON DEFAULT** | Legacy fallback primary key (`sys_id`). Every table must explicitly configure `nkey`. |
| `pipeline_defaults.glue_catalog.trigger_crawler` | `uax_silver_etl.py` L1922 | **`false`** | Strictly `false` to prevent Glue Crawlers from corrupting Iceberg catalog definitions. |
| `source_systems.<source>.tables.<table>.nkey` | `uax_silver_etl.py` L1563 | **MANDATORY** | Natural/composite primary keys uniquely identifying entities. |
| `source_systems.<source>.tables.<table>.deduplication_order_by` | `uax_silver_etl.py` L1596 | **MANDATORY** | Ordering timestamp column(s) used to resolve tie-breaks during deduplication. |
