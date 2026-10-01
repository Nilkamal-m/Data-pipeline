# Gold Layer — Component Reference & Code Location Index

This document provides a master index and component map for the entire Gold Layer codebase. It details where every class, method, serving engine, configuration property, and Glue argument is defined, implemented, and referenced.

---

## 1. Directory & File Inventory

| File Path | Component Role | Key Responsibilities |
| :--- | :--- | :--- |
| [`gold/script/gold_layer_manager.py`](../../gold/script/gold_layer_manager.py) | **Main Gold Orchestrator & Standalone Glue Entrypoint** | Topological SQL DAG dependency resolution, cross-layer temp view pre-registration (`raw_tbl_*`, `tbl_*`, `gold_*`), schema reconciliation (timestamp standardization + string enforcement), Iceberg physical table materialization, ghost table auto-remediation, Iceberg schema incompatibility auto-resolution, zero-downtime atomic swap to MySQL, schema introspection & evolution. |
| [`gold/script/gold_config_loader.py`](../../gold/script/gold_config_loader.py) | **Configuration Engine (`GoldConfigLoader`)** | S3/local JSON loading, `{env}` token interpolation, caching, serving engine resolution, credential key lookups. |
| [`gold/script/gold_initial_load.py`](../../gold/script/gold_initial_load.py) | **Cold-Start Historical Ingestion Engine** | Historical full-table bootstrapping, schema reconciliation (timestamp standardization + string enforcement), Protegrity PII encryption, query schema probing, Iceberg incompatibility auto-resolution, target Iceberg table provisioning. |
| [`gold/script/config/gold_config.json`](../../gold/script/config/gold_config.json) | **Pipeline Configuration** | Blueprint defining Gold table targets, serving engines, natural keys, SQL queries, initial load thresholds, encryption columns, and DB credentials. |
| [`gold/script/custom_transforms/`](../../gold/script/custom_transforms/) | **Custom Script Extensions** | External PySpark transform scripts dynamically loaded per mart (e.g. LLM embeddings, statistical scoring). |
| [`gold/script/protegrity_encryption.py`](../../gold/script/protegrity_encryption.py) | **Protegrity PII Tokenizer** | PySpark DataFrame distributed encryption via `mapInPandas`, 500-record batching, Protegrity Protector Lambda API invocation (`/v2/protect`), shared `_clean_key()` column normalizer. |

---

## 2. Code Component Map: `gold_initial_load.py`

### Class: `GoldInitialLoader` (Lines 58–1282)

| Function / Component | Line Range | Description & Architectural Purpose |
| :--- | :--- | :--- |
| `sanitize_column_name()` | L64–L85 | Cleanses and standardizes column names for SQL/Parquet/Iceberg/Athena compatibility. Converts spaces, dots, hyphens, slashes, brackets, colons, and hashes to underscores. Lowercases and collapses duplicate underscores. |
| `_is_timestamp_column()` | L88–L153 | Determines whether a column represents a date/time/timestamp using PySpark data type inspection and naming convention heuristics (exact, suffix, prefix, compound phrases). Excludes `_updated_at`, `_inserted_at`, `_data_as_of`. |
| `_conform_timestamp_to_string()` | L156–L236 | Converts identified timestamp columns to `YYYY-MM-DD HH:MM:SS` string format. Handles native timestamps, epochs (seconds/millis), ISO 8601, slash-delimited, date-only, and null/empty values. |
| `reconcile_schema()` | L239–L317 | Core schema reconciliation: cleanses column names, identifies timestamps via heuristics, standardizes to string format, pads missing target columns with NULL, retains extra columns for schema evolution. Technical audit columns remain as `TimestampType`. |
| `_probe_target_schema_from_query()` | L320–L436 | Discovers the target Gold table schema from the corresponding SQL query file. Executes a zero-record probe query (`WHERE 1=0`) via Spark SQL to extract schema fields without reading data. |
| `_deduplicate_by_nkey()` | L439–L463 | Ensures row uniqueness by natural keys using windowed `ROW_NUMBER()` with `_updated_at DESC` ordering. |
| `_apply_custom_transform()` | L466–L526 | Dynamically imports and executes external custom Python transform hook (`transform(df, spark, context)`) during initial load. Injects `is_initial_load=True` and `external_columns` so historical pre-enriched data is preserved. |
| `_resolve_and_deduplicate_columns()` | L528–L573 | Enforces strict Precedence Hierarchy: `Custom Transform (df.withColumn) > SQL Query Column > Config Default (external_columns NULL)`. Drops case-insensitive duplicate references, prioritizes transform outputs, and guarantees schema stability. |
| `_purge_s3_prefix()` | L577–L608 | Safely deletes orphaned or corrupted metadata/data files under an S3 URI prefix. |
| `_resolve_aws_region()` | L611–L647 | Dynamically resolves the AWS region from params, environment variables, boto3 session, or S3 bucket location. |
| `_purge_table_registration()` | L650–L688 | Completely purges a table from Spark SQL, Spark catalog cache, Glue Catalog, and S3. |
| `_create_iceberg_table_from_df()` | L691–L775 | Creates or recreates an Iceberg table from a DataFrame. Includes Spark SQL DDL fallback and automatic ghost table purge on collision. |
| `_check_and_resolve_iceberg_incompatibilities()` | L778–L842 | Detects column type mismatches between incoming DataFrame and existing Iceberg table. Purges and signals recreation if incompatible types found (e.g., `bigint` → `string`). |
| `_get_gold_layer_manager()` | L845–L864 | Lazily imports `GoldLayerManager` to prevent circular import locks. |
| `_get_protegrity_encryption_manager()` | L867–L891 | Lazily and dynamically imports `ProtegrityEncryptionManager` class from multiple candidate paths. |
| `run_initial_load()` | L894–L1438 | **Main orchestration method**: Reads CSV/Parquet, cleanses column names, encrypts PII (if configured), reconciles schema, triggers custom transform hook with rate-limit context, enforces column precedence, deduplicates, upserts into Iceberg table, and optionally serves to Aurora MySQL. |
| `main()` | L1441–L1517 | Glue job entrypoint with SparkConf, GlueContext, and Iceberg catalog configuration. |

---

## 3. Code Component Map: `gold_layer_manager.py`

### Class: `GoldLayerManager` (Lines 67–4023)

| Function / Component | Line Range | Description & Architectural Purpose | References / Callers |
| :--- | :--- | :--- | :--- |
| `_load_gold_config()` | L74–L307 | Reads `gold_config.json` from S3 or local path, interpolates `{env}`, and resolves pipeline defaults. | Called in `run_gold_pipeline()`. |
| `_resolve_natural_keys()` | L308–L418 | Extracts business natural keys from configuration or SQL annotations for deduplication and incremental upserts. | Called during query planning. |
| `_purge_s3_prefix()` | L420–L454 | Recursively deletes orphan S3 objects during initial load or ghost table recovery. | Initial load & recovery. |
| `run_gold_pipeline()` | L455–L939 | **Core Orchestration Engine**: Discovers SQL queries, resolves DAG dependencies, executes queries in topological order, calls `reconcile_schema()`, manages Iceberg materialization, and delegates to Aurora serving. | Main entrypoint invoked by `main()`. |
| `sanitize_column_name()` | ~L3180 | Column name sanitization (identical to `GoldInitialLoader.sanitize_column_name()`). | Used by `reconcile_schema()`. |
| `_is_timestamp_column()` | ~L3200 | Timestamp detection via data type inspection and naming heuristics (identical to `GoldInitialLoader`). | Used by `reconcile_schema()`. |
| `_conform_timestamp_to_string()` | ~L3230 | Converts timestamps to `YYYY-MM-DD HH:MM:SS` string format (identical to `GoldInitialLoader`). | Used by `reconcile_schema()`. |
| `reconcile_schema()` | L3281–L3349 | Schema reconciliation with timestamp standardization + universal string enforcement. Identical logic to `GoldInitialLoader.reconcile_schema()`. | Called in `run_gold_pipeline()` at L3720. |
| `_probe_target_schema_from_query()` | L3352–L3408 | Zero-record probe query for schema discovery. | Schema extraction for initial load. |
| Physical Iceberg Materialization | L1420–L1650 | Gold layer materializes authoritative physical Iceberg tables directly (no presentation views required). | Primary data lake serving target. |
| `_serve_to_mysql()` | L1133–L1277 | High-performance RDS/MySQL serving layer. Coordinates staging table write, schema validation, index creation, and zero-downtime atomic swap or upsert. | Called when `aurora` engine is enabled. |
| `_sync_iceberg_schema()` | L1279–L1301 | Introspects incoming Spark DataFrame columns against target Gold Iceberg table and executes schema updates. | Called before Iceberg table writes. |
| `_get_gold_initial_loader()` | L1303–L1312 | Dynamically imports and instantiates `GoldInitialLoader`. | Used by cold-start checks. |
| `_s3_path_exists()` | L1314–L1353 | Tests whether an S3 prefix or Iceberg metadata pointer exists using `boto3`. | Cold-start validation & ghost table detection. |
| `_check_and_run_initial_load()` | L1355–L1529 | Cold-start & ghost table guard: purges orphan catalog entries if metadata is missing and triggers `GoldInitialLoader`. | Called prior to executing incremental marts. |
| `_materialize_athena_table()` | L1531–L1727 | Writes query results directly to AWS Glue Iceberg catalog table with schema evolution and partition alignment. | Called when query specifies `table` materialization. |
| `_deduplicate_by_nkey()` | L1849–L1883 | In-memory windowed deduplication over natural keys (`ROW_NUMBER() OVER (PARTITION BY nkey ORDER BY ...)`). | Called prior to serving write. |
| `_resolve_and_deduplicate_columns()` | L1885–L1934 | Enforces strict Precedence Hierarchy: `Custom Transform (df.withColumn) > SQL Query Column > Config Default (external_columns NULL)`. Drops case-insensitive duplicate references, prioritizes transform outputs, and deterministically anchors natural keys. | Called in `_process_table()` & `GoldInitialLoader.run_initial_load()`. |
| `filter_incremental_delta()` | L1937–L2020 | Anti-joins incoming delta against current target table state to filter out unchanged records. | Incremental delta processing. |
| `_validate_gold_table_name()` | L2022–L2026 | Enforces naming conventions and prevents SQL injection on table identifiers. | Called before all DDL/DML. |
| `_validate_droppable_table_name()` | L2034–L2042 | Security guard: strictly prevents dropping non-staging tables during swap cleanup. | Called in `_cleanup_staging_table()`. |
| `_cleanup_staging_table()` | L2044–L2059 | Drops transient staging tables in MySQL (`staging_tbl_*`) upon failure or completion. | Staging lifecycle management. |
| `_verify_schema_exists_or_raise()` | L2061–L2099 | Verifies target schema/database exists in MySQL before initiating write operations. | Pre-flight check. |
| `_spark_type_to_mysql()` | L2118–L2143 | Translates PySpark data types to native MySQL DDL types. | Dynamic DDL generation. |
| `_detect_schema_evolution()` | L2145–L2225 | Introspects MySQL `information_schema.COLUMNS`, detects missing columns, and executes `ALTER TABLE ... ADD COLUMN`. | MySQL schema evolution. |
| `_write_staging_table()` | L2247–L2284 | Writes PySpark DataFrame to MySQL staging table via JDBC using connection pooling and batch commits. | Called in atomic swap workflow. |
| `_execute_isolated_atomic_swap()` | L2297–L2359 | **Zero-Downtime Swap**: Atomically renames `target -> target_old, staging -> target`, then drops `target_old`. | Called when `mode == 'atomic_swap'`. |
| `_ensure_mysql_index()` | L2361–L2390 | Dynamically creates B-Tree indexes on natural keys in MySQL if not already present. | Post-swap optimization. |
| `_apply_custom_transform()` | L2577–L2634 | Dynamically loads and runs custom Python transformation scripts defined in `custom_transforms/`, injecting operational context (`is_initial_load`, `llm_column`, `rate_limit_rps`, `batch_size`). | Custom mart transformation & initial load. |
| `_discover_queries()` | L2470–L2534 | Scans S3 or local SQL directories to locate all `.sql` query definition files. | Discovery phase. |
| `_sort_queries_by_dependency()` | L2594–L2618 | Builds a directed graph of mart queries and performs topological sorting to determine execution order. | DAG ordering. |
| `_ensure_dependency_views_registered()` | L2680–L2763 | Introspects SQL tokens and pre-registers Spark temp views from Bronze (`raw_tbl_*`), Silver (`tbl_*`), or Gold (`gold_*`). | Cross-layer query preparation. |
| `_resolve_mysql_connection_info()` | L2765–L2929 | Resolves host, port, user, and password for MySQL from AWS Secrets Manager using standard and fallback key names. | Database connection resolution. |
| `_format_error_diagnostic_card()` | L2931–L2966 | Formats rich ANSI diagnostic summary cards in CloudWatch when a query or serving operation fails. | Error handling and telemetry. |
| `_log_final_gold_summary()` | L2968–L3009 | Prints tabular execution summary showing table name, rows processed, duration, and status. | Job completion summary. |
| `parse_spark_arguments()` | L3497–L3548 | Standalone CLI & Glue argument parser resolving environment, S3 paths, and mandatory Gold parameters. | CLI/Glue argument resolution. |
| `main()` | L3550–L3589 | Standalone AWS Glue Job entrypoint (`GlueContext`, `Job.init`, `Job.commit`). | Glue job runtime. |

---

## 4. Code Component Map: `protegrity_encryption.py`

### Class: `ProtegrityEncryptionManager` (Lines 36–567)

| Function / Component | Line Range | Description & Architectural Purpose |
| :--- | :--- | :--- |
| `__init__()` | L41–L77 | Initializes from pipeline configuration. Resolves `lambda_arn`, `batch_size`, `default_data_element`, `max_retries`, and AWS region. Lambda ARN is validated lazily only when encryption is triggered for table columns. |
| `_clean_key()` | L86–L88 | **Static method** — sanitizes and normalizes column names: `re.sub(r'[^a-zA-Z0-9_]', '', re.sub(r'[\s\.\-]+', '_', str(k).strip())).lower()`. Shared identically with `bronze/script/protegrity_encryption.py`. |
| `_resolve_user_secret()` | L97–L140 | Resolves Protegrity user identity secret from direct config or AWS Secrets Manager. |
| `encrypt_record_chunk()` | ~L150–L300 | Encrypts in-memory Python dictionaries (Bronze layer). Packages PII columns into synchronous Lambda invocations with jitter exponential backoff. |
| `encrypt_spark_dataframe()` | ~L300–L567 | Encrypts PySpark DataFrames (Gold layer). Uses `mapInPandas` for distributed micro-batch processing across Spark workers. |

---

## 5. Configuration Engine: `gold_config_loader.py`

### Class: `GoldConfigLoader` (Lines 18–662)

| Method | Line Range | Architectural Purpose |
| :--- | :--- | :--- |
| `interpolate_env()` | L64–L83 | Recursively traverses nested dicts and strings, replacing `{env}` with current environment (`dev`, `stage`, `prod`). |
| `load_config()` | L85–L297 | Loads config from S3 (`s3://.../gold_config.json`) or local disk, caches in memory, and validates required keys. |
| `get_defaults()` | L299–L303 | Returns `pipeline_defaults` block. |
| `get_glue_database()` | L305–L319 | Returns target AWS Glue database name for Gold layer (`uax_datalake_db_{env}`). |
| `get_source_config()` | L321–L335 | Returns configuration block for a specific Gold mart / source system. |
| `get_table_config()` | L337–L372 | Resolves table-level configuration merged with parent defaults. |
| `get_primary_key()` | L374–L394 | Resolves natural keys / primary keys for deduplication and upserts. |
| `is_incremental()` | L396–L412 | Returns whether table is configured for incremental delta loads vs full rebuild. |
| `get_external_columns()` | L414–L438 | Resolves list of configured external / enrichment columns (e.g., `["conversation_summary"]`) for custom transforms and precedence resolution. |
| `get_llm_column()` | L440–L451 | Backward-compatibility helper returning the primary external column if configured. |
| `get_secret_name()` | L453–L479 | Resolves AWS Secrets Manager secret name for database credentials. |
| `get_target_engines()` | L512–L549 | Returns list of enabled serving engines (`athena`, `aurora`). |
| `get_custom_transform_path()` | L597–L634 | Resolves S3 path to custom Python transform script if configured. |

---

## 6. Glue Job Arguments Reference

| Glue Parameter | Resolution Logic | Fallback | Description |
| :--- | :--- | :--- | :--- |
| `--JOB_NAME` | Mandatory CLI | None | Unique name of the AWS Glue job run. |
| `--SOURCE_SYSTEM` | Mandatory CLI | None | Target source system identifier (`genesys`, `moveworks`, `servicenow`). |
| `--ENV` | CLI / Fallback | `dev` | Target environment (`dev`, `stage`, `prod`). |
| `--GOLD_CONFIG_S3_PATH` | CLI / Fallback | `s3://{bucket}/gold/script/config/gold_config.json` | Path to Gold pipeline configuration JSON. |
| `--TABLE_NAME` | CLI / Fallback | All source tables | Filters execution to a single mart or runs all discovered queries. |
| `--GOLD_SCHEMA` | CLI / Config | `enterprise_reporting` | Target MySQL schema name. Mandatory per zero-fallback policy. |
| `--RDS_SECRET_NAME` | CLI / Config | `source_systems.<source>.secret_name` | AWS Secrets Manager secret storing MySQL credentials. |
| `--DATA_LAKE_BUCKET` | CLI / Config | `uax-datalake-{env}-bucket` | S3 bucket where Gold Iceberg data files reside. |
| `--GLUE_DATABASE` | CLI / Config | `uax_datalake_db_{env}` | AWS Glue catalog database for Gold tables. |
| `--FULL_REFRESH` | CLI / Flag | `false` | Forces full re-aggregation from Silver tables (`true`/`false`). |
| `--INCREMENTAL` | CLI / Flag | `true` | Toggles incremental watermark filtering (`true`/`false`). |
| `--RELOAD_INITIAL` | CLI / Flag | `false` | Forces re-ingestion of historical initial CSV export. |
