# Gold Layer — Component Reference & Code Location Index

This document provides a line-by-line master index and component map for the entire Gold Layer codebase. It details where every class, method, serving engine, configuration property, and Glue argument is defined, implemented, and referenced.

---

## 1. Directory & File Inventory

| File Path | Component Role | Key Responsibilities |
| :--- | :--- | :--- |
| [`gold/script/gold_layer_manager.py`](../../gold/script/gold_layer_manager.py) | **Main Gold Orchestrator & Standalone Glue Entrypoint** | Topological SQL DAG dependency resolution, cross-layer temp view pre-registration (`raw_tbl_*`, `tbl_*`, `gold_*`), Iceberg physical table materialization, ghost table auto-remediation, zero-downtime atomic swap to MySQL, schema introspection & evolution. |
| [`gold/script/gold_config_loader.py`](../../gold/script/gold_config_loader.py) | **Configuration Engine (`GoldConfigLoader`)** | S3/local JSON loading, `{env}` token interpolation, caching, serving engine resolution, credential key lookups. |
| [`gold/script/gold_initial_load.py`](../../gold/script/gold_initial_load.py) | **Cold-Start Historical Ingestion Engine** | Historical full-table bootstrapping, schema reconciliation, target Iceberg table provisioning before incremental cycles begin. |
| [`gold/script/config/gold_config.json`](../../gold/script/config/gold_config.json) | **Pipeline Configuration** | Blueprint defining Gold table targets, serving engines, natural keys, SQL queries, initial load thresholds, and DB credentials. |
| [`gold/script/custom_transforms/`](../../gold/script/custom_transforms/) | **Custom Script Extensions** | External PySpark transform scripts dynamically loaded per mart (e.g. LLM embeddings, statistical scoring). |
| [`gold/script/protegrity_encryption.py`](../../gold/script/protegrity_encryption.py) | **Protegrity PII Tokenizer** | PySpark DataFrame distributed encryption via `mapInPandas`, 500-record batching, Protegrity Protector Lambda API invocation (`/v2/protect`). |

---

## 2. Code Component Map: `gold_layer_manager.py`

### Class: `GoldLayerManager` (Lines 51–3495)

| Function / Component | Line Range | Description & Architectural Purpose | References / Callers |
| :--- | :--- | :--- | :--- |
| `_load_gold_config()` | L72–L307 | Reads `gold_config.json` from S3 or local path, interpolates `{env}`, and resolves pipeline defaults. | Called in `run_gold_pipeline()` at L465. |
| `_resolve_natural_keys()` | L308–L418 | Extracts business natural keys from configuration or SQL annotations for deduplication and incremental upserts. | Called during query planning at L500. |
| `_purge_s3_prefix()` | L420–L454 | Recursively deletes orphan S3 objects during initial load or ghost table recovery. | Initial load & recovery. |
| `run_gold_pipeline()` | L455–L939 | **Core Orchestration Engine**: Discovers SQL queries, resolves DAG dependencies, executes queries in topological order, manages Iceberg materialization, and delegates to Aurora serving. | Main entrypoint invoked by `main()` at L3583. |
| `create_athena_view()` | L941–L1131 | Generates and executes DDL in Athena to create or replace optional presentation views over underlying Iceberg tables. | Called when query specifies `view` materialization. |
| `_serve_to_mysql()` | L1133–L1277 | High-performance RDS/MySQL serving layer. Coordinates staging table write, schema validation, index creation, and zero-downtime atomic swap or upsert. | Called when `aurora` engine is enabled at L880. |
| `_sync_iceberg_schema()` | L1279–L1301 | Introspects incoming Spark DataFrame columns against target Gold Iceberg table and executes schema updates. | Called before Iceberg table writes at L1610. |
| `_get_gold_initial_loader()` | L1303–L1312 | Dynamically imports and instantiates `GoldInitialLoader`. | Used by cold-start checks. |
| `_s3_path_exists()` | L1314–L1353 | Tests whether an S3 prefix or Iceberg metadata pointer exists using `boto3`. | Cold-start validation & ghost table detection. |
| `_check_and_run_initial_load()` | L1355–L1529 | Cold-start & ghost table guard: purges orphan catalog entries if metadata is missing and triggers `GoldInitialLoader`. | Called prior to executing incremental marts at L530. |
| `_materialize_athena_table()` | L1531–L1727 | Writes query results directly to AWS Glue Iceberg catalog table with schema evolution and partition alignment. | Called when query specifies `table` materialization at L680. |
| `_deduplicate_by_nkey()` | L1729–L1766 | In-memory windowed deduplication over natural keys (`ROW_NUMBER() OVER (PARTITION BY nkey ORDER BY ...)`). | Called prior to serving write. |
| `filter_incremental_delta()` | L1768–L1851 | Anti-joins incoming delta against current target table state to filter out unchanged records. | Incremental delta processing at L640. |
| `_validate_gold_table_name()` | L1853–L1857 | Enforces naming conventions and prevents SQL injection on table identifiers. | Called before all DDL/DML. |
| `_validate_view_name()` | L1859–L1863 | Enforces naming conventions and prevents SQL injection on view identifiers. | Called before view creation. |
| `_validate_droppable_table_name()` | L1865–L1873 | Security guard: strictly prevents dropping non-staging tables during swap cleanup. | Called in `_cleanup_staging_table()`. |
| `_cleanup_staging_table()` | L1875–L1890 | Drops transient staging tables in MySQL (`staging_tbl_*`) upon failure or completion. | Staging lifecycle management. |
| `_verify_schema_exists_or_raise()` | L1892–L1930 | Verifies target schema/database exists in MySQL before initiating write operations. | Pre-flight check at L1150. |
| `_log_schema_introspection()` | L1932–L1947 | Pretty-prints PySpark DataFrame schema for debugging and CloudWatch auditability. | Pre-write logging. |
| `_spark_type_to_mysql()` | L1949–L1974 | Translates PySpark data types (e.g. `StringType`, `TimestampType`, `DecimalType`) to native MySQL DDL types. | Dynamic DDL generation. |
| `_detect_schema_evolution()` | L1976–L2056 | Introspects MySQL `information_schema.COLUMNS`, detects missing columns, and executes `ALTER TABLE ... ADD COLUMN`. | MySQL schema evolution at L1190. |
| `_log_mysql_schema_introspection()` | L2058–L2076 | Logs MySQL column names and data types post-evolution. | Diagnostic logging. |
| `_write_staging_table()` | L2078–L2115 | Writes PySpark DataFrame to MySQL staging table via JDBC using connection pooling and batch commits. | Called in atomic swap workflow at L1205. |
| `_table_exists()` | L2117–L2126 | Checks if a table exists in MySQL `information_schema.TABLES`. | Target existence check. |
| `_execute_isolated_atomic_swap()` | L2128–L2190 | **Zero-Downtime Swap**: Atomically renames `target -> target_old, staging -> target`, then drops `target_old`. Ensures readers experience zero downtime. | Called when `mode == 'atomic_swap'` at L1220. |
| `_ensure_mysql_index()` | L2192–L2221 | Dynamically creates B-Tree indexes on natural keys in MySQL if not already present. | Post-swap optimization at L1240. |
| `_replace_mysql_records()` | L2223–L2248 | Executes atomic `DELETE FROM target WHERE nkey IN (...)` followed by batch insert. | Replace serving mode. |
| `_upsert_mysql_table()` | L2250–L2303 | Performs `INSERT ... ON DUPLICATE KEY UPDATE` in MySQL for incremental upserting without full table rebuild. | Upsert serving mode. |
| `_build_mysql_ssl_context()` | L2305–L2315 | Builds TLS/SSL context enforcing encrypted communication with AWS RDS. | Connection security. |
| `_get_mysql_connection()` | L2317–L2352 | Returns an authenticated, SSL-hardened `pymysql` connection instance. | Used by all MySQL DDL/DML utilities. |
| `_execute_sql_query()` | L2354–L2369 | Executes parameterized SELECT queries against MySQL and returns rows as tuples. | Introspection queries. |
| `_execute_ddl()` | L2371–L2389 | Executes DDL statements (`CREATE`, `ALTER`, `RENAME`, `DROP`) with automatic commit. | Schema migration execution. |
| `_extract_sql_annotations()` | L2391–L2409 | Regex parser extracting metadata headers from SQL files (`-- @materialization`, `-- @natural_keys`, `-- @scd`). | Query parsing. |
| `_apply_custom_transform()` | L2411–L2468 | Dynamically loads and runs custom Python transformation scripts defined in `custom_transforms/`. | Custom mart transformation at L610. |
| `_discover_queries()` | L2470–L2534 | Scans S3 or local SQL directories to locate all `.sql` query definition files. | Discovery phase at L470. |
| `_strip_sql_comments()` | L2536–L2543 | Strips `--` and `/* */` comments from SQL before analyzing dependencies. | Dependency analysis. |
| `_check_mart_dependency()` | L2545–L2592 | Inspects SQL text to identify references to other Gold data marts (using negative lookbehinds). | Dependency analysis. |
| `_sort_queries_by_dependency()` | L2594–L2618 | Builds a directed graph of mart queries and performs topological sorting to determine execution order. | DAG ordering at L490. |
| `_resolve_query_dependencies()` | L2620–L2678 | Validates DAG for cycles and missing parent dependencies. | DAG validation. |
| `_ensure_dependency_views_registered()`| L2680–L2763 | Introspects SQL tokens and pre-registers Spark temp views from Bronze (`raw_tbl_*`), Silver (`tbl_*`), or Gold (`gold_*`). | Cross-layer query preparation at L580. |
| `_resolve_mysql_connection_info()` | L2765–L2929 | Resolves host, port, user, and password for MySQL from AWS Secrets Manager using standard and fallback key names (`rds_passwords`, etc.). | Database connection resolution. |
| `_format_error_diagnostic_card()` | L2931–L2966 | Formats rich ANSI diagnostic summary cards in CloudWatch when a query or serving operation fails. | Error handling and telemetry. |
| `_log_final_gold_summary()` | L2968–L3009 | Prints tabular execution summary showing table name, rows processed, duration, and status. | Job completion summary. |
| `parse_spark_arguments()` | L3497–L3548 | Standalone CLI & Glue argument parser resolving environment, S3 paths, and mandatory Gold parameters. | CLI/Glue argument resolution. |
| `main()` | L3550–L3589 | Standalone AWS Glue Job entrypoint (`GlueContext`, `Job.init`, `Job.commit`). | Glue job runtime. |

---

## 3. Configuration Engine: `gold_config_loader.py`

### Class: `GoldConfigLoader` (Lines 18–600)

| Method | Line Range | Architectural Purpose |
| :--- | :--- | :--- |
| `interpolate_env()` | L58–L77 | Recursively traverses nested dicts and strings, replacing `{env}` with current environment (`dev`, `stage`, `prod`). |
| `load_config()` | L79–L291 | Loads config from S3 (`s3://.../gold_config.json`) or local disk, caches in memory, and validates required keys. |
| `get_defaults()` | L293–L297 | Returns `pipeline_defaults` block. |
| `get_glue_database()` | L299–L313 | Returns target AWS Glue database name for Gold layer (`uax_datalake_db_{env}`). |
| `get_source_config()` | L315–L329 | Returns configuration block for a specific Gold mart / source system. |
| `get_table_config()` | L331–L358 | Resolves table-level configuration merged with parent defaults. |
| `get_primary_key()` | L360–L380 | Resolves natural keys / primary keys for deduplication and upserts. |
| `is_incremental()` | L382–L398 | Returns whether table is configured for incremental delta loads vs full rebuild. |
| `get_secret_name()` | L414–L440 | Resolves AWS Secrets Manager secret name for database credentials. |
| `get_target_engines()` | L473–L510 | Returns list of enabled serving engines (`athena`, `aurora`). |
| `get_custom_transform_path()` | L558–L595 | Resolves S3 path to custom Python transform script if configured. |

---

## 4. Glue Job Arguments Reference

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
