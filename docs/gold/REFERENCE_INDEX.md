# Gold Layer — Component Reference & Code Location Index

This document provides a line-by-line master index and component map for the entire Gold Layer codebase. It details where every class, method, serving engine, configuration property, and Glue argument is defined, implemented, and referenced.

---

## 1. Directory & File Inventory

| File Path | Component Role | Key Responsibilities |
| :--- | :--- | :--- |
| [`gold/script/gold_layer_manager.py`](../../gold/script/gold_layer_manager.py) | **Main Gold Orchestrator & Serving Engine** | Topological SQL dependency resolution, Iceberg/Athena materialization, zero-downtime atomic swap to MySQL, multi-engine serving (Redshift, Snowflake, Databricks), schema introspection & evolution. |
| [`gold/script/gold_config_loader.py`](../../gold/script/gold_config_loader.py) | **Configuration Engine (`GoldConfigLoader`)** | S3/local JSON loading, `{env}` token interpolation, caching, serving engine resolution, credential key lookups. |
| [`gold/script/gold_initial_load.py`](../../gold/script/gold_initial_load.py) | **Cold-Start Historical Ingestion Engine** | Historical full-table bootstrapping, schema reconciliation, target Iceberg table provisioning before incremental cycles begin. |
| [`gold/script/config/gold_config.json`](../../gold/script/config/gold_config.json) | **Pipeline Configuration** | Blueprint defining Gold table targets, serving engines, natural keys, SQL queries, initial load thresholds, and DB credentials. |
| [`gold/script/custom_transforms/`](../../gold/script/custom_transforms/) | **Custom Script Extensions** | External PySpark transform scripts dynamically loaded per mart (e.g. LLM embeddings, statistical scoring). |

---

## 2. Code Component Map: `gold_layer_manager.py`

### Class: `GoldLayerManager` (Lines 51–3250)

| Function / Component | Line Range | Description & Architectural Purpose | References / Callers |
| :--- | :--- | :--- | :--- |
| `_load_gold_config()` | L59–L295 | Reads `gold_config.json` from S3 or local path, interpolates `{env}`, and resolves pipeline defaults. | Called in `run_gold_pipeline()` at L425. |
| `_resolve_natural_keys()` | L297–L408 | Extracts business natural keys from configuration or SQL annotations for deduplication and incremental upserts. | Called during query planning at L480. |
| `run_gold_pipeline()` | L410–L936 | **Core Orchestration Engine**: Discovers SQL queries, resolves DAG dependencies, executes queries in topological order, manages Athena materialization, and delegates to serving engines. | Main entrypoint invoked by `main()` at L3650. |
| `create_athena_view()` | L938–L1128 | Generates and executes DDL in Athena to create or replace logical views over underlying Silver/Gold Iceberg tables. | Called when query specifies `view` materialization at L680. |
| `_serve_to_mysql()` | L1130–L1290 | High-performance RDS/MySQL serving layer. Coordinates staging table write, schema validation, index creation, and zero-downtime atomic swap or upsert. | Called when `mysql` engine is enabled at L750. |
| `_run_redshift_serving()` | L1292–L1341 | Multi-engine adapter: writes data frames to Amazon Redshift via JDBC with schema verification. | Called when `redshift` engine is enabled at L780. |
| `_run_snowflake_serving()` | L1343–L1392 | Multi-engine adapter: writes conformed gold data to Snowflake warehouse. | Called when `snowflake` engine is enabled at L810. |
| `_run_databricks_serving()` | L1394–L1471 | Multi-engine adapter: writes conformed gold data to Databricks Unity Catalog / Delta Lake. | Called when `databricks` engine is enabled at L840. |
| `_sync_iceberg_schema()` | L1473–L1497 | Introspects incoming Spark DataFrame columns against target Gold Iceberg table and issues `ALTER TABLE ... ADD COLUMNS`. | Called before Iceberg table writes at L1760. |
| `_get_gold_initial_loader()` | L1499–L1508 | Dynamically imports and instantiates `GoldInitialLoader`. | Used by cold-start checks. |
| `_s3_path_exists()` | L1510–L1550 | Tests whether an S3 prefix or Iceberg metadata pointer exists using `boto3`. | Cold-start validation. |
| `_check_and_run_initial_load()` | L1552–L1732 | Cold-start guard: checks if target table is empty or missing metadata and triggers `GoldInitialLoader` if needed. | Called prior to executing incremental marts at L510. |
| `_materialize_athena_table()` | L1734–L1843 | Writes query results directly to AWS Glue Iceberg catalog table with schema evolution and partition alignment. | Called when query specifies `table` materialization at L710. |
| `_deduplicate_by_nkey()` | L1845–L1882 | In-memory windowed deduplication over natural keys (`ROW_NUMBER() OVER (PARTITION BY nkey ORDER BY ...)`). | Called prior to serving write at L1160. |
| `filter_incremental_delta()` | L1884–L1977 | Anti-joins incoming delta against current target table state to filter out unchanged records. | Incremental delta processing at L1175. |
| `_validate_gold_table_name()` | L1979–L1986 | Enforces naming conventions and prevents SQL injection on table identifiers. | Called before all DDL/DML. |
| `_validate_view_name()` | L1988–L1995 | Enforces naming conventions and prevents SQL injection on view identifiers. | Called before view creation. |
| `_validate_droppable_table_name()` | L1997–L2009 | Security guard: strictly prevents dropping non-staging tables during swap cleanup. | Called in `_cleanup_staging_table()`. |
| `_cleanup_staging_table()` | L2011–L2034 | Drops transient staging tables in MySQL (`staging_tbl_*`) upon failure or completion. | Staging lifecycle management. |
| `_verify_schema_exists_or_raise()` | L2036–L2081 | Verifies target schema/database exists in MySQL before initiating write operations. | Pre-flight check at L1145. |
| `_log_schema_introspection()` | L2083–L2098 | Pretty-prints PySpark DataFrame schema for debugging and CloudWatch auditability. | Pre-write logging. |
| `_spark_type_to_mysql()` | L2100–L2125 | Translates PySpark data types (e.g. `StringType`, `TimestampType`, `DecimalType`) to native MySQL DDL types. | Dynamic DDL generation. |
| `_detect_schema_evolution()` | L2127–L2212 | Introspects MySQL `information_schema.COLUMNS`, detects missing columns, and executes `ALTER TABLE ... ADD COLUMN`. | MySQL schema evolution at L1190. |
| `_log_mysql_schema_introspection()` | L2214–L2232 | Logs MySQL column names and data types post-evolution. | Diagnostic logging. |
| `_write_staging_table()` | L2234–L2277 | Writes PySpark DataFrame to MySQL staging table via JDBC using connection pooling and batch commits. | Called in atomic swap workflow at L1205. |
| `_table_exists()` | L2279–L2291 | Checks if a table exists in MySQL `information_schema.TABLES`. | Target existence check. |
| `_execute_isolated_atomic_swap()` | L2293–L2400 | **Zero-Downtime Swap**: Atomically renames `target -> target_old, staging -> target`, then drops `target_old`. Ensures readers experience zero downtime. | Called when `mode == 'atomic_swap'` at L1220. |
| `_ensure_mysql_index()` | L2401–L2435 | Dynamically creates B-Tree or unique indexes on natural keys in MySQL if not already present. | Post-swap optimization at L1240. |
| `_replace_mysql_records()` | L2437–L2470 | Executes atomic `DELETE FROM target WHERE nkey IN (...)` followed by batch insert. | Replace serving mode. |
| `_upsert_mysql_table()` | L2472–L2532 | Performs `INSERT ... ON DUPLICATE KEY UPDATE` in MySQL for incremental upserting without full table rebuild. | Upsert serving mode. |
| `_build_mysql_ssl_context()` | L2534–L2548 | Builds TLS/SSL context enforcing encrypted communication with AWS RDS. | Connection security. |
| `_get_mysql_connection()` | L2550–L2589 | Returns an authenticated, SSL-hardened `pymysql` connection instance. | Used by all MySQL DDL/DML utilities. |
| `_execute_sql_query()` | L2591–L2606 | Executes parameterized SELECT queries against MySQL and returns rows as tuples. | Introspection queries. |
| `_execute_ddl()` | L2608–L2626 | Executes DDL statements (`CREATE`, `ALTER`, `RENAME`, `DROP`) with automatic commit. | Schema migration execution. |
| `_extract_sql_annotations()` | L2628–L2648 | Regex parser extracting metadata headers from SQL files (`-- @materialization`, `-- @natural_keys`, `-- @scd`). | Query parsing at L2730. |
| `_apply_custom_transform()` | L2650–L2707 | Dynamically loads and runs custom Python transformation scripts defined in `custom_transforms/`. | Custom mart transformation at L620. |
| `_discover_queries()` | L2709–L2762 | Scans S3 or local SQL directories to locate all `.sql` query definition files. | Discovery phase at L440. |
| `_strip_sql_comments()` | L2764–L2773 | Strips `--` and `/* */` comments from SQL before analyzing dependencies. | Dependency analysis. |
| `_check_mart_dependency()` | L2775–L2827 | Inspects SQL text to identify references to other Gold data marts. | Dependency analysis. |
| `_sort_queries_by_dependency()` | L2829–L2857 | Builds a directed graph of mart queries and performs topological sorting to determine execution order. | DAG ordering at L460. |
| `_resolve_query_dependencies()` | L2859–L2923 | Validates DAG for cycles and missing parent dependencies. | DAG validation. |
| `_ensure_dependency_views_registered()`| L2925–L2991 | Registers intermediate upstream queries as temporary Spark views so downstream queries can reference them. | Query execution loop at L590. |
| `_resolve_mysql_connection_info()` | L2993–L3175 | Resolves host, port, user, and password for MySQL from AWS Secrets Manager using standard and fallback key names. | Database connection resolution. |
| `_format_error_diagnostic_card()` | L3177–L3212 | Formats rich ANSI diagnostic summary cards in CloudWatch when a query or serving operation fails. | Error handling and telemetry. |
| `_log_final_gold_summary()` | L3214–L3250 | Prints tabular execution summary showing table name, rows processed, duration, and status. | Job completion summary. |

---

## 3. Cold-Start Historical Loader: `gold_initial_load.py`

### Class: `GoldInitialLoader` (Lines 47–535)

| Function / Component | Line Range | Description & Architectural Purpose | References / Callers |
| :--- | :--- | :--- | :--- |
| `sanitize_column_name()` | L65–L80 | Normalizes column names (replaces spaces/special characters with underscores, lowercase). | Applied during schema reconciliation. |
| `reconcile_schema()` | L82–L145 | Reconciles incoming Spark DataFrame schema against target catalog table to prevent type mismatches. | Pre-load schema alignment. |
| `_probe_target_schema_from_query()` | L147–L205 | Executes dry-run / `LIMIT 0` query to inspect schema of SQL query without pulling full dataset. | Initial schema discovery. |
| `run_initial_load()` | L207–L535 | Performs historical full load of Gold table from Silver sources, creates Iceberg metadata, and registers in Glue Catalog. | Called during cold start bootstrapping. |
| `main()` | L537–L590 | CLI / Glue entrypoint for standalone initial load execution. | Standalone Glue job execution. |

---

## 4. Configuration Engine: `gold_config_loader.py`

### Class: `GoldConfigLoader` (Lines 18–600)

| Method | Line Range | Architectural Purpose |
| :--- | :--- | :--- |
| `interpolate_env()` | L58–L77 | Recursively traverses nested dicts and strings, replacing `{env}` with current environment (`dev`, `stage`, `prod`). |
| `load_config()` | L79–L291 | Loads config from S3 (`s3://.../gold_config.json`) or local disk, caches in memory, and validates required keys. |
| `get_defaults()` | L293–L297 | Returns `pipeline_defaults` block. |
| `get_glue_database()` | L299–L313 | Returns target AWS Glue database name for Gold layer (`uax_datalake_gold_{env}`). |
| `get_source_config()` | L315–L329 | Returns configuration block for a specific Gold mart / source system. |
| `get_table_config()` | L331–L358 | Resolves table-level configuration merged with parent defaults. |
| `get_primary_key()` | L360–L380 | Resolves natural keys / primary keys for deduplication and upserts. |
| `is_incremental()` | L382–L398 | Returns whether table is configured for incremental delta loads vs full rebuild. |
| `get_secret_name()` | L414–L440 | Resolves AWS Secrets Manager secret name for database credentials. |
| `get_target_engines()` | L473–L510 | Returns list of enabled serving engines (`mysql`, `redshift`, `snowflake`, `databricks`). |
| `get_custom_transform_path()` | L558–L595 | Resolves S3 path to custom Python transform script if configured. |

---

## 5. Configuration Blueprint Cross-Reference

| `gold_config.json` Parameter | Source Code Location | Fallback Value | Code Usage Description |
| :--- | :--- | :--- | :--- |
| `pipeline_defaults.gold_bucket` | `gold_layer_manager.py` L85 | `uax-datalake-gold-{env}` | Target S3 bucket where conformed Gold Iceberg data and metadata files reside. |
| `pipeline_defaults.glue_database` | `gold_layer_manager.py` L90 | `uax_datalake_gold_{env}` | AWS Glue catalog database for Gold tables and views. |
| `pipeline_defaults.target_format` | `gold_layer_manager.py` L95 | `iceberg` | Storage format for conformed tables (`iceberg`). |
| `serving_targets.mysql.enabled` | `gold_layer_manager.py` L745 | `false` | Flag to enable MySQL/RDS serving. |
| `serving_targets.mysql.mode` | `gold_layer_manager.py` L1150 | `atomic_swap` | MySQL serving strategy (`atomic_swap`, `upsert`, `replace`, `append`). |
| `serving_targets.mysql.secret_name` | `gold_layer_manager.py` L2995 | — | AWS Secrets Manager secret storing MySQL credentials. |
| `marts[].materialization` | `gold_layer_manager.py` L670 | `table` | Target materialization type: `table` (Iceberg table) or `view` (Athena view). |
| `marts[].natural_keys` | `gold_layer_manager.py` L297 | `[]` | Business key columns used for in-memory deduplication and MySQL upserts. |
| `marts[].sql_path` | `gold_layer_manager.py` L445 | — | S3 or local path to SQL query file. |
| `marts[].custom_transform` | `gold_layer_manager.py` L620 | `null` | Optional path to custom PySpark Python script. |

---

## 6. Glue Job Arguments Reference

| Glue Parameter | Resolution Logic | Fallback | Description |
| :--- | :--- | :--- | :--- |
| `--ENV` | `parse_spark_arguments` | `dev` | Target environment (`dev`, `stage`, `prod`). |
| `--CONFIG_PATH` | CLI / Default | `s3://uax-datalake-config-{env}/gold/config/gold_config.json` | Path to Gold pipeline configuration JSON. |
| `--SQL_DIR` | CLI / Config | `s3://uax-datalake-config-{env}/gold/queries/` | S3 directory holding `.sql` queries. |
| `--MART_NAME` | CLI / None | `ALL` | Filters execution to a single Gold mart or runs all discovered queries. |
| `--INITIAL_LOAD` | CLI / Flag | `false` | Forces cold-start historical bootstrap load. |
| `--FULL_REFRESH` | CLI / Flag | `false` | Forces complete wipe and rebuild of target tables and views. |
