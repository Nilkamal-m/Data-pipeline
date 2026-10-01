# Gold Layer — Component Reference & Code Location Index

This document provides a master index and component map for the entire Gold Layer codebase. It details where every class, method, serving engine, configuration property, and Glue argument is defined, implemented, and referenced.

---

## 1. Directory & File Inventory

| File Path | Component Role | Key Responsibilities |
| :--- | :--- | :--- |
| [`gold/script/gold_layer_manager.py`](../../gold/script/gold_layer_manager.py) | **Main Gold Orchestrator & Standalone Glue Entrypoint** | Topological SQL DAG dependency resolution, cross-layer temp view pre-registration (`raw_tbl_*`, `tbl_*`, `gold_*`), schema reconciliation (timestamp standardization + universal string enforcement), Iceberg physical table materialization, ghost table auto-remediation, Iceberg schema incompatibility auto-resolution, dynamic custom transform hook invocation, column conflict resolution & deduplication, zero-downtime atomic swap to MySQL, schema introspection & evolution. |
| [`gold/script/gold_initial_load.py`](../../gold/script/gold_initial_load.py) | **Cold-Start Historical Ingestion Engine** | Historical full-table bootstrapping, schema reconciliation (timestamp standardization + universal string enforcement), Protegrity PII encryption, query schema probing, Iceberg incompatibility auto-resolution, dynamic custom transform hook invocation (`is_initial_load=True`), column precedence resolution & deduplication, target Iceberg table provisioning. |
| [`gold/script/gold_config_loader.py`](../../gold/script/gold_config_loader.py) | **Configuration Engine (`GoldConfigLoader`)** | S3/local JSON loading, `{env}` token interpolation, caching, serving engine resolution, credential key lookups, external columns discovery, custom transform script path resolution. |
| [`gold/script/config/gold_config.json`](../../gold/script/config/gold_config.json) | **Pipeline Configuration** | Blueprint defining Gold table targets, serving engines, natural keys, SQL queries, initial load thresholds, encryption columns, external enrichment columns, and DB credentials. |
| [`gold/script/custom_transforms/`](../../gold/script/custom_transforms/) | **Custom Script Extensions Directory** | External PySpark transform scripts dynamically loaded per mart (e.g. LLM enrichments, external API lookups, statistical scoring). |
| [`gold/script/custom_transforms/genesys_conversations.py`](../../gold/script/custom_transforms/genesys_conversations.py) | **Sample Custom Transform (Genesys)** | Production sample transform hook for Genesys conversations implementing `transform(df, spark, context)`, external column iteration, API authentication, and batch processing. |
| [`gold/script/custom_transforms/moveworks_interactions.py`](../../gold/script/custom_transforms/moveworks_interactions.py) | **Sample Custom Transform (Moveworks)** | Production sample transform hook for Moveworks interactions implementing `transform(df, spark, context)`, partition-level micro-batching, and ticket resolution summarization. |
| [`gold/script/protegrity_encryption.py`](../../gold/script/protegrity_encryption.py) | **Protegrity PII Tokenizer** | PySpark DataFrame distributed encryption via `mapInPandas`, 500-record batching, Protegrity Protector Lambda API invocation (`/v2/protect`), shared `_clean_key()` column normalizer. |

---

## 2. Code Component Map: `gold_initial_load.py`

### Class: [`GoldInitialLoader`](../../gold/script/gold_initial_load.py#L58-L1618) (Lines 58–1618)

| Function / Component | Line Range | Description & Architectural Purpose |
| :--- | :--- | :--- |
| [`sanitize_column_name()`](../../gold/script/gold_initial_load.py#L64-L85) | L64–L85 | Cleanses and standardizes column names for SQL/Parquet/Iceberg/Athena compatibility. Converts spaces, dots, hyphens, slashes, brackets, colons, and hashes to underscores. Lowercases and collapses duplicate underscores. |
| [`_is_timestamp_column()`](../../gold/script/gold_initial_load.py#L88-L153) | L88–L153 | Determines whether a column represents a date/time/timestamp using PySpark data type inspection and naming convention heuristics (exact, suffix, prefix, compound phrases). Excludes `_updated_at`, `_inserted_at`, `_data_as_of`. |
| [`_conform_timestamp_to_string()`](../../gold/script/gold_initial_load.py#L156-L236) | L156–L236 | Converts identified timestamp columns to `YYYY-MM-DD HH:MM:SS` string format. Handles native timestamps, epochs (seconds/millis), ISO 8601, slash-delimited, date-only, and null/empty values. |
| [`reconcile_schema()`](../../gold/script/gold_initial_load.py#L239-L317) | L239–L317 | Core schema reconciliation: cleanses column names, identifies timestamps via heuristics, standardizes to string format, pads missing target columns with NULL, retains extra columns for schema evolution. Technical audit columns remain as `TimestampType`. |
| [`_probe_target_schema_from_query()`](../../gold/script/gold_initial_load.py#L320-L436) | L320–L436 | Discovers the target Gold table schema from the corresponding SQL query file. Executes a zero-record probe query (`WHERE 1=0`) via Spark SQL to extract schema fields without reading data. |
| [`_deduplicate_by_nkey()`](../../gold/script/gold_initial_load.py#L439-L463) | L439–L463 | Ensures row uniqueness by natural keys using windowed `ROW_NUMBER()` with `_updated_at DESC` ordering. |
| [`_apply_custom_transform()`](../../gold/script/gold_initial_load.py#L466-L695) | L466–L695 | Dynamically locates, imports, and executes external custom Python transform hook (`transform(df, spark, context)`) during initial load. Features multi-tier search (configured path, local singular/plural tolerance, `importlib.util.find_spec` in `sys.path`, and automatic S3 fallback download from Data Lake bucket to `/tmp/custom_transforms/`). Injects `is_initial_load=True` and `external_columns`. |
| [`_resolve_and_deduplicate_columns()`](../../gold/script/gold_initial_load.py#L698-L759) | L698–L759 | Enforces strict Precedence Hierarchy: `Custom Transform (df.withColumn) > SQL Query Column > Config Default (external_columns NULL)`. Eliminates case-insensitive duplicate references, prioritizes transform outputs, and guarantees schema stability. |
| [`_purge_s3_prefix()`](../../gold/script/gold_initial_load.py#L762-L793) | L762–L793 | Safely deletes orphaned or corrupted metadata/data files under an S3 URI prefix using boto3. |
| [`_resolve_aws_region()`](../../gold/script/gold_initial_load.py#L796-L832) | L796–L832 | Dynamically resolves the AWS region from job parameters, environment variables, boto3 session, or S3 bucket location constraints. |
| [`_purge_table_registration()`](../../gold/script/gold_initial_load.py#L835-L873) | L835–L873 | Completely purges a table from Spark SQL, Spark catalog cache, Glue Catalog, and S3 metadata storage. |
| [`_create_iceberg_table_from_df()`](../../gold/script/gold_initial_load.py#L876-L960) | L876–L960 | Creates or recreates an Iceberg table from a DataFrame. Includes Spark SQL DDL fallback and automatic ghost table purge on collision. |
| [`_check_and_resolve_iceberg_incompatibilities()`](../../gold/script/gold_initial_load.py#L963-L1027) | L963–L1027 | Detects column type mismatches between incoming DataFrame and existing Iceberg table. Purges and signals recreation if incompatible types found (e.g., `bigint` → `string`). |
| [`_get_gold_layer_manager()`](../../gold/script/gold_initial_load.py#L1030-L1049) | L1030–L1049 | Lazily imports `GoldLayerManager` to prevent circular import locks. |
| [`_get_protegrity_encryption_manager()`](../../gold/script/gold_initial_load.py#L1052-L1076) | L1052–L1076 | Lazily and dynamically imports `ProtegrityEncryptionManager` class from multiple candidate directory paths. |
| [`run_initial_load()`](../../gold/script/gold_initial_load.py#L1079-L1618) | L1079–L1618 | **Main orchestration method**: Reads CSV/Parquet, cleanses column names, encrypts PII (if configured), reconciles schema, triggers custom transform hook, enforces column precedence, deduplicates by natural keys, upserts into Iceberg table, and optionally serves to Aurora MySQL. |
| [`main()`](../../gold/script/gold_initial_load.py#L1620-L1696) | L1620–L1696 | AWS Glue job entrypoint with SparkConf, GlueContext, and Iceberg catalog configuration. |

---

## 3. Code Component Map: `gold_layer_manager.py`

### Class: [`GoldLayerManager`](../../gold/script/gold_layer_manager.py#L67-L3350) (Lines 67–3350)

| Function / Component | Line Range | Description & Architectural Purpose | References / Callers |
| :--- | :--- | :--- | :--- |
| [`_load_gold_config()`](../../gold/script/gold_layer_manager.py#L75-L309) | L75–L309 | Reads `gold_config.json` from S3 or local path, interpolates `{env}`, and resolves pipeline defaults. | Called in `run_gold_pipeline()`. |
| [`_resolve_natural_keys()`](../../gold/script/gold_layer_manager.py#L311-L421) | L311–L421 | Extracts business natural keys from configuration or SQL annotations for deduplication and incremental upserts. | Called during query planning. |
| [`_purge_s3_prefix()`](../../gold/script/gold_layer_manager.py#L423-L456) | L423–L456 | Recursively deletes orphan S3 objects during initial load or ghost table recovery. | Initial load & recovery. |
| [`_resolve_aws_region()`](../../gold/script/gold_layer_manager.py#L458-L497) | L458–L497 | Resolves AWS region using parameters, environment variables, boto3 session, or S3 bucket location constraint. | Glue catalog & Iceberg initialization. |
| [`_purge_table_registration()`](../../gold/script/gold_layer_manager.py#L499-L555) | L499–L555 | Completely purges Iceberg table from Spark SQL, Glue Catalog, and S3 data paths upon corrupt/incompatible states. | Ghost table auto-remediation. |
| [`_create_iceberg_table_from_df()`](../../gold/script/gold_layer_manager.py#L557-L647) | L557–L647 | Proactively creates authoritative Iceberg table partitioned and formatted correctly with ghost table auto-purge. | Initial provisioning & recreation. |
| [`run_gold_pipeline()`](../../gold/script/gold_layer_manager.py#L649-L1169) | L649–L1169 | **Core Orchestration Engine**: Discovers SQL queries, resolves DAG dependencies, executes queries in topological order, invokes custom transform extensions, enforces column precedence, manages Iceberg physical table materialization, and coordinates Aurora MySQL serving. | Main entrypoint invoked by `main()`. |
| [`_serve_to_mysql()`](../../gold/script/gold_layer_manager.py#L1171-L1311) | L1171–L1311 | High-performance RDS/MySQL serving layer. Coordinates staging table write, schema validation, index creation, and zero-downtime atomic swap or upsert. | Called when `aurora` engine is enabled. |
| [`_sync_iceberg_schema()`](../../gold/script/gold_layer_manager.py#L1313-L1335) | L1313–L1335 | Introspects incoming Spark DataFrame columns against target Gold Iceberg table and executes schema updates (`ALTER TABLE ... ADD COLUMNS`). | Called before Iceberg table writes. |
| [`_check_and_resolve_iceberg_incompatibilities()`](../../gold/script/gold_layer_manager.py#L1337-L1402) | L1337–L1402 | Introspects data types of existing Iceberg table vs incoming DataFrame. Triggers purge & recreation if incompatible types detected. | Iceberg schema evolution guard. |
| [`_get_gold_initial_loader()`](../../gold/script/gold_layer_manager.py#L1404-L1413) | L1404–L1413 | Dynamically imports and instantiates `GoldInitialLoader`. | Used by cold-start checks. |
| [`_s3_path_exists()`](../../gold/script/gold_layer_manager.py#L1415-L1454) | L1415–L1454 | Tests whether an S3 prefix or Iceberg metadata pointer exists using `boto3`. | Cold-start validation & ghost table detection. |
| [`_check_and_run_initial_load()`](../../gold/script/gold_layer_manager.py#L1456-L1630) | L1456–L1630 | Cold-start & ghost table guard: purges orphan catalog entries if metadata is missing and triggers `GoldInitialLoader`. | Called prior to executing incremental marts. |
| [`_materialize_athena_table()`](../../gold/script/gold_layer_manager.py#L1632-L1842) | L1632–L1842 | Writes query results directly to AWS Glue Iceberg catalog table with schema evolution and partition alignment. | Called when query specifies `table` materialization. |
| [`_deduplicate_by_nkey()`](../../gold/script/gold_layer_manager.py#L1844-L1878) | L1844–L1878 | In-memory windowed deduplication over natural keys (`ROW_NUMBER() OVER (PARTITION BY nkey ORDER BY _updated_at DESC)`). | Called prior to serving write. |
| [`_resolve_and_deduplicate_columns()`](../../gold/script/gold_layer_manager.py#L1880-L1945) | L1880–L1945 | Enforces strict Precedence Hierarchy: `Custom Transform (df.withColumn) > SQL Query Column > Config Default (external_columns NULL)`. Drops case-insensitive duplicate references, prioritizes transform outputs, and deterministically anchors natural keys. | Called in `run_gold_pipeline()`. |
| [`filter_incremental_delta()`](../../gold/script/gold_layer_manager.py#L1947-L2030) | L1947–L2030 | Anti-joins incoming delta against current target table state to filter out unchanged records. | Incremental delta processing. |
| [`_validate_gold_table_name()`](../../gold/script/gold_layer_manager.py#L2032-L2036) | L2032–L2036 | Enforces naming conventions and prevents SQL injection on table identifiers. | Called before all DDL/DML. |
| [`_validate_view_name()`](../../gold/script/gold_layer_manager.py#L2038-L2042) | L2038–L2042 | Validates Spark view names against alphanumeric and underscore patterns. | Called before view registration. |
| [`_validate_droppable_table_name()`](../../gold/script/gold_layer_manager.py#L2044-L2052) | L2044–L2052 | Security guard: strictly prevents dropping non-staging tables during swap cleanup. | Called in `_cleanup_staging_table()`. |
| [`_cleanup_staging_table()`](../../gold/script/gold_layer_manager.py#L2054-L2069) | L2054–L2069 | Drops transient staging tables in MySQL (`staging_tbl_*`) upon failure or completion. | Staging lifecycle management. |
| [`_verify_schema_exists_or_raise()`](../../gold/script/gold_layer_manager.py#L2071-L2109) | L2071–L2109 | Verifies target schema/database exists in MySQL before initiating write operations. | Pre-flight check. |
| [`_log_schema_introspection()`](../../gold/script/gold_layer_manager.py#L2111-L2126) | L2111–L2126 | Formats and logs schema field names, nullability, and PySpark data types. | Telemetry & debugging. |
| [`_spark_type_to_mysql()`](../../gold/script/gold_layer_manager.py#L2128-L2153) | L2128–L2153 | Translates PySpark data types to native MySQL DDL types (`VARCHAR`, `BIGINT`, `DATETIME`, etc.). | Dynamic DDL generation. |
| [`_detect_schema_evolution()`](../../gold/script/gold_layer_manager.py#L2155-L2235) | L2155–L2235 | Introspects MySQL `information_schema.COLUMNS`, detects missing columns, and executes `ALTER TABLE ... ADD COLUMN`. | MySQL schema evolution. |
| [`_log_mysql_schema_introspection()`](../../gold/script/gold_layer_manager.py#L2237-L2255) | L2237–L2255 | Logs MySQL column names and native database types. | Schema verification. |
| [`_write_staging_table()`](../../gold/script/gold_layer_manager.py#L2257-L2294) | L2257–L2294 | Writes PySpark DataFrame to MySQL staging table via JDBC using connection pooling and batch commits. | Called in atomic swap workflow. |
| [`_table_exists()`](../../gold/script/gold_layer_manager.py#L2296-L2305) | L2296–L2305 | Queries MySQL `information_schema.TABLES` to determine if table exists. | Pre-swap validation. |
| [`_execute_isolated_atomic_swap()`](../../gold/script/gold_layer_manager.py#L2307-L2366) | L2307–L2366 | **Zero-Downtime Swap**: Atomically renames `target -> target_old, staging -> target`, then drops `target_old`. | Called when `mode == 'atomic_swap'`. |
| [`_ensure_mysql_index()`](../../gold/script/gold_layer_manager.py#L2368-L2397) | L2368–L2397 | Dynamically creates B-Tree indexes on natural keys in MySQL if not already present. | Post-swap optimization. |
| [`_replace_mysql_records()`](../../gold/script/gold_layer_manager.py#L2399-L2424) | L2399–L2424 | Performs `REPLACE INTO` statements for full-partition replacement in MySQL. | MySQL full refresh serving. |
| [`_upsert_mysql_table()`](../../gold/script/gold_layer_manager.py#L2426-L2479) | L2426–L2479 | Executes `INSERT INTO ... ON DUPLICATE KEY UPDATE` across natural keys for incremental delta serving. | Incremental serving. |
| [`_build_mysql_ssl_context()`](../../gold/script/gold_layer_manager.py#L2481-L2491) | L2481–L2491 | Constructs TLS/SSL context for secure PyMySQL connections to Aurora MySQL. | Database connection layer. |
| [`_get_mysql_connection()`](../../gold/script/gold_layer_manager.py#L2493-L2528) | L2493–L2528 | Manages PyMySQL database connection acquisition with connection retry and credentials resolution. | Database connection pool. |
| [`_execute_sql_query()`](../../gold/script/gold_layer_manager.py#L2530-L2545) | L2530–L2545 | Executes parameterized SELECT queries in MySQL and returns result tuples. | Introspection queries. |
| [`_execute_ddl()`](../../gold/script/gold_layer_manager.py#L2547-L2565) | L2547–L2565 | Executes DDL statements (`CREATE TABLE`, `RENAME TABLE`, `ALTER TABLE`) in MySQL. | Schema management & atomic swaps. |
| [`_extract_sql_annotations()`](../../gold/script/gold_layer_manager.py#L2567-L2585) | L2567–L2585 | Parses `-- @primary_key`, `-- @materialize`, and other custom annotations from SQL query files. | Query metadata extraction. |
| [`_apply_custom_transform()`](../../gold/script/gold_layer_manager.py#L2587-L2814) | L2587–L2814 | Dynamically loads and runs custom Python transformation scripts defined in `custom_transforms/`, injecting operational context (`is_initial_load=False`, `external_columns`, `api_secret_name`). Multi-tier search: configured path $\to$ local directories (with singular/plural tolerance) $\to$ `importlib.util.find_spec` in `sys.path` $\to$ S3 Data Lake download fallback to `/tmp/custom_transforms/`. | Custom mart transformation. |
| [`_discover_queries()`](../../gold/script/gold_layer_manager.py#L2816-L2880) | L2816–L2880 | Scans S3 or local SQL directories to locate all `.sql` query definition files. | Discovery phase. |
| [`_strip_sql_comments()`](../../gold/script/gold_layer_manager.py#L2882-L2889) | L2882–L2889 | Strips line (`--`) and block (`/* ... */`) comments for accurate dependency token scanning. | Token parsing. |
| [`_check_mart_dependency()`](../../gold/script/gold_layer_manager.py#L2891-L2938) | L2891–L2938 | Determines whether a query has cross-mart dependencies on other Gold tables. | DAG cycle detection. |
| [`_sort_queries_by_dependency()`](../../gold/script/gold_layer_manager.py#L2940-L2964) | L2940–L2964 | Builds a directed graph of mart queries and performs topological sorting to determine execution order. | DAG ordering. |
| [`_resolve_query_dependencies()`](../../gold/script/gold_layer_manager.py#L2966-L3024) | L2966–L3024 | Recursively resolves upstream prerequisite queries needed for a target query. | Selective query execution. |
| [`_ensure_dependency_views_registered()`](../../gold/script/gold_layer_manager.py#L3026-L3109) | L3026–L3109 | Introspects SQL tokens and pre-registers Spark temp views from Bronze (`raw_tbl_*`), Silver (`tbl_*`), or Gold (`gold_*`). | Cross-layer query preparation. |
| [`_resolve_mysql_connection_info()`](../../gold/script/gold_layer_manager.py#L3111-L3275) | L3111–L3275 | Resolves host, port, user, and password for MySQL from AWS Secrets Manager using standard and fallback key names. | Database connection resolution. |
| [`_format_error_diagnostic_card()`](../../gold/script/gold_layer_manager.py#L3277-L3312) | L3277–L3312 | Formats rich ANSI diagnostic summary cards in CloudWatch when a query or serving operation fails. | Error handling and telemetry. |
| [`_log_final_gold_summary()`](../../gold/script/gold_layer_manager.py#L3314-L3350) | L3314–L3350 | Prints tabular execution summary showing table name, rows processed, duration, and status. | Job completion summary. |

### Embedded Standalone Engine: [`GoldInitialLoader`](../../gold/script/gold_layer_manager.py#L3352-L4162) (Lines 3352–4162)

Embedded copy within `gold_layer_manager.py` allowing zero-dependency execution as a monolithic AWS Glue script:
- [`sanitize_column_name()`](../../gold/script/gold_layer_manager.py#L3359-L3370) (L3359–L3370)
- [`_is_timestamp_column()`](../../gold/script/gold_layer_manager.py#L3372-L3438) (L3372–L3438)
- [`_conform_timestamp_to_string()`](../../gold/script/gold_layer_manager.py#L3440-L3520) (L3440–L3520)
- [`reconcile_schema()`](../../gold/script/gold_layer_manager.py#L3523-L3591) (L3523–L3591)
- [`_probe_target_schema_from_query()`](../../gold/script/gold_layer_manager.py#L3594-L3647) (L3594–L3647)
- [`_purge_s3_prefix()`](../../gold/script/gold_layer_manager.py#L3649-L3652) (L3649–L3652)
- [`_get_protegrity_encryption_manager()`](../../gold/script/gold_layer_manager.py#L3654-L3679) (L3654–L3679)
- [`run_initial_load()`](../../gold/script/gold_layer_manager.py#L3681-L4162) (L3681–L4162)

### Standalone Glue Entrypoints

| Function / Component | Line Range | Architectural Purpose |
| :--- | :--- | :--- |
| [`parse_spark_arguments()`](../../gold/script/gold_layer_manager.py#L4164-L4215) | L4164–L4215 | Standalone CLI & Glue argument parser resolving environment, S3 paths, and mandatory Gold parameters. |
| [`main()`](../../gold/script/gold_layer_manager.py#L4217-L4306) | L4217–L4306 | Standalone AWS Glue Job entrypoint (`GlueContext`, `Job.init`, `Job.commit`). |

---

## 4. Code Component Map: `protegrity_encryption.py`

### Class: [`ProtegrityEncryptionManager`](../../gold/script/protegrity_encryption.py#L36-L567) (Lines 36–567)

| Function / Component | Line Range | Description & Architectural Purpose |
| :--- | :--- | :--- |
| [`__init__()`](../../gold/script/protegrity_encryption.py#L41-L77) | L41–L77 | Initializes from pipeline configuration. Resolves `lambda_arn`, `batch_size`, `default_data_element`, `max_retries`, and AWS region. Lambda ARN is validated lazily only when encryption is triggered for table columns. |
| [`_extract_region_from_arn()`](../../gold/script/protegrity_encryption.py#L80-L84) | L80–L84 | Parses AWS region from Lambda ARN string (e.g. `us-east-1`). |
| [`_clean_key()`](../../gold/script/protegrity_encryption.py#L86-L89) | L86–L89 | **Static method** — cleanses and normalizes column names: `re.sub(r'[^a-zA-Z0-9_]', '', re.sub(r'[\s\.\-]+', '_', str(k).strip())).lower()`. Shared identically with Bronze layer. |
| [`lambda_client()`](../../gold/script/protegrity_encryption.py#L91-L95) | L91–L95 | Returns thread-safe, cached boto3 Lambda client for API invocations. |
| [`_resolve_user_secret()`](../../gold/script/protegrity_encryption.py#L97-L130) | L97–L130 | Resolves Protegrity user identity secret from direct config or AWS Secrets Manager. |
| [`normalize_columns_config()`](../../gold/script/protegrity_encryption.py#L132-L178) | L132–L178 | Standardizes column encryption configuration mappings to `{clean_col: data_element}` format. |
| [`_invoke_protector_lambda()`](../../gold/script/protegrity_encryption.py#L180-L255) | L180–L255 | Invokes Protegrity Protector Lambda API (`/v2/protect`) with randomized exponential jitter backoff. |
| [`_validate_lambda_arn()`](../../gold/script/protegrity_encryption.py#L257-L279) | L257–L279 | Validates Lambda ARN format lazily when encryption is actively requested for columns. |
| [`encrypt_record_chunk()`](../../gold/script/protegrity_encryption.py#L281-L373) | L281–L373 | Encrypts in-memory Python dictionaries (Bronze layer). Packages PII columns into synchronous Lambda invocations. |
| [`encrypt_spark_dataframe()`](../../gold/script/protegrity_encryption.py#L375-L567) | L375–L567 | Encrypts PySpark DataFrames (Gold layer). Uses `mapInPandas` for distributed micro-batch processing across Spark workers. |

---

## 5. Configuration Engine: `gold_config_loader.py`

### Class: [`GoldConfigLoader`](../../gold/script/gold_config_loader.py#L18-L683) (Lines 18–683)

| Method | Line Range | Architectural Purpose |
| :--- | :--- | :--- |
| [`_clean_table_name()`](../../gold/script/gold_config_loader.py#L28-L42) | L28–L42 | Strips table prefixes (e.g. `gold_`, `gold_<source>_`, `tbl_`) to standardize config lookups. |
| [`clear_cache()`](../../gold/script/gold_config_loader.py#L44-L53) | L44–L53 | Clears in-memory configuration cache across environments. |
| [`set_loaded_config()`](../../gold/script/gold_config_loader.py#L55-L62) | L55–L62 | Explicitly injects a loaded config dictionary for a given environment into the cache. |
| [`interpolate_env()`](../../gold/script/gold_config_loader.py#L64-L83) | L64–L83 | Recursively traverses nested dicts and strings, replacing `{env}` with current environment (`dev`, `stage`, `prod`). |
| [`load_config()`](../../gold/script/gold_config_loader.py#L85-L297) | L85–L297 | Loads config from S3 (`s3://.../gold_config.json`) or local disk, caches in memory, and validates required keys. |
| [`get_defaults()`](../../gold/script/gold_config_loader.py#L299-L303) | L299–L303 | Returns `pipeline_defaults` block. |
| [`get_glue_database()`](../../gold/script/gold_config_loader.py#L305-L319) | L305–L319 | Returns target AWS Glue database name for Gold layer (`uax_datalake_db_{env}`). |
| [`get_source_config()`](../../gold/script/gold_config_loader.py#L321-L335) | L321–L335 | Returns configuration block for a specific Gold mart / source system. |
| [`get_table_config()`](../../gold/script/gold_config_loader.py#L337-L372) | L337–L372 | Resolves table-level configuration merged with parent defaults. |
| [`get_primary_key()`](../../gold/script/gold_config_loader.py#L374-L394) | L374–L394 | Resolves natural keys / primary keys for deduplication and upserts. |
| [`is_incremental()`](../../gold/script/gold_config_loader.py#L396-L412) | L396–L412 | Returns whether table is configured for incremental delta loads vs full rebuild. |
| [`get_external_columns()`](../../gold/script/gold_config_loader.py#L414-L438) | L414–L438 | Resolves list of configured external / enrichment columns (e.g., `["conversation_summary"]`) for custom transforms and precedence resolution. |
| [`get_llm_column()`](../../gold/script/gold_config_loader.py#L440-L451) | L440–L451 | Backward-compatibility helper returning the primary external column if configured. |
| [`get_secret_name()`](../../gold/script/gold_config_loader.py#L453-L479) | L453–L479 | Resolves AWS Secrets Manager secret name for database credentials. |
| [`get_api_secret_name()`](../../gold/script/gold_config_loader.py#L481-L510) | L481–L510 | Resolves AWS Secrets Manager secret name for external APIs / LLM integrations. |
| [`get_target_engines()`](../../gold/script/gold_config_loader.py#L512-L549) | L512–L549 | Returns list of enabled serving engines (`athena`, `aurora`). |
| [`get_initial_load_config()`](../../gold/script/gold_config_loader.py#L551-L595) | L551–L595 | Resolves cold-start configuration: S3 input path, delimiter, header flag, threshold. |
| [`get_custom_transform_path()`](../../gold/script/gold_config_loader.py#L597-L655) | L597–L655 | Resolves path to custom Python transform script if configured or discovers local convention file. |
| [`get_target_table_name()`](../../gold/script/gold_config_loader.py#L657-L683) | L657–L683 | Generates target Gold table name (`gold_<source>_<basetablename>`). Physical tables only. |

---

## 6. Custom Transform Extension Architecture & Sample Scripts

The Gold Layer features a decoupled, pluggable architecture for custom data enrichments (e.g. LLM summarizations, sentiment scoring, external REST API lookups) invoked during both **cold-start initial load** and **incremental pipeline runs**.

### Transform Hook Signature

Every custom transform script must expose a `transform` function:

```python
def transform(
    df: DataFrame,
    spark: Optional[SparkSession] = None,
    context: Optional[Dict[str, Any]] = None
) -> DataFrame:
    """
    Args:
        df: PySpark DataFrame (either historical initial load data or incremental SQL mart output).
        spark: Active SparkSession instance.
        context: Execution metadata dictionary containing:
            - source_system (str): Source identifier (e.g. 'genesys', 'moveworks').
            - table_name (str): Mart name (e.g. 'conversations', 'interactions').
            - is_initial_load (bool): True if cold-start bootstrap; False if incremental run.
            - external_columns (List[str]): Configured enrichment columns (e.g. ['conversation_summary']).
            - llm_column (str): Backward-compatible single external column name.
            - env (str): Current environment ('dev', 'stage', 'prod').
            - api_secret_name (str): Secrets Manager secret containing external API keys.
            - logger (logging.Logger): Active pipeline logger.
    Returns:
        DataFrame: Enriched PySpark DataFrame.
    """
```

### Four-Tier Dynamic Resolution Strategy

When executing a custom transform, `GoldLayerManager` and `GoldInitialLoader` resolve the script using a robust four-tier lookup strategy:

1. **Configured Path**: Explicit `custom_transform_path` defined in `gold_config.json`.
2. **Convention-Based Local Search**: Automatic discovery of `<source_system>_<table_name>.py` and `<table_name>.py` across candidate root directories (`custom_transforms/`, `custom_transform/`), with singular/plural system tolerance (`movework` $\leftrightarrow$ `moveworks`, `genesys` $\leftrightarrow$ `genesyss`).
3. **Python Module Spec Lookup**: `importlib.util.find_spec` across `sys.path` to locate packaged wheels or zip archives submitted via AWS Glue `--extra-py-files`.
4. **Amazon S3 Fallback Download**: If running on an ephemeral AWS Glue worker without local repository checkouts, the orchestrator automatically downloads the script from `s3://{data_lake_bucket}/gold/script/custom_transforms/<filename>` into `/tmp/custom_transforms/`.

### Strict Column Conflict Resolution & Deduplication Precedence

To guarantee deterministic schemas and prevent duplicate column collisions between Spark SQL projections, configuration defaults, and custom script enrichments, the pipeline applies a strict Precedence Hierarchy:

$$\mathbf{Custom\ Transform\ (df.withColumn)} > \mathbf{SQL\ Query\ Column} > \mathbf{Config\ Default\ (external\_columns\ NULL)}$$

- **Implementation**: [`_resolve_and_deduplicate_columns()`](../../gold/script/gold_layer_manager.py#L1880-L1945) in `gold_layer_manager.py` and [`_resolve_and_deduplicate_columns()`](../../gold/script/gold_initial_load.py#L698-L759) in `gold_initial_load.py`.
- **Deduplication**: Case-insensitive column resolution ensures duplicate column aliases are eliminated, custom transform outputs take absolute precedence, and technical audit columns (`_updated_at`, `_inserted_at`) and natural keys remain deterministically anchored.

### Sample Extension Inventory

- [`gold/script/custom_transforms/genesys_conversations.py`](../../gold/script/custom_transforms/genesys_conversations.py): Reference implementation for Genesys contact center conversations. Demonstrates batch API calls, token authorization, partition iteration, and error handling for `conversation_summary` and `sentiment_score`.
- [`gold/script/custom_transforms/moveworks_interactions.py`](../../gold/script/custom_transforms/moveworks_interactions.py): Reference implementation for Moveworks AI chatbot interactions. Demonstrates partition micro-batching, tokenized HTTP payload packaging, and error-tolerant ticket resolution summarization.

---

## 7. Glue Job Arguments Reference

| Glue Parameter | Resolution Logic | Fallback | Description |
| :--- | :--- | :--- | :--- |
| `--JOB_NAME` | Mandatory CLI | None | Unique name of the AWS Glue job run. |
| `--SOURCE_SYSTEM` | Mandatory CLI | None | Target source system identifier (`genesys`, `moveworks`, `servicenow`). |
| `--ENV` | CLI / Fallback | `dev` | Target environment (`dev`, `stage`, `prod`). |
| `--GOLD_CONFIG_S3_PATH` | CLI / Fallback | `s3://{bucket}/gold/script/config/gold_config.json` | Path to Gold pipeline configuration JSON. |
| `--TABLE_NAME` / `--MART_NAME` | CLI / Fallback | All source tables | Filters execution to a single mart or runs all discovered queries. |
| `--GOLD_SCHEMA` / `--RDS_GOLD_SCHEMA` | CLI / Config | `enterprise_reporting` | Target MySQL schema name. Mandatory per zero-fallback policy. |
| `--RDS_SECRET_NAME` | CLI / Config | `source_systems.<source>.secret_name` | AWS Secrets Manager secret storing MySQL credentials. |
| `--API_SECRET_NAME` / `--LLM_SECRET_NAME` | CLI / Config | `source_systems.<source>.api_secret_name` | AWS Secrets Manager secret storing external API / LLM credentials. |
| `--DATA_LAKE_BUCKET` / `--GOLD_BUCKET` | CLI / Config | `uax-datalake-{env}-bucket` | S3 bucket where Gold Iceberg data files reside. |
| `--GLUE_DATABASE` | CLI / Config | `uax_datalake_db_{env}` | AWS Glue catalog database for Gold tables. |
| `--FULL_REFRESH` | CLI / Flag | `false` | Forces full re-aggregation from Silver tables (`true`/`false`). |
| `--INCREMENTAL` | CLI / Flag | `true` | Toggles incremental watermark filtering (`true`/`false`). |
| `--RELOAD_INITIAL` | CLI / Flag | `false` | Forces re-ingestion of historical initial CSV export. |
| `--SKIP_INITIAL_LOAD` | CLI / Flag | `false` | Bypasses cold-start check even if target Iceberg table does not exist. |
| `--CSV_PATH` / `--INITIAL_LOAD_PATH` | CLI / Config | `initial_load.s3_path` | S3 URI for cold-start historical CSV/Parquet dataset. |
| `--DELIMITER` | CLI / Config | `,` | Delimiter for historical CSV file. |
| `--HAS_HEADER` | CLI / Config | `true` | Indicates whether historical CSV file has a header row. |
| `--ENABLE_ENCRYPTION` | CLI / Config | `false` | Toggles Protegrity PII tokenization on cold-start data. |
| `--GOLD_TARGETS` / `--TARGET_ENGINES` | CLI / Config | `aurora` | Comma-delimited list of target serving engines (`athena`, `aurora`). |
| `--AWS_REGION` | CLI / Fallback | `us-east-1` | AWS region override. |
