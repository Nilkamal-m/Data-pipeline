# AWS Glue & Athena Helper Lambda (`lambda_function.py`)

The Helper Lambda function provides a unified control plane and execution interface to:
1. **Query Amazon Athena directly**: Run SQL queries against the AWS Glue Data Catalog (`uax_datalake_db_dev`) or Apache Iceberg tables and inspect formatted tabular results directly in CloudWatch / Lambda execution logs.
2. **Trigger and Monitor Single AWS Glue Jobs**: Trigger and synchronously/asynchronously monitor:
   - **Bronze Ingestion** (REST API, Flat File S3, or JDBC Database Ingestion)
   - **Silver Iceberg ETL** (PySpark Iceberg Deduplication & Merging)
   - **Gold Serving Marts** (Analytical SQL marts & shared Aurora MySQL publishing)
3. **Orchestrate End-to-End Multi-Stage Pipelines ("Run All")**: Sequentially execute multi-stage pipelines (`Bronze -> Silver -> Gold`, `Silver -> Gold`, or custom stage lists) in a single invocation, polling each stage to completion and halting immediately if an error occurs.
4. **Trigger AWS Step Functions State Machines**: Kick off enterprise-grade automated pipeline state machines (`action: "stepfunction"`) with zero mandatory payload dependencies.
5. **Trigger AWS Glue Workflows**: Trigger and monitor multi-job DAG workflows (`action: "workflow"`) passing dynamic execution properties.
6. **Data & Metadata Maintenance**:
   - **Watermark NDJSON Sanitization (`action: "sanitize_watermarks"`)**: Scans S3 `watermark.json` files and rewrites multi-line JSON into single-line NDJSON format, preventing Athena Presto/Trino from returning `NULL` columns.
   - **Parquet Column Sanitization (`action: "delete_from_parquet"`)**: Deletes duplicate or deprecated columns (e.g. `_ingested_at`) directly from physical Parquet files in S3 and cleans the Glue Catalog schema.

---

## 1. Supported Operations Summary

| Operation | Trigger Indicator | Target Engine | Purpose |
| :--- | :--- | :--- | :--- |
| **Bronze Ingestion** | `"layer": "bronze"` | Glue Python Shell 3.9 | Ingests raw source data from APIs/databases/S3 into S3 Bronze partitioned by `_ingested_at=<ISO_TIMESTAMP>`. |
| **Silver Iceberg ETL** | `"layer": "silver"` | Glue PySpark 4.0 | Deduplicates Bronze raw data, applies audit columns, and merges into Iceberg tables (`tbl_<name>`). |
| **Gold Serving Marts** | `"layer": "gold"` | Glue PySpark 4.0 | Runs analytical SQL marts (`bucket/gold/query/<source>/*.sql`) and publishes to shared Aurora MySQL with atomic swap. |
| **End-to-End Pipeline** | `"layer": "all"` or `"layers": [...]` | Multi-Stage Sequential | Sequentially executes stages (`Bronze -> Silver -> Gold`), failing fast if any stage fails. |
| **Step Functions Orchestrator** | `"action": "stepfunction"` or `"state_machine_arn"` | AWS Step Functions | Triggers full 3-stage sequential state machine (`uax-pipeline-orchestrator-{env}`) with automated SNS alerts. |
| **Glue Workflow** | `"action": "workflow"` or `"workflow_name"` | AWS Glue Workflow | Triggers and monitors multi-job DAG workflows (`Bronze -> Silver -> Gold`) with run properties. |
| **Athena SQL Query** | `"query"`, `"sql"`, or `"query_file"` | Amazon Athena v3 | Executes multiline queries, SQL files (e.g. `interactions.sql`), or ad-hoc queries with CLI tables in logs. |
| **Watermark NDJSON Repair** | `"action": "sanitize_watermarks"` | S3 Metadata Storage | Rewrites multi-line `watermark.json` files to single-line NDJSON to resolve Athena returning NULL values. |
| **Parquet Column Deletion** | `"action": "delete_from_parquet"` or `"column_name"` | S3 / Glue Catalog | Deletes columns directly from physical Parquet files in S3 and updates Glue Catalog schema without crawlers. |
| **Catalog Maintenance** | `"action": "delete_column"` (`catalog_only: true`) | AWS Glue Data Catalog | Removes duplicate columns from `StorageDescriptor.Columns` in 2 seconds without rewriting S3 data. |

---

## 2. Event Payload Parameters Reference

### 2.1 Elimination of Glue Crawlers & Deterministic Catalog Synchronization

> [!IMPORTANT]
> **Glue Crawlers are 100% eliminated from standard production workflows**:
> - **Bronze**: Partitions and table schemas are registered directly via AWS Glue SDK (`glue:CreateTable`, `glue:CreatePartition`, `glue:UpdatePartition`) in <100ms. Newly ingested data is instantly queryable in Athena without crawler lag.
> - **Silver & Gold**: Managed natively through Apache Iceberg (`SparkCatalog`) backed by AWS Glue Catalog. Manifest files and snapshots are committed atomically per transaction.
> - **Zero Corruption**: Eliminates duplicate Hive tables created when crawlers scan Iceberg metadata directories.

---

### 2.2 Core & Common Parameters (All Glue Jobs & Pipelines)

| Parameter | Type | Applicable Layers | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `layer` | string | All | `"bronze"` | Execution mode: `"bronze"`, `"silver"`, `"gold"`, or `"all"` (runs Bronze $\rightarrow$ Silver $\rightarrow$ Gold). |
| `layers` | list | All | `None` | Custom stage list: e.g. `["silver", "gold"]` or `["bronze", "silver", "gold"]`. |
| `skip_bronze` | boolean | All | `false` | When `layer="all"`, set to `true` to execute `Silver -> Gold` only. |
| `source_system` | string | All | **Required** | Source system name (e.g. `servicenow`, `moveworks`, `genesys`, `postgresql`, `s3_file`). |
| `source_table_name` | string \| list | Bronze, Silver | Config defaults | Target table name(s). Accepts single string (`"incident"`), array (`["incident", "sys_user"]`), or comma-separated string (`"incident, sys_user"`). Aliases: `table_name`, `tables`. |
| `env` / `environment` | string | All | `dev` | Target environment (`dev`, `stage`, `prod`). Automatically interpolates `{env}` tokens in paths and secrets. |
| `job_name` | string | Bronze, Silver, Gold | Auto-resolved | Explicit Glue job name override (e.g. `uax-datalake-bronze-ingestion-dev`). |
| `wait_until_completion`| boolean | All | `true` | If `true`, polls status until completion. If `false`, returns HTTP 202 immediately. |
| `poll_interval_seconds`| integer | All | `10` | Polling interval in seconds while monitoring status. |
| `timeout_seconds` | integer | All | `540` | Maximum wait duration before Lambda exits (Default: 9 minutes). |
| `arguments` | object | All | `{}` | Arbitrary dictionary of `--KEY` overrides passed directly to the Glue job. |

---

### 2.3 Bronze Ingestion Layer Parameters (`layer: "bronze"`)

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `secret_name` | string | `bronze_config.json` | AWS Secrets Manager secret name or ARN containing source API / DB credentials (e.g. `uax-datalake/servicenow-credentials-{env}`). If omitted, resolved from `bronze_config.json`. |
| `initial_load_date` | string | `bronze_config.json` | Starting cutoff watermark timestamp (`YYYY-MM-DD HH:MM:SS`). Bypasses S3 watermark for cold starts (e.g. `1900-01-01 00:00:00`). Aliases: `initial_load_date_cli`. |
| `upper_bound` | string | `""` (now) | Optional upper bound timestamp (`YYYY-MM-DD HH:MM:SS`) to cap delta extraction for bounded backfills. |
| `batch_size` | integer | `1000` | Number of records requested per API page or DB cursor fetch. |
| `s3_chunk_size` | integer | `10000` | Number of in-memory records accumulated before flushing an S3 Parquet file chunk. |
| `custom_query` | string | `""` | Ad-hoc query override or delta filter expression passed directly to the connector. |
| `base_url` | string | `bronze_config.json` | API base URL override (e.g. `https://custom-instance.service-now.com`). |
| `bronze_bucket` | string | `bronze_config.json` | Target S3 bucket for Bronze staging and partitioned raw data. |
| `state_bucket` | string | `bronze_bucket` | S3 bucket storing state metadata (`watermark.json`). |
| `config_s3_path` | string | Auto-resolved | S3 URI to `bronze_config.json`. |
| `output_format` | string | `"parquet"` | Storage file format (`parquet` or `json`). |
| `error_handling_mode` | string | `"CONTINUE_ON_ERROR"`| Extraction error policy: `CONTINUE_ON_ERROR` (skip failed tables) or `HALT_ON_ERROR`. |
| `fetch_mode` | string | `"all"` | For S3 File connector: `'all'` (all modified files) or `'latest'` (most recent file only). |

---

### 2.4 Silver Iceberg ETL Parameters (`layer: "silver"`)

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `watermark_enabled` | boolean | `true` | If `true`, incrementally filters Bronze data using the last successful watermark timestamp. |
| `full_refresh` | boolean | `false` | If `true`, scans all historical Bronze data, ignoring watermarks and recreating/updating Iceberg tables. |
| `watermark_column` | string | `"_ingested_at"` | Timestamp column used for incremental change detection. |
| `nkey` / `natural_key` | string \| list | Config defaults | Natural / business key(s) used for row deduplication and Iceberg MERGE. Aliases: `nkeys`, `primary_key`. |
| `deduplication_order_by`| string | `"_updated_at DESC"`| SQL ordering expression used by windowed `ROW_NUMBER()` deduplication. |
| `merge_strategy` | string | `"upsert"` | Iceberg write strategy (`upsert`, `append`, or `overwrite`). |
| `scd_type` | string | `"type1"` | Slowly Changing Dimension policy (`type1` for latest state, `type2` for versioning). |
| `external_columns` | string \| list | `[]` | Dynamic external columns appended to Silver table schemas (e.g. `["enrichment_score"]`). |
| `silver_config_s3_path`| string | Auto-resolved | S3 URI to `silver_config.json`. |
| `data_lake_bucket` | string | Auto-resolved | S3 bucket containing Bronze input data and Silver Iceberg warehouse. |
| `glue_database` | string | `uax_datalake_db_dev` | Glue Data Catalog database containing target Iceberg tables (`tbl_<name>`). |
| `table_prefix` | string | `"tbl_"` | Prefix for target Silver Iceberg tables in Glue Catalog. |

---

### 2.5 Gold Serving Layer Parameters (`layer: "gold"`)

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `gold_schema` | string | **Required for Aurora** | Target MySQL schema name (e.g. `enterprise_reporting`). **Strict policy: Zero fallback permitted**. Aliases: `rds_gold_schema`, `rds_schema`. |
| `rds_secret_name` | string | `gold_config.json` | AWS Secrets Manager secret name or ARN containing MySQL credentials (`host`, `port`, `username`, `password`). Alias: `db_secret`. |
| `rds_host` | string | Optional | RDS MySQL host override. Alias: `rds_url`, `host`. |
| `rds_port` | string \| int | `3306` | RDS MySQL port. Alias: `port`. |
| `rds_user` | string | Optional | RDS MySQL username override. Alias: `rds_username`, `user`. |
| `rds_password` | string | Optional | Manual MySQL password override. Alias: `password`. |
| `connection_name` | string | Config defaults | AWS Glue Connection name with JDBC / VPC requirements. Alias: `glue_connection_name`. |
| `gold_target` | string | `"both"` | Serving target engines: `"iceberg"`, `"aurora"`, or `"both"`. Alias: `gold_targets`. |
| `primary_key` | string \| list | Config defaults | Primary key(s) for Aurora MySQL table creation and Iceberg upserts. Alias: `nkey`. |
| `mart_name` | string | Optional | Specific Gold mart name to execute (e.g. `v_interactions`). Alias: `table_name`. |
| `table_list` | string \| list | Optional | Subset of Gold tables to run (comma-separated or JSON list). |
| `api_secret_name` | string | `gold_config.json` | AWS Secrets Manager secret containing external API or LLM credentials used by custom transforms. Alias: `llm_secret_name`. |
| `incremental` | boolean | `true` | If `true`, executes incremental SQL DAG processing; if `false`, executes historical cold-start. |
| `gold_query_s3_path` | string | Config defaults | S3 URI for Gold SQL query definitions (`s3://<bucket>/gold/query/<source>/`). |
| `gold_data_s3_path` | string | Config defaults | S3 URI for intermediate Parquet staging data. |
| `gold_config_s3_path` | string | Auto-resolved | S3 URI to `gold_config.json`. |
| `athena_workgroup` | string | `uax-datalake-workgroup-dev` | Target Athena workgroup for query probes and view registration. Alias: `workgroup`. |

---

### 2.6 AWS Step Functions Orchestration Parameters (`action: "stepfunction"`)

To trigger an AWS Step Functions State Machine, **only 2 parameters are required**:
1. `"action": "stepfunction"`
2. `"stepfunction_name": "uax-pipeline-orchestrator-dev"`

All pipeline configurations, task definitions, and Glue job arguments are encapsulated directly within the Step Functions state machine itself, meaning **there are zero mandatory dependencies or external pipeline arguments needed in the Lambda payload**.

| Parameter | Type | Required / Optional | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `action` | string | **Required** | - | Set to `"stepfunction"` (or `"step_function"`, `"sfn"`, `"state_machine"`). |
| `stepfunction_name` | string | **Required** | `uax-pipeline-orchestrator-dev` | Name of the Step Function State Machine (or full ARN). |
| `state_machine_arn` | string | Optional | Auto-resolved | Full ARN override of the Step Function State Machine. |
| `execution_name` | string | Optional | Auto-generated | Custom execution identifier (auto-generated as `<name>-<timestamp>-<uuid>` if omitted). |
| `wait_until_completion` | boolean | Optional | `false` | Default `false` (asynchronous fire-and-forget, returns HTTP 202). Set to `true` to synchronously poll. |
| `poll_interval_seconds` | integer | Optional | `10` | Polling interval in seconds when `wait_until_completion: true`. |
| `timeout_seconds` | integer | Optional | `540` | Polling timeout in seconds. |
| `input` / `payload` | object | Optional | Pass-through | Dictionary passed directly into the Step Function's input JSON. Any extra top-level keys are also forwarded. |

---

### 2.7 AWS Glue Workflow Parameters (`action: "workflow"`)

To trigger an AWS Glue Workflow (multi-job DAG orchestrating sequential Bronze, Silver, and Gold Glue jobs with automated SNS alerts):

| Parameter | Type | Required / Optional | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `action` / `layer` | string | Optional | - | Set to `"workflow"`, `"glue_workflow"`, or `"start_workflow"`. |
| `workflow_name` | string | **Required** | Resolved from source/env | Name of the AWS Glue Workflow (e.g. `"uax-datalake-moveworks-workflow-dev"`). |
| `run_properties` | object | Optional | `{}` | Key-value dictionary of workflow execution properties passed to all jobs in the DAG via Glue API `start_workflow_run`. |
| `wait_until_completion` | boolean | Optional | `true` | If `true`, polls until terminal status (`COMPLETED`, `STOPPED`, `ERROR`). If `false`, returns HTTP 202 immediately. |
| `poll_interval_seconds` | integer | Optional | `10` | Polling interval in seconds when monitoring synchronously. |
| `timeout_seconds` | integer | Optional | `900` | Maximum wait duration in seconds before Lambda timeout (Default: 15 minutes). |

---

### 2.8 Athena Query & Multiline SQL Parameters

If `"query"`, `"sql"`, `"athena_query"`, or `"query_file"` is provided, Lambda routes directly to Athena. Supports **multiline SQL queries**, **CTE pipelines**, **SQL file references**, and **multi-statement execution**:

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `query` / `sql` | string | Optional* | - | Multiline SQL query string (e.g. `WITH ... SELECT ...`). Trailing semicolons and leading comments are automatically stripped. |
| `query_file` / `sql_file` | string | Optional* | - | Path to a local SQL file (e.g. `gold/query/moveworks/v_interactions.sql`). Aliases: `query_path`, `sql_path`. |
| `sql_s3_path` / `query_s3_path` | string | Optional* | - | S3 URI to a SQL query (e.g. `s3://<bucket>/gold/query/moveworks/v_interactions.sql`). Alias: `s3_query_uri`. |
| `query_name` | string | Optional* | - | SQL file name looked up relative to `source_system` (e.g. `"v_interactions.sql"`). |
| `database` | string | Optional | `uax_datalake_db_dev` | Target AWS Glue Data Catalog database. Optional if query specifies `<db>.<table>`. Aliases: `db`, `glue_database`. |
| `workgroup` | string | Optional | `uax-datalake-workgroup-dev` | Amazon Athena workgroup name. Alias: `athena_workgroup`. |
| `output_location` | string | Optional | Workgroup default | S3 bucket path for query results. Alias: `s3_output`. |
| `region` | string | Optional | Default session | AWS Region for Athena client. Alias: `athena_region`. |
| `params` / `parameters` | object | Optional | `{}` | Key-value dictionary for template substitution (`${VAR}`, `{VAR}`, `<VAR>`, `:VAR`). |
| `table_replacements` | object | Optional | `{}` | Dictionary mapping table names (e.g. `{"tbl_interactions": "silver_tbl_moveworks_interactions"}`). |
| `max_results` | integer | Optional | `None` (All records) | Maximum rows to retrieve and print. Defaults to `null` to return **ALL** records. |
| `timeout_seconds` | integer | Optional | `120` | Query polling timeout in seconds before cancellation. |
| `poll_interval_seconds` | number | Optional | `1.0` | Athena status check polling interval in seconds. |

*\*At least one of `query`, `sql`, `query_file`, `sql_s3_path`, or `query_name` must be provided.*

---

### 2.9 Watermark NDJSON Sanitization & Parquet Maintenance Parameters

| Parameter | Type | Operation | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `action` | string | Both | - | Set to `"sanitize_watermarks"` (watermarks) or `"delete_from_parquet"` (Parquet files). |
| `bucket` | string | Watermarks | Auto-resolved | S3 bucket containing `metadata/bronze` and `metadata/silver` watermarks. Aliases: `state_bucket`, `data_lake_bucket`. |
| `prefixes` | list | Watermarks | `["metadata/bronze", "metadata/silver"]` | List of S3 key prefixes to scan for `watermark.json` files. |
| `column_name` | string \| list | Parquet / Catalog | `"_ingested_at"` | Column(s) to remove from Parquet files or Glue Catalog. Aliases: `columns`, `exclude_columns`, `drop_column`. |
| `table` / `table_name`| string | Parquet / Catalog | Optional | Glue Catalog table name (e.g. `raw_tbl_interactions`). |
| `s3_path` / `s3_uri` | string | Parquet | Auto-resolved | Direct S3 path to Parquet directory (e.g. `s3://bucket/bronze/data/servicenow/incident/`). |
| `catalog_only` | boolean | Catalog | `false` | If `true`, only removes column from Glue Catalog schema, skipping physical S3 file rewrites. |

---

## 3. Sample Event Payloads

### A. Bronze Layer Ingestion Payloads (`layer: "bronze"`)

#### 1. Single Table Ingestion with Default Config
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "wait_until_completion": true
}
```

#### 2. Pass Explicit `secret_name` from Payload (Secrets Manager Override)
Overrides configuration to dynamically read credentials from a designated AWS Secrets Manager secret:
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "secret_name": "uax-datalake/servicenow-credentials-dev",
  "wait_until_completion": true
}
```

#### 3. Cold Start / Full Historical Bootstrapping (`initial_load_date: "1900-01-01"`)
Extracts 100% of historical records using cursor pagination:
```json
{
  "layer": "bronze",
  "source_system": "moveworks",
  "source_table_name": "interactions",
  "initial_load_date": "1900-01-01 00:00:00",
  "wait_until_completion": true
}
```

#### 4. Bounded Backfill with Watermark Window (`initial_load_date` + `upper_bound`)
Extracts data strictly within a bounded historical time window:
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "initial_load_date": "2026-01-01 00:00:00",
  "upper_bound": "2026-06-01 00:00:00",
  "wait_until_completion": true
}
```

#### 5. Multi-Table Ingestion Array (`["incident", "change_request", "sys_user"]`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": ["incident", "change_request", "sys_user"],
  "wait_until_completion": true
}
```

#### 6. Ingest Flat Files from S3 via S3 File Connector (`s3_file`)
```json
{
  "layer": "bronze",
  "source_system": "s3_file",
  "source_table_name": "hr_roster",
  "fetch_mode": "latest",
  "wait_until_completion": true
}
```

---

### B. Silver Layer Iceberg ETL Payloads (`layer: "silver"`)

#### 7. Incremental Silver Iceberg ETL (`raw_tbl_incident`)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "watermark_enabled": true,
  "wait_until_completion": true
}
```

#### 8. Full Historical Refresh (Ignore Watermark)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "full_refresh": true,
  "wait_until_completion": true
}
```

#### 9. Moveworks Silver Cleanse with Custom Transform (`raw_tbl_interactions`)
Applies illegal character removal and Moveworks domain/entity mapping:
```json
{
  "layer": "silver",
  "source_system": "moveworks",
  "source_table_name": "raw_tbl_interactions",
  "wait_until_completion": true
}
```

#### 10. Multi-Table Silver Batch Ingestion
```json
{
  "layer": "silver",
  "source_system": "moveworks",
  "source_table_name": ["raw_tbl_interactions", "raw_tbl_conversations", "raw_tbl_plugin_calls", "raw_tbl_users"],
  "wait_until_completion": true
}
```

---

### C. Gold Serving Mart Payloads (`layer: "gold"`)

#### 11. Trigger Gold Layer using AWS Secrets Manager DB Secret (Recommended)
Zero hardcoded credentials: AWS Glue extracts `host`, `port`, `username`, and `password` directly from Secrets Manager:
```json
{
  "layer": "gold",
  "source_system": "moveworks",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "wait_until_completion": true,
  "poll_interval_seconds": 10
}
```

#### 12. Trigger Gold Layer with Manual DB Credentials (Testing / Ad-Hoc)
```json
{
  "layer": "gold",
  "source_system": "moveworks",
  "rds_schema": "enterprise_reporting",
  "rds_host": "aurora-mysql-prod.cluster-xyz.us-east-1.rds.amazonaws.com",
  "rds_port": 3306,
  "rds_user": "reporting_user",
  "rds_password": "manual_database_password",
  "wait_until_completion": true
}
```

#### 13. Gold Serving with Dual Target (`gold_target: "both"`) & Specific Mart
Publishes to both Apache Iceberg table in S3 and Aurora MySQL table:
```json
{
  "layer": "gold",
  "source_system": "moveworks",
  "mart_name": "v_interactions",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "gold_target": "both",
  "wait_until_completion": true
}
```

#### 14. Gold Serving with External API / LLM Enrichment Secret
Passes external API credentials for custom PySpark enrichment transforms:
```json
{
  "layer": "gold",
  "source_system": "genesys",
  "gold_schema": "contact_center_analytics",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "api_secret_name": "prod/llm/anthropic_credentials",
  "wait_until_completion": true
}
```

---

### D. End-to-End Multi-Stage Pipeline Payloads ("Run All")

#### 15. Run All: Bronze $\rightarrow$ Silver $\rightarrow$ Gold Sequentially
```json
{
  "layer": "all",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "secret_name": "uax-datalake/servicenow-credentials-dev",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "poll_interval_seconds": 10,
  "timeout_seconds": 600
}
```

#### 16. Run Pipeline: Silver $\rightarrow$ Gold Only (`skip_bronze: true`)
```json
{
  "layer": "all",
  "skip_bronze": true,
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials"
}
```

#### 17. Custom Pipeline Stage Sequence (`layers: ["bronze", "silver", "gold"]`)
```json
{
  "layers": ["bronze", "silver", "gold"],
  "source_system": "moveworks",
  "source_table_name": ["interactions", "conversations"],
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials"
}
```

---

### E. AWS Step Functions State Machine Payloads (`action: "stepfunction"`)

#### 18. Minimal Pipeline Trigger (Zero External Dependencies)
Triggers the full state machine with **only these 2 parameters**. All pipeline tasks, configurations, and Glue job parameters are defined within the Step Function itself:
```json
{
  "action": "stepfunction",
  "stepfunction_name": "uax-pipeline-orchestrator-dev"
}
```

#### 19. Trigger Step Function for a Single Target Table Override
```json
{
  "action": "stepfunction",
  "stepfunction_name": "uax-pipeline-orchestrator-dev",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "env": "dev"
}
```

#### 20. Synchronous Step Function Polling (Wait for Completion)
```json
{
  "action": "stepfunction",
  "stepfunction_name": "uax-pipeline-orchestrator-dev",
  "source_system": "moveworks",
  "env": "dev",
  "wait_until_completion": true,
  "poll_interval_seconds": 15,
  "timeout_seconds": 540
}
```

---

### F. AWS Glue Workflow DAG Payloads (`action: "workflow"`)

#### 21. Trigger Moveworks Production Workflow
```json
{
  "action": "workflow",
  "workflow_name": "uax-datalake-moveworks-workflow-dev",
  "run_properties": {
    "ENV": "dev",
    "SOURCE_SYSTEM": "moveworks"
  }
}
```

#### 22. Trigger ServiceNow Ingestion & Transformation Workflow
```json
{
  "action": "workflow",
  "workflow_name": "uax-datalake-servicenow-workflow-dev",
  "run_properties": {
    "ENV": "dev",
    "SOURCE_SYSTEM": "servicenow"
  }
}
```

---

### G. Athena Multiline SQL & File Query Payloads (`"query"` / `"query_file"`)

#### 23. Execute Moveworks Gold Query File with Table Replacements
```json
{
  "query_file": "gold/query/moveworks/v_interactions.sql",
  "database": "uax_datalake_db_dev",
  "table_replacements": {
    "tbl_interactions": "silver_tbl_moveworks_interactions",
    "tbl_conversations": "silver_tbl_moveworks_conversations",
    "tbl_plugin_calls": "silver_tbl_moveworks_plugin_calls",
    "tbl_plugin_resources": "silver_tbl_moveworks_plugin_resources",
    "tbl_users": "silver_tbl_moveworks_users"
  },
  "max_results": 20
}
```

#### 24. Execute SQL Directly from S3 URI
```json
{
  "sql_s3_path": "s3://uax-datalake-dev-bucket/gold/query/moveworks/v_interactions.sql",
  "database": "uax_datalake_db_dev",
  "table_replacements": {
    "tbl_interactions": "silver_tbl_moveworks_interactions",
    "tbl_conversations": "silver_tbl_moveworks_conversations",
    "tbl_plugin_calls": "silver_tbl_moveworks_plugin_calls",
    "tbl_plugin_resources": "silver_tbl_moveworks_plugin_resources",
    "tbl_users": "silver_tbl_moveworks_users"
  },
  "max_results": 10
}
```

#### 25. Execute Multiline CTE Query with Active Record Filtering
```json
{
  "query": "WITH conversation_topics AS (\n    SELECT conversation_id, ARRAY_JOIN(ARRAY_AGG(DISTINCT detail_entity), ', ') AS conversation_topic\n    FROM raw_tbl_interactions\n    WHERE _is_current = 'Y' AND _is_deleted = 'N' AND detail_entity IS NOT NULL\n    GROUP BY conversation_id\n)\nSELECT ui.id, ui.detail_content, ct.conversation_topic\nFROM raw_tbl_interactions ui\nLEFT JOIN conversation_topics ct ON ui.conversation_id = ct.conversation_id\nWHERE ui.actor = 'user' AND ui._is_current = 'Y' AND ui._is_deleted = 'N'\nLIMIT 10;",
  "database": "uax_datalake_db_dev",
  "max_results": 10
}
```

#### 26. Inspect Available Data Catalog Tables
```json
{
  "query": "SHOW TABLES IN uax_datalake_db_dev"
}
```

#### 27. Inspect High-Watermark Progress Table (`raw_tbl_watermarks`)
```json
{
  "query": "SELECT source_system, table_name, last_load_date, last_status, records_ingested, updated_at FROM raw_tbl_watermarks ORDER BY updated_at DESC",
  "database": "uax_datalake_db_dev"
}
```

---

### H. Watermark Repair & Parquet Sanitization Payloads

#### 28. Repair Multi-Line Watermark JSON into Single-Line NDJSON (Fix Athena NULLs)
> **Use Case**: Fixes the issue where Athena Presto/Trino returns `NULL` values when querying watermarks by re-serializing pretty-printed JSON into single-line NDJSON format.
```json
{
  "action": "sanitize_watermarks",
  "bucket": "uax-datalake-dev-bucket",
  "prefixes": ["metadata/bronze", "metadata/silver"]
}
```

#### 29. In-Place Parquet Column Deletion (Drop `_ingested_at` from S3 files)
> **Use Case**: Reads Parquet files in S3, drops specified column(s) from the schema, and rewrites the files in-place with snappy compression.
```json
{
  "action": "delete_from_parquet",
  "s3_path": "s3://uax-datalake-bronze-dev-bucket/bronze/data/moveworks/interactions/",
  "column_name": "_ingested_at"
}
```

#### 30. Fast Catalog-Only Fix (Zero S3 I/O)
> **Use Case**: Only removes the column from the AWS Glue Data Catalog table schema without touching physical S3 files.
```json
{
  "action": "delete_column",
  "database": "uax_datalake_db_dev",
  "table": "raw_tbl_interactions",
  "column_name": "_ingested_at",
  "catalog_only": true
}
```

---

## 4. Response Payload Formats

### A. Successful Single Glue Job Run (HTTP 200)
```json
{
  "statusCode": 200,
  "body": {
    "layer": "bronze",
    "job_name": "uax-datalake-bronze-ingestion-dev",
    "job_run_id": "jr_01a2b3c4d5e6f7",
    "status": "SUCCEEDED",
    "execution_time_seconds": 45,
    "arguments": {
      "--SOURCE_SYSTEM": "servicenow",
      "--SOURCE_TABLE_NAME": "incident",
      "--SECRET_NAME": "uax-datalake/servicenow-credentials-dev"
    },
    "log_group": "/aws-glue/jobs/output"
  }
}
```

### B. Asynchronous Job Trigger (HTTP 202 Accepted)
When `"wait_until_completion": false`:
```json
{
  "statusCode": 202,
  "body": {
    "message": "Glue job (BRONZE) started asynchronously.",
    "layer": "bronze",
    "job_name": "uax-datalake-bronze-ingestion-dev",
    "job_run_id": "jr_01a2b3c4d5e6f7",
    "status": "STARTING",
    "arguments": {
      "--SOURCE_SYSTEM": "servicenow",
      "--SOURCE_TABLE_NAME": "incident"
    }
  }
}
```

### C. Successful Multi-Stage Pipeline Execution (HTTP 200)
```json
{
  "statusCode": 200,
  "body": {
    "status": "SUCCEEDED",
    "pipeline": "bronze -> silver -> gold",
    "source_system": "servicenow",
    "total_duration_seconds": 95,
    "stage_results": {
      "bronze": {
        "job_name": "uax-datalake-bronze-ingestion-dev",
        "job_run_id": "jr_bronze_001",
        "status": "SUCCEEDED",
        "execution_time_seconds": 30,
        "log_group": "/aws-glue/jobs/output"
      },
      "silver": {
        "job_name": "uax-datalake-silver-etl-dev",
        "job_run_id": "jr_silver_002",
        "status": "SUCCEEDED",
        "execution_time_seconds": 42,
        "log_group": "/aws-glue/jobs/output"
      },
      "gold": {
        "job_name": "uax-datalake-gold-marts-dev",
        "job_run_id": "jr_gold_003",
        "status": "SUCCEEDED",
        "execution_time_seconds": 23,
        "log_group": "/aws-glue/jobs/output"
      }
    }
  }
}
```

### D. Failed Multi-Stage Pipeline Execution (HTTP 500 — Fail-Fast)
```json
{
  "statusCode": 500,
  "body": {
    "status": "FAILED",
    "failed_stage": "silver",
    "error_message": "Iceberg commit failed: Concurrent update detected",
    "total_duration_seconds": 52,
    "stage_results": {
      "bronze": {
        "job_name": "uax-datalake-bronze-ingestion-dev",
        "job_run_id": "jr_bronze_001",
        "status": "SUCCEEDED",
        "execution_time_seconds": 30
      },
      "silver": {
        "job_name": "uax-datalake-silver-etl-dev",
        "job_run_id": "jr_silver_002",
        "status": "FAILED",
        "execution_time_seconds": 22,
        "error_message": "Iceberg commit failed: Concurrent update detected"
      }
    }
  }
}
```

### E. Successful Athena Query Execution (HTTP 200 & CloudWatch Log Output)

**Response Body:**
```json
{
  "statusCode": 200,
  "body": {
    "query_execution_id": "f5a7b8c9-1234-5678-9abc-def012345678",
    "status": "SUCCEEDED",
    "query": "SELECT incident_number, priority, state FROM tbl_incident WHERE _is_current = 'Y' LIMIT 2",
    "database": "uax_datalake_db_dev",
    "workgroup": "uax-datalake-workgroup-dev",
    "execution_time_ms": 1150,
    "data_scanned_bytes": 524288,
    "columns": ["incident_number", "priority", "state"],
    "row_count": 2,
    "records": [
      {"incident_number": "INC0000001", "priority": "1", "state": "In Progress"},
      {"incident_number": "INC0000002", "priority": "2", "state": "Closed"}
    ]
  }
}
```

**Formatted CloudWatch Log Output:**
```
================================================================================
ATHENA QUERY EXECUTION RESULT SUMMARY
Query           : SELECT incident_number, priority, state FROM tbl_incident WHERE _is_current = 'Y' LIMIT 2
Database        : uax_datalake_db_dev
WorkGroup       : uax-datalake-workgroup-dev
Execution ID    : f5a7b8c9-1234-5678-9abc-def012345678
Engine Time     : 1150 ms (1.15 s)
Data Scanned    : 524,288 bytes (0.5000 MB)
Rows Returned   : 2 (capped at 10)
--------------------------------------------------------------------------------
PANDAS DATAFRAME VIEW:
   incident_number  priority  state
1  INC0000001       1         In Progress
2  INC0000002       2         Closed
DataFrame Shape: 2 rows x 3 columns
--------------------------------------------------------------------------------
DATABASE TABLE VIEW:
+---+-----------------+----------+-------------+
| # | incident_number | priority | state       |
+---+-----------------+----------+-------------+
| 1 | INC0000001       | 1        | In Progress |
| 2 | INC0000002       | 2        | Closed      |
+---+-----------------+----------+-------------+
(2 rows in set)
================================================================================
```

### F. Watermark NDJSON Sanitization Result (HTTP 200)
```json
{
  "statusCode": 200,
  "body": {
    "status": "SUCCEEDED",
    "bucket": "uax-datalake-dev-bucket",
    "total_scanned": 12,
    "total_repaired": 4,
    "prefix_details": {
      "metadata/bronze": {
        "scanned": 6,
        "repaired_to_single_line": 2
      },
      "metadata/silver": {
        "scanned": 6,
        "repaired_to_single_line": 2
      }
    }
  }
}
```

---

## 5. Testing in the AWS Lambda Console

1. Open **AWS Lambda Console** &rarr; Functions &rarr; Select `uax-datalake-glue-job-trigger-dev`.
2. Navigate to the **Test** tab.
3. Paste any test payload from **Section 3** into the Event JSON editor:
   - For Bronze ingestion with `secret_name`: Paste payload **#2**.
   - For bounded delta backfill: Paste payload **#4**.
   - For Silver Iceberg ETL: Paste payload **#7** or **#8**.
   - For Gold serving with Secrets Manager: Paste payload **#11**.
   - For sequential multi-stage pipeline: Paste payload **#15** through **#17**.
   - For Step Functions State Machine: Paste payload **#18** through **#20**.
   - For Glue Workflows: Paste payload **#21** or **#22**.
   - For Athena SQL queries: Paste payload **#23** through **#27**.
   - For Watermark NDJSON repair: Paste payload **#28**.
   - For S3 Parquet column deletion: Paste payload **#29**.
4. Click **Test**.
5. Inspect the execution logs and JSON output card directly in the console.
