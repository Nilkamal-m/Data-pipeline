# AWS Glue & Athena Helper Lambda (`lambda_function.py`)

The Helper Lambda function provides a unified control plane and execution interface to:
1. **Query Amazon Athena directly**: Run SQL queries against the AWS Glue Data Catalog (`uax_datalake_db_dev`) or Apache Iceberg tables and inspect formatted tabular results directly in CloudWatch / Lambda execution logs.
2. **Trigger and Monitor Single AWS Glue Jobs**: Trigger and synchronously/asynchronously monitor:
   - **Bronze Ingestion** (REST API / DB Ingestion)
   - **Silver Iceberg ETL** (PySpark Iceberg Deduplication & Merging)
   - **Gold Serving Marts** (Analytical SQL marts & shared MySQL publishing)
3. **Orchestrate End-to-End Multi-Stage Pipelines ("Run All")**: Sequentially execute multi-stage pipelines (`Bronze -> Silver -> Gold` or custom stages) in a single invocation, polling each stage to completion and halting immediately if an error occurs.

---

## 1. Supported Operations Summary

| Operation | Trigger Indicator | Target Engine | Purpose |
| :--- | :--- | :--- | :--- |
| **Glue Crawler** | `"layer": "crawler"` or `"crawler_name"` | AWS Glue Crawler | Triggers and monitors Glue Crawlers to discover newly ingested S3 partitions & evolve table schemas. |
| **Athena SQL Query** | `"query"`, `"sql"`, or `"query_file"` | Amazon Athena v3 | Executes multiline queries, SQL files (e.g. `interactions.sql`), or ad-hoc queries with CLI tables in logs. |
| **Bronze Ingestion** | `"layer": "bronze"` | Glue Python Shell 3.9 | Ingests raw source data from APIs/databases into S3 Bronze partitioned by `_ingested_at=<ISO_TIMESTAMP>`. |
| **Silver Iceberg ETL** | `"layer": "silver"` | Glue PySpark 4.0 | Deduplicates Bronze raw data, applies audit columns, and merges into Iceberg tables (`tbl_<name>`). |
| **Gold Serving Marts** | `"layer": "gold"` | Glue PySpark 4.0 | Runs source-specific SQL marts (`bucket/gold/query/<source>/*.sql`) and publishes to shared MySQL with atomic swap. |
| **End-to-End Pipeline** | `"layer": "all"` or `"layers": [...]` | Multi-Stage Sequential | Sequentially executes stages (e.g. `Bronze -> Crawler -> Silver -> Gold`), failing fast if any stage fails. |

---

## 2. Event Payload Parameters Reference

### 2.1 Glue Crawler Parameters

If `"layer": "crawler"`, `"action": "crawler"`, or `"crawler_name"` is provided, Lambda triggers and monitors an AWS Glue Crawler:

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `layer` / `action` | string | Optional | - | Set to `"crawler"`, `"glue_crawler"`, or `"run_crawler"`. |
| `crawler_name` | string | Optional | `uax-datalake-bronze-crawler-dev` | Name of the Glue Crawler to trigger. |
| `wait_until_completion` | boolean | Optional | `true` | When `true`, polls until `READY` and reports metrics. When `false`, returns HTTP 202 immediately. |
| `poll_interval_seconds` | integer | Optional | `5` | Crawler polling interval in seconds. |
| `timeout_seconds` | integer | Optional | `540` | Maximum wait duration before timeout. |

---

### 2.2 Athena Query & Multiline SQL Parameters

If `"query"`, `"sql"`, `"athena_query"`, or `"query_file"` is provided, Lambda routes directly to Athena. Supports **multiline SQL queries**, **CTE pipelines**, **SQL file references**, and **multi-statement execution**:

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `query` / `sql` | string | Optional* | - | Multiline SQL query string (e.g. `WITH ... SELECT ...`). Trailing semicolons are automatically stripped. |
| `query_file` / `sql_file` | string | Optional* | - | Path to a local SQL file (e.g. `gold/query/moveworks/interactions.sql`). |
| `sql_s3_path` / `query_s3_path` | string | Optional* | - | S3 URI to a SQL query (e.g. `s3://<bucket>/gold/query/moveworks/interactions.sql`). |
| `database` | string | Optional | `uax_datalake_db_dev` | Target AWS Glue Data Catalog database. Optional if query specifies `<db>.<table>`. |
| `workgroup` | string | Optional | `uax-datalake-workgroup-dev` | Amazon Athena workgroup name. |
| `output_location` | string | Optional | Workgroup default | S3 bucket path for query results. |
| `params` / `parameters` | object | Optional | `{}` | Key-value dictionary for template substitution (`${VAR}`, `{VAR}`, `<VAR>`, `:VAR`). |
| `table_replacements` | object | Optional | `{}` | Dictionary mapping table names (e.g. `{"tbl_interactions": "raw_tbl_interactions"}`). |
| `max_results` | integer | Optional | `None` (All records) | Maximum rows to retrieve and print. Defaults to `null` to return **ALL** records. |
| `timeout_seconds` | integer | Optional | `120` | Query polling timeout in seconds before cancellation. |
| `poll_interval_seconds` | number | Optional | `1.0` | Athena status check polling interval in seconds. |

*\*At least one of `query`, `sql`, `query_file`, or `sql_s3_path` must be provided.*

---

### 2.3 Glue Job & Pipeline Parameters (Bronze, Silver, Gold, All)

| Parameter | Type | Applicable Layers | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `layer` | string | All | `"bronze"` | Execution mode: `"bronze"`, `"silver"`, `"gold"`, or `"all"` (runs Bronze $\rightarrow$ Silver $\rightarrow$ Gold). |
| `layers` | list | All | `None` | Custom stage list: e.g. `["silver", "gold"]` or `["bronze", "silver", "gold"]`. |
| `skip_bronze` | boolean | All | `false` | When `layer="all"`, set to `true` to execute `Silver -> Gold` only. |
| `source_system` | string | All | **Required** | Source system name (e.g. `servicenow`, `moveworks`, `genesys`, `postgresql`). |
| `source_table_name` | string \| list | Bronze, Silver | Config defaults | Target table name(s). Accepts single string (`"incident"`), array (`["incident", "sys_user"]`), or comma-separated string (`"incident, sys_user"`). |
| `job_name` | string | Bronze, Silver, Gold | Auto-resolved | Explicit Glue job name override. |
| `gold_schema` | string | Gold, All | **Required for Gold** | Target MySQL schema name (e.g. `enterprise_reporting`). **Zero fallback permitted**. |
| `rds_secret_name` | string | Gold, All | Optional | AWS Secrets Manager secret name or ARN containing MySQL credentials. |
| `rds_password` | string | Gold, All | Optional | Manual MySQL password override. |
| `rds_host` | string | Gold, All | Optional | RDS MySQL host override. |
| `rds_port` | string \| int | Gold, All | `3306` | RDS MySQL port. |
| `rds_user` | string | Gold, All | Optional | RDS MySQL username override. |
| `connection_name` | string | Gold, All | Config defaults | AWS Glue Connection name with JDBC / VPC requirements. |
| `gold_query_s3_path` | string | Gold, All | Config defaults | S3 path for SQL queries (`s3://<bucket>/gold/query/<source>/`). |
| `gold_data_s3_path` | string | Gold, All | Config defaults | S3 path for intermediate Parquet data (`s3://<bucket>/gold/data/<source>/`). |
| `external_columns` | string \| list | Silver, All | Config defaults | Extensible external columns attached to Silver tables (e.g. `["vendor_api_status"]`). |
| `full_refresh` | boolean | Silver, All | `false` | If `true`, scans all historical Bronze data, ignoring watermarks. |
| `watermark_enabled` | boolean | Silver, All | `true` | If `true`, filters Bronze data incrementally via watermark. |
| `wait_until_completion`| boolean | Single Jobs | `true` | If `true`, polls status until completion. If `false`, returns HTTP 202 immediately. |
| `poll_interval_seconds`| integer | All | `10` | Polling interval in seconds. |
| `timeout_seconds` | integer | All | `540` | Maximum wait duration before Lambda exits (Default: 9 minutes). |

---

## 3. Sample Event Payloads

### A. Bronze Layer Ingestion Payloads (`layer: "bronze"`)

#### 1. Single Table Ingestion (ServiceNow `incident`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "wait_until_completion": true
}
```

#### 2. Moveworks Full Initial Historical Ingestion (`interactions`)
Extracts 100% of historical data using cursor pagination via `@odata.nextLink`:
```json
{
  "layer": "bronze",
  "source_system": "moveworks",
  "source_table_name": "interactions",
  "initial_load_date": "1900-01-01 00:00:00",
  "wait_until_completion": true
}
```

#### 3. Multi-Table Ingestion (`["incident", "sys_user"]`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": ["incident", "sys_user"],
  "wait_until_completion": true
}
```

#### 4. Incremental Delta Ingestion with Custom Watermark Window
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

---

### B. Silver Layer Iceberg ETL Payloads (`layer: "silver"`)

#### 5. Incremental Silver Iceberg ETL (`raw_tbl_incident`)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "watermark_enabled": true,
  "wait_until_completion": true
}
```

#### 6. Moveworks Silver Cleanse with Custom Transform (`raw_tbl_interactions`)
Applies illegal character removal and Moveworks domain/entity mapping:
```json
{
  "layer": "silver",
  "source_system": "moveworks",
  "source_table_name": "raw_tbl_interactions",
  "wait_until_completion": true
}
```

#### 7. Full Historical Refresh (Ignore Watermark)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "full_refresh": true,
  "wait_until_completion": true
}
```

#### 8. Multi-Table Silver Batch Ingestion
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

#### 9. Gold Serving Execution with AWS Secrets Manager
Executes `s3://<bucket>/gold/query/servicenow/*.sql` and publishes into the MySQL schema using credentials from Secrets Manager.
```json
{
  "layer": "gold",
  "source_system": "servicenow",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "wait_until_completion": true
}
```

#### 10. Gold Serving Execution with Manual Password Override
```json
{
  "layer": "gold",
  "source_system": "servicenow",
  "gold_schema": "enterprise_reporting",
  "rds_password": "manual_database_password",
  "rds_user": "reporting_user",
  "wait_until_completion": true
}
```

#### 11. Gold Serving with Custom Query Path & Connection
```json
{
  "layer": "gold",
  "source_system": "moveworks",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "gold_query_s3_path": "s3://uax-datalake-dev-bucket/gold/query/moveworks/interactions.sql",
  "connection_name": "uax-datalake-rds-connection-dev",
  "wait_until_completion": true
}
```

---

### D. End-to-End Multi-Stage Pipeline Payloads ("Run All")

#### 12. Run All: Bronze $\rightarrow$ Silver $\rightarrow$ Gold Sequentially
```json
{
  "layer": "all",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "poll_interval_seconds": 10,
  "timeout_seconds": 600
}
```

#### 13. Run Pipeline: Silver $\rightarrow$ Gold Only (`skip_bronze: true`)
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

#### 14. Full Pipeline with Catalog Crawler Chaining (`Bronze -> Crawler -> Silver -> Gold`)
Discovers new Bronze partitions & newly evolved Parquet columns before Silver runs:
```json
{
  "layers": ["bronze", "crawler", "silver", "gold"],
  "source_system": "moveworks",
  "crawler_name": "uax-datalake-bronze-crawler-dev",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials"
}
```

#### 15. Custom Pipeline Stages (`Crawler -> Silver`)
```json
{
  "layers": ["crawler", "silver"],
  "source_system": "moveworks",
  "crawler_name": "uax-datalake-bronze-crawler-dev",
  "source_table_name": "raw_tbl_interactions"
}
```

---

### E. AWS Glue Crawler Payloads (`layer: "crawler"`)

#### 16. Trigger Bronze Data Catalog Crawler (Synchronous with Metrics)
```json
{
  "layer": "crawler",
  "crawler_name": "uax-datalake-bronze-crawler-dev",
  "wait_until_completion": true,
  "poll_interval_seconds": 5
}
```

#### 17. Trigger Crawler Asynchronously (Fire-and-Forget)
```json
{
  "layer": "crawler",
  "crawler_name": "uax-datalake-bronze-crawler-dev",
  "wait_until_completion": false
}
```

#### 18. Trigger Source-Specific Crawler by `source_system`
```json
{
  "layer": "crawler",
  "source_system": "moveworks"
}
```

---

### F. Athena Multiline SQL & File Query Payloads (`"query"` / `"query_file"`)

#### 19. Execute Moveworks Gold Query File (`interactions.sql`)
Executes the full multiline Gold aggregation query from the repository:
```json
{
  "query_file": "gold/query/moveworks/interactions.sql",
  "database": "uax_datalake_db_dev",
  "table_replacements": {
    "tbl_interactions": "silver_tbl_moveworks_interactions",
    "tbl_conversations": "silver_tbl_moveworks_conversations",
    "tbl_plugin_calls": "silver_tbl_moveworks_plugin_calls",
    "tbl_plugin_resources": "silver_tbl_moveworks_plugin_resources",
    "tbl_users": "silver_tbl_moveworks_users"
  }
}
```

#### 20. Execute SQL Query Directly from S3 URI with Dynamic Parameters
```json
{
  "sql_s3_path": "s3://uax-datalake-dev-bucket/gold/query/moveworks/interactions.sql",
  "database": "uax_datalake_db_dev",
  "params": {
    "start_time": "2025-07-15 00:00:00.000 UTC",
    "end_time": "2025-07-15 23:59:59.999 UTC"
  }
}
```

#### 21. Execute Multiline CTE Query with Active Record Filtering
```json
{
  "query": "WITH conversation_topics AS (\n    SELECT conversation_id, ARRAY_JOIN(ARRAY_AGG(DISTINCT detail_entity), ', ') AS conversation_topic\n    FROM raw_tbl_interactions\n    WHERE _is_current = 'Y' AND _is_deleted = 'N' AND detail_entity IS NOT NULL\n    GROUP BY conversation_id\n)\nSELECT ui.id, ui.detail_content, ct.conversation_topic\nFROM raw_tbl_interactions ui\nLEFT JOIN conversation_topics ct ON ui.conversation_id = ct.conversation_id\nWHERE ui.actor = 'user' AND ui._is_current = 'Y' AND ui._is_deleted = 'N'\nLIMIT 10;",
  "database": "uax_datalake_db_dev",
  "max_results": 10
}
```

#### 22. Inspect Available Data Catalog Tables
```json
{
  "query": "SHOW TABLES IN uax_datalake_db_dev"
}
```

#### 23. Query Bronze Raw Data Table with Flattened Nested Columns
```json
{
  "query": "SELECT id, detail_content, detail_domain, detail_entity, detail_platform_name, _ingested_at FROM raw_tbl_interactions WHERE _is_current = 'Y' LIMIT 10",
  "database": "uax_datalake_db_dev",
  "max_results": 10
}
```

#### 24. Query Silver Iceberg Table with Active SCD Flags
```json
{
  "query": "SELECT id, detail_content, detail_domain, detail_entity, _is_current, _valid_from, _updated_at FROM tbl_interactions WHERE _is_current = 'Y' AND _is_deleted = 'N' LIMIT 10",
  "database": "uax_datalake_db_dev",
  "max_results": 10
}
```

#### 25. Compare Bronze Raw Counts vs. Silver Iceberg Current Counts
```json
{
  "query": "SELECT 'bronze_raw' AS layer, COUNT(*) AS cnt FROM raw_tbl_interactions UNION ALL SELECT 'silver_current' AS layer, COUNT(*) AS cnt FROM tbl_interactions WHERE _is_current = 'Y' AND _is_deleted = 'N'",
  "database": "uax_datalake_db_dev"
}
```

#### 26. Inspect High-Watermark Progress Table (`raw_tbl_watermarks`)
```json
{
  "query": "SELECT source_system, table_name, last_load_date, last_status, records_ingested, updated_at FROM raw_tbl_watermarks ORDER BY updated_at DESC",
  "database": "uax_datalake_db_dev"
}
```

---

## 4. Response Payload Formats

### A. Successful Multi-Stage Pipeline Execution (HTTP 200)
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
        "job_name": "uax-datalake-silver-etl-dev",
        "job_run_id": "jr_gold_003",
        "status": "SUCCEEDED",
        "execution_time_seconds": 23,
        "log_group": "/aws-glue/jobs/output"
      }
    }
  }
}
```

### B. Failed Pipeline Execution (HTTP 500 — Fail-Fast)
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

### C. Successful Athena Query Execution (HTTP 200 & CloudWatch Log Output)

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

---

## 5. Testing in the AWS Lambda Console

1. Open **AWS Lambda Console** &rarr; Functions &rarr; Select `uax-datalake-glue-job-trigger-dev`.
2. Navigate to the **Test** tab.
3. Paste any test payload from **Section 3** into the Event JSON editor:
   - For ad-hoc queries: Paste payload #13 through #18.
   - For Bronze ingestion: Paste payload #1 through #3.
   - For Silver ETL: Paste payload #4 through #6.
   - For Gold serving: Paste payload #7 through #9.
   - For full pipeline execution: Paste payload #10 through #12.
4. Click **Test**.
5. Inspect the execution logs and JSON output card directly in the console.
