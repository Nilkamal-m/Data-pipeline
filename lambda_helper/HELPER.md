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
| **Athena SQL Query** | `"query"`, `"sql"`, or `"athena_query"` | Amazon Athena v3 | Executes ad-hoc or validation queries against Data Catalog / Iceberg tables with formatted CLI tables in logs. |
| **Bronze Ingestion** | `"layer": "bronze"` | Glue Python Shell 3.9 | Ingests raw source data from APIs/databases into S3 Bronze partitioned by `_ingested_at=<ISO_TIMESTAMP>`. |
| **Silver Iceberg ETL** | `"layer": "silver"` | Glue PySpark 4.0 | Deduplicates Bronze raw data, applies audit columns, and merges into Iceberg tables (`tbl_<name>`). |
| **Gold Serving Marts** | `"layer": "gold"` | Glue PySpark 4.0 | Runs source-specific SQL marts (`bucket/gold/query/<source>/*.sql`) and publishes to shared MySQL with atomic swap. |
| **End-to-End Pipeline** | `"layer": "all"` or `"layers": [...]` | Multi-Stage Sequential | Sequentially executes `Bronze -> Silver -> Gold` (or `Silver -> Gold`), failing fast if any stage fails. |

---

## 2. Event Payload Parameters Reference

### 2.1 Athena Query Parameters

If `"query"`, `"athena_query"`, or `"sql"` is provided in the event payload, Lambda routes directly to Athena:

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `query` / `sql` | string | **Required** | - | The SQL query string to execute (e.g. `SELECT * FROM tbl_incident LIMIT 10`). |
| `database` | string | Optional | `uax_datalake_db_dev` | Target AWS Glue Data Catalog database. Optional if query specifies `<db>.<table>`. |
| `workgroup` | string | Optional | `uax-datalake-workgroup-dev` | Amazon Athena workgroup name. |
| `output_location` | string | Optional | Workgroup default | S3 bucket path for query results (e.g. `s3://uax-datalake-dev-bucket/athena-results/`). |
| `max_results` | integer | Optional | `None` (All records) | Maximum rows to retrieve and print. Defaults to `null` to return **ALL** records. |
| `timeout_seconds` | integer | Optional | `120` | Query polling timeout in seconds before cancellation. |
| `poll_interval_seconds` | number | Optional | `1.0` | Athena status check polling interval in seconds. |

---

### 2.2 Glue Job & Pipeline Parameters (Bronze, Silver, Gold, All)

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

#### 1. Single Table Bronze Ingestion (`incident`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "wait_until_completion": true
}
```

#### 2. Multi-Table Bronze Ingestion (`["incident", "sys_user"]`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": ["incident", "sys_user"],
  "wait_until_completion": true
}
```

#### 3. Delta Ingestion with Custom Load Date
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "initial_load_date": "2026-01-01T00:00:00Z",
  "wait_until_completion": true
}
```

---

### B. Silver Layer Iceberg ETL Payloads (`layer: "silver"`)

#### 4. Incremental Silver Iceberg ETL (`raw_tbl_incident`)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "watermark_enabled": true,
  "wait_until_completion": true
}
```

#### 5. Full Historical Refresh
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "full_refresh": true,
  "wait_until_completion": true
}
```

#### 6. Multi-Table Silver ETL
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": ["raw_tbl_incident", "raw_tbl_sys_user"],
  "wait_until_completion": true
}
```

---

### C. Gold Serving Mart Payloads (`layer: "gold"`)

#### 7. Gold Serving Execution with AWS Secrets Manager
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

#### 8. Gold Serving Execution with Manual Password Override
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

#### 9. Gold Serving with Custom Query Path & Connection
```json
{
  "layer": "gold",
  "source_system": "servicenow",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials",
  "gold_query_s3_path": "s3://uax-datalake-dev-bucket/gold/query/servicenow/incident_kpi.sql",
  "connection_name": "uax-datalake-rds-connection-dev"
}
```

---

### D. End-to-End Multi-Stage Pipeline Payloads ("Run All")

#### 10. Run All: Bronze $\rightarrow$ Silver $\rightarrow$ Gold Sequentially
Executes Bronze Ingestion, then Silver Iceberg ETL, then Gold Marts publishing in sequence. Halts immediately if any stage fails.
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

#### 11. Run Pipeline: Silver $\rightarrow$ Gold Only (`skip_bronze: true`)
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

#### 12. Run Pipeline with Explicit Custom Stages (`layers: [...]`)
```json
{
  "layers": ["silver", "gold"],
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "gold_schema": "enterprise_reporting",
  "rds_secret_name": "prod/rds/mysql_credentials"
}
```

---

### E. Athena SQL Query Payloads (`"query"`)

#### 13. Inspect Available Data Catalog Tables
```json
{
  "query": "SHOW TABLES IN uax_datalake_db_dev"
}
```

#### 14. Query Bronze Raw Data Table (`raw_tbl_incident`)
```json
{
  "query": "SELECT sys_id, number, priority, state, _ingested_at FROM raw_tbl_incident ORDER BY _ingested_at DESC LIMIT 10",
  "database": "uax_datalake_db_dev",
  "max_results": 10
}
```

#### 15. Query Silver Iceberg Table (`tbl_incident`)
```json
{
  "query": "SELECT incident_number, priority, state, _is_current, _valid_from, _updated_at FROM tbl_incident WHERE _is_current = 'Y' LIMIT 10",
  "database": "uax_datalake_db_dev",
  "max_results": 10
}
```

#### 16. Compare Bronze Raw Counts vs. Silver Iceberg Current Counts
```json
{
  "query": "SELECT 'bronze_raw' AS layer, COUNT(*) AS cnt FROM raw_tbl_incident UNION ALL SELECT 'silver_current' AS layer, COUNT(*) AS cnt FROM tbl_incident WHERE _is_current = 'Y'",
  "database": "uax_datalake_db_dev"
}
```

#### 17. Inspect High-Watermark Progress Table (`tbl_watermarks`)
```json
{
  "query": "SELECT source_system, table_name, last_watermark, updated_at FROM tbl_watermarks ORDER BY updated_at DESC"
}
```

#### 18. Ad-Hoc Analytical Mart Validation Query
```json
{
  "query": "SELECT priority, state, count(*) AS total_incidents, AVG(CAST(reassignment_count AS double)) AS avg_reassignments FROM tbl_incident WHERE _is_current = 'Y' GROUP BY priority, state ORDER BY priority, state"
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
