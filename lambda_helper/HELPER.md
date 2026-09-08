# AWS Glue & Athena Helper Lambda

This helper Lambda function allows engineers to:
1. **Query Amazon Athena directly**: Run SQL queries against the Glue Data Catalog (`uax_datalake_db_dev`) or Iceberg tables and inspect formatted tabular results directly in the Lambda execution logs.
2. **Trigger and Monitor AWS Glue Jobs**: Trigger and synchronously monitor **Bronze (REST API Ingestion)** and **Silver (PySpark Iceberg ETL)** AWS Glue jobs directly via the AWS Lambda Console, AWS CLI, or SDKs without requiring direct access to the AWS Glue Console.

---

## 1. Supported Operations

| Operation | Trigger Condition | Engine | Purpose |
| :--- | :--- | :--- | :--- |
| **Athena SQL Query** *(New)* | Payload contains `"query"`, `"athena_query"`, or `"sql"` | Amazon Athena Engine v3 | Executes SQL queries against Bronze / Silver tables and logs formatted results directly to CloudWatch. |
| **Bronze Ingestion Job** | `layer = "bronze"` (or default) | Python Shell 3.9 | Ingests raw API / DB data into S3 partitioned by `_ingested_at=<ISO_TIMESTAMP>` and registers Glue Catalog external tables. |
| **Silver Iceberg ETL Job** | `layer = "silver"` | Glue PySpark 4.0 | Deduplicates Bronze raw data, applies audit columns (`_is_deleted`, `_is_current`, `_updated_at`, `_inserted_at`), and merges into Iceberg tables (`tbl_<name>`). |

*(Job names automatically resolve via environment variables `DEFAULT_BRONZE_JOB` and `DEFAULT_SILVER_JOB`, or can be explicitly overridden in the event payload via `"job_name"`).*

---

## 2. Event Payload Parameters Reference

### 2.1 Athena Query Parameters *(New)*

If `"query"`, `"athena_query"`, or `"sql"` is provided in the payload, Lambda runs the query in Athena and prints the tabular results directly into the Lambda log:

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `query` / `sql` | string | **Required** | - | SQL query string to execute (e.g. `SELECT * FROM raw_tbl_incident LIMIT 10`). |
| `database` | string | Optional | `uax_datalake_db_dev` | Target AWS Glue Data Catalog database. |
| `workgroup` | string | Optional | `uax-datalake-workgroup-dev` | Amazon Athena workgroup name. |
| `output_location` | string | Optional | Workgroup default | S3 path for Athena query results (e.g. `s3://uax-datalake-dev-bucket/athena-results/`). |
| `max_results` | integer | Optional | `50` | Maximum number of rows to retrieve and print in the CloudWatch logs. |
| `timeout_seconds` | integer | Optional | `120` | Maximum time to wait for query execution before timing out. |

---

### 2.2 AWS Glue Job Parameters *(Existing)*

| Parameter | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `layer` | string | Optional | `"bronze"` | Target layer: `"bronze"` or `"silver"`. |
| `job_name` | string | Optional | Auto-resolved | Explicit Glue job name override (e.g. `uax-datalake-silver-etl-dev`). |
| `source_system` | string | **Required** | - | Source system name (`servicenow`, `moveworks`, `genesys`, `postgresql`, `mysql`). |
| `source_table_name` | string \| list | Optional | Config defaults | Target table name(s) to process. Accepts a **single string** (`"raw_tbl_interactions"`), a **JSON list** (`["raw_tbl_interactions", "raw_tbl_users"]`), or a **comma-separated string** (`"raw_tbl_interactions,raw_tbl_users"`). For Bronze: `"incident"` or `["incident", "sys_user"]`; for Silver: `"raw_tbl_incident"` or `["raw_tbl_interactions", "raw_tbl_events"]`. |
| `secret_name` | string | Optional | Config defaults | AWS Secrets Manager secret ARN or name override for credentials. |
| `custom_query` | string | Optional | Config defaults | Custom SQL query or API filter override. |
| `full_refresh` | boolean | Optional | `false` | For Silver: If `true`, ignores the watermark and processes all historical Bronze data. |
| `watermark_enabled`| boolean | Optional | `true` | For Silver: If `true`, filters Bronze data by incremental watermark (`_ingested_at > last_watermark`). |
| `crawler_name` | string | Optional | Auto-resolved | AWS Glue Crawler name override to trigger after load (e.g. `uax-datalake-silver-iceberg-crawler-dev`). |
| `trigger_crawler` | boolean | Optional | `auto` | If `true`, triggers the Glue crawler on completion (automatically triggered if a new table was created or schema changed). |
| `initial_load_date`| string | Optional | Config defaults | For Bronze: Overrides the starting delta extraction timestamp (e.g., `"2024-01-01T00:00:00Z"`). |
| `wait_until_completion` | boolean | Optional | `true` | If `true`, Lambda waits and polls status until completion. If `false`, fires asynchronously and returns HTTP 202 immediately. |
| `poll_interval_seconds` | integer | Optional | `10` | Polling interval in seconds. |
| `timeout_seconds` | integer | Optional | `540` | Maximum wait duration before Lambda exits (Default: 9 minutes). |
| `arguments` | object | Optional | `{}` | Key-value dictionary to pass any arbitrary custom Glue CLI arguments (e.g. `{"--CONF": "..."}`). |

> [!TIP]
> **Passing Multiple Tables**: You can pass `source_table_name` as a JSON array (`["tbl_1", "tbl_2"]`) or a comma-delimited string (`"tbl_1, tbl_2"`). The Lambda helper automatically formats it into the standard Glue CLI argument format.

---

## 3. Sample Test Events (JSON Payloads)

### A. Bronze Layer Test Payloads

#### 1. ServiceNow Bronze Ingestion — Single Table (`incident`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": "incident",
  "secret_name": "uax-datalake/servicenow-credentials-dev",
  "wait_until_completion": true
}
```

#### 2. ServiceNow Bronze Ingestion — List of Tables (`["incident", "sys_user"]`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "source_table_name": ["incident", "sys_user"],
  "secret_name": "uax-datalake/servicenow-credentials-dev",
  "wait_until_completion": true
}
```

#### 3. Moveworks Bronze Ingestion (`interactions`)
```json
{
  "layer": "bronze",
  "source_system": "moveworks",
  "source_table_name": "interactions",
  "secret_name": "uax-datalake/moveworks-credentials-dev",
  "wait_until_completion": true
}
```

#### 4. Genesys Bronze Ingestion (`conversations`)
```json
{
  "layer": "bronze",
  "source_system": "genesys",
  "source_table_name": "conversations",
  "secret_name": "uax-datalake/genesys-credentials-dev",
  "wait_until_completion": true
}
```

#### 5. Relational Database Bronze Ingestion (`orders`)
```json
{
  "layer": "bronze",
  "source_system": "postgresql",
  "source_table_name": "orders",
  "secret_name": "uax-datalake/postgresql-credentials-dev",
  "wait_until_completion": true
}
```

---

### B. Silver Layer Test Payloads (Apache Iceberg ETL)

#### 6. ServiceNow Silver Iceberg ETL — Single Table (`raw_tbl_incident`)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "wait_until_completion": true
}
```

#### 7. Moveworks Silver Iceberg ETL — Single Table (`raw_tbl_interactions`)
```json
{
  "layer": "silver",
  "source_system": "moveworks",
  "source_table_name": "raw_tbl_interactions",
  "wait_until_completion": true
}
```

#### 8. Moveworks Silver Iceberg ETL — List of Tables (`["raw_tbl_interactions", "raw_tbl_events"]`)
```json
{
  "layer": "silver",
  "source_system": "moveworks",
  "source_table_name": ["raw_tbl_interactions", "raw_tbl_events"],
  "wait_until_completion": true
}
```

#### 9. ServiceNow Silver Full Refresh (Scans entire Bronze table)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "source_table_name": "raw_tbl_incident",
  "full_refresh": true,
  "wait_until_completion": true
}
```

#### 10. Genesys Silver Iceberg ETL (`raw_tbl_conversations`)
```json
{
  "layer": "silver",
  "source_system": "genesys",
  "source_table_name": "raw_tbl_conversations",
  "wait_until_completion": true
}
```

---

### C. Asynchronous Execution (Fire and Forget)

#### 11. Trigger Job and Return Immediately
```json
{
  "layer": "silver",
  "source_system": "moveworks",
  "source_table_name": "raw_tbl_interactions",
  "wait_until_completion": false
}
```

---

### D. Athena Query Test Payloads *(New)*

#### 12. Show All Tables in Database
```json
{
  "query": "SHOW TABLES IN uax_datalake_db_dev"
}
```

#### 13. Query Bronze Raw Data Table (`raw_tbl_incident`)
```json
{
  "query": "SELECT sys_id, number, state, _ingested_at FROM raw_tbl_incident ORDER BY _ingested_at DESC LIMIT 10",
  "database": "uax_datalake_db_dev",
  "max_results": 10
}
```

#### 14. Query Silver Iceberg Data Table (`tbl_interactions`)
```json
{
  "query": "SELECT interaction_id, user_email, _is_current, _updated_at FROM tbl_interactions WHERE _is_current = true LIMIT 10",
  "database": "uax_datalake_db_dev",
  "workgroup": "uax-datalake-workgroup-dev",
  "max_results": 10
}
```

#### 15. Check High-Watermark State Table (`raw_tbl_watermarks` / `tbl_watermarks`)
```json
{
  "query": "SELECT * FROM tbl_watermarks ORDER BY updated_at DESC"
}
```

---

## 4. Example Responses

### A. Successful Synchronous Execution (HTTP 200)
```json
{
  "statusCode": 200,
  "body": "{\"job_name\": \"uax-datalake-silver-etl-dev\", \"job_run_id\": \"jr_1234567890abcdef\", \"job_status\": \"SUCCEEDED\", \"execution_time_seconds\": 45, \"source_system\": \"moveworks\", \"source_table_name\": \"raw_tbl_interactions\", \"cloudwatch_log_group\": \"/aws-glue/jobs/output\", \"error_message\": null}"
}
```

### B. Successful Asynchronous Trigger (HTTP 202)
```json
{
  "statusCode": 202,
  "body": "{\"message\": \"Glue job started asynchronously.\", \"job_name\": \"uax-datalake-silver-etl-dev\", \"job_run_id\": \"jr_9876543210fedcba\", \"status\": \"STARTING\", \"arguments\": {\"--SOURCE_SYSTEM\": \"moveworks\", \"--SOURCE_TABLE_NAME\": \"raw_tbl_interactions\", \"--TABLE_NAME\": \"raw_tbl_interactions\"}}"
}
```

### C. Failed Execution (HTTP 500)
```json
{
  "statusCode": 500,
  "body": "{\"job_name\": \"uax-datalake-silver-etl-dev\", \"job_run_id\": \"jr_abcdef1234567890\", \"job_status\": \"FAILED\", \"execution_time_seconds\": 22, \"source_system\": \"moveworks\", \"source_table_name\": \"raw_tbl_interactions\", \"cloudwatch_log_group\": \"/aws-glue/jobs/output\", \"error_message\": \"...\"}"
}
```

### D. Successful Athena Query Execution (HTTP 200) *(New)*

**Response JSON:**
```json
{
  "statusCode": 200,
  "body": {
    "query_execution_id": "a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d",
    "status": "SUCCEEDED",
    "query": "SELECT sys_id, number, state, _ingested_at FROM raw_tbl_incident LIMIT 2",
    "database": "uax_datalake_db_dev",
    "workgroup": "uax-datalake-workgroup-dev",
    "execution_time_ms": 1240,
    "data_scanned_bytes": 1048576,
    "columns": ["sys_id", "number", "state", "_ingested_at"],
    "row_count": 2,
    "records": [
      {
        "sys_id": "9d380721eb311100d4360c5111061735",
        "number": "INC0000001",
        "state": "1",
        "_ingested_at": "2026-09-08T16:00:00Z"
      },
      {
        "sys_id": "e8caed5b1b4001103c8b4088b04bcba7",
        "number": "INC0000002",
        "state": "2",
        "_ingested_at": "2026-09-08T16:00:00Z"
      }
    ]
  }
}
```

**Formatted CloudWatch Log Output (Visible in Lambda Logs):**
```
================================================================================
ATHENA QUERY EXECUTION RESULT SUMMARY
Query           : SELECT sys_id, number, state, _ingested_at FROM raw_tbl_incident LIMIT 2
Database        : uax_datalake_db_dev
WorkGroup       : uax-datalake-workgroup-dev
Execution ID    : a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d
Engine Time     : 1240 ms (1.24 s)
Data Scanned    : 1,048,576 bytes (1.0000 MB)
Rows Returned   : 2 (max_results: 10)
--------------------------------------------------------------------------------
sys_id                           | number     | state | _ingested_at        
---------------------------------+------------+-------+---------------------
9d380721eb311100d4360c5111061735 | INC0000001 | 1     | 2026-09-08T16:00:00Z
e8caed5b1b4001103c8b4088b04bcba7 | INC0000002 | 2     | 2026-09-08T16:00:00Z
--------------------------------------------------------------------------------
JSON Records Output:
[
  {
    "sys_id": "9d380721eb311100d4360c5111061735",
    "number": "INC0000001",
    "state": "1",
    "_ingested_at": "2026-09-08T16:00:00Z"
  },
  {
    "sys_id": "e8caed5b1b4001103c8b4088b04bcba7",
    "number": "INC0000002",
    "state": "2",
    "_ingested_at": "2026-09-08T16:00:00Z"
  }
]
================================================================================
```

---

## 5. How to Test in AWS Lambda Console

1. Open **AWS Lambda Console** &rarr; Functions &rarr; Select `uax-datalake-glue-job-trigger-dev`.
2. Select the **Test** tab.
3. Paste any sample payload from **Section 3** above into the Event JSON editor.
4. Click **Test**.
5. Inspect the execution logs and JSON output card directly in the console.

---

## 6. IAM Role Setup & Permissions

To re-use the existing Glue IAM Execution Role (`uax-datalake-glue-execution-role-dev`) for the Lambda function:

1. Ensure `lambda.amazonaws.com` is added to the IAM Role's **Trust Relationship**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "glue.amazonaws.com",
          "lambda.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

2. Ensure the policy attached to the role includes permissions for AWS Glue, Amazon Athena, and Amazon S3 query results:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun",
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StopQueryExecution",
        "athena:GetWorkGroup"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::uax-datalake-*-bucket",
        "arn:aws:s3:::uax-datalake-*-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/lambda/*"
    }
  ]
}
```

