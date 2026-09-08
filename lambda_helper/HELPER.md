# AWS Glue Helper Lambda (Bronze & Silver Jobs)

This helper Lambda function allows engineers to trigger and synchronously monitor **Bronze (REST API Ingestion)** and **Silver (PySpark Iceberg ETL)** AWS Glue jobs directly via the AWS Lambda Console, AWS CLI, or SDKs without requiring direct access to the AWS Glue Console.

---

## 1. Supported Glue Jobs

| Layer | Job Name | Engine | Purpose |
| :--- | :--- | :--- | :--- |
| **Bronze** | `uax-datalake-bronze-ingestion-dev` | Python Shell 3.9 | Ingests raw API / DB data into S3 partitioned by `_ingested_at=<ISO_TIMESTAMP>` and registers Glue Catalog external tables. |
| **Silver** | `uax-datalake-silver-etl-dev` | Glue PySpark 4.0 | Deduplicates Bronze raw data, applies audit columns (`_is_deleted`, `_is_current`, `_updated_at`, `_inserted_at`), and merges into Iceberg tables (`tbl_<name>`). |

*(Job names automatically resolve via environment variables `DEFAULT_BRONZE_JOB` and `DEFAULT_SILVER_JOB`, or can be explicitly overridden in the event payload via `"job_name"`).*

---

## 2. Event Payload Parameters Reference

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

2. Ensure the policy attached to the role includes permissions to trigger and monitor Glue jobs:
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
        "glue:BatchStopJobRun"
      ],
      "Resource": "arn:aws:glue:*:*:job/uax-datalake-*"
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

