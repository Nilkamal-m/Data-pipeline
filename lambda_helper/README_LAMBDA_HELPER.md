# AWS Glue Helper Lambda (Bronze & Silver Jobs)

This helper Lambda function allows developers to trigger and synchronously monitor **Bronze (REST API Ingestion)** and **Silver (PySpark Iceberg ETL)** AWS Glue jobs when they do **not** have direct access to the AWS Glue Console.

---

## Features

1. **Multi-Layer Job Trigger**: Supports triggering both **Bronze Ingestion** (`layer: "bronze"`) and **Silver Iceberg ETL** (`layer: "silver"`) jobs or any explicit `job_name`.
2. **Dynamic Parameter Passing**: Easily pass source systems (`servicenow`, `moveworks`, `genesys`), table lists, custom queries, batch sizes, secrets, Glue catalog databases, and S3 paths.
3. **Synchronous Monitoring**: Option to wait (`wait_until_completion: true`) and poll status every 10 seconds. Returns full status (`SUCCEEDED`, `FAILED`), duration, error tracebacks, and CloudWatch log groups directly in the Lambda output!
4. **Reuses Glue IAM Execution Role**: Configured to run under the existing Glue Execution IAM Role (`uax-datalake-glue-execution-role-dev`), ensuring seamless S3, Secrets Manager, Glue, and CloudWatch access.
5. **Asynchronous Mode**: Set `wait_until_completion: false` for instant fire-and-forget triggering.

---

## IAM Role Re-Use Setup

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
2. Attach the policy containing `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns`, `glue:BatchStopJobRun` to the role.

---

## How to Test in AWS Lambda Console

1. Open **AWS Lambda Console** -> Functions -> Select `uax-datalake-glue-job-trigger-dev`.
2. Go to the **Test** tab.
3. Create a new test event and paste one of the JSON payloads below.
4. Click **Test**.
5. View the **Execution Result** tab for real-time status and output!

---

## Sample Test Events (JSON Payloads)

---

### 1. BRONZE LAYER TEST PAYLOADS

#### Payload A: ServiceNow Bronze Ingestion (`incident`)
```json
{
  "layer": "bronze",
  "source_system": "servicenow",
  "table_name": "incident",
  "secret_name": "uax-datalake/servicenow-credentials-dev",
  "wait_until_completion": true
}
```

#### Payload B: Moveworks Bronze Ingestion (`interactions`)
```json
{
  "layer": "bronze",
  "source_system": "moveworks",
  "table_name": "interactions",
  "secret_name": "uax-datalake/moveworks-credentials-dev",
  "wait_until_completion": true
}
```

#### Payload C: Genesys Bronze Ingestion (`conversations`)
```json
{
  "layer": "bronze",
  "source_system": "genesys",
  "table_name": "conversations",
  "secret_name": "uax-datalake/genesys-credentials-dev",
  "wait_until_completion": true
}
```

---

### 2. SILVER LAYER TEST PAYLOADS (Apache Iceberg ETL)

#### Payload D: ServiceNow Silver Iceberg ETL (`incident`)
```json
{
  "layer": "silver",
  "source_system": "servicenow",
  "table_name": "incident",
  "wait_until_completion": true
}
```

#### Payload E: Moveworks Silver Iceberg ETL (`interactions`)
```json
{
  "layer": "silver",
  "source_system": "moveworks",
  "table_name": "interactions",
  "wait_until_completion": true
}
```

#### Payload F: Genesys Silver Iceberg ETL (`conversations`)
```json
{
  "layer": "silver",
  "source_system": "genesys",
  "table_name": "conversations",
  "wait_until_completion": true
}
```

---

### 3. EXPLICIT JOB NAME / ASYNCHRONOUS TEST PAYLOADS

#### Payload G: Trigger Explicit Job Asynchronously (Fire and Forget)
```json
{
  "job_name": "uax-datalake-bronze-ingestion-dev",
  "source_system": "servicenow",
  "table_name": "incident",
  "wait_until_completion": false
}
```

---

## Example Successful Output (HTTP 200)

```json
{
  "statusCode": 200,
  "body": "{\"job_name\": \"uax-datalake-bronze-ingestion-dev\", \"job_run_id\": \"jr_1234567890abcdef\", \"job_status\": \"SUCCEEDED\", \"execution_time_seconds\": 45, \"source_system\": \"servicenow\", \"table_name\": \"incident\", \"cloudwatch_log_group\": \"/aws-glue/jobs/output\", \"error_message\": null}"
}
```

## Example Error Output (HTTP 500)

```json
{
  "statusCode": 500,
  "body": "{\"job_name\": \"uax-datalake-silver-iceberg-etl-dev\", \"job_run_id\": \"jr_9876543210fedcba\", \"job_status\": \"FAILED\", \"execution_time_seconds\": 18, \"source_system\": \"servicenow\", \"table_name\": \"incident\", \"cloudwatch_log_group\": \"/aws-glue/jobs/output\", \"error_message\": \"AnalysisException: Table uax-datalake-db-dev.servicenow_incident does not exist\"}"
}
```
