# 🩺 Bronze Layer: Troubleshooting & Error Resolution Guide

This runbook provides a complete reference of all possible errors that can occur during the Bronze Ingestion phase, where to inspect logs and artifacts, and the exact steps to resolve each issue.

---

## 📍 1. Where to Check (Diagnostic Inspection Matrix)

When a Bronze Glue Job fails or behaves unexpectedly, inspect these locations in order:

| Investigation Area | Target Location / Service | What to Look For |
| :--- | :--- | :--- |
| **1. Primary Error Logs** | **CloudWatch Log Group**:<br>`/aws-glue/jobs/error` | Python stack traces, unhandled exceptions, and critical errors |
| **2. Detailed Step Logs** | **CloudWatch Log Group**:<br>`/aws-glue/jobs/output` | Record counts per chunk, API request URLs, token renewals, execution timing |
| **3. Execution Audit Log** | **S3 Audit Prefix**:<br>`s3://<bucket>/logs/bronze/<source>/<table_name>/<execution_id>.json` | High-level summary: start/end time, record counts, byte size, duration, status |
| **4. Quarantine / DLQ** | **S3 Quarantine Prefix**:<br>`s3://<bucket>/quarantine/<source>/<table_name>/<execution_id>/` | Corrupted, malformed, or unparseable raw payloads diverted from normal ingestion |
| **5. Watermark State** | **S3 Metadata Prefix**:<br>`s3://<bucket>/metadata/bronze/<source>/<table_name>_watermark.json` | Current high-watermark timestamp, last load date, and execution metadata |
| **6. Active / Failed Staging** | **S3 Staging Prefix**:<br>`s3://<bucket>/staging/bronze/<execution_id>/` | Temporary chunk parquet files during active run (should be empty when job ends) |
| **7. Glue Data Catalog** | **AWS Glue Console** → Databases → `uax_datalake_db_dev` | Ensure database exists; verify table `raw_tbl_<table_name>` and `tbl_watermarks` |
| **8. Table Partitions** | **AWS Glue Console** → `raw_tbl_<table_name>` → View Partitions | Verify partition `_ingested_at=<TIMESTAMP>` exists and points to valid S3 location |
| **9. Secrets & Credentials** | **AWS Secrets Manager Console** → `uax-datalake/<source>-credentials-dev` | Ensure secret exists, JSON keys (`client_id`, `client_secret`, `base_url`) are present |
| **10. IAM Permissions** | **IAM Console** → `uax-datalake-glue-execution-role-dev` | Ensure policy grants `s3:*`, `glue:*`, `secretsmanager:GetSecretValue`, `cloudwatch:*` |

---

## ⚠️ 2. Possible Errors, Root Causes & Fixes

### Category A: Configuration & CLI Argument Errors

#### A1. `CRITICAL CONFIG ERROR: 'database_name' is missing or empty...`
- **Symptom**: Job fails immediately at startup during `parse_arguments()`.
- **Root Cause**: Neither `--GLUE_DATABASE` CLI argument nor `pipeline_defaults.glue_catalog.database_name` in `bronze_config.json` was provided.
- **Where to Check**: CloudWatch `/aws-glue/jobs/error`.
- **Fix**:
  - Pass `--GLUE_DATABASE uax_datalake_db_dev` as a Glue Job parameter, OR
  - Update `bronze/script/config/bronze_config.json`:
    ```json
    "glue_catalog": { "database_name": "uax_datalake_db_dev" }
    ```
  - Re-upload `bronze_config.json` to S3:
    ```bash
    aws s3 cp bronze/script/config/bronze_config.json s3://<bucket>/bronze/script/config/bronze_config.json
    ```

#### A2. `Missing required parameter '--SOURCE_SYSTEM'`
- **Symptom**: Job fails at initialization.
- **Root Cause**: No source system name was passed to the Glue Job.
- **Where to Check**: Glue Job Run Parameters in AWS Console.
- **Fix**: Add `--SOURCE_SYSTEM <system_name>` (e.g. `--SOURCE_SYSTEM servicenow`) to Job parameters.

#### A3. `CRITICAL CONFIG ERROR: Invalid JSON in bronze_config.json`
- **Symptom**: `json.decoder.JSONDecodeError` during configuration load.
- **Where to Check**: CloudWatch `/aws-glue/jobs/error` line pointing to `config_loader.py`.
- **Fix**: Validate `bronze_config.json` using `python3 -m json.tool bronze/script/config/bronze_config.json` before uploading to S3.

---

### Category B: Authentication & Secrets Manager Errors

#### B1. `ResourceNotFoundException: Secrets Manager can't find the specified secret`
- **Symptom**: Job terminates when attempting to fetch credentials.
- **Root Cause**: The secret name defined in `--SECRET_NAME` or default `${app_name}/${source}-credentials-${env}` does not exist in AWS Secrets Manager in the current AWS region.
- **Where to Check**:
  - AWS Secrets Manager Console (check the exact secret name and region).
  - CloudWatch logs showing: `Fetching secret: <secret_name>`.
- **Fix**:
  - Create the secret in Secrets Manager:
    ```bash
    aws secretsmanager create-secret \
      --name "uax-datalake/servicenow-credentials-dev" \
      --secret-string '{"base_url":"https://instance.service-now.com","client_id":"...","client_secret":"...","username":"...","password":"..."}'
    ```
  - Or pass the exact secret name using `--SECRET_NAME <correct_name>`.

#### B2. `AccessDeniedException: User/Role is not authorized to perform secretsmanager:GetSecretValue`
- **Symptom**: Job crashes with HTTP 403 / AccessDenied when accessing Secrets Manager.
- **Root Cause**: The Glue IAM Execution Role (`uax-datalake-glue-execution-role-dev`) lacks permission to decrypt/read the secret or KMS key.
- **Where to Check**: IAM Role policies for `uax-datalake-glue-execution-role-dev`.
- **Fix**: Verify Terraform policy in `terraform/1_bronze/bronze.tf` has `secretsmanager:GetSecretValue` allowed for `arn:aws:secretsmanager:*:*:secret:uax-datalake/*`.

#### B3. `HTTP 401 Unauthorized / Invalid Client Credentials`
- **Symptom**: API extraction fails with `401 Unauthorized` response from ServiceNow, Moveworks, or Genesys.
- **Where to Check**:
  - CloudWatch `/aws-glue/jobs/output` showing `OAuth token request failed`.
- **Fix**:
  - Verify credentials inside Secrets Manager.
  - For ServiceNow: Verify that the service account is not locked and has `rest_service` role.
  - For OAuth2: Verify that `client_id` and `client_secret` match the registered external application.

---

### Category C: Source API, Throttling & Network Errors

#### C1. `HTTP 429 Too Many Requests (Rate Limiting)`
- **Symptom**: API calls start failing mid-extraction after thousands of records.
- **Root Cause**: The source system API enforces rate limits (e.g. max 100 requests/minute).
- **Where to Check**: CloudWatch `/aws-glue/jobs/output` logs showing HTTP response codes.
- **Fix**:
  - The built-in `ResilientHttpClient` automatically retries with exponential backoff up to 5 times.
  - If rate limits persist, increase `chunk_size` (e.g. from 1,000 to 5,000) to make fewer requests with larger batch payloads.
  - Adjust `backoff_factor` or `max_retries` in `bronze_config.json`.

#### C2. `HTTP 504 Gateway Timeout / ConnectTimeout`
- **Symptom**: API request hangs and times out after 60+ seconds.
- **Root Cause**: The source table is huge and the date filter query is unindexed upstream.
- **Where to Check**: CloudWatch `/aws-glue/jobs/output` for the exact URL and `sysparm_query` being sent.
- **Fix**:
  - Ensure the watermark column (e.g. `sys_updated_on`) is indexed on the source table.
  - Decrease `chunk_size` so the source system generates smaller query response payloads.

#### C3. Database Connection Failure (JDBC / Relational DB)
- **Symptom**: `OperationalError: could not connect to server: Connection timed out`.
- **Root Cause**: Network path blocked between AWS Glue and database (Security Group, VPC Subnet routing, or NAT Gateway).
- **Where to Check**: VPC Security Groups and Glue Connection settings.
- **Fix**:
  - Ensure Glue Job is associated with a VPC Connection that has outbound access to the database port (e.g., 5432 for Postgres, 3306 for MySQL).

---

### Category D: S3 Storage & Permission Errors

#### D1. `AccessDenied (403) on s3:PutObject`
- **Symptom**: Job fails when writing chunk parquet files to `staging/` or promoting to `bronze/data/`.
- **Where to Check**: S3 bucket policy and Glue IAM role policies.
- **Fix**:
  - Ensure Glue execution role has `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, and `s3:ListBucket` on `arn:aws:s3:::uax-datalake-dev-bucket/*`.
  - If S3 bucket uses KMS encryption, ensure `kms:GenerateDataKey` and `kms:Decrypt` are granted on the KMS Key.

#### D2. Corrupt or Orphan Staging Files
- **Symptom**: Old files lingering in `s3://<bucket>/staging/bronze/`.
- **Root Cause**: Job was killed via hard timeout or AWS Console before `cleanup_failed_staging()` could run.
- **Fix**:
  - Staging files are isolated by `<execution_id>` and never read by downstream Athena/Silver jobs.
  - Clean up manually or rely on S3 lifecycle expiration rule (recommended: expire `staging/` after 3 days):
    ```bash
    aws s3 rm s3://<bucket>/staging/bronze/ --recursive
    ```

---

### Category E: AWS Glue Catalog & Crawler Errors

#### E1. `EntityNotFoundException: Database uax_datalake_db_dev not found`
- **Symptom**: Table partition creation fails with `Database ... not found`.
- **Root Cause**: The Glue Data Catalog database has not yet been created in the target region.
- **Where to Check**: AWS Glue Console → Databases.
- **Fix**:
  - Run Terraform in `terraform/1_bronze` (`terraform apply`), OR
  - Create the database manually:
    ```bash
    aws glue create-database --database-input '{"Name":"uax_datalake_db_dev"}'
    ```

#### E2. `AlreadyExistsException: Partition already exists`
- **Symptom**: Job logs warning during partition registration.
- **Root Cause**: A partition with the exact same `_ingested_at` timestamp was already registered (e.g. from an immediate re-run in the same second).
- **Behavior**: `uax_bronze_load.py` catches this exception safely and updates the existing partition location without failing.

#### E3. `CrawlerRunningException: Crawler ... is already running`
- **Symptom**: Crawler trigger reports warning in CloudWatch.
- **Behavior**: Handled gracefully! The script logs an informative message and completes successfully because raw data is already synced via the Glue Catalog API.

---

### Category F: Serialization & Data Quality Errors

#### F1. `ArrowTypeError: Field type mismatch / Could not convert`
- **Symptom**: PyArrow raises serialization error when creating Parquet table from Python dicts.
- **Root Cause**: A column contains conflicting data types across records (e.g. string in one row, integer in another row).
- **Where to Check**:
  - Check CloudWatch error log for the offending column name.
  - If `error_handling_mode` is `QUARANTINE`: Inspect `s3://<bucket>/quarantine/<source>/<table_name>/<execution_id>/` for the dumped bad record.
- **Fix**:
  - Set column type casting in `bronze_config.json` under `table_configs.<table_name>.column_type_overrides`.

---

## 🛠️ 3. Step-by-Step Troubleshooting Runbooks

### Runbook 1: How to Investigate an Ingestion Failure
1. Go to **AWS Glue Console** → **ETL jobs** → Select `uax-datalake-bronze-ingestion-dev` → **Runs** tab.
2. Click on the failed **Run ID**.
3. Under **Run details**, click **Error logs** to jump directly into CloudWatch `/aws-glue/jobs/error`.
4. Search for `CRITICAL`, `ERROR`, or Python tracebacks.
5. Identify the source system and table that failed.
6. Check if staging was cleaned: verify `s3://<bucket>/staging/bronze/<execution_id>/` was emptied.
7. Address the root cause based on Category A-F above and re-trigger.

---

### Runbook 2: How to Force a Full Historical Refresh (Backfill)
If data was deleted or corrupted upstream and you need to re-pull all historical records:
1. Trigger the Glue Job with `--FULL_REFRESH true`:
   ```bash
   aws glue start-job-run \
     --job-name "uax-datalake-bronze-ingestion-dev" \
     --arguments '{
       "--SOURCE_SYSTEM": "servicenow",
       "--TABLE_NAME": "incident",
       "--FULL_REFRESH": "true"
     }'
   ```
2. The job will ignore `metadata/bronze/servicenow/incident_watermark.json` and ingest from `1970-01-01T00:00:00Z`.
3. Once completed, the watermark state file and `tbl_watermarks` are automatically updated with the latest high-watermark timestamp.

---

### Runbook 3: Useful Diagnostic CLI Commands

```bash
# 1. View current watermark state for a table
aws s3 cp s3://uax-datalake-dev-bucket/metadata/bronze/servicenow/incident_watermark.json -

# 2. Check the latest execution audit log
aws s3 ls s3://uax-datalake-dev-bucket/logs/bronze/servicenow/incident/ --recursive | sort | tail -n 1

# 3. Check for quarantined (DLQ) records
aws s3 ls s3://uax-datalake-dev-bucket/quarantine/ --recursive

# 4. List the latest ingested Bronze partitions
aws s3 ls s3://uax-datalake-dev-bucket/bronze/data/servicenow/incident/

# 5. Query the Bronze table via Amazon Athena
# SELECT _ingested_at, count(*) FROM "uax_datalake_db_dev"."raw_tbl_incident" GROUP BY _ingested_at ORDER BY _ingested_at DESC LIMIT 10;
```
