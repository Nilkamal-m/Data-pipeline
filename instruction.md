# AWS Glue Bronze Data Pipeline - Deployment & Manual Testing Guide (instruction.md)

This document provides a step-by-step guide for packaging, deploying, and testing the **AWS Glue Bronze Layer REST API & Relational Database Ingestion Pipeline**.

---

## 🏗️ 1. Architecture Overview

- **Main Script**: `incremental_load_handler.py` (AWS Glue Python Shell job script).
- **Zipped Python Dependencies**: `connectors.zip` (containing `config_loader.py` and the `connectors/` directory). Attached to Glue via `--extra-py-files`.
- **Static Configuration**: `bronze_config.json` (uploaded separately to S3 at `s3://<bucket>/scripts/bronze/config/bronze_config.json`).
- **Dynamic CLI Overrides**: Any argument passed via `--SOURCE_SYSTEM`, `--TABLE_NAME`, `--INITIAL_LOAD_DATE`, `--SECRET_NAME`, `--BRONZE_BUCKET`, etc., dynamically overrides the static configuration.

---

## 📦 2. Step-by-Step Code Packaging & S3 Upload

### Step 2.1: Create the Python Dependencies Zip (`connectors.zip`)
From the root of your project directory, zip the `connectors/` package and `config_loader.py`:

```bash
# Navigate to the bronze glue jobs directory
cd glue_jobs/bronze

# Create connectors.zip containing connectors/ and config_loader.py
zip -r connectors.zip connectors/ config_loader.py

# Return to project root
cd ../..
```

---

### Step 2.2: Upload Artifacts to AWS S3 Data Lake Bucket
Upload the main script, the zipped python modules, and `bronze_config.json` to S3:

```bash
# Set your environment S3 bucket name
DATA_LAKE_BUCKET="uax-data-lake-bucket-dev"

# 1. Upload Main Python Shell Script
aws s3 cp glue_jobs/bronze/incremental_load_handler.py \
  s3://${DATA_LAKE_BUCKET}/scripts/bronze/incremental_load_handler.py

# 2. Upload Python Dependencies Zip (--extra-py-files)
aws s3 cp glue_jobs/bronze/connectors.zip \
  s3://${DATA_LAKE_BUCKET}/scripts/bronze/connectors.zip

# 3. Upload Static Configuration JSON separately
aws s3 cp glue_jobs/bronze/config/bronze_config.json \
  s3://${DATA_LAKE_BUCKET}/scripts/bronze/config/bronze_config.json
```

---

## ⚙️ 3. AWS Glue Job Configuration

In the AWS Glue Console (or via CloudFormation), configure your Glue Python Shell Job as follows:

- **Job Name**: `glue-incremental-load-bronze`
- **IAM Role**: `AWSGlueServiceRole-DataLake` (with Secrets Manager & S3 permissions)
- **Type**: `Python Shell`
- **Python Version**: `Python 3.9` or `3.13`
- **Script Path**: `s3://uax-data-lake-bucket-dev/scripts/bronze/incremental_load_handler.py`

### Glue Default Job Parameters:
```text
--extra-py-files        s3://uax-data-lake-bucket-dev/scripts/bronze/connectors.zip
--CONFIG_S3_PATH        s3://uax-data-lake-bucket-dev/scripts/bronze/config/bronze_config.json
--BRONZE_BUCKET         uax-data-lake-bucket-dev
--STATE_BUCKET          uax-data-lake-bucket-dev
--OUTPUT_FORMAT         parquet
--PARQUET_COMPRESSION   snappy
--ERROR_HANDLING_MODE   CONTINUE_ON_ERROR
--CLOUDWATCH_NAMESPACE  UAX/DataPipeline/Ingestion
```

---

## 🧪 4. Manual Testing Execution Examples (AWS CLI & Console)

You can trigger manual test runs directly via AWS CLI or the AWS Glue Console, passing runtime parameters to test specific sources or tables:

### Example 4.1: Test ServiceNow Ingestion (Manual Run)
```bash
aws glue start-job-run \
  --job-name glue-incremental-load-bronze \
  --arguments '{
    "--SOURCE_SYSTEM": "servicenow",
    "--TABLE_NAME": "incident,change_request",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/servicenow-credentials-dev"
  }'
```

### Example 4.2: Test Genesys Cloud Ingestion (Manual Run)
```bash
aws glue start-job-run \
  --job-name glue-incremental-load-bronze \
  --arguments '{
    "--SOURCE_SYSTEM": "genesys",
    "--TABLE_NAME": "conversations",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/genesys-credentials-dev"
  }'
```

### Example 4.3: Test Moveworks Records Ingestion (Manual Run)
```bash
aws glue start-job-run \
  --job-name glue-incremental-load-bronze \
  --arguments '{
    "--SOURCE_SYSTEM": "moveworks",
    "--TABLE_NAME": "interactions",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/moveworks-credentials-dev"
  }'
```

### Example 4.4: Test Relational Database Ingestion (PostgreSQL / MySQL)
```bash
aws glue start-job-run \
  --job-name glue-incremental-load-bronze \
  --arguments '{
    "--SOURCE_SYSTEM": "postgresql",
    "--TABLE_NAME": "orders",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/postgresql-credentials-dev"
  }'
```

---

## 🔄 5. AWS Step Functions Orchestration Configuration

When invoked automatically by AWS Step Functions, pass runtime parameters dynamically in the State Machine task definition:

```json
{
  "Type": "Task",
  "Resource": "arn:aws:states:::glue:startJobRun.sync",
  "Parameters": {
    "JobName": "glue-incremental-load-bronze",
    "Arguments": {
      "--SOURCE_SYSTEM.$": "$.SourceSystem",
      "--TABLE_NAME.$": "$.TableName",
      "--SECRET_NAME.$": "$.SecretName",
      "--INITIAL_LOAD_DATE.$": "$.InitialLoadDate",
      "--CONFIG_S3_PATH": "s3://uax-data-lake-bucket-dev/scripts/bronze/config/bronze_config.json"
    }
  },
  "Next": "TriggerSilverIcebergETL"
}
```

---

## 🔍 6. Verification Checklist

After running a job test:
1. **Check Staging Promotion**:
   Verify raw Parquet records are promoted from `_staging/` to `s3://uax-data-lake-bucket-dev/bronze/<source_system>/<table_name>/year=YYYY/month=MM/day=DD/`.
2. **Check High-Water Mark File**:
   Verify the S3 watermark metadata file exists and was updated:
   `s3://uax-data-lake-bucket-dev/metadata/<source_system>/<table_name>/watermark.json`.
3. **Check CloudWatch Custom Metrics**:
   Open AWS CloudWatch Metrics $\rightarrow$ Namespace `UAX/DataPipeline/Ingestion` $\rightarrow$ inspect `RecordsIngested`, `IngestionDurationSeconds`, and `TableExtractionSuccess`.
