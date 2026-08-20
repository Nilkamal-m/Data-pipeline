# AWS Data Pipeline - Single Standalone Terraform Deployment & Manual Testing Guide (instruction.md)

This document provides a step-by-step guide for packaging code, deploying AWS infrastructure using a **Single Self-Contained Standalone Terraform Executable (`main.tf`)**, and executing manual tests for the **UAX Data Lake Pipeline**.

---

## 🏗️ 1. Architecture Overview

- **Single Executable File**: `terraform/main.tf` (All Bronze, Silver, Athena, SNS, Step Functions, and EventBridge resources defined in 1 standalone file).
- **Pre-Existence Protection**: Includes `use_existing_*` boolean safety variables so that if a service (S3 bucket, IAM role, Glue database, Athena workgroup) already exists in your AWS account, Terraform safely reuses the existing service without throwing a creation conflict error!

---

## 📦 2. Code Packaging & S3 Upload

Before executing Terraform, package your Python dependencies into `.zip` files and upload script artifacts to S3:

### Step 2.1: Zip Bronze Python Dependencies
```bash
# Navigate to bronze script directory
cd bronze/script

# Zip connectors package and config_loader.py
zip -r connectors.zip connectors/ config_loader.py

# Return to root directory
cd ../..
```

### Step 2.2: Upload Scripts & Config Artifacts to S3
```bash
DATA_LAKE_BUCKET="uax-data-lake-bucket-dev"

# Upload Bronze Script Artifacts
aws s3 cp bronze/script/incremental_load_handler.py s3://${DATA_LAKE_BUCKET}/bronze/script/incremental_load_handler.py
aws s3 cp bronze/script/connectors.zip s3://${DATA_LAKE_BUCKET}/bronze/script/connectors.zip
aws s3 cp bronze/script/config/bronze_config.json s3://${DATA_LAKE_BUCKET}/bronze/script/config/bronze_config.json

# Upload Silver Script Artifacts
aws s3 cp silver/script/silver_iceberg_etl.py s3://${DATA_LAKE_BUCKET}/silver/script/silver_iceberg_etl.py
aws s3 cp silver/script/silver_config_loader.py s3://${DATA_LAKE_BUCKET}/silver/script/silver_config_loader.py
aws s3 cp silver/script/transformer.py s3://${DATA_LAKE_BUCKET}/silver/script/transformer.py
aws s3 cp silver/script/config/silver_config.json s3://${DATA_LAKE_BUCKET}/silver/script/config/silver_config.json
```

---

## 🚀 3. Single-Command Terraform Deployment

### Step 3.1: Run Full Deployment (Single Executable File)
Execute `terraform apply` directly inside `terraform/`. All variables are defined inside `main.tf` with default values:

```bash
cd terraform
terraform init
terraform apply -auto-approve
```

---

### 🛡️ 4. Handling Pre-Existing AWS Resources (Ignore If Present)

If an S3 bucket, IAM role, Glue Database, or Athena Workgroup already exists in your AWS account, pass the safety flag to reuse the existing service without throwing creation conflict errors:

```bash
cd terraform

# Example: Reuse existing S3 Bucket and Glue Database
terraform apply \
  -var="use_existing_s3_bucket=true" \
  -var="use_existing_glue_database=true" \
  -auto-approve
```

#### Available Pre-Existence Safety Toggles in `main.tf`:
- `-var="use_existing_s3_bucket=true"`: Reuses existing `uax-data-lake-bucket-dev` bucket.
- `-var="use_existing_iam_role=true"`: Reuses existing Glue execution IAM role.
- `-var="use_existing_glue_database=true"`: Reuses existing `uax_data_lake_db_dev` Glue database.
- `-var="use_existing_athena_workgroup=true"`: Reuses existing Athena workgroup.

---

## 🧪 5. Manual Testing Execution Examples (AWS CLI)

### Example 5.1: Test ServiceNow Ingestion (Bronze Layer)
```bash
aws glue start-job-run \
  --job-name uax-data-pipeline-bronze-ingestion-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "servicenow",
    "--TABLE_NAME": "incident",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/servicenow-credentials-dev"
  }'
```

### Example 5.2: Test Genesys Cloud Ingestion (Bronze Layer)
```bash
aws glue start-job-run \
  --job-name uax-data-pipeline-bronze-ingestion-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "genesys",
    "--TABLE_NAME": "conversations",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/genesys-credentials-dev"
  }'
```

### Example 5.3: Test Moveworks Ingestion (Bronze Layer)
```bash
aws glue start-job-run \
  --job-name uax-data-pipeline-bronze-ingestion-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "moveworks",
    "--TABLE_NAME": "interactions",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/moveworks-credentials-dev"
  }'
```

### Example 5.4: Test Silver PySpark Apache Iceberg Transformation
```bash
aws glue start-job-run \
  --job-name uax-data-pipeline-silver-iceberg-etl-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "servicenow",
    "--TABLE_NAME": "incident",
    "--MERGE_STRATEGY": "upsert"
  }'
```

---

## 🔄 6. Step Functions Orchestration Triggers

Trigger full pipeline state machine executions manually:

```bash
# Execute ServiceNow End-to-End State Machine
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:uax-data-pipeline-servicenow-orchestrator-dev"
```

---

## 🧹 7. Destroying Infrastructure

To tear down all deployed resources:
```bash
cd terraform
terraform destroy -auto-approve
```
