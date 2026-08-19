# AWS Data Pipeline - Terraform Deployment & Operational Guide (instruction.md)

This document provides a step-by-step guide for packaging code, deploying AWS infrastructure using **HashiCorp Terraform**, and executing manual tests for the **UAX Data Lake Pipeline**.

---

## 🏗️ 1. Architecture Overview

- **Terraform Infrastructure**: Managed under `terraform/` (`main.tf`, `iam.tf`, `step_functions.tf`, `variables.tf`, `outputs.tf`).
- **Single S3 Data Lake Bucket**: `s3://uax-data-lake-bucket-${environment}/` containing partitioned data folders (`bronze/`, `silver/`, `metadata/`, `athena-results/`) and script folders (`bronze/script/`, `silver/script/`).
- **Glue Bronze Python Shell Job**: Ingests REST APIs (ServiceNow, Moveworks, Genesys) and Databases (PostgreSQL, MySQL, Oracle, SQL Server).
- **Glue Silver PySpark Iceberg ETL Job**: Deduplicates and transforms raw Bronze records into curated Apache Iceberg tables with SCD Type 1 UPSERT and SCD Type 2 history tracking.
- **Orchestration**: 3 AWS Step Functions State Machines triggered automatically by EventBridge Cron Schedules.

---

## 📦 2. Code Packaging & S3 Upload

Before applying Terraform, package your Python dependencies into `.zip` files and upload script artifacts to S3:

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

## 🚀 3. Deploy Infrastructure via Terraform

### Step 3.1: Initialize & Validate Terraform
```bash
cd terraform

# Initialize Terraform AWS provider
terraform init

# Validate configuration syntax
terraform validate
```

### Step 3.2: Review & Apply Infrastructure Plan
```bash
# Review proposed resources
terraform plan \
  -var="environment=dev" \
  -var="data_lake_bucket_name=uax-data-lake-bucket" \
  -var="alert_email_address=your-email@company.com"

# Deploy infrastructure to AWS
terraform apply \
  -var="environment=dev" \
  -var="data_lake_bucket_name=uax-data-lake-bucket" \
  -var="alert_email_address=your-email@company.com" \
  -auto-approve
```

---

## 🧪 4. Manual Testing Execution Examples (AWS CLI)

You can trigger manual test runs directly via AWS CLI, passing runtime parameters to test specific sources or tables:

### Example 4.1: Test ServiceNow Ingestion (Bronze Layer)
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

### Example 4.2: Test Genesys Cloud Ingestion (Bronze Layer)
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

### Example 4.3: Test Moveworks Ingestion (Bronze Layer)
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

### Example 4.4: Test Silver PySpark Apache Iceberg Transformation
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

## 🔄 5. Step Functions Orchestration Triggers

Trigger full pipeline state machine executions manually:

```bash
# Execute ServiceNow End-to-End State Machine
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:uax-data-pipeline-servicenow-orchestrator-dev"
```

---

## 🧹 6. Destroying Infrastructure

To tear down all deployed resources:
```bash
cd terraform
terraform destroy -auto-approve
```
