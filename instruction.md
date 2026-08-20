# AWS Data Pipeline - 4-File Modular Terraform Deployment Guide (instruction.md)

This document provides a step-by-step guide for packaging code, deploying AWS infrastructure using **4 distinct self-contained Terraform layer files** (`1_bronze.tf`, `2_silver.tf`, `3_athena.tf`, `4_step_functions.tf`), and executing manual tests for the **UAX Data Lake Pipeline**.

---

## 🏗️ 1. Architecture & 4-File Layout

The Terraform infrastructure is divided into **4 dedicated layer files** inside `terraform/`:

```text
terraform/
├── variables.tf             # Shared variables & pre-existence safety skip toggles
├── 1_bronze.tf              # FILE 1: Core S3 Bucket, OAuth Secrets, Bronze IAM Role, Bronze Glue Job
├── 2_silver.tf              # FILE 2: Silver S3 Objects, Glue Catalog DB, Iceberg Crawler, Silver PySpark Job
├── 3_athena.tf              # FILE 3: Athena S3 Object & Athena Dedicated WorkGroup
├── 4_step_functions.tf      # FILE 4: Step Functions IAM, SNS Topic, 3 State Machines, 3 EventBridge Cron Rules
├── outputs.tf               # Layered output values
└── terraform.tfvars.example # Example variable values file
```

---

## 📦 2. Code Packaging & S3 Upload

Before deploying Terraform, package your Python dependencies into `.zip` files and upload script artifacts to S3:

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

## 🚀 3. Deploying Specific Layer Files Individually

You can deploy each `.tf` file independently using Terraform target commands:

### Step 3.1: Initialize Terraform
```bash
cd terraform
terraform init
```

### Step 3.2: Deploy FILE 1 - Bronze Layer (`1_bronze.tf`)
```bash
terraform apply \
  -target=aws_s3_bucket.data_lake \
  -target=aws_secretsmanager_secret.servicenow_secret \
  -target=aws_secretsmanager_secret.moveworks_secret \
  -target=aws_secretsmanager_secret.genesys_secret \
  -target=aws_iam_role.glue_execution_role \
  -target=aws_glue_job.bronze_ingestion_job \
  -auto-approve
```

### Step 3.3: Deploy FILE 2 - Silver Layer (`2_silver.tf`)
```bash
terraform apply \
  -target=aws_glue_catalog_database.silver_db \
  -target=aws_glue_crawler.silver_iceberg_crawler \
  -target=aws_glue_job.silver_iceberg_job \
  -auto-approve
```

### Step 3.4: Deploy FILE 3 - Athena WorkGroup (`3_athena.tf`)
```bash
terraform apply \
  -target=aws_athena_workgroup.data_pipeline \
  -auto-approve
```

### Step 3.5: Deploy FILE 4 - Step Functions & SNS (`4_step_functions.tf`)
```bash
terraform apply -auto-approve
```

---

## 🛡️ 4. Skip Pre-Existing Services (Ignore If Present)

If a service already exists in your AWS account by name, set the corresponding safety toggle to `true` in `variables.tf` or pass it via CLI to skip creation and reuse the existing service:

```bash
cd terraform

# Example: Reuse existing S3 Bucket, Glue Database, and Athena Workgroup
terraform apply \
  -var="use_existing_s3_bucket=true" \
  -var="use_existing_glue_database=true" \
  -var="use_existing_athena_workgroup=true" \
  -auto-approve
```

#### Available Pre-Existence Safety Toggles:
- `-var="use_existing_s3_bucket=true"`: Reuses existing `uax-data-lake-bucket-dev` bucket.
- `-var="use_existing_iam_role=true"`: Reuses existing Glue execution IAM role.
- `-var="use_existing_secrets=true"`: Reuses existing Secrets Manager secrets.
- `-var="use_existing_glue_database=true"`: Reuses existing `uax_data_lake_db_dev` database.
- `-var="use_existing_athena_workgroup=true"`: Reuses existing Athena workgroup.
- `-var="use_existing_sns_topic=true"`: Reuses existing SNS alert topic.
- `-var="use_existing_step_functions_role=true"`: Reuses existing Step Functions IAM role.

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

### Example 5.2: Test Silver PySpark Apache Iceberg Transformation
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
