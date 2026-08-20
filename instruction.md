# AWS Data Pipeline - Modular Part-by-Part Terraform Deployment & Manual Testing Guide (instruction.md)

This document provides a step-by-step guide for packaging code, deploying AWS infrastructure part-by-part using **HashiCorp Terraform** (`bronze.tf` $\rightarrow$ `silver.tf` $\rightarrow$ `step_functions.tf`), and executing manual tests for the **UAX Data Lake Pipeline**.

---

## 🏗️ 1. Layered Architecture Overview

The Terraform infrastructure is divided into **3 distinct layer files** under `terraform/`:

```text
terraform/
├── provider.tf              # Terraform AWS Provider setup
├── variables.tf             # Input variables (environment, app_name, data_lake_bucket_name)
├── bronze.tf                # PART 1: Core S3 Bucket, Secrets Manager Secrets, Bronze IAM Role, Bronze Glue Job
├── silver.tf                # PART 2: Silver S3 Objects, Glue Catalog DB, Iceberg Crawler, Silver PySpark Job, Athena
├── step_functions.tf        # PART 3: Step Functions IAM, SNS Topic, 3 State Machines, 3 EventBridge Cron Rules
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

## 🚀 3. Part-by-Part Terraform Deployment

### Step 3.1: Initialize Terraform
```bash
cd terraform
terraform init
```

### Step 3.2: Deploy PART 1 - Core & Bronze Layer (`bronze.tf`)
To deploy **ONLY** the Bronze layer (S3 Bucket, OAuth Secrets, Bronze Glue Job):
```bash
terraform apply \
  -target=aws_s3_bucket.data_lake \
  -target=aws_secretsmanager_secret.servicenow_secret \
  -target=aws_secretsmanager_secret.moveworks_secret \
  -target=aws_secretsmanager_secret.genesys_secret \
  -target=aws_iam_role.glue_execution_role \
  -target=aws_glue_job.bronze_ingestion_job \
  -var="environment=dev" \
  -var="data_lake_bucket_name=uax-data-lake-bucket" \
  -var="alert_email_address=your-email@company.com" \
  -auto-approve
```

### Step 3.3: Deploy PART 2 - Silver Layer (`silver.tf`)
To deploy the Silver Iceberg ETL layer (Glue Catalog Database, Crawler, PySpark Iceberg ETL Job, Athena WorkGroup):
```bash
terraform apply \
  -target=aws_glue_catalog_database.silver_db \
  -target=aws_glue_crawler.silver_iceberg_crawler \
  -target=aws_glue_job.silver_iceberg_job \
  -target=aws_athena_workgroup.data_pipeline \
  -var="environment=dev" \
  -var="data_lake_bucket_name=uax-data-lake-bucket" \
  -var="alert_email_address=your-email@company.com" \
  -auto-approve
```

### Step 3.4: Deploy PART 3 - Step Functions & Orchestration (`step_functions.tf`)
To deploy Orchestration (SNS Alert Topic, Step Functions State Machines, EventBridge Cron Rules):
```bash
terraform apply \
  -var="environment=dev" \
  -var="data_lake_bucket_name=uax-data-lake-bucket" \
  -var="alert_email_address=your-email@company.com" \
  -auto-approve
```

---

## 🧪 4. Manual Testing Execution Examples (AWS CLI)

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
