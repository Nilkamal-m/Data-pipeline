# AWS Data Pipeline - Self-Contained 4-Folder Terraform Deployment Guide (instruction.md)

This document provides a step-by-step guide for packaging code, deploying AWS infrastructure using **4 100% Self-Contained Layer Folders** (`1_bronze/`, `2_silver/`, `3_athena/`, `4_step_functions/`), and executing manual tests for the **UAX Data Lake Pipeline**.

---

## 🏗️ 1. Architecture & 4-Folder Layout

Each layer folder is **completely standalone** and contains all its own variables, defaults, pre-existence skip toggles, resources, and outputs inside a single file. There is **NO shared/common `variables.tf` file**:

```text
terraform/
├── 1_bronze/
│   └── bronze.tf          # Core S3 Bucket, Secrets Manager Secrets, Glue IAM Role, Bronze Glue Job
├── 2_silver/
│   └── silver.tf          # Silver S3 Objects, Glue Catalog DB, Iceberg Crawler, Silver PySpark Job
├── 3_athena/
│   └── athena.tf          # Athena Results S3 Object & Dedicated Athena WorkGroup
└── 4_step_functions/
    └── step_functions.tf  # SNS Alert Topic, Step Functions IAM Role, 3 State Machines, 3 EventBridge Cron Rules
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

## 🚀 3. Executing Layer Folders Independently

Navigate to each folder and execute `terraform apply` directly. All variables and defaults are defined inside that folder's file:

### Step 3.1: Deploy 1. Bronze Layer (`terraform/1_bronze/bronze.tf`)
```bash
cd terraform/1_bronze
terraform init
terraform apply -auto-approve
cd ../..
```

### Step 3.2: Deploy 2. Silver Layer (`terraform/2_silver/silver.tf`)
```bash
cd terraform/2_silver
terraform init
terraform apply -auto-approve
cd ../..
```

### Step 3.3: Deploy 3. Athena WorkGroup (`terraform/3_athena/athena.tf`)
```bash
cd terraform/3_athena
terraform init
terraform apply -auto-approve
cd ../..
```

### Step 3.4: Deploy 4. Step Functions & SNS (`terraform/4_step_functions/step_functions.tf`)
```bash
cd terraform/4_step_functions
terraform init
terraform apply -auto-approve
cd ../..
```

---

## 🛡️ 4. Skip Pre-Existing Services (Ignore If Present)

If a service already exists by name in your AWS account, pass the safety flag when running `terraform apply` in that folder to skip creating it and reuse the existing service:

```bash
# Example: Deploying Bronze layer using an existing S3 Bucket
cd terraform/1_bronze
terraform apply -var="use_existing_s3_bucket=true" -auto-approve

# Example: Deploying Silver layer using an existing Glue Database
cd terraform/2_silver
terraform apply -var="use_existing_glue_database=true" -auto-approve

# Example: Deploying Athena layer using an existing Athena Workgroup
cd terraform/3_athena
terraform apply -var="use_existing_athena_workgroup=true" -auto-approve

# Example: Deploying Step Functions layer using an existing SNS Alert Topic
cd terraform/4_step_functions
terraform apply -var="use_existing_sns_topic=true" -auto-approve
```

---

## 🧪 5. Manual Testing Execution Examples (AWS CLI)

### Example 5.1: Test ServiceNow Ingestion (Bronze Layer)
```bash
aws glue start-job-run \
  --job-name hr-datalake-bronze-ingestion-dev \
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
  --job-name hr-datalake-silver-iceberg-etl-dev \
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
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:hr-datalake-servicenow-orchestrator-dev"
```

---

## 🧹 7. Destroying Infrastructure

To tear down resources for a specific layer:
```bash
cd terraform/1_bronze && terraform destroy -auto-approve
cd terraform/2_silver && terraform destroy -auto-approve
cd terraform/3_athena && terraform destroy -auto-approve
cd terraform/4_step_functions && terraform destroy -auto-approve
```
