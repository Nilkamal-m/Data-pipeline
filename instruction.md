# AWS Data Pipeline - Terraform Community Module Deployment Guide (instruction.md)

This document provides a step-by-step guide for packaging code, deploying AWS infrastructure using **Official AWS Community Modules (`terraform-aws-modules`)** directly in the `terraform/` path, and executing manual tests for the **HR-Datalake Pipeline**.

---

## 🏗️ 1. Architecture & Terraform Path Layout

All infrastructure configuration files reside directly inside the `/Users/nilkamalmahato/Documents/Data-pipeline/terraform` directory:

```text
terraform/
├── 1_bronze.tf          # S3 Bucket, Secrets Manager, Glue IAM Role (via terraform-aws-modules)
├── 2_silver.tf          # Glue Catalog DB, Iceberg Crawler, & PySpark ETL Job
├── 3_athena.tf          # Athena WorkGroup & query result configurations
├── 4_step_functions.tf  # SNS Topic, Step Functions/EventBridge IAM (via terraform-aws-modules), 3 State Machines
├── variables.tf         # Centralized variables & pre-existence safety toggles
└── outputs.tf           # Pipeline resource ARNs and names
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

## 🚀 3. Executing Infrastructure from the `terraform/` Path

Navigate to the `terraform/` path and run Terraform directly:

```bash
cd terraform
terraform init
terraform apply -auto-approve
```

Alternatively, you can target individual layers part-by-part from the `terraform/` directory:

```bash
# Deploy Bronze Layer only
terraform apply -target=module.s3_bucket -target=module.glue_iam_role -target=aws_glue_job.bronze_ingestion_job -auto-approve

# Deploy Silver Layer
terraform apply -target=aws_glue_catalog_database.silver_db -target=aws_glue_job.silver_iceberg_job -auto-approve

# Deploy Step Functions & SNS Orchestration
terraform apply -target=module.sns_topic -target=aws_sfn_state_machine.servicenow_orchestrator -auto-approve
```

---

## 🛡️ 4. Skip Pre-Existing Services (Ignore If Present)

If a service already exists by name in your AWS account, pass the safety flag during `terraform apply`:

```bash
# Reusing an existing S3 Bucket and Glue IAM Role
terraform apply -var="use_existing_s3_bucket=true" -var="use_existing_iam_role=true" -auto-approve
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
