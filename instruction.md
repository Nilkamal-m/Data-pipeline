# AWS Data Pipeline - 4 Self-Contained Terraform Layer Folders Guide (instruction.md)

This document provides a step-by-step guide for packaging code, deploying AWS infrastructure using **4 Standalone Folders with Embedded Variable Declarations and `terraform-aws-modules`**, and executing manual tests for the **HR-Datalake Pipeline**.

---

## 🏗️ 1. Architecture & 4-Folder Standalone Layout

There is **NO central `variables.tf` file**. All variable declarations, default values, pre-existence safety toggles, resources, community modules (`terraform-aws-modules`), and outputs are embedded directly inside each file:

```text
terraform/
├── 1_bronze/
│   └── bronze.tf          # S3 Bucket, Secrets Manager, Glue IAM Role, Bronze Glue Job + Embedded Bronze Variables
├── 2_silver/
│   └── silver.tf          # Silver S3 Objects, Glue Catalog DB, Iceberg Crawler, Silver PySpark Job + Embedded Silver Variables
├── 3_athena/
│   └── athena.tf          # Athena Results S3 Object & Dedicated Athena WorkGroup + Embedded Athena Variables
└── 4_step_functions/
    └── step_functions.tf  # SNS Alert Topic, Step Functions IAM Role, 3 State Machines, 3 EventBridge Rules + Embedded Step Functions Variables
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
DATA_LAKE_BUCKET="uax-datalake-dev-bucket"

# Upload Bronze Script Artifacts
aws s3 cp bronze/script/uax_bronze_load.py s3://${DATA_LAKE_BUCKET}/bronze/script/uax_bronze_load.py
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

Navigate to each folder and execute `terraform apply` directly:

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

If a service already exists by name in your AWS account, pass the safety flag when running `terraform apply` in that folder:

```bash
# Deploying Bronze layer using an existing S3 Bucket
cd terraform/1_bronze
terraform apply -var="use_existing_s3_bucket=true" -auto-approve

# Deploying Silver layer using an existing Glue Database
cd terraform/2_silver
terraform apply -var="use_existing_glue_database=true" -auto-approve
```

---

## 🧪 5. Manual Testing Execution Examples (AWS CLI)

### Example 5.1: Test ServiceNow Ingestion (Bronze Layer)
```bash
aws glue start-job-run \
  --job-name uax-datalake-bronze-ingestion-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "servicenow",
    "--TABLE_NAME": "incident",
    "--INITIAL_LOAD_DATE": "2024-01-01T00:00:00Z",
    "--SECRET_NAME": "data-lake/servicenow-credentials-dev"
  }'
```

### Example 5.2: Test Silver Iceberg Transformation
```bash
aws glue start-job-run \
  --job-name uax-datalake-silver-iceberg-etl-dev \
  --arguments '{
    "--DATA_LAKE_BUCKET": "uax-datalake-dev-bucket",
    "--GLUE_DATABASE": "uax-datalake-db-dev"
  }'
```

### Example 5.3: Execute Full Pipeline Orchestration
```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:uax-datalake-servicenow-orchestrator-dev"
```
