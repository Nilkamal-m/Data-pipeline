# Deployment Guide: UAX Data Pipeline Engine

This document provides a step-by-step operational guide for **First-Time Initial Deployments (Fix Version Release)** as well as **Subsequent Version Upgrades & Hotfixes**.

---

## 📋 Prerequisites & Initial Environment Checklist

Before beginning the initial deployment, ensure the following tools and permissions are configured:

1. **AWS CLI v2**: Installed and authenticated with permissions to create S3, IAM, Secrets Manager, Glue, Athena, Step Functions, and EventBridge resources.
2. **Terraform CLI (v1.5+)**: Installed and configured to pull modules from the enterprise private registry (`cps-terraform.anthem.com`).
3. **Zip Utility**: Installed (`zip`) to package python connectors.
4. **AWS Region**: Default set to `us-east-1` (or your target deployment region).

---

## 🚀 Part 1: First-Time Deployment Guide (Initial Fix Version)

Follow these exact 6 steps when deploying the UAX Data Pipeline for the very first time in an AWS Account.

```text
Step 1: Deploy Bronze Infrastructure (S3, IAM, Secrets, Job)
   │
   ▼
Step 2: Populate Secrets Manager Credentials
   │
   ▼
Step 3: Package & Upload Python Script Artifacts to S3
   │
   ▼
Step 4: Deploy Silver Infrastructure (Glue DB, Iceberg Crawler, PySpark Job)
   │
   ▼
Step 5: Deploy Athena & Step Functions Orchestration
   │
   ▼
Step 6: Execute First-Time Baseline Pipeline Verification
```

---

### Step 1: Deploy Bronze Infrastructure (Layer 1)

Navigate to `terraform/1_bronze` to create the core S3 data lake bucket, Secrets Manager containers, IAM role, and Bronze Glue job:

```bash
cd terraform/1_bronze

# Initialize Terraform modules
terraform init

# Review execution plan
terraform plan

# Apply infrastructure creation
terraform apply -auto-approve
```

> **Outputs Printed**: Note down `data_lake_s3_bucket_name` (e.g. `uax-datalake-dev-bucket`), `glue_iam_role_name`, and secret names.

---

### Step 2: Populate Initial Credentials in AWS Secrets Manager

Terraform creates placeholder secrets. Update the credentials in AWS Secrets Manager with actual credentials:

#### 1. ServiceNow Secret (Basic Auth)
```bash
aws secretsmanager put-secret-value \
  --secret-id "uax-datalake/servicenow-credentials-dev" \
  --secret-string '{
    "auth_type": "basic",
    "grant_type": "",
    "client_id": "",
    "client_secret": "",
    "username": "ACTUAL_SERVICENOW_USER",
    "password": "ACTUAL_SERVICENOW_PASSWORD",
    "token_url": "",
    "scope": ""
  }'
```

#### 2. Moveworks Secret (OAuth 2.0)
```bash
aws secretsmanager put-secret-value \
  --secret-id "uax-datalake/moveworks-credentials-dev" \
  --secret-string '{
    "auth_type": "oauth2",
    "grant_type": "client_credentials",
    "client_id": "ACTUAL_MOVEWORKS_CLIENT_ID",
    "client_secret": "ACTUAL_MOVEWORKS_CLIENT_SECRET",
    "username": "",
    "password": "",
    "token_url": "https://api.moveworks.ai/rest/v1/oauth/token",
    "scope": "export:read"
  }'
```

#### 3. Genesys Cloud Secret (OAuth 2.0)
```bash
aws secretsmanager put-secret-value \
  --secret-id "uax-datalake/genesys-credentials-dev" \
  --secret-string '{
    "auth_type": "oauth2",
    "grant_type": "client_credentials",
    "client_id": "ACTUAL_GENESYS_CLIENT_ID",
    "client_secret": "ACTUAL_GENESYS_CLIENT_SECRET",
    "username": "",
    "password": "",
    "token_url": "https://login.mypurecloud.com/oauth/token",
    "scope": ""
  }'
```

---

### Step 3: Package & Upload Python Scripts to S3

Package the python connectors package and upload all code and configuration files into `s3://uax-datalake-dev-bucket/`:

```bash
# Return to repository root
cd ../..

# Package connectors zip archive
cd bronze/script
zip -r connectors.zip connectors/ -x "*.pyc" -x "*__pycache__*"
cd ../..

# Define Target Bucket Variable
DATA_LAKE_BUCKET="uax-datalake-dev-bucket"

# Upload Bronze Layer Artifacts
aws s3 cp bronze/script/uax_bronze_load.py s3://${DATA_LAKE_BUCKET}/bronze/script/uax_bronze_load.py
aws s3 cp bronze/script/config_loader.py s3://${DATA_LAKE_BUCKET}/bronze/script/config_loader.py
aws s3 cp bronze/script/connectors.zip s3://${DATA_LAKE_BUCKET}/bronze/script/connectors.zip
aws s3 cp bronze/script/config/bronze_config.json s3://${DATA_LAKE_BUCKET}/bronze/script/config/bronze_config.json

# Upload Silver Layer Artifacts
aws s3 cp silver/script/silver_iceberg_etl.py s3://${DATA_LAKE_BUCKET}/silver/script/silver_iceberg_etl.py
aws s3 cp silver/script/silver_config_loader.py s3://${DATA_LAKE_BUCKET}/silver/script/silver_config_loader.py
aws s3 cp silver/script/transformer.py s3://${DATA_LAKE_BUCKET}/silver/script/transformer.py
aws s3 cp silver/script/config/silver_config.json s3://${DATA_LAKE_BUCKET}/silver/script/config/silver_config.json
```

---

### Step 4: Deploy Silver Infrastructure (Layer 2)

Deploy the AWS Glue Catalog Database (`uax-datalake-db-dev`), Iceberg Crawler, and PySpark Glue Job:

```bash
cd terraform/2_silver
terraform init
terraform apply -auto-approve
```

---

### Step 5: Deploy Athena & Step Functions Orchestration (Layers 3 & 4)

Deploy Amazon Athena query workgroup and Step Functions State Machines with EventBridge cron rules:

```bash
# Deploy Athena Workgroup
cd ../3_athena
terraform init
terraform apply -auto-approve

# Deploy Step Functions Orchestration & EventBridge Rules
cd ../4_step_functions
terraform init
terraform apply -auto-approve
```

---

### Step 6: First-Time Pipeline Verification

Run a test extraction for ServiceNow tables to confirm the initial load:

```bash
# Start ServiceNow End-to-End Orchestrator State Machine
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:uax-datalake-servicenow-orchestrator-dev"
```

Verify logs in CloudWatch Log Groups:
- `/aws-glue/jobs/uax-datalake-bronze-ingestion-dev`
- `/aws-glue/jobs/uax-datalake-silver-iceberg-etl-dev`

---

## 🔄 Part 2: Version Upgrade & Hotfix Release Guide

When updating Python code, connectors, or configuration settings for existing infrastructure:

### 1. Code-Only Updates (No Infrastructure Changes)

```bash
DATA_LAKE_BUCKET="uax-datalake-dev-bucket"

# Re-package connectors zip
cd bronze/script
zip -r connectors.zip connectors/ -x "*.pyc" -x "*__pycache__*"
cd ../..

# Sync updated Python scripts & configs to S3
aws s3 cp bronze/script/uax_bronze_load.py s3://${DATA_LAKE_BUCKET}/bronze/script/uax_bronze_load.py
aws s3 cp bronze/script/connectors.zip s3://${DATA_LAKE_BUCKET}/bronze/script/connectors.zip
aws s3 cp bronze/script/config/bronze_config.json s3://${DATA_LAKE_BUCKET}/bronze/script/config/bronze_config.json

aws s3 cp silver/script/silver_iceberg_etl.py s3://${DATA_LAKE_BUCKET}/silver/script/silver_iceberg_etl.py
aws s3 cp silver/script/config/silver_config.json s3://${DATA_LAKE_BUCKET}/silver/script/config/silver_config.json
```

---

### 2. Infrastructure Updates (Terraform Changes)

To apply updates to Terraform resources:

```bash
# Apply Layer 1 updates
cd terraform/1_bronze && terraform apply -auto-approve

# Apply Layer 2 updates
cd ../2_silver && terraform apply -auto-approve

# Apply Layer 3 updates
cd ../3_athena && terraform apply -auto-approve

# Apply Layer 4 updates
cd ../4_step_functions && terraform apply -auto-approve
```
