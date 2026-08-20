# UAX Data Pipeline Engine

An enterprise-grade, modular, and config-driven serverless data lake pipeline built on **AWS Glue**, **Apache Iceberg**, **AWS Secrets Manager**, **Amazon Athena**, and **AWS Step Functions**, fully automated via modular **Terraform** infrastructure.

---

## 🌟 Core System Features & Capabilities

### 1. Multi-Source Ingestion Engine (Bronze Layer)
*   **REST API Connector**: Native OAuth 2.0 (Client Credentials / Password Flow) and HTTP Basic Auth support with automatic token caching & 401 retry handling (ServiceNow, Moveworks, Genesys).
*   **Database JDBC Connector**: Incremental delta extraction for PostgreSQL, MySQL, Oracle, and SQL Server databases.
*   **S3 File Bucket Ingestion Connector**: Reads raw files directly from external S3 buckets supporting **CSV, Flat, Text, JSON, NDJSON, and Parquet** file formats.
*   **Config-Driven Orchestration**: All extraction logic, API endpoints, delta filters, file patterns, and initial load dates are controlled centrally via `bronze_config.json`.
*   **Strict High-Water Mark (HWM) Enforcement**: Prevents silent data corruption by throwing explicit validation errors if initial load dates are missing/null.
*   **Memory-Safe Streaming & Atomic Staging**: Extracts data in configurable memory chunks, flushes to `_staging/` directories in S3, and atomically promotes files to Bronze partitions ONLY upon successful completion.

### 2. Analytical Storage & Transformation (Silver & Athena Layer)
*   **Apache Iceberg Table Format**: High-performance ACID transactions, time-travel queries, and schema evolution.
*   **PySpark Iceberg ETL Engine**: Performs deduplication, custom data transformations, and SQL `MERGE INTO` (UPSERT / SCD Type 2) into AWS Glue Data Catalog tables.
*   **Automated Schema Crawling**: AWS Glue Crawlers scan Iceberg metadata to sync catalog tables dynamically.
*   **Amazon Athena Analytics Engine**: Dedicated query workgroup enforcing isolated S3 query result storage (`s3://<bucket>/athena-results/`).

### 3. Orchestration & Monitoring (Step Functions & EventBridge)
*   **Serverless Workflow Orchestration**: Step Functions State Machines coordinate Bronze extraction, Silver PySpark ETL, Glue Crawlers, and SNS alerts.
*   **Automated Cron Scheduling**: EventBridge rules trigger state machines on customizable cron schedules.
*   **SNS Notifications**: Sends instant email notifications on pipeline success or failure.

---

## 📁 Repository Directory Structure

```text
Data-pipeline/
├── bronze/                             # Bronze Layer Script & Configuration Artifacts
│   └── script/
│       ├── config/
│       │   └── bronze_config.json      # Centralized Bronze configuration (endpoints, buckets, dates)
│       ├── connectors/                 # Modular Source Connectors
│       │   ├── __init__.py             # Connector factory & type registry
│       │   ├── database.py             # JDBC SQL database connector
│       │   ├── genesys.py              # Genesys Cloud REST API connector
│       │   ├── http_client.py          # Unified Basic Auth & OAuth HTTP client
│       │   ├── moveworks.py            # Moveworks REST API connector
│       │   ├── oauth.py                # OAuth 2.0 token manager with in-memory cache
│       │   ├── s3_file.py              # S3 file connector (CSV, Flat, Text, JSON, Parquet)
│       │   └── servicenow.py           # ServiceNow REST API connector
│       ├── config_loader.py            # Configuration parser & S3 loader module
│       ├── connectors.zip              # Pre-bundled connectors package for AWS Glue
│       └── uax_bronze_load.py          # Main AWS Glue Python Shell job script
├── silver/                             # Silver Layer Transformation Code
│   └── script/
│       ├── config/
│       │   └── silver_config.json      # Silver configuration, deduplication & merge settings
│       ├── custom_transforms/          # Table-specific custom PySpark transformation scripts
│       ├── silver_config_loader.py     # Silver configuration loader module
│       ├── silver_iceberg_etl.py       # Main PySpark Iceberg ETL script
│       └── transformer.py              # Core PySpark DataFrame transformer
├── terraform/                          # 100% Module-based Terraform Infrastructure
│   ├── 1_bronze/
│   │   └── bronze.tf                   # S3 Bucket, Secrets Manager, IAM Roles, Glue Job
│   ├── 2_silver/
│   │   └── silver.tf                   # Glue Catalog DB, Iceberg Crawler, PySpark ETL Job
│   ├── 3_athena/
│   │   └── athena.tf                   # Amazon Athena Query WorkGroup
│   └── 4_step_functions/
│       └── step_functions.tf           # SNS Topics, State Machines, EventBridge Rules
├── ARCHITECTURE.md                     # Universal Technical Architecture Specification
└── README.md                           # Universal Operations & Troubleshooting Manual
```

---

## 🚀 Deployment & Utilization Guide

### Step 1: Deploy Terraform Infrastructure

All Terraform scripts use enterprise private modules (`cps-terraform.anthem.com`). Deploy layer-by-layer:

```bash
# 1. Deploy Bronze Infrastructure (S3 Bucket, Secrets, IAM Role, Glue Ingestion Job)
cd terraform/1_bronze
terraform init
terraform apply -auto-approve

# 2. Deploy Silver Infrastructure (Glue Database, Iceberg Crawler, PySpark Job)
cd ../2_silver
terraform init
terraform apply -auto-approve

# 3. Deploy Athena Analytics WorkGroup
cd ../3_athena
terraform init
terraform apply -auto-approve

# 4. Deploy Step Functions Orchestration & EventBridge Cron Rules
cd ../4_step_functions
terraform init
terraform apply -auto-approve
```

---

### Step 2: Upload Script Artifacts to S3 Bucket

Upload local scripts and configuration files to the newly created S3 data lake bucket (`uax-datalake-dev-bucket`):

```bash
DATA_LAKE_BUCKET="uax-datalake-dev-bucket"

# Upload Bronze Scripts & Config
aws s3 cp bronze/script/uax_bronze_load.py s3://${DATA_LAKE_BUCKET}/bronze/script/uax_bronze_load.py
aws s3 cp bronze/script/config_loader.py s3://${DATA_LAKE_BUCKET}/bronze/script/config_loader.py
aws s3 cp bronze/script/connectors.zip s3://${DATA_LAKE_BUCKET}/bronze/script/connectors.zip
aws s3 cp bronze/script/config/bronze_config.json s3://${DATA_LAKE_BUCKET}/bronze/script/config/bronze_config.json

# Upload Silver Scripts & Config
aws s3 cp silver/script/silver_iceberg_etl.py s3://${DATA_LAKE_BUCKET}/silver/script/silver_iceberg_etl.py
aws s3 cp silver/script/silver_config_loader.py s3://${DATA_LAKE_BUCKET}/silver/script/silver_config_loader.py
aws s3 cp silver/script/transformer.py s3://${DATA_LAKE_BUCKET}/silver/script/transformer.py
aws s3 cp silver/script/config/silver_config.json s3://${DATA_LAKE_BUCKET}/silver/script/config/silver_config.json
```

---

### Step 3: Trigger Pipeline Execution

#### Option A: Trigger Step Functions Orchestrator (Recommended)
Step Functions requires passing **ONLY** `--SOURCE_SYSTEM`. It pulls all endpoints, tables, and settings automatically from `bronze_config.json`:

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:123456789012:stateMachine:uax-datalake-servicenow-orchestrator-dev"
```

#### Option B: Trigger Bronze Glue Job Directly
```bash
aws glue start-job-run \
  --job-name uax-datalake-bronze-ingestion-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "servicenow"
  }'
```

#### Option C: Trigger S3 File Ingestion
```bash
aws glue start-job-run \
  --job-name uax-datalake-bronze-ingestion-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "vendor_s3_files"
  }'
```

---

## 🛠️ Configuration Schema Guide (`bronze_config.json`)

To add or modify source systems, update `bronze/script/config/bronze_config.json`:

```json
{
  "source_systems": {
    "servicenow": {
      "base_url": "https://your-instance.service-now.com",
      "api_endpoint_template": "/api/now/table/{table_name}",
      "default_tables": ["incident", "change_request", "problem"],
      "table_initial_load_dates": {
        "incident": "2024-01-01T00:00:00Z"
      }
    },
    "vendor_s3_files": {
      "type": "s3_file",
      "source_bucket": "external-vendor-data-bucket",
      "file_prefix_template": "raw_feed/{table_name}/",
      "file_format": "csv",
      "delimiter": ",",
      "has_header": true,
      "default_tables": ["employee_feed", "vendor_reports"],
      "table_initial_load_dates": {
        "employee_feed": "2024-01-01T00:00:00Z"
      }
    }
  }
}
```

---

## ⚠️ Comprehensive Error Handling & Troubleshooting Guide

| Issue / Error Message | Root Cause | Resolution |
| :--- | :--- | :--- |
| **`ValueError: Initial load date for table 'X' is null or missing`** | Strict validation rule triggered because `table_initial_load_dates` is not defined for table `X` in `bronze_config.json`. | Add table entry to `table_initial_load_dates` in `bronze_config.json` or pass `--INITIAL_LOAD_DATE "2024-01-01T00:00:00Z"`. |
| **`401 Unauthorized / Token Resolution Failed`** | Expired OAuth 2.0 token or incorrect client credentials in AWS Secrets Manager. | Update secret value in AWS Secrets Manager (`uax-datalake/<source>-credentials-dev`). Engine will auto-retry. |
| **`BucketAlreadyExists / AccessDenied`** | Attempting to create an existing bucket or missing IAM permissions. | Set `-var="use_existing_s3_bucket=true"` in `1_bronze/bronze.tf` to reuse existing bucket. |
| **`S3 Staging Promotion Failure`** | Job failed mid-way before promoting files from `_staging/` to `bronze/`. | Automatic cleanup (`cleanup_failed_staging`) purges uncommitted staging files. Re-run job. |
| **`No non-completed scalar values found in schema`** | Parquet serialization failed due to invalid dynamic schema or complex un-flattened dictionary. | Ensure `"flatten_nested_json": true` is set in `bronze_config.json`. |
| **`AccessDeniedException on CloudWatch Logs`** | IAM execution policy does not allow creating log groups for Glue. | Glue IAM policy is scoped to `arn:aws:logs:*:log-group:/aws-glue/jobs/uax-datalake*`. Ensure job name starts with `uax-datalake`. |
