# AWS Glue Incremental Data Ingestion Pipeline

A modular, serverless data pipeline designed for **incremental (delta) data ingestion** from REST API sources (ServiceNow, Genesys, Moveworks) into S3 Bronze storage using **AWS Glue Python Shell** and **S3 JSON Metadata State Files** for High-Water Mark state tracking.

---

## 📁 Repository Structure

```text
Data-pipeline/.
├── bronze/                           # Bronze Layer Glue Python Shell Ingestion Code
│   └── script/                       # Bronze script directory (maps to s3://<bucket>/bronze/script/)
│       ├── config/
│       │   └── bronze_config.json    # Centralized Bronze static configuration & defaults
│       ├── connectors/               # Extensible API & RDBMS Connector Plugin Modules
│       │   ├── __init__.py           # Connector factory (get_connector)
│       │   ├── database.py           # Relational DB (PostgreSQL, MySQL, Oracle, SQL Server) connector
│       │   ├── genesys.py            # Genesys Analytics API connector module
│       │   ├── http_client.py        # Central HTTP GET client with 401 refresh & exponential backoff
│       │   ├── moveworks.py          # Moveworks OData API connector module
│       │   ├── oauth.py              # OAuth 2.0 Client Credentials & Password Token Manager
│       │   └── servicenow.py         # ServiceNow REST API connector module
│       ├── config_loader.py          # Bronze configuration loader class
│       └── uax_bronze_load.py # Main AWS Glue Python Shell job script
├── silver/                           # Silver Layer Glue Iceberg Transformation Code
│   └── script/                       # Silver script directory (maps to s3://<bucket>/silver/script/)
│       ├── config/
│       │   └── silver_config.json    # Centralized Silver configuration, merge & SCD2 settings
│       ├── custom_transforms/        # Table-specific custom PySpark transformation scripts
│       ├── silver_config_loader.py   # Silver configuration loader class
│       ├── transformer.py            # Declarative & custom script transformation engine
│       └── silver_iceberg_etl.py     # PySpark Iceberg ETL script
├── terraform/                        # Terraform Infrastructure as Code (IaC)
│   ├── main.tf                       # Provider, S3, Secrets Manager, Glue Jobs, Crawler, Athena, SNS, EventBridge
│   ├── iam.tf                        # IAM Roles & Policies for Glue, Step Functions, EventBridge
│   ├── step_functions.tf             # ServiceNow, Moveworks, and Genesys Step Functions State Machines
│   ├── variables.tf                  # Input variables
│   ├── outputs.tf                    # Resource outputs
│   └── terraform.tfvars.example      # Example input variables file
├── instruction.md                    # Step-by-step packaging, Terraform deployment, and testing manual
├── docs/
│   ├── incremental_load_architecture.md # Technical architectural guide & diagrams
│   └── step_functions_orchestration.json# Step Functions state machine JSON definition
└── README.md                         # Project documentation and quickstart guide
```

---

## ⚡ Features & Advantages

- **Single S3 Data Lake Bucket**: All layers (`bronze/`, `silver/`, `athena-results/`, `metadata/`, `scripts/bronze/`, `scripts/silver/`) organized cleanly under isolated S3 folder prefixes within a single bucket (`s3://<DataLakeBucketName>/`).
- **S3 JSON High-Water Mark Tracking**: Automatically manages load timestamps in an S3 metadata JSON file (`s3://<bucket>/metadata/<source_system>/watermark.json`).
- **Modular API Connectors**: Decoupled connector modules under `glue_jobs/bronze/connectors/` with dynamic factory loading.
- **Serverless & Cost-Effective**: Powered by AWS Glue Python Shell (1/16 DPU), incurring minimal cost compared to Spark.
- **Fail-Safe Integrity**: State file `last_load_date` is updated **only** when data extraction and S3 persistence succeed.
- **Partitioned S3 Storage**: Writes raw payloads into Hive-compatible partition directories (`year=YYYY/month=MM/day=DD`).

---

## 🛠️ Required Glue Job Arguments

| Argument Key | Required | Description | Default Fallback |
|---|---|---|---|
| `--SOURCE_SYSTEM` | Yes | Target API source (`servicenow`, `genesys`, `moveworks`) | None (Required) |
| `--SECRET_NAME` | No | Secrets Manager secret name for API credentials | `data-lake/{source_system}-credentials` |
| `--BRONZE_BUCKET` | No | Target S3 bucket name for raw Bronze data storage | `uax-data-lake-bucket` |
| `--STATE_BUCKET` | No | Target S3 bucket for metadata JSON state file | Value of `--BRONZE_BUCKET` |
| `--INITIAL_LOAD_DATE` | No | Fallback high-water mark for 1st execution | `2024-01-01T00:00:00Z` |
| `--OUTPUT_FORMAT` | No | Target serialization format (`parquet`, `json`) | `parquet` |
| `--PARQUET_COMPRESSION` | No | Compression algorithm for Parquet (`snappy`, `gzip`) | `snappy` |
| `--ERROR_HANDLING_MODE` | No | Multi-table error policy (`CONTINUE_ON_ERROR`, `HALT_ON_ERROR`) | `CONTINUE_ON_ERROR` |
| `--CLOUDWATCH_NAMESPACE` | No | Custom CloudWatch metrics namespace | `UAX/DataPipeline/Ingestion` |

---

## 🚀 Deployment & Usage

### Step 1: Upload Scripts & Config to Single S3 Bucket
Upload the local `glue_jobs/` folders to your single S3 Data Lake Bucket (`s3://<YOUR_DATA_LAKE_BUCKET>/`):

1. **Upload Bronze Scripts (`s3://<YOUR_DATA_LAKE_BUCKET>/scripts/bronze/`)**:
   - Main Ingestion Script: [`bronze/script/uax_bronze_load.py`](file:///Users/nilkamalmahato/Documents/Data-pipeline/bronze/script/uax_bronze_load.py) $\rightarrow$ `s3://<YOUR_DATA_LAKE_BUCKET>/bronze/script/uax_bronze_load.py`
   - Config Loader: [`glue_jobs/bronze/config_loader.py`](file:///Users/nilkamalmahato/Documents/Data-pipeline/glue_jobs/bronze/config_loader.py) $\rightarrow$ `s3://<YOUR_DATA_LAKE_BUCKET>/scripts/bronze/config_loader.py`
   - Connectors Package: Zip [`glue_jobs/bronze/connectors/`](file:///Users/nilkamalmahato/Documents/Data-pipeline/glue_jobs/bronze/connectors/) into `connectors.zip` $\rightarrow$ `s3://<YOUR_DATA_LAKE_BUCKET>/scripts/bronze/connectors.zip`
   - Pipeline Config: [`glue_jobs/bronze/config/pipeline_config.json`](file:///Users/nilkamalmahato/Documents/Data-pipeline/glue_jobs/bronze/config/pipeline_config.json) $\rightarrow$ `s3://<YOUR_DATA_LAKE_BUCKET>/scripts/bronze/config/pipeline_config.json`

2. **Upload Silver Scripts (`s3://<YOUR_DATA_LAKE_BUCKET>/scripts/silver/`)**:
   - Silver PySpark Iceberg ETL Script: [`glue_jobs/silver/silver_iceberg_etl.py`](file:///Users/nilkamalmahato/Documents/Data-pipeline/glue_jobs/silver/silver_iceberg_etl.py) $\rightarrow$ `s3://<YOUR_DATA_LAKE_BUCKET>/scripts/silver/silver_iceberg_etl.py`

### Step 2: Deploy Full Infrastructure via CloudFormation
Deploy the single-bucket infrastructure stack using CloudFormation:

```bash
aws cloudformation deploy \
  --template-file infrastructure/cloudformation.yml \
  --stack-name uax-data-pipeline-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    Environment=dev \
    AppName=uax-data-pipeline \
    DataLakeBucketName=uax-data-lake-bucket \
    AlertEmailAddress=your-email@company.com \
    ScheduleExpression="cron(0 6 * * ? *)"
```

### Step 4: Test Execution via AWS CLI
```bash
aws glue start-job-run \
  --job-name uax-data-pipeline-api-ingestion-dev \
  --arguments '{
    "--SOURCE_SYSTEM": "servicenow",
    "--SECRET_NAME": "data-lake/servicenow-credentials-dev",
    "--BRONZE_BUCKET": "uax-data-pipeline-bronze-bucket-dev"
  }'
```

---

## 📖 Architecture & Design

For in-depth details on the High-Water Mark state workflow, REST API delta query formulations, and Step Functions orchestration patterns, see [`docs/incremental_load_architecture.md`](file:///Users/nilkamalmahato/Documents/Data-pipeline/docs/incremental_load_architecture.md).
