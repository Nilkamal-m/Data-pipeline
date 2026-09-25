# Engineering Onboarding & Operations Guide

Welcome to the **Enterprise Medallion Data Lakehouse Platform**! This guide provides data engineers with the architectural principles, automated infrastructure deployment instructions, standardized secrets schema, pipeline CLI execution parameters, and an end-to-end recipe to onboard new data sources from Bronze through Silver and Gold.

---

## 1. Environment & Infrastructure Setup

The entire data pipeline infrastructure, storage, security, and execution runtime environments are fully automated using infrastructure Terraform deployment scripts.

> [!IMPORTANT]
> **Local Environment Setup Is Ignored & Not Required**:
> You do not need to configure local PySpark, Java runtimes, or local databases. Triggering the Terraform deployment scripts automatically provisions and configures all required cloud components across environments (`dev`, `stage`, `prod`):
> * **Storage**: Amazon S3 Data Lake buckets (`bronze`, `silver`, `gold`, `state`) with lifecycle and partitioning rules.
> * **Security & IAM**: Least-privilege IAM service roles and AWS Secrets Manager credential definitions.
> * **Catalogs & Metastore**: AWS Glue Data Catalog databases, table definitions, and operational crawlers.
> * **Serverless Compute**: AWS Glue Job execution environments pre-configured with Spark 3.3, PyArrow, Iceberg runtimes, and external Python library dependencies.
> * **Query Engine**: Amazon Athena workgroups with isolated query result storage.

Once the Terraform scripts are executed for your target environment, the platform is immediately ready for job runs and table onboarding.

---

## 2. Secrets Manager Naming Convention & Schemas

All connection strings, API tokens, and database passwords must be provisioned in **AWS Secrets Manager**. Never commit credentials or endpoint passwords into configuration JSON files or repository source code.

### 2.1 Standardized Secret Naming Structure
Every secret follows the enterprise naming format:
```
{appname}/<sourcename>-credentials-{env}
```
* **Application Name (`appname`)**: `uax-datalake`
* **Source Name (`<sourcename>`)**: Upstream or downstream system identifier (e.g., `servicenow`, `genesys`, `moveworks`, `aurora`)
* **Environment (`{env}`)**: `dev`, `stage`, or `prod`

#### Concrete Secret Identifier Examples:
* **Bronze ServiceNow (Dev)**: `uax-datalake/servicenow-credentials-dev`
* **Bronze Genesys (Prod)**: `uax-datalake/genesys-credentials-prod`
* **Bronze Moveworks (Dev)**: `uax-datalake/moveworks-credentials-dev`
* **Bronze PostgreSQL (Stage)**: `uax-datalake/postgresql-credentials-stage`
* **Gold Aurora MySQL (Dev)**: `uax-datalake/aurora-credentials-dev`

---

### 2.2 Expected Secret JSON Schemas

#### REST API — OAuth2 Client Credentials (e.g., Genesys, Moveworks)
```json
{
  "auth_type": "oauth",
  "token_url": "https://login.mypurecloud.com/oauth/token",
  "client_id": "00000000-0000-0000-0000-000000000000",
  "client_secret": "abcdefghijklmnopqrstuvwxyz123456",
  "grant_type": "client_credentials",
  "api_base_url": "https://api.mypurecloud.com",
  "batch_size": "100"
}
```

#### REST API — Basic Authentication (e.g., ServiceNow)
```json
{
  "auth_type": "basic",
  "api_base_url": "https://your-instance.service-now.com",
  "username": "svc_uax_datalake",
  "password": "SuperSecretPassword123!",
  "batch_size": "1000"
}
```

#### Relational Database — Aurora MySQL Serving / RDS Ingestion
```json
{
  "host": "aurora-cluster.cluster-xyz.us-east-1.rds.amazonaws.com",
  "port": 3306,
  "username": "pipeline_app_user",
  "password": "SecureDatabasePassword456!",
  "engine": "mysql"
}
```
* **`host`**: Aurora MySQL / RDS cluster writer endpoint URL.
* **`port`**: Port number (`3306`).
* **`username`**: Database application username.
* **`password`**: User password.
* **Target Schema**: Specified via `--GOLD_SCHEMA` (or `aurora.schema` in `gold_config.json`, e.g. `enterprise_reporting`). Mandatory per zero-fallback policy.

---

## 3. AWS Glue Job Execution & Arguments Reference

Each pipeline layer runs as an **AWS Glue Job** orchestrated by AWS Step Functions or triggered via `aws glue start-job-run`. Parameters follow a strict hierarchy: **Glue Job Arguments (`--KEY value`) take top priority; if omitted, the job resolves parameters from its JSON configuration.**

---

### 3.1 Bronze Layer Glue Job Arguments (`uax_bronze_load.py`)

```bash
aws glue start-job-run \
  --job-name "glue-bronze-servicenow-dev" \
  --arguments '{
    "--JOB_NAME": "glue-bronze-servicenow-dev",
    "--SOURCE_SYSTEM": "servicenow",
    "--ENV": "dev",
    "--SOURCE_TABLE_NAME": "incident",
    "--CONFIG_S3_PATH": "s3://uax-datalake-config-dev/bronze/config/bronze_config.json",
    "--BRONZE_BUCKET": "uax-datalake-bronze-dev",
    "--STATE_BUCKET": "uax-datalake-state-dev"
  }'
```

| Parameter | Required? | Fallback Behavior if Omitted | Description |
| :--- | :--- | :--- | :--- |
| `--JOB_NAME` | **Mandatory** | None | Unique name of the AWS Glue job run. |
| `--SOURCE_SYSTEM` | **Mandatory** | None | Target source system key (e.g. `servicenow`, `genesys`, `moveworks`). |
| `--ENV` | Optional | `dev` | Target deployment environment (`dev`, `stage`, `prod`). |
| `--CONFIG_S3_PATH` | Optional | `s3://{bronze_bucket}/bronze/script/config/bronze_config.json` | S3 URI to `bronze_config.json`. |
| `--SOURCE_TABLE_NAME` | Optional | All tables configured under `source_systems.<source>.tables` | Specific table or comma-separated list of tables to extract. |
| `--BRONZE_BUCKET` | Optional | `pipeline_defaults.bronze_bucket` in config | S3 bucket destination for raw Parquet files. |
| `--STATE_BUCKET` | Optional | `pipeline_defaults.state_bucket` or `--BRONZE_BUCKET` | S3 bucket holding watermark state JSON files. |
| `--SECRET_NAME` | Optional | `source_config.secret_name` in config | AWS Secrets Manager secret holding API credentials. |
| `--BATCH_SIZE` | Optional | `source_config.batch_size` or default `1000` | Records per API page / JDBC streaming batch. |
| `--INITIAL_LOAD_DATE` | Optional | `initial_load_date` in table config | Manual override start timestamp. |
| `--UPPER_BOUND` | Optional | Current job start UTC timestamp | Upper limit timestamp cutoff for extraction. |
| `--FLATTEN_NESTED_JSON` | Optional | `source_config.flatten_nested_json` or `true` | Flatten nested JSON payloads (`true`/`false`). |
| `--ERROR_HANDLING_MODE` | Optional | `CONTINUE_ON_ERROR` | Error policy: `CONTINUE_ON_ERROR` or `HALT_ON_ERROR`. |

---

### 3.2 Silver Layer Glue Job Arguments (`uax_silver_etl.py`)

```bash
aws glue start-job-run \
  --job-name "glue-silver-servicenow-dev" \
  --arguments '{
    "--JOB_NAME": "glue-silver-servicenow-dev",
    "--SOURCE_SYSTEM": "servicenow",
    "--ENV": "dev",
    "--SOURCE_TABLE_NAME": "tbl_incident",
    "--CONFIG_S3_PATH": "s3://uax-datalake-config-dev/silver/config/silver_config.json"
  }'
```

| Parameter | Required? | Fallback Behavior if Omitted | Description |
| :--- | :--- | :--- | :--- |
| `--JOB_NAME` | **Mandatory** | None | Unique name of the AWS Glue job run. |
| `--SOURCE_SYSTEM` | **Mandatory** | None | Upstream source identifier matching `silver_config.json`. |
| `--ENV` | Optional | `dev` | Target environment (`dev`, `stage`, `prod`). |
| `--CONFIG_S3_PATH` | Optional | `s3://{data_lake_bucket}/silver/script/config/silver_config.json` | S3 URI to `silver_config.json`. |
| `--SOURCE_TABLE_NAME` | Optional | All tables configured under `source_systems.<source>.tables` | Specific Silver Iceberg table name to process. |
| `--PROCESS_LAYER` | Optional | `silver` | Pipeline stage: `silver`, `gold`, or `both`. |
| `--DATA_LAKE_BUCKET` | Optional | `pipeline_defaults.data_lake_bucket` in config | S3 bucket containing Bronze data and Silver Iceberg tables. |
| `--GLUE_DATABASE` | Optional | `pipeline_defaults.glue_database` or `uax_datalake_db_{env}` | AWS Glue Data Catalog database name. |
| `--TABLE_PREFIX` | Optional | `pipeline_defaults.table_prefix` or `tbl_` | Standard prefix for conformed Silver tables. |
| `--FULL_REFRESH` | Optional | `false` | Set `true` to reprocess all Bronze partitions from the beginning. |
| `--INCREMENTAL` | Optional | `true` | Enables incremental filtering using `_ingested_at > last_watermark`. |

---

### 3.3 Gold Layer Glue Job Arguments (`gold_layer_manager.py`)

```bash
aws glue start-job-run \
  --job-name "glue-gold-moveworks-dev" \
  --arguments '{
    "--JOB_NAME": "glue-gold-moveworks-dev",
    "--SOURCE_SYSTEM": "moveworks",
    "--ENV": "dev",
    "--TABLE_NAME": "interactions",
    "--GOLD_SCHEMA": "enterprise_reporting",
    "--RDS_SECRET_NAME": "uax-datalake/aurora-credentials-dev",
    "--GOLD_CONFIG_S3_PATH": "s3://uax-datalake-config-dev/gold/config/gold_config.json"
  }'
```

| Parameter | Required? | Fallback Behavior if Omitted | Description |
| :--- | :--- | :--- | :--- |
| `--JOB_NAME` | **Mandatory** | None | Unique name of the AWS Glue job run. |
| `--SOURCE_SYSTEM` | **Mandatory** | None | Target business domain source (e.g. `moveworks`, `genesys`). |
| `--GOLD_SCHEMA` | **Required for Aurora** | `aurora.schema` in `gold_config.json` | Target MySQL schema (e.g. `enterprise_reporting`). No fallback allowed. |
| `--RDS_SECRET_NAME` | **Required for Aurora** | `source_systems.<source>.secret_name` in config | AWS Secrets Manager secret holding Aurora MySQL credentials. |
| `--ENV` | Optional | `dev` | Target environment (`dev`, `stage`, `prod`). |
| `--GOLD_CONFIG_S3_PATH`| Optional | `s3://{gold_bucket}/gold/script/config/gold_config.json` | S3 URI to `gold_config.json`. |
| `--TABLE_NAME` | Optional | All marts configured under `source_systems.<source>.tables` | Specific Gold mart table to transform and serve. |
| `--DATA_LAKE_BUCKET` / `--GOLD_BUCKET` | Optional | `pipeline_defaults.gold_bucket` in config | S3 bucket holding Gold Iceberg data and initial CSV drops. |
| `--GLUE_DATABASE` | Optional | `pipeline_defaults.glue_database` or `uax_datalake_db_{env}` | AWS Glue Data Catalog database name. |
| `--GOLD_TARGETS` | Optional | `source_config.target_engines` in config | Comma-separated serving targets (e.g. `aurora,athena,databricks,redshift`). |
| `--FULL_REFRESH` | Optional | `false` | Forces complete rebuild of Gold marts from Silver tables. |
| `--INCREMENTAL` | Optional | `true` | Toggles incremental delta evaluation. |

---

## 4. End-to-End Onboarding: Adding a New Entity (Bronze $\to$ Silver $\to$ Gold)

Follow this recipe to onboard an entity from source extraction to business serving:

### Step 1: Bronze Ingestion
1. Provision the credential secret in AWS Secrets Manager: `uax-datalake/acme_crm-credentials-{env}`.
2. Add the table definition in `bronze/script/config/bronze_config.json`:
   ```json
   "acme_crm": {
     "base_url": "https://api.acmecrm.com",
     "api_endpoint_template": "/v1/{table_name}",
     "tables": {
       "customers": {
         "initial_load_date": "2024-01-01 00:00:00"
       }
     }
   }
   ```
3. Run the Bronze job to ingest raw data to S3.
   * Data lands at: `s3://{bronze_bucket}/bronze/data/acme_crm/customers/_ingested_at={ISO8601_TIMESTAMP}/`
   * Watermark state lands at: `s3://{state_bucket}/metadata/bronze/acme_crm/customers/watermark.json`

### Step 2: Silver Conformation & Iceberg Persistence
1. Add the conformed table specification in `silver/script/config/silver_config.json`:
   ```json
   "acme_crm": {
     "tables": {
       "tbl_customers": {
         "source_table_name": "raw_tbl_customers",
         "nkey": "customer_id",
         "deduplication_keys": ["customer_id"],
         "deduplication_order_by": ["updated_at", "_ingested_at"],
         "merge_strategy": "upsert",
         "scd_type": "scd1"
       }
     }
   }
   ```
2. Run the Silver job. Verify the Iceberg table in Athena:
   ```sql
   SELECT * FROM "uax_datalake_db_dev"."tbl_customers" LIMIT 10;
   ```
   * Iceberg data lands at: `s3://{silver_bucket}/silver/data/acme_crm/tbl_customers/`
   * Watermark state lands at: `s3://{state_bucket}/metadata/silver/acme_crm_tbl_customers_watermark.json`

### Step 3: Gold Dimensional Mart & Multi-Target Serving
1. Create SQL transformation query `gold/query/acme_crm/customers.sql`:
   ```sql
   SELECT
       c.customer_id,
       c.company_name,
       c.plan_type,
       c.created_at,
       c.updated_at AS _updated_at
   FROM
       uax_datalake_db_{env}.tbl_customers c
   WHERE
       c.is_active = 'true'
   ```
2. Register the mart in `gold/script/config/gold_config.json`:
   ```json
   "acme_crm": {
     "target_engines": ["athena", "aurora"],
     "tables": {
       "customers": {
         "nkey": ["customer_id"],
         "incremental": true,
         "aurora": {
           "schema": "enterprise_reporting",
           "table_name": "gold_acme_customers"
         }
       }
     }
   }
   ```
3. Run the Gold manager to build the Athena Iceberg mart and sync downstream to Aurora MySQL via zero-downtime PK UPSERT.
