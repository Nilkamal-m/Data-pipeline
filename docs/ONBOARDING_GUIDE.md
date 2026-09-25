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

#### Relational Database — JDBC Ingestion / Aurora Serving
```json
{
  "db_type": "mysql",
  "host": "aurora-cluster.prod.internal",
  "port": 3306,
  "dbname": "enterprise_reporting",
  "username": "uax_app_user",
  "password": "SecureDatabasePassword456!"
}
```

---

## 3. Command-Line Interface (CLI) Reference

Each pipeline layer can be triggered on-demand via Python Shell / PySpark Glue job runs.

### 3.1 Bronze Layer CLI (`uax_bronze_load.py`)
```bash
python3 bronze/script/uax_bronze_load.py \
  --CONFIG_S3_PATH "s3://uax-datalake-config-dev/bronze/config/bronze_config.json" \
  --ENV "dev" \
  --SOURCE_SYSTEM "servicenow" \
  --TABLE_NAME "incident" \
  --BRONZE_BUCKET "uax-datalake-bronze-dev" \
  --STATE_BUCKET "uax-datalake-state-dev" \
  --INITIAL_LOAD_DATE "2024-01-01 00:00:00" \
  --FULL_REFRESH "false"
```

| Parameter | Required? | Description |
| :--- | :--- | :--- |
| `--CONFIG_S3_PATH` | Optional | S3 URI to `bronze_config.json`. If omitted, loads local config. |
| `--ENV` | Optional | Target environment (`dev`, `stage`, `prod`). Defaults to `dev`. |
| `--SOURCE_SYSTEM` | **Required** | Source system key under `source_systems` in config. |
| `--TABLE_NAME` | Optional | Specific table to process. If omitted, runs all tables under source. |
| `--BRONZE_BUCKET` | Optional | Destination S3 bucket for raw Parquet files. |
| `--STATE_BUCKET` | Optional | S3 bucket storing state watermarks JSON. |
| `--INITIAL_LOAD_DATE`| Optional | Overrides table initial start timestamp. |
| `--UPPER_BOUND` | Optional | Upper limit timestamp cutoff for historical backfills. |
| `--FULL_REFRESH` | Optional | Set `true` to ignore S3 state and reload from `initial_load_date`. |

---

### 3.2 Silver Layer CLI (`uax_silver_etl.py`)
```bash
python3 silver/script/uax_silver_etl.py \
  --CONFIG_S3_PATH "s3://uax-datalake-config-dev/silver/config/silver_config.json" \
  --ENV "dev" \
  --SOURCE_SYSTEM "servicenow" \
  --TABLE_NAME "tbl_incident" \
  --DATA_LAKE_BUCKET "uax-datalake-silver-dev" \
  --STATE_BUCKET "uax-datalake-state-dev" \
  --FULL_REFRESH "false"
```

| Parameter | Required? | Description |
| :--- | :--- | :--- |
| `--CONFIG_S3_PATH` | Optional | S3 URI to `silver_config.json`. Defaults to local config if omitted. |
| `--ENV` | Optional | Target environment (`dev`, `stage`, `prod`). |
| `--SOURCE_SYSTEM` | **Required** | Source system key (e.g., `servicenow`, `genesys`). |
| `--TABLE_NAME` | Optional | Target Silver Iceberg table name (e.g., `tbl_incident`). |
| `--DATA_LAKE_BUCKET`| Optional | S3 bucket containing Bronze data and Silver Iceberg tables. |
| `--STATE_BUCKET` | Optional | S3 bucket holding Silver watermarks JSON. |
| `--FULL_REFRESH` | Optional | Set `true` to re-process all Bronze partitions from the beginning. |

---

### 3.3 Gold Layer CLI (`gold_layer_manager.py`)
```bash
python3 gold/script/gold_layer_manager.py \
  --CONFIG_S3_PATH "s3://uax-datalake-config-dev/gold/config/gold_config.json" \
  --ENV "dev" \
  --SOURCE_SYSTEM "moveworks" \
  --TABLE_NAME "interactions" \
  --GOLD_BUCKET "uax-datalake-gold-dev" \
  --FULL_REFRESH "false" \
  --RELOAD_INITIAL "false"
```

| Parameter | Required? | Description |
| :--- | :--- | :--- |
| `--CONFIG_S3_PATH` | Optional | S3 URI to `gold_config.json`. |
| `--ENV` | Optional | Target environment (`dev`, `stage`, `prod`). |
| `--SOURCE_SYSTEM` | **Required** | Source system key (e.g., `moveworks`, `genesys`). |
| `--TABLE_NAME` | Optional | Specific Gold mart table to transform and serve. |
| `--GOLD_BUCKET` | Optional | S3 bucket holding Gold Iceberg data and initial CSV drops. |
| `--FULL_REFRESH` | Optional | Forces complete rebuild of Gold marts from Silver tables. |
| `--RELOAD_INITIAL` | Optional | Forces re-ingestion of archived historical CSV files. |

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
