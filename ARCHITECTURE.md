# Technical Architecture Specification: UAX Data Pipeline

This document details the low-level technical architecture, connector design, state machine orchestration, and layer data flows of the **UAX Data Pipeline**.

---

## 🏗️ 1. End-to-End System Architecture

```text
               +-------------------------------------------------------------+
               |                  EVENTBRIDGE CRON SCHEDULES                 |
               +------------------------------+------------------------------+
                                              |
                                              v
               +-------------------------------------------------------------+
               |             AWS STEP FUNCTIONS STATE MACHINES               |
               +------------------------------+------------------------------+
                                              |
                     +------------------------+------------------------+
                     |                                                 |
                     v                                                 v
   +------------------------------------+           +------------------------------------+
   |    SOURCE REST APIs / SQL DBs      |           |     EXTERNAL S3 FILE BUCKETS       |
   | (ServiceNow, Moveworks, Genesys)   |           |    (CSV, Flat, Text, JSON, Parquet) |
   +-----------------+------------------+           +-----------------+------------------+
                     |                                                 |
                     +------------------------+------------------------+
                                              |
                                              v
               +-------------------------------------------------------------+
               |            BRONZE INGESTION ENGINE (AWS GLUE)               |
               |                 (uax_bronze_load.py)                        |
               |                                                             |
               |   1. Reads Secrets Manager Credentials                     |
               |   2. Fetches High-Water Mark (watermark.json)               |
               |   3. Memory-Safe Batch Extraction -> S3 _staging/          |
               |   4. Atomic Staging -> Bronze Promotion                     |
               |   5. Updates HWM State File & CloudWatch Metrics            |
               +------------------------------+------------------------------+
                                              |
                                              v
                +-------------------------------------------------------------+
                |             RAW BRONZE PARQUET DATA (S3)                    |
                |   s3://uax-datalake-dev-bucket/bronze/data/<source>/<table/ |
                +------------------------------+------------------------------+
                                               |
                                               v
                +-------------------------------------------------------------+
                |            SILVER ICEBERG ETL ENGINE (AWS GLUE)             |
                |                (silver_iceberg_etl.py)                      |
                |                                                             |
                |   1. Deduplicates Raw Bronze Parquet Records                |
                |   2. Applies Business Transformations                       |
                |   3. Executes SQL MERGE INTO (UPSERT / SCD2 History)        |
                +------------------------------+------------------------------+
                                               |
                                               v
                +-------------------------------------------------------------+
                |            SILVER APACHE ICEBERG DATA LAKE (S3)             |
                |   s3://uax-datalake-dev-bucket/silver/data/<table_name>/    |
                +------------------------------+------------------------------+
                                              |
                     +------------------------+------------------------+
                     |                                                 |
                     v                                                 v
   +------------------------------------+           +------------------------------------+
   |   AWS GLUE DATA CATALOG DATABASE   |           |    AMAZON ATHENA QUERY WORKGROUP   |
   |      (uax-datalake-db-dev)         |           |       (uax-datalake-workgroup-dev) |
   +------------------------------------+           +------------------------------------+
```

---

## 🧩 2. Bronze Ingestion Layer Specification (`bronze/script/`)

The Bronze Layer is designed around a **Factory Pattern** connecting multiple source systems via specialized connector classes.

### Connector Registry (`connectors/`)

| Connector Class | File Path | Extraction Mechanism | Authentication Support | Supported Formats |
| :--- | :--- | :--- | :--- | :--- |
| **`ServiceNowConnector`** | `connectors/servicenow.py` | REST API Table API | Basic Auth / OAuth 2.0 | JSON |
| **`MoveworksConnector`** | `connectors/moveworks.py` | REST API Export API | OAuth 2.0 Client Credentials | JSON |
| **`GenesysConnector`** | `connectors/genesys.py` | REST API Analytics API | OAuth 2.0 Client Credentials | JSON |
| **`DatabaseConnector`** | `connectors/database.py` | JDBC SQL Query Extraction | Database Username/Password | Relational Rows |
| **`S3FileConnector`** | `connectors/s3_file.py` | S3 Object Listing & File Streaming | AWS IAM Role / Cross-Account Keys | CSV, Flat, Text, JSON, NDJSON, Parquet |

---

### Step-by-Step Bronze Ingestion Pipeline (`uax_bronze_load.py`)

1. **Parameter Resolution**:
   - Parses CLI arguments (`--SOURCE_SYSTEM`, `--TABLE_NAME`, `--CONFIG_S3_PATH`).
   - Downloads `bronze_config.json` from S3 or local disk.
   - Extracts source configuration parameters.

2. **Secret Retrieval**:
   - Calls `boto3.client('secretsmanager').get_secret_value()` to fetch API credentials.
   - Falls back gracefully to empty dictionary `{}` if no secret name is passed (e.g. S3 file ingestion).

3. **High-Water Mark (HWM) Resolution**:
   - Downloads `s3://<bucket>/metadata/<source>/<table_name>_state.json`.
   - Resolves `last_load_date`. If state file does not exist, looks up `table_initial_load_dates` in `bronze_config.json`.
   - **Strict Validation**: If initial load date is null or missing, raises `ValueError`.

4. **Chunked Extraction & Staging**:
   - Connects to source API / Database / S3 Bucket.
   - Flushes extracted records in memory batches (e.g. 10,000 records) to temporary staging:
     `s3://<bucket>/_staging/exec_<execution_id>/<source>/<table_name>/part_XXXX.parquet`
   - Enriches each record with audit metadata (`_ingested_at`, `_source_system`, `_table_name`, `_execution_id`).

5. **Atomic Promotion**:
   - Copies staging objects to final Bronze partition:
     `s3://<bucket>/bronze/data/<source>/<table_name>/year=YYYY/month=MM/day=DD/`
   - Deletes staging files.

6. **State & Metrics Update**:
   - Updates `watermark.json` with the current run timestamp.
   - Reports custom ingestion metrics to CloudWatch (`UAX/DataPipeline/Ingestion`).

---

## 🧊 3. Silver Layer Apache Iceberg Specification (`silver/script/`)

The Silver Layer converts Bronze raw Parquet files into ACID-compliant **Apache Iceberg** analytical tables.

### Key Components:
1. **`silver_iceberg_etl.py`**:
   - PySpark Glue 4.0 job configured with Apache Iceberg extensions (`IcebergSparkSessionExtensions`).
   - Reads raw Bronze files from `s3://<bucket>/bronze/data/<source>/<table_name>/`.
2. **`transformer.py`**:
   - Deduplicates records using PySpark window functions `ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY updated_at DESC)`.
   - Applies schema casting, null replacements, and timestamp normalizations.
3. **`silver_config.json`**:
   - Configures table-specific merge strategies (`upsert`, `insert_only`, `scd2`).
4. **AWS Glue Iceberg Crawler**:
   - Crawls `s3://<bucket>/silver/data/` to keep Glue Data Catalog schemas up-to-date.

---

## 📊 4. Amazon Athena Analytics Specification (`terraform/3_athena/`)

1. **Athena WorkGroup**: `uax-datalake-workgroup-dev`
2. **Result Isolation**: All query results are automatically saved to `s3://uax-datalake-dev-bucket/athena-results/`.
3. **Engine Version**: Athena Engine Version 3 (Presto / Trino based).

---

## 🔄 5. State Machine Orchestration (`terraform/4_step_functions/`)

Step Functions State Machines coordinate the pipeline steps:

```text
[Start EventBridge Cron]
        │
        v
[Task 1: Execute Bronze Ingestion Job (uax_bronze_load.py)]
        │
        +---> (On Failure) ---> [Send SNS Alert Email]
        │
        v
[Task 2: Execute Silver PySpark ETL Job (silver_iceberg_etl.py)]
        │
        +---> (On Failure) ---> [Send SNS Alert Email]
        │
        v
[Task 3: Run Glue Iceberg Crawler]
        │
        v
[Task 4: Send SNS Success Notification]
```
