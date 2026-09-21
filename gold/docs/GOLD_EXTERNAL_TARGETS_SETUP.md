# Enterprise Knowledge Guide: Connecting Gold Layer to External Data Warehouses (RDS MySQL, Redshift Spectrum, Snowflake)

## 1. Architectural Overview & Design Philosophy

The UAX Data Lakehouse architecture separates computation, storage, and presentation:
- **Silver Layer**: Apache Iceberg tables stored in Amazon S3 (`s3://<bucket>/silver/data/<source>/<table_name>/`) registered in the AWS Glue Data Catalog (`uax_datalake_db_<env>`).
- **Gold Serving Layer**:
  1. **Mandatory Foundation (Athena View)**: Every Gold mart query (`gold/query/<source>/v_<table_name>.sql`) is compiled and registered **first** as an Athena Presentation View (`v_<table_name>`) in the AWS Glue Data Catalog. This serves as the single source of truth for all enterprise reporting.
  2. **Multi-Target Serving (Downstream)**: After the Athena view is registered, the pipeline can expose the data to downstream platforms:
     - **RDS / Aurora MySQL**: Pre-aggregated marts loaded via JDBC with zero-downtime atomic table swaps (`gold_tbl_<table_name>`) and presentation views (`v_<table_name>`).
     - **Amazon Redshift (Spectrum)**: Zero-copy access directly over the Glue Catalog and S3 Iceberg datasets without moving data.
     - **Snowflake (External Iceberg)**: Zero-copy External Iceberg Tables managed by AWS Glue Data Catalog and Snowflake External Volumes.

```
                     +-----------------------------------+
                     |        Bronze Layer (S3)          |
                     +-----------------------------------+
                                       |
                                       v
                     +-----------------------------------+
                     |      Silver Iceberg Tables        |
                     | (Glue Catalog: tbl_<table_name>)  |
                     +-----------------------------------+
                                       |
                                       v
                     +-----------------------------------+
                     |   MANDATORY FIRST STEP:           |
                     |   Athena Presentation View        |
                     |   (Glue Catalog: v_<table_name>)  |
                     +-----------------------------------+
                                       |
         +-----------------------------+-----------------------------+
         |                             |                             |
         v                             v                             v
+-------------------+        +--------------------+        +---------------------+
| RDS / Aurora MySQL|        |  Amazon Redshift   |        |      Snowflake      |
| gold_tbl_<mart>   |        |  Redshift Spectrum |        |  External Iceberg   |
| v_<mart>          |        |  (Zero-Copy View)  |        |  (Zero-Copy View)   |
+-------------------+        +--------------------+        +---------------------+
```

---

## 2. Target 1: Amazon RDS / Aurora MySQL Setup

### 2.1 Early Setup & Prerequisites
1. **Network / VPC Setup**:
   - AWS Glue runs inside a managed VPC subnet. To reach RDS/Aurora, AWS Glue requires an **AWS Glue Network Connection** associated with private subnets and a security group.
   - **Security Group Ingress Rule**: The RDS security group must allow inbound MySQL traffic on port `3306` from the Glue security group.
   - **Self-Referencing Rule**: AWS Glue requires its own security group to have an inbound rule allowing all traffic from itself (`SG -> SG: ALL traffic`).
2. **S3 VPC Endpoint**:
   - If AWS Glue runs inside private subnets without an Internet Gateway or NAT Gateway, an **S3 Gateway VPC Endpoint** must be attached to the VPC route tables so Glue can read S3 configuration and data.

### 2.2 AWS Glue Connection Setup
1. In the **AWS Glue Console** -> **Data Catalog** -> **Connections** -> **Create connection**:
   - **Connection type**: `Network` (or `JDBC`)
   - **VPC**: Select your application VPC where RDS resides.
   - **Subnet**: Select a private subnet with route to the S3 VPC Endpoint.
   - **Security groups**: Select the Glue security group with self-referencing ingress.
   - **Connection name**: `glue-to-rds-aurora-conn`

### 2.3 AWS Secrets Manager Secret Schema
Create a secret in AWS Secrets Manager (e.g. `uax/aurora/mysql/credentials`):
```json
{
  "host": "aurora-mysql-cluster.cluster-xyz.us-east-1.rds.amazonaws.com",
  "port": 3306,
  "username": "uax_etl_user",
  "password": "YourStrongPasswordHere!",
  "dbname": "enterprise_reporting"
}
```

### 2.4 Initial MySQL Database & User Setup (DBA SQL)
In accordance with the **Zero-DDL Policy**, the pipeline will **never** execute `CREATE DATABASE` or `CREATE SCHEMA`. The target schema must pre-exist:
```sql
-- 1. Create target Gold reporting schema
CREATE DATABASE IF NOT EXISTS enterprise_reporting
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

-- 2. Create ETL service user with restricted permissions
CREATE USER IF NOT EXISTS 'uax_etl_user'@'%' IDENTIFIED BY 'YourStrongPasswordHere!';

-- 3. Grant table and view privileges strictly within the Gold schema
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW
  ON enterprise_reporting.* TO 'uax_etl_user'@'%';

-- 4. Flush privileges
FLUSH PRIVILEGES;
```

### 2.5 Glue Job CLI Parameters for RDS/MySQL
When invoking the Glue job for RDS/MySQL:
```bash
aws glue start-job-run \
  --job-name glue-silver-etl-servicenow \
  --arguments '{
    "--PROCESS_LAYER": "both",
    "--SOURCE_SYSTEM": "servicenow",
    "--GOLD_TARGETS": "athena,aurora",
    "--GOLD_SCHEMA": "enterprise_reporting",
    "--RDS_SECRET_NAME": "uax/aurora/mysql/credentials",
    "--CONNECTION_NAME": "glue-to-rds-aurora-conn",
    "--DATA_LAKE_BUCKET": "uax-datalake-prod-bucket",
    "--GLUE_DATABASE": "uax_datalake_db_prod"
  }'
```

---

## 3. Target 2: Amazon Redshift & Redshift Spectrum Setup

### 3.1 Architecture: Zero-Copy Redshift Spectrum
Amazon Redshift Spectrum allows Redshift queries to read directly from Iceberg tables and Athena presentation views stored in S3 and cataloged in AWS Glue Data Catalog. No data loading or ETL replication is required.

### 3.2 Early Setup & IAM Role for Redshift
1. **Create an IAM Role for Redshift** (e.g. `RedshiftGlueSpectrumRole`):
   - **Trusted Entity**: `redshift.amazonaws.com`
   - **Permissions Policies**:
     - `AWSGlueConsoleFullAccess` (or custom policy allowing `glue:GetDatabase`, `glue:GetDatabases`, `glue:GetTable`, `glue:GetTables`, `glue:GetPartitions`)
     - Custom S3 Read Policy for Data Lake bucket:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::uax-datalake-prod-bucket",
        "arn:aws:s3:::uax-datalake-prod-bucket/*"
      ]
    }
  ]
}
```
2. **Attach IAM Role to Redshift Cluster**:
   - In Redshift Console -> Clusters -> Select cluster -> **Actions** -> **Manage IAM roles** -> Attach `RedshiftGlueSpectrumRole`.

### 3.3 Redshift External Schema DDL
Run the following SQL in your Redshift query editor:
```sql
-- Create an external schema pointing to AWS Glue Data Catalog
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_gold_schema
FROM DATA CATALOG
DATABASE 'uax_datalake_db_prod'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftGlueSpectrumRole'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Query the Silver Iceberg tables or Gold Athena Views directly from Redshift:
SELECT * FROM spectrum_gold_schema.v_interactions LIMIT 10;
SELECT * FROM spectrum_gold_schema.v_incident_kpi LIMIT 10;
```

### 3.4 Glue Job CLI Parameters for Redshift
```bash
aws glue start-job-run \
  --job-name glue-silver-etl-moveworks \
  --arguments '{
    "--PROCESS_LAYER": "gold",
    "--SOURCE_SYSTEM": "moveworks",
    "--GOLD_TARGETS": "athena,redshift",
    "--REDSHIFT_SCHEMA": "spectrum_gold_schema",
    "--REDSHIFT_IAM_ROLE": "arn:aws:iam::123456789012:role/RedshiftGlueSpectrumRole",
    "--DATA_LAKE_BUCKET": "uax-datalake-prod-bucket",
    "--GLUE_DATABASE": "uax_datalake_db_prod"
  }'
```

---

## 4. Target 3: Snowflake Setup (External Iceberg & AWS Glue Catalog)

### 4.1 Architecture: Snowflake External Tables with Glue Catalog
Snowflake supports Apache Iceberg tables natively using AWS Glue Data Catalog as the external catalog source. Snowflake reads data and metadata directly from S3, giving BI users immediate access with zero ETL latency and zero storage duplication.

### 4.2 Step 1: AWS IAM Role & Trust Policy for Snowflake
1. **Create an AWS IAM Role** (e.g. `SnowflakeIcebergGlueRole`):
   - Initial Trust Relationship: Allow current AWS account.
   - Permissions:
     - S3 access to Data Lake bucket (`s3:GetObject`, `s3:GetObjectVersion`, `s3:ListBucket`).
     - Glue Catalog read access (`glue:GetDatabase`, `glue:GetTable`, `glue:GetTables`, `glue:GetPartitions`).
2. **Create Snowflake Storage Integration**:
```sql
CREATE OR REPLACE STORAGE INTEGRATION uax_s3_snowflake_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeIcebergGlueRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://uax-datalake-prod-bucket/silver/', 's3://uax-datalake-prod-bucket/gold/');
```
3. **Update AWS IAM Role Trust Policy**:
   - Run `DESCRIBE INTEGRATION uax_s3_snowflake_int;` in Snowflake.
   - Retrieve `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.
   - Update trust policy of `SnowflakeIcebergGlueRole` in AWS IAM:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```

### 4.3 Step 2: Snowflake External Volume & Catalog Integration
In Snowflake:
```sql
-- 1. Create External Volume pointing to your S3 bucket
CREATE OR REPLACE EXTERNAL VOLUME uax_s3_iceberg_volume
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'uax-s3-prod'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://uax-datalake-prod-bucket/silver/data/'
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeIcebergGlueRole'
      )
    );

-- 2. Create Target Database and Schema
CREATE DATABASE IF NOT EXISTS UAX_ANALYTICS_DB;
CREATE SCHEMA IF NOT EXISTS UAX_ANALYTICS_DB.GOLD_MARTS;

-- 3. Register Iceberg Table backed by AWS Glue Data Catalog
CREATE OR REPLACE ICEBERG TABLE UAX_ANALYTICS_DB.GOLD_MARTS.tbl_interactions
  EXTERNAL_VOLUME = 'uax_s3_iceberg_volume'
  CATALOG = 'AWS_GLUE'
  CATALOG_TABLE_NAME = 'tbl_interactions';

-- 4. Create Reporting View in Snowflake over Iceberg Tables
CREATE OR REPLACE VIEW UAX_ANALYTICS_DB.GOLD_MARTS.v_interactions AS
  SELECT * FROM UAX_ANALYTICS_DB.GOLD_MARTS.tbl_interactions
  WHERE _is_current = 'Y' AND _is_deleted = 'N';
```

### 4.4 Glue Job CLI Parameters for Snowflake
```bash
aws glue start-job-run \
  --job-name glue-silver-etl-moveworks \
  --arguments '{
    "--PROCESS_LAYER": "gold",
    "--SOURCE_SYSTEM": "moveworks",
    "--GOLD_TARGETS": "athena,snowflake",
    "--SNOWFLAKE_DATABASE": "UAX_ANALYTICS_DB",
    "--SNOWFLAKE_SCHEMA": "GOLD_MARTS",
    "--SNOWFLAKE_EXTERNAL_VOLUME": "uax_s3_iceberg_volume",
    "--DATA_LAKE_BUCKET": "uax-datalake-prod-bucket",
    "--GLUE_DATABASE": "uax_datalake_db_prod"
  }'
```

---

## 5. AWS Glue Job CLI Parameters Reference

| Parameter Name | Mandatory | Default | Description |
|---|---|---|---|
| `--PROCESS_LAYER` | Yes | `silver` | Pipeline execution layer: `silver`, `gold`, or `both`. |
| `--SOURCE_SYSTEM` | Yes | - | Source system identifier (e.g. `servicenow`, `moveworks`, `genesys`). |
| `--SOURCE_TABLE_NAME` | No | Config defaults | Comma-separated table list to process (e.g. `tbl_incident,tbl_change_request`). |
| `--DATA_LAKE_BUCKET` | No | Config / Env | Target S3 Data Lake Bucket. |
| `--GLUE_DATABASE` | No | Config default | AWS Glue Catalog database (e.g. `uax_datalake_db_dev`). |
| `--TABLE_PREFIX` | No | `tbl_` | Table prefix for Silver Iceberg tables. |
| `--GOLD_TARGETS` | No | `athena` | Comma-separated downstream targets: `athena`, `aurora`, `redshift`, `snowflake`. |
| `--GOLD_SCHEMA` | If Aurora | - | Target schema in MySQL (required when `aurora` is in targets). |
| `--RDS_SECRET_NAME` | If Aurora | - | AWS Secrets Manager secret name for MySQL credentials. |
| `--CONNECTION_NAME` | If Aurora | - | AWS Glue Network Connection name for VPC routing to RDS. |
| `--REDSHIFT_SCHEMA` | If Redshift | `gold_spectrum_schema` | External schema name in Amazon Redshift. |
| `--REDSHIFT_IAM_ROLE` | If Redshift | - | IAM Role ARN attached to Redshift cluster. |
| `--SNOWFLAKE_DATABASE`| If Snowflake | `UAX_ANALYTICS_DB` | Target database in Snowflake. |
| `--SNOWFLAKE_SCHEMA` | If Snowflake | `GOLD_MARTS` | Target schema in Snowflake. |
| `--SNOWFLAKE_EXTERNAL_VOLUME` | If Snowflake | `UAX_S3_ICEBERG_VOLUME` | External Volume name configured in Snowflake. |

---

## 6. End-to-End Verification & Health Checks

### 6.1 Verify Mandatory Athena View
Execute in the **AWS Athena Console**:
```sql
-- Check view definition and query top 5 records
SHOW CREATE VIEW uax_datalake_db_dev.v_interactions;
SELECT * FROM uax_datalake_db_dev.v_interactions LIMIT 5;
```

### 6.2 Verify Aurora MySQL Target
Execute via MySQL client or bastion host:
```sql
USE enterprise_reporting;
SHOW FULL TABLES WHERE Table_type = 'VIEW';
SELECT COUNT(*) FROM gold_tbl_interactions;
SELECT * FROM v_interactions LIMIT 5;
```

### 6.3 Verify Redshift Spectrum Target
Execute in Amazon Redshift query editor:
```sql
SELECT * FROM svv_external_tables WHERE schemaname = 'spectrum_gold_schema';
SELECT COUNT(*) FROM spectrum_gold_schema.v_interactions;
```

### 6.4 Verify Snowflake Target
Execute in Snowflake Worksheets:
```sql
DESCRIBE ICEBERG TABLE UAX_ANALYTICS_DB.GOLD_MARTS.tbl_interactions;
SELECT COUNT(*) FROM UAX_ANALYTICS_DB.GOLD_MARTS.v_interactions;
```
