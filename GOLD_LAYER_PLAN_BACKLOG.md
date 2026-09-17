# Backlog Architecture & Implementation Specification: Simplified Silver & Multi-Target Gold Serving Layer (Shared DB Safe & Highly Observable)

> [!NOTE]
> **Status: REVIEW & EXECUTION READY**  
> **Core Principle: Simple, Safe, and Zero Over-Engineering.**  
> Designed for effortless maintenance by an individual data engineer, preserving 100% of working Silver logic, strictly isolated for a **shared MySQL database**, with rich step-by-step developer debugging and schema introspection logs.

---

## 1. Process Layer Options: Strictly 2 Options (`silver` and `gold`)

The pipeline runtime accepts strictly **two** values for `--PROCESS_LAYER`:
1. `--PROCESS_LAYER=silver`: Executes only Silver Apache Iceberg transformation and merge (Default).
2. `--PROCESS_LAYER=gold`: Executes only Gold SQL discovery, transformation, and database load.

*(Any value other than `silver` or `gold` will raise an immediate validation error).*

---

## 2. High-Visibility Observability & Developer Debugging Engine

To ensure an individual data engineer can effortlessly troubleshoot, audit, and debug issues months later, both Silver and Gold implement comprehensive logging:

### A. Step-by-Step Progress & Lifecycle Logs
Every major operation logs an explicit banner:
```text
[GOLD STEP 1/5] Validating target schema 'gold_marts' in MySQL... [EXISTS]
[GOLD STEP 2/5] Executing query 's3://bucket/gold/query/incident_kpi.sql' over Silver Iceberg...
[GOLD STEP 3/5] Discovered 14 columns. Materializing parquet to s3://bucket/gold/data/incident_kpi/ (12,450 rows in 3.12s)
[GOLD STEP 4/5] Writing to staging table 'gold_marts.gold_tbl_incident_kpi_staging' via Spark JDBC...
[GOLD STEP 5/5] Performing atomic table swap:
                - Verified: Target table strictly matches 'gold_tbl_*'
                - RENAME: 'gold_tbl_incident_kpi' -> 'gold_tbl_incident_kpi_old'
                - RENAME: 'gold_tbl_incident_kpi_staging' -> 'gold_tbl_incident_kpi'
                - DROP:   'gold_tbl_incident_kpi_old'
                - VIEW:   CREATE OR REPLACE VIEW v_incident_kpi AS SELECT * FROM gold_tbl_incident_kpi
```

### B. Full Column Schema Introspection & Evolution Tracking
Before and after writing to Silver Iceberg and Gold MySQL, the script logs:
1. **Complete Column List & Data Types**:
   ```text
   [SCHEMA INTROSPECTION] Table: gold_tbl_incident_kpi (14 columns)
   |-- state                  : string
   |-- priority               : string
   |-- category               : string
   |-- total_incidents        : bigint
   |-- avg_resolution_seconds : double
   |-- _computed_at           : timestamp
   ```
2. **Schema Evolution Detection**:
   If a new column was added to the S3 `.sql` query, it explicitly highlights:
   ```text
   [SCHEMA EVOLUTION] 2 new column(s) detected compared to previous active table:
   ├── Added: 'department_code' (string)
   └── Added: 'sla_breach_flag' (string)
   ```

### C. Structured Error Diagnostics & S3 Audit Persistence
If any step fails (e.g. MySQL connection timeout, bad SQL query syntax, schema missing):
1. **Immediate Contextual Log**:
   ```text
   +================================================================================+
   |  ERROR IN GOLD SERVING ENGINE: TABLE 'incident_kpi' [FAILED]
   +--------------------------------------------------------------------------------+
   |  * Failed Step       : Step 4 (Staging JDBC Write)
   |  * Target Database   : gold_marts
   |  * Target Table      : gold_tbl_incident_kpi_staging
   |  * Query File        : s3://bucket/gold/query/incident_kpi.sql
   |  * Error Type        : java.sql.SQLException
   |  * Error Message     : Access denied for user 'pipeline_user'@'10.189.18.45'
   |  * Stack Trace       : [Full formatted Python / PySpark traceback attached]
   +================================================================================+
   ```
2. **S3 Audit Log**: Written to `s3://<bucket>/metadata/logs/gold/execution_<id>.json` for compliance and historical troubleshooting.

---

## 3. Shared MySQL Database Safety & Strict Schema Verification

> [!CAUTION]
> **Shared Database Policy**: Because this is a **shared MySQL database** with existing business and application tables belonging to other teams:
>
> 1. **No Schema Creation (`NO CREATE DATABASE`)**:
>    - The script **NEVER** runs `CREATE DATABASE` or `CREATE SCHEMA`.
>    - It verifies that the database/schema specified by `--GOLD_SCHEMA` already exists via:
>      ```sql
>      SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s
>      ```
>    - If the schema is **NOT present**, it immediately raises an explicit error and exits:
>      `RuntimeError: Target schema '<GOLD_SCHEMA>' does not exist in MySQL database. Please contact your DBA to provision it.`
> 2. **Zero External Table Modification**: The script **NEVER** deletes, drops, alters, truncates, or inserts into any table outside its designated naming scope.
> 3. **Strict Prefix Guardrails**:
>    - Every target table MUST begin with `gold_tbl_`.
>    - Every staging table MUST begin with `gold_tbl_` and end with `_staging`.
>    - Every presentation view MUST begin with `v_`.
>    - The script implements an explicit programmatic assertion:
>      ```python
>      assert target_table.startswith("gold_tbl_"), f"Safety Error: Invalid table name {target_table}"
>      assert view_name.startswith("v_"), f"Safety Error: Invalid view name {view_name}"
>      ```
> 4. **Isolated Blue/Green Table Swap**:
>    The atomic swap strictly affects only the specific mart being updated:
>    ```sql
>    -- Only gold_tbl_<table_name> and gold_tbl_<table_name>_staging are ever manipulated:
>    DROP TABLE IF EXISTS gold_tbl_<table_name>_old;
>    CREATE TABLE IF NOT EXISTS gold_tbl_<table_name> LIKE gold_tbl_<table_name>_staging;
>    RENAME TABLE gold_tbl_<table_name> TO gold_tbl_<table_name>_old,
>                 gold_tbl_<table_name>_staging TO gold_tbl_<table_name>;
>    DROP TABLE IF EXISTS gold_tbl_<table_name>_old;
>    CREATE OR REPLACE VIEW v_<table_name> AS SELECT * FROM gold_tbl_<table_name>;
>    ```
> 5. **No Wildcards or Sweeping Drops**: No `DROP DATABASE`, no `DROP TABLE LIKE ...`, and no bulk schema-level changes.

---

## 4. Silver Layer: Process Preserved + Future API Enrichment Hook

The core Silver transformation and merge engine remains **100% identical to its current stable implementation** (Apache Iceberg, SCD1 in-place upsert, SCD2 historical versioning, deduplication via natural keys, and system audit columns at the end).

### Inter-Table Calling in Custom Transforms & Future API Enrichment Capability
1. **Calling Other Tables in `custom_transforms/`**:
   - If a table transformation requires data from another table (e.g. user details, department lookups, or category reference tables), the script calls that table directly in `custom_transforms/<script>.py`.
   - `SilverTransformer` supplies `(df, spark, context)` to custom scripts:
     - `spark`: Active SparkSession to query catalog tables: `spark.table(f"glue_catalog.{context['glue_database']}.tbl_<name>")` or `spark.read.parquet(...)`.
     - `context`: Runtime dictionary containing `glue_database`, `source_system`, `table_name`, `data_lake_bucket`, `silver_data_prefix`, and `table_cfg`.
     - Logs external tables called, join keys, and newly added columns.
2. **Extensible API Enrichment Hook**:
   - To satisfy the requirement where a future team can call an external API with specific columns and store the returned result in a new column:
   - We introduce a declarative **`api_enrichment`** hook in `SilverTransformer`.
   - **Current Behavior**: Automatically attaches configured enrichment column(s) initialized with a **`null` string default value** (`lit(None).cast("string")`).
   - **Future Behavior**: When the team provides the API endpoint and credentials, the hook will pass designated source columns, invoke the REST endpoint, and populate the column without modifying any core Silver Iceberg merge logic.

---

## 5. Gold Layer Architecture & S3 Organization

### 1. Single Unified Glue Job
- Both Silver and Gold execute within the **same Glue PySpark job** (`silver/script/uax_silver_etl.py`).
- Controlled via `--PROCESS_LAYER=silver|gold`.

### 2. S3 Directory Structure: All Gold Files Under `bucket/gold/*`
All files relating to the Gold layer reside strictly inside the `s3://<bucket>/gold/*` prefix:

```
s3://<bucket>/gold/
  ├── script/                  # Python helper scripts (e.g. gold_layer_manager.py)
  ├── query/                   # SQL query definition files by source system
  │   ├── <source>/            # Source-specific query directory (e.g. servicenow/)
  │   │   └── <table_name>.sql # Mart SQL query (e.g. incident_kpi.sql)
  │   └── ...
  └── data/                    # Gold layer intermediate/persisted Parquet data
      ├── <source>/            # Source-specific Parquet data (e.g. servicenow/)
      │   └── <table_name>/    # Parquet dataset (e.g. incident_kpi/)
      └── ...
```

### 3. Aurora MySQL Serving Engine (Current Primary Target)
- **Zero Fallback Schema Policy**: Target schema MUST be passed dynamically via `--GOLD_SCHEMA` (e.g. `--GOLD_SCHEMA=enterprise_reporting`). In accordance with organization shared database policy, no fallback schema is permitted; omitting `--GOLD_SCHEMA` raises an immediate guiding `ValueError`. The schema is strictly verified for pre-existence via `INFORMATION_SCHEMA.SCHEMATA` and is never created by the pipeline.
- **Database Password Options**:
  1. **AWS Secrets Manager**: Pass `--RDS_SECRET_NAME <secret_name>` (e.g. `--RDS_SECRET_NAME prod/rds/mysql_credentials`). The pipeline fetches and decodes JSON credentials (`password`, `username`, `host`, `port`) or raw string passwords.
  2. **Manual Password**: Pass `--RDS_PASSWORD <password>` via CLI or set the `RDS_PASSWORD` environment variable.
  3. **AWS Glue Connection**: Pass `--CONNECTION_NAME <connection_name>`.
  4. If no password is provided, raises an explicit `ValueError` guiding the engineer.
- **Simplified Ultimate Query Path**: Discovered strictly from `s3://<bucket>/gold/query/<source>/<table_name>.sql` (e.g. `s3://<bucket>/gold/query/servicenow/incident_kpi.sql`). No multi-directory sprawl or complex fallbacks.
- **Physical Table**: `gold_tbl_<table_name>` (e.g. `gold_tbl_incident_kpi`).
- **Staging Table**: `gold_tbl_<table_name>_staging` (e.g. `gold_tbl_incident_kpi_staging`).
- **Presentation View**: `v_<table_name>` (e.g. `v_incident_kpi`).
- **Blue/Green Swap**: Atomic table swap runs in $<1\text{ms}$ with zero downtime for Power BI.

### 4. Future Options: Amazon Redshift & Snowflake
- Parameter: `--GOLD_TARGET=aurora|redshift|snowflake` (default: `aurora`).
- Clean, modular stubs ready to activate when the enterprise provisions Redshift or Snowflake.

### 5. Simplicity Principle (No Over-Engineering)
- A single, self-contained Python helper `gold/script/gold_layer_manager.py` (~180 lines).
- Straightforward, clean, reliable, and easily maintainable by an individual contributor.

---

## 6. Self-Contained Terraform Configuration (`terraform/2_silver/silver.tf`)

Configured using parameterized variables with zero hardcoded infrastructure IDs:

```hcl
variable "rds_security_group_id" {
  type        = string
  default     = ""
  description = "Security Group ID of the target RDS MySQL instance."
}

variable "rds_subnet_id" {
  type        = string
  default     = ""
  description = "Private Subnet ID where Glue ENI will be deployed."
}

variable "rds_availability_zone" {
  type        = string
  default     = ""
  description = "Availability Zone of the target RDS MySQL instance."
}

module "glue_connection" {
  source = "cps-terraform.anthem.com/DIG/terraform-aws-glue-connection/aws"

  create_new_connection = true
  name                  = "${var.app_name}-rds-connection-${var.environment}"
  description           = "AWS Glue connection for RDS MySQL serving layer"

  physical_connection_requirements = [
    {
      availability_zone      = var.rds_availability_zone
      subnet_id              = var.rds_subnet_id
      security_group_id_list = [var.rds_security_group_id]
    }
  ]
}

module "silver_iceberg_job" {
  source = "glue/aws//modules/job"
  # ...
  connections = [module.glue_connection.name]
}
```
