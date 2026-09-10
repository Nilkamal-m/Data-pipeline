# Backlog Proposal: Parameterized Gold Layer in Unified Glue Job with Automated Aurora MySQL Schema Evolution

> [!NOTE]
> **Status: BACKLOG (Pending Manager Approval)**  
> **Target Execution**: To be implemented upon managerial go-ahead.  
> **Scope**: Add Gold Serving Layer to Aurora MySQL without modifying existing Bronze or Silver code.

---

## 1. Executive Summary

This architecture design addresses the enterprise Power BI reporting requirement by leveraging our **existing Amazon Aurora MySQL** cluster as the Gold Serving Layer. 

It eliminates:
- The need to install Simba Athena ODBC drivers on individual analyst laptops.
- The need to distribute or manage AWS IAM credentials on client machines.
- The requirement to deploy and maintain a Windows EC2 instance for a Power BI Gateway.
- Athena S3 scan costs ($5/TB) on every dashboard click.

---

## 2. Core Architectural Principles

1. **Zero Regression on Bronze & Silver**:
   - `uax_bronze_load.py` and `uax_silver_etl.py` core transformation logic remain intact and fully functional.
2. **Single Unified Multi-Stage Glue Job**:
   - Reuses the existing Glue Job (`uax-datalake-silver-etl-dev`) parameterized via `--PROCESS_LAYER`:
     - `--PROCESS_LAYER=silver`: Runs only Silver Iceberg ETL (Default).
     - `--PROCESS_LAYER=gold`: Runs only Gold S3 SQL discovery and Aurora MySQL load.
     - `--PROCESS_LAYER=all`: Executes Silver followed immediately by Gold in a single cluster run.
3. **Automated S3 SQL Discovery**:
   - Analysts and engineers define Gold marts by simply dropping standard ANSI `.sql` files into `s3://<bucket>/gold/queries/*.sql`.
   - The job automatically discovers all `.sql` files in S3 and executes them against the Silver Iceberg tables.
4. **100% Automated RDS/Aurora DDL & Schema Evolution**:
   - Automatically creates the `gold_marts` schema if it does not exist.
   - Automatically creates and updates tables via Spark JDBC.
   - Automatically evolves schemas when new columns are added or renamed in the `.sql` files without any manual DDL.
   - Performs a **Blue/Green Atomic Swap** (`tbl_<mart>_staging` &rarr; `tbl_<mart>`) and refreshes `v_<mart>` in less than 1 millisecond, ensuring **zero downtime** for Power BI users.

---

## 3. High-Level & Low-Level Data Flow

```
                                  +-------------------------------------------------------------+
                                  |         AWS Glue PySpark Job (`uax_silver_etl.py`)          |
                                  +-------------------------------------------------------------+
                                                                 |
                                +--------------------------------+--------------------------------+
                                |                                                                 |
               If `--PROCESS_LAYER=silver` (Default)                             If `--PROCESS_LAYER=gold`
                                |                                                                 |
                                v                                                                 v
                [ Silver Iceberg ETL (Existing) ]                                 [ Gold Layer Manager (New) ]
            • Reads Bronze raw JSON/Parquet from S3                            • Discovers `.sql` files in S3:
            • Deduplicates & merges into Iceberg tables                          `s3://<bucket>/gold/queries/*.sql`
            • Updates Silver Glue Catalog metadata                             • Executes queries over Silver Iceberg tables
            • Completely skips Aurora MySQL                                    • Connects to Aurora MySQL via Glue Connection
                                                                               • Dynamically handles DDL & Schema Evolution:
                                                                                 1. Creates `gold_marts` DB if not exists
                                                                                 2. Writes to `tbl_<mart>_staging`
                                                                                 3. Performs Atomic Table Swap (< 1ms)
                                                                                 4. Updates View: `v_<mart>` for Power BI
                                                                                 5. Power BI reads via Reader Endpoint
```

---

## 4. Database Object Lifecycle in Aurora MySQL

For each Gold mart (e.g. `gold_mart_incident_kpi`):

1. **`tbl_gold_mart_incident_kpi_staging`**:
   - High-speed JDBC bulk write target (`rewriteBatchedStatements=true`, `batchsize=10000`, `numPartitions=4`).
   - Power BI never connects to this table, isolating active users from write operations.
2. **`tbl_gold_mart_incident_kpi`**:
   - Physical backing table holding validated, active production data.
3. **`v_gold_mart_incident_kpi`**:
   - View created via: `CREATE OR REPLACE VIEW v_gold_mart_incident_kpi AS SELECT * FROM tbl_gold_mart_incident_kpi;`
   - **Power BI connects exclusively to this view.**

---

## 5. Implementation Roadmap (When Approved)

When managerial approval is granted, execution will proceed through these exact steps:

1. **Create `silver/script/gold_layer_manager.py`**:
   - Modular helper handling S3 SQL discovery, Secrets Manager resolution, JDBC writes, quality checks, and atomic view swaps.
2. **Add `--PROCESS_LAYER` routing to `silver/script/uax_silver_etl.py`**:
   - Conditional execution based on parameter without touching existing Silver logic.
3. **Update Lambda Helper (`lambda_helper/lambda_function.py`)**:
   - Add `"layer": "gold"` payload routing.
4. **Deploy AWS Glue Connection in Terraform**:
   - Connect Glue to the Aurora MySQL VPC and subnet.
5. **Upload Sample Query to `s3://<bucket>/gold/queries/`**:
   - Test end-to-end execution and verify views in Aurora MySQL.
6. **Connect Power BI to Aurora Reader Endpoint**:
   - Configure scheduled refresh on `v_<mart_name>` with zero local gateway overhead.
