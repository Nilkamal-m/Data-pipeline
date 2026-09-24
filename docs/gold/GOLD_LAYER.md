# Gold Layer — Enterprise Analytics Marts & Multi-Target Serving Engine

## 1. Executive Summary & Purpose

The **Gold Layer** is the business presentation and consumption layer of the Medallion Data Lakehouse architecture. It transforms conformed Silver Iceberg entities into high-value, dimensional business data marts and synchronizes them across a multi-target enterprise serving ecosystem.

### Core Architectural Responsibilities
* **Declarative SQL Business Marts**: Executes modular SQL queries (`gold/query/<source>/*.sql`) joining and aggregating Silver Iceberg tables.
* **Pluggable Python Business Transforms**: Provides script hooks (`gold/script/custom_transforms/*.py`) for machine learning enrichment, advanced KPI calculation, and unstructured text parsing.
* **Primary Athena Iceberg Marts**: Persists physical Iceberg tables registered in the AWS Glue Data Catalog with dynamic schema padding and zero hardcoded column definitions.
* **Multi-Target Serving Synchronizer**: Synchronizes transformed marts into operational data stores and data warehouses:
  - **Amazon Aurora MySQL**: High-performance operational serving with isolated staging tables and record-level PK UPSERT (`ON DUPLICATE KEY UPDATE`).
  - **Amazon Redshift Spectrum**: Direct external Iceberg catalog querying.
  - **Snowflake**: External tables over Lakehouse Iceberg metadata.
  - **Databricks**: Unity Catalog foreign Iceberg integration.
* **Historical Data Migration & Archival**: Ingests historical reporting CSV exports (`gold/initial_exports/`), parses complex multiline records, dedupes against incoming telemetry, and automatically archives processed source files to S3 `_archived/` directories.

---

## 2. Low-Level Execution Lifecycle

The following Mermaid diagram illustrates the Gold Layer transformation, initial load, and serving workflow:

```mermaid
flowchart TD
    subgraph Trigger ["1. Trigger & Config Resolution"]
        Start(["Trigger: Silver Completion or CLI Run"]) --> InitManager["GoldLayerManager.run()"]
        InitManager --> LoadConfig["GoldConfigLoader.load_config()\nInterpolate {env} Variables"]
    end

    subgraph InitialLoadCheck ["2. Historical Load & S3 Archival"]
        LoadConfig --> CheckInitial{"Initial CSV Exists\nin gold/initial_exports/?"}
        CheckInitial -- Yes --> ParseCSV["GoldInitialLoader.load():\nRead CSV with multiLine and escape options"]
        ParseCSV --> DedupeCSV["Deduplicate by nkey\nOrdered by _updated_at DESC"]
        DedupeCSV --> ArchiveS3["Move CSV to S3 _archived/:\ns3://.../gold/initial_exports/{source}/_archived/"]
        ArchiveS3 --> InitAthena["Write Initial Baseline to Athena Iceberg"]
        CheckInitial -- No --> TransformPhase
        InitAthena --> TransformPhase
    end

    subgraph TransformationPhase ["3. Mart Execution & Business Logic"]
        TransformPhase["Execute Business SQL Query:\ngold/query/{source}/{table}.sql"] --> CheckCustom{"Custom Transform\nScript Configured?"}
        CheckCustom -- Yes --> ExecCustomScript["Execute custom_transforms/{source}_{table}.py\n(e.g., Genesys transcript & metric parsing)"]
        CheckCustom -- No --> AlignSchema["Dynamic Schema Alignment & Padding\n(Zero Hardcoded Columns)"]
        ExecCustomScript --> AlignSchema
    end

    subgraph TargetOne ["4. Target 1: Authoritative Athena Iceberg Upsert"]
        AlignSchema --> FetchIcebergSchema["Inspect Target Iceberg Schema in Glue"]
        FetchIcebergSchema --> PadColumns["Pad Missing Columns as NULL\nReorder Columns to Match Iceberg Catalog"]
        PadColumns --> IcebergMerge["Spark SQL MERGE INTO target AS t USING source AS s\nON t.nkey = s.nkey\nWHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"]
        IcebergMerge --> IcebergComplete["Iceberg Gold Table Complete & Authoritative\nRefreshes in-session temp views for downstream marts"]
    end

    subgraph MultiTargetServing ["5. Multi-Target Serving Synchronization"]
        IcebergComplete --> CheckTargets{"Evaluate target_engines\n(Configured per table)"}
        CheckTargets -- "Aurora MySQL" --> ReadIceberg["Read Authoritative Athena Iceberg Table\n(Directly after Iceberg completion)"]
        ReadIceberg --> AuroraStaging["Write Full Conformed DataFrame\nto Aurora Staging Table: {table}_staging"]
        AuroraStaging --> AuroraSwap["Execute Zero-Downtime Atomic Swap:\nRENAME TABLE target TO old, staging TO target\nDROP TABLE old"]
        AuroraSwap --> AuroraComplete["Aurora Gold Table Live\n(Operational query & dashboard serving)"]
        CheckTargets -- "Redshift Spectrum" --> RedshiftSync["Update External Schema Mapping"]
        CheckTargets -- "Snowflake" --> SnowflakeSync["Refresh External Stage / Iceberg Table"]
        CheckTargets -- "Databricks" --> DatabricksSync["Sync Unity Catalog Iceberg Reference"]
    end

    subgraph Complete ["6. Completion"]
        AuroraComplete --> Success(["Job Complete: Marts Updated Across All Engines"])
        RedshiftSync --> Success
        SnowflakeSync --> Success
        DatabricksSync --> Success
    end
```

---

## 3. Python Module Linkage & File Organization

```
gold/
├── script/
│   ├── gold_layer_manager.py           # Master Gold orchestration engine
│   ├── gold_config_loader.py           # Configuration loader & {env} interpolator
│   ├── gold_initial_load.py            # Historical CSV loader, parser & auto-archiver
│   ├── config/
│   │   └── gold_config.json            # Multi-target configuration blueprint
│   ├── adapters/                       # Downstream serving engine connectors
│   │   ├── __init__.py                 # Adapter discovery registry
│   │   ├── aurora.py                   # Aurora MySQL staging & zero-downtime PK upsert
│   │   ├── redshift.py                 # Amazon Redshift Spectrum catalog adapter
│   │   ├── snowflake.py                # Snowflake Iceberg external table adapter
│   │   └── databricks.py               # Databricks Unity Catalog connector
│   └── custom_transforms/              # Domain-specific PySpark transformation hooks
│       ├── genesys_conversations.py    # Custom Genesys transcript & metric parsing
│       └── moveworks_interactions.py   # Custom Moveworks conversation aggregations
├── query/                              # Business SQL queries
│   ├── genesys/
│   │   └── conversations.sql           # Dimensional conversation mart SQL
│   └── moveworks/
│       └── interactions.sql            # Interaction and session analytics mart SQL
└── initial_exports/                    # Historical data drop location
    ├── genesys/
    │   └── conversations.csv
    └── moveworks/
        └── interactions.csv
```

### Why Standalone Python Modules?
1. **Target Decoupling (`adapters/`)**: Each downstream database adapter manages its own connection pooling, dialect-specific SQL, and error handling. A connection timeout to Snowflake will not affect Athena Iceberg commits.
2. **Zero-Hardcoding Resilience**: Schema alignment dynamically queries the Glue Catalog at runtime. When upstream business queries add a column, the Gold engine automatically reconciles the schema without requiring code changes.
3. **Dedicated Migration Mechanics (`gold_initial_load.py`)**: One-time historical ingestion logic is separated from recurring delta pipelines, keeping daily jobs lean while preserving disaster recovery reload capabilities (`--RELOAD_INITIAL=true`).

---

## 4. Primary Target: Athena Iceberg Engine Mechanics

Athena Iceberg serves as the **single source of truth** for all business data marts.

### 4.1 Dynamic Schema Alignment (Zero Hardcoding)
In production, schema drift between source SQL queries and target Iceberg tables must be handled gracefully. `_upsert_iceberg_table` executes the following algorithm:
1. **Catalog Inspection**: Queries `spark.table(f"iceberg_catalog.{db}.{target_table}").schema` to retrieve the current physical table schema.
2. **Column Padding**: Any column existing in the target table but missing from the source DataFrame is injected with `lit(None).cast(target_col_type)`.
3. **Column Reordering**: The DataFrame columns are reordered to match the exact ordinal positions in the target Iceberg catalog.
4. **Atomic Merge**: Spark SQL executes `MERGE INTO`, updating modified records and inserting new ones without full table rewrites.

---

## 5. Downstream Serving: Amazon Aurora MySQL Adapter

The Aurora MySQL adapter serves operational web applications and real-time dashboards (Power BI, Tableau) requiring sub-second query performance.

### 5.1 Architecture: Iceberg Gold Table $\to$ Aurora Gold Staging $\to$ Aurora Gold Table

To eliminate duplicate record accumulation and maintain exact parity with the authoritative Athena Iceberg mart, the platform enforces the **Atomic Table Swap Strategy**:

```
[Athena Iceberg Table] (Authoritative Single Source of Truth)
       │
       ▼
[Aurora Staging Table] (`{target_table}_staging`)
       │
       ▼ (Zero-Downtime Atomic DDL Swap)
[Aurora Target Table]  (`{target_table}`)
```

#### Detailed Execution Sequence:
1. **Authoritative Ingestion**: `GoldLayerManager` queries the fully merged, deduplicated physical Athena Iceberg table directly (`spark.table(f"{glue_db}.{target_table}")`), ensuring the complete conformed dataset (initial loads + all delta increments) is processed.
2. **Staging Materialization**: The complete conformed dataset is streamed into an isolated staging table: `{target_table}_staging`.
3. **Atomic Table Swap**: MySQL executes a single, microsecond DDL statement:
   ```sql
   RENAME TABLE
       `enterprise_reporting`.`gold_moveworks_interactions` TO `enterprise_reporting`.`gold_moveworks_interactions_old`,
       `enterprise_reporting`.`gold_moveworks_interactions_staging` TO `enterprise_reporting`.`gold_moveworks_interactions`;
   ```
4. **Instant Cleanup & Indexing**: The previous table version (`_old`) is safely dropped, and query performance indexes are verified on the natural keys (`pks`).

#### Key Advantages:
* **Zero Downtime**: Power BI and BI analysts querying `{target_table}` experience continuous availability without locking or missing-table exceptions.
* **Guaranteed Parity**: Aurora record counts always match Athena Iceberg record counts exactly (e.g. 5,477 rows in Athena $\equiv$ 5,477 rows in Aurora), preventing runaway duplicate counts on recurring runs.

---

## 6. Historical Data Migration & S3 Auto-Archival

When migrating historical analytics data from legacy data warehouses or vendor export dumps:

1. **Storage Path**: Drop historical files under `s3://{gold_bucket}/gold/initial_exports/{source}/{table}.csv`.
2. **Multiline Parsing**: `GoldInitialLoader` configures Spark CSV options:
   ```python
   spark.read.format("csv") \
       .option("header", "true") \
       .option("multiLine", "true") \
       .option("escape", '"') \
       .option("quote", '"')
   ```
3. **Automatic S3 Archival**: Upon successful ingestion into Iceberg, `_archive_s3_file` moves the CSV to:
   `s3://{gold_bucket}/gold/initial_exports/{source}/_archived/{table}_{timestamp}.csv`
   This guarantees that subsequent daily incremental runs will not re-parse the multi-gigabyte historical export.
4. **Disaster Recovery Overrides**:
   - `--RELOAD_INITIAL=true`: Forces the engine to scan and re-ingest archived historical exports.
   - `--FULL_REFRESH=true`: Re-evaluates all Silver data and re-seeds downstream target engines.

---

## 7. Developer Onboarding: Adding a New Gold Mart

### Step 1: Write Business SQL Query
Create `gold/query/<source>/<mart_name>.sql`:
```sql
SELECT
    t.sys_id AS incident_id,
    t.number AS incident_number,
    t.priority,
    t.state,
    u.name AS assigned_to_name,
    t.sys_updated_on AS _updated_at
FROM
    uax_datalake_db_{env}.tbl_incident t
LEFT JOIN
    uax_datalake_db_{env}.tbl_user u ON t.assigned_to = u.sys_id
```

### Step 2: Update `gold_config.json`
Define the mart and target engines under `source_systems.<source>.tables`:
```json
"incidents_summary": {
  "nkey": ["incident_id"],
  "incremental": true,
  "aurora": {
    "schema": "enterprise_reporting",
    "table_name": "gold_servicenow_incidents_summary"
  }
}
```

### Step 3: Run & Validate
```bash
python3 gold/script/gold_layer_manager.py \
  --CONFIG_S3_PATH "s3://uax-datalake-config-dev/gold/config/gold_config.json" \
  --ENV "dev" \
  --SOURCE_SYSTEM "servicenow" \
  --TABLE_NAME "incidents_summary" \
  --GOLD_BUCKET "uax-datalake-gold-dev"
```
Verify Athena and Aurora:
* Athena: `SELECT COUNT(*) FROM uax_datalake_db_dev.gold_servicenow_incidents_summary;`
* Aurora: `SELECT COUNT(*) FROM enterprise_reporting.gold_servicenow_incidents_summary;`

---

## 8. Developer Enhancement Reference

For information on how the Gold codebase is structured for contributors — adding marts, custom transforms, new target adapters, or modifying incremental behavior — see [ENHANCEMENT_GUIDE.md](ENHANCEMENT_GUIDE.md).
