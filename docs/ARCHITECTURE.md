# UAX DataLake — End-to-End Architecture

> **Project:** UAX DataLake  
> **Pattern:** Medallion (Multi-Hop) Architecture — Bronze → Silver → Gold  
> **Orchestration:** Amazon EventBridge → AWS Step Functions → AWS Glue Spark Jobs

---

## 1. High-Level Overview

UAX DataLake ingests operational data from multiple enterprise source systems, refines it through three isolated processing layers, and serves business-ready data marts to downstream analytics and BI tools.

Each layer runs as an independent AWS Glue PySpark job. Step Functions coordinates the sequential execution order. EventBridge triggers the entire pipeline on a schedule or on an S3 arrival event.

```mermaid
flowchart TD
    subgraph Orchestration["Pipeline Orchestration"]
        EB["Amazon EventBridge\n(Scheduled Rule / S3 Event)"]
        SF["AWS Step Functions\n(State Machine)"]
        EB -->|Trigger| SF
    end

    subgraph Sources["Source Systems"]
        REST["REST APIs\n(ServiceNow, Genesys, Moveworks)"]
        DB["Relational DBs\n(PostgreSQL, MySQL)"]
        S3SRC["S3 File Drops\n(CSV / JSON / Parquet)"]
    end

    subgraph Bronze["Bronze Layer — Raw Ingestion"]
        BJ["uax_bronze_load.py\n(AWS Glue Job)"]
        BS3[("S3 Raw Parquet\nbronze/data/{source}/{table}/\nyear=YYYY/month=MM/day=DD")]
        BWMK[("S3 State\nmetadata/bronze/{source}/{table}/watermark.json")]
    end

    subgraph Silver["Silver Layer — Conformed Iceberg"]
        SJ["uax_silver_etl.py\n(AWS Glue Job)"]
        SICEBERG[("Apache Iceberg Tables\nsilver/data/{source}/{table}/")]
        SWMK[("S3 State\nmetadata/silver/{source}_{table}_watermark.json")]
    end

    subgraph Gold["Gold Layer — Business Marts"]
        GJ["gold_layer_manager.py\n(AWS Glue Job)"]
        GICEBERG[("Athena Iceberg Marts\ngold/data/{source}/{table}/")]
        GAURORA[("Amazon Aurora MySQL\nenterprise_reporting.*")]
    end

    subgraph Consume["Downstream Consumers"]
        ATHENA["Amazon Athena"]
        BI["Power BI / Tableau"]
        RS["Redshift Spectrum"]
        SNO["Snowflake / Databricks"]
    end

    SF -->|Step 1| BJ
    SF -->|Step 2| SJ
    SF -->|Step 3| GJ

    REST --> BJ
    DB --> BJ
    S3SRC --> BJ

    BJ --> BS3
    BJ <--> BWMK

    BS3 --> SJ
    SJ --> SICEBERG
    SJ <--> SWMK

    SICEBERG --> GJ
    GJ --> GICEBERG
    GJ --> GAURORA

    GICEBERG --> ATHENA
    GICEBERG --> RS
    GICEBERG --> SNO
    GAURORA --> BI
```

---

## 2. Orchestration: EventBridge → Step Functions

### 2.1 Trigger

An **Amazon EventBridge rule** fires on a cron schedule (e.g., `cron(0 6 * * ? *)` for daily at 06:00 UTC) or on an S3 event (new file dropped in a vendor bucket). EventBridge invokes the Step Functions state machine as its target.

### 2.2 Step Functions State Machine

The state machine runs the three Glue jobs **sequentially** using `GlueStartJobRun` and `GlueStartJobRunSync` task states. Each step waits for job completion before the next one starts.

```
EventBridge Trigger
        │
        ▼
[State: Run Bronze Job]    ──► GlueStartJobRunSync(uax_bronze_load)
        │ SUCCESS
        ▼
[State: Run Silver Job]    ──► GlueStartJobRunSync(uax_silver_etl)
        │ SUCCESS
        ▼
[State: Run Gold Job]      ──► GlueStartJobRunSync(gold_layer_manager)
        │ SUCCESS
        ▼
[State: Pipeline Complete] ──► SNS Notification (optional)
        │ ANY FAILURE
        ▼
[State: Error Handler]     ──► SNS Alert + CloudWatch Alarm
```

### 2.3 Why Sequential, Not Parallel?

| Concern | Reasoning |
|---|---|
| **Data dependency** | Silver reads Bronze output. Gold reads Silver Iceberg. The order enforces availability. |
| **Watermark integrity** | Each layer commits its watermark only after a successful write. Parallel execution would break state boundaries. |
| **Failure isolation** | Step Functions retries the failed Glue job independently before failing the pipeline. |

---

## 3. Layer Summary

| Layer | Job Script | Input | Output | Storage |
|---|---|---|---|---|
| **Bronze** | `uax_bronze_load.py` | APIs / DBs / S3 Files | Append-only Parquet partitioned by date | S3 `bronze/data/` |
| **Silver** | `uax_silver_etl.py` | Bronze Parquet (incremental) | Apache Iceberg v2 (ACID upsert / SCD) | S3 `silver/data/` + Glue Catalog |
| **Gold** | `gold_layer_manager.py` | Silver Iceberg (incremental delta) | Athena Iceberg Mart + Aurora MySQL sync | S3 `gold/data/` + Aurora |

---

## 4. End-to-End Sequence

```mermaid
sequenceDiagram
    autonumber
    participant EB as EventBridge
    participant SF as Step Functions
    participant BZ as Bronze Job
    participant SV as Silver Job
    participant GD as Gold Job
    participant S3B as S3 Bronze
    participant S3S as S3 Silver Iceberg
    participant S3G as S3 Gold Iceberg
    participant AURORA as Aurora MySQL

    EB->>SF: Trigger State Machine
    SF->>BZ: GlueStartJobRun (uax_bronze_load)
    BZ->>BZ: Resolve watermark from S3 state file
    BZ->>BZ: Authenticate source (OAuth2 / Basic / IAM)
    BZ->>BZ: Extract and flatten records
    BZ->>S3B: Write Snappy Parquet (year/month/day partitions)
    BZ->>BZ: Commit new watermark to S3

    SF->>SV: GlueStartJobRun (uax_silver_etl)
    SV->>SV: Read Silver watermark from S3
    SV->>S3B: Scan Bronze Parquet (_ingested_at > watermark)
    SV->>SV: Deduplicate by nkey, apply transforms
    SV->>S3S: MERGE INTO Iceberg table (SCD1 or SCD2)
    SV->>SV: Update Silver watermark

    SF->>GD: GlueStartJobRun (gold_layer_manager)
    GD->>GD: Execute mart SQL over Silver Iceberg
    GD->>GD: Filter delta (_updated_at > max Gold _updated_at)
    GD->>S3G: MERGE INTO Gold Iceberg mart
    GD->>S3G: Read authoritative Iceberg dataset
    GD->>AURORA: Write staging table -> atomic swap to production
```

---

## 5. Layer Responsibilities

### Bronze — Raw Ingestion
- Connects to upstream sources via pluggable connectors (REST, JDBC, S3 file).
- Extracts records incrementally using per-table S3 watermark state files.
- Flattens nested JSON, injects audit columns (`_ingested_at`, `_source_system`, etc.), writes Snappy Parquet partitioned by `year/month/day`.
- Commits the new watermark only after data is confirmed written.

### Silver — Conformed Iceberg
- Reads only new Bronze records (`_ingested_at > last_watermark`).
- Deduplicates within each micro-batch using natural keys and ordering columns.
- Applies SCD Type 1 (in-place upsert) or SCD Type 2 (historical version tracking).
- Automatically evolves the Iceberg schema when new columns arrive from Bronze.

### Gold — Business Marts
- Executes modular SQL queries (`gold/query/<source>/<table>.sql`) over Silver Iceberg.
- For incremental tables, only processes records changed since the last Gold run (`_updated_at > max(gold._updated_at)`).
- For full-refresh tables, re-materializes the entire mart from all Silver records.
- Materializes results into Athena Iceberg as the **single source of truth**.
- Syncs to Aurora MySQL using an atomic staging-swap for zero-downtime BI serving.

---

## 6. Security Architecture

All credentials are stored in **AWS Secrets Manager**. No secrets appear in config files or code.

| Auth Type | Used For | Mechanics |
|---|---|---|
| `oauth` (client_credentials) | Genesys Cloud, Moveworks | Cached Bearer token, auto-refreshed 60s before expiry |
| `basic` | ServiceNow (legacy instances) | Base64 header injected per request |
| `api_key` | Internal microservices | Static header injection |
| `iam_role` | Cross-account S3 reads | Managed by boto3 / Glue execution IAM role |

---

## 7. Repository Structure

```
Data-pipeline/
├── docs/
│   ├── ARCHITECTURE.md            # This document
│   ├── ONBOARDING_GUIDE.md        # Developer runbook
│   ├── bronze/
│   │   ├── BRONZE_LAYER.md        # Bronze engine internals
│   │   ├── CONFIG_BLUEPRINT.md    # bronze_config.json parameter reference
│   │   └── ENHANCEMENT_GUIDE.md   # How the Bronze code is structured for contributors
│   ├── silver/
│   │   ├── SILVER_LAYER.md        # Silver engine internals
│   │   ├── CONFIG_BLUEPRINT.md    # silver_config.json parameter reference
│   │   └── ENHANCEMENT_GUIDE.md   # How the Silver code is structured for contributors
│   └── gold/
│       ├── GOLD_LAYER.md          # Gold engine internals
│       ├── CONFIG_BLUEPRINT.md    # gold_config.json parameter reference
│       └── ENHANCEMENT_GUIDE.md   # How the Gold code is structured for contributors
├── bronze/script/
│   ├── uax_bronze_load.py
│   ├── config_loader.py
│   ├── config/bronze_config.json
│   └── connectors/
├── silver/script/
│   ├── uax_silver_etl.py
│   ├── silver_config_loader.py
│   ├── transformer.py
│   ├── custom_transforms/
│   └── config/silver_config.json
├── gold/script/
│   ├── gold_layer_manager.py
│   ├── gold_config_loader.py
│   ├── gold_initial_load.py
│   ├── custom_transforms/
│   └── config/gold_config.json
└── gold/query/
    ├── genesys/
    └── moveworks/
```
