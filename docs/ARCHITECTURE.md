# Enterprise Multi-Hop Lakehouse Pipeline — Architecture & Technical Specifications

This repository hosts a production-grade, enterprise data lakehouse platform designed to process high-throughput batch and streaming-like micro-batch workloads. Built natively on **Apache Spark (AWS Glue)**, **Apache Iceberg**, and **AWS Glue Data Catalog**, the platform enforces strict data governance, idempotent loading, schema evolution, and multi-engine serving.

---

## 1. High-Level Lakehouse Architecture

The architecture follows the **Medallion (Multi-Hop) Architecture** pattern, isolating ingestion, cleansing, and business aggregation into decoupled processing layers:

```mermaid
flowchart TD
    subgraph Sources["External Source Systems"]
        S_REST["REST APIs<br/>(ServiceNow, Genesys, Moveworks)"]
        S_DB["Relational DBs<br/>(PostgreSQL, MySQL, Oracle)"]
        S_S3["File Drops<br/>(S3 Parquet / CSV / JSON)"]
    end

    subgraph BronzeLayer["Bronze Layer (Raw Ingestion)"]
        B_JOB["AWS Glue Spark Job<br/>(uax_bronze_load.py)"]
        B_STATE[("S3 State Watermarks<br/>metadata/bronze/...")]
        B_S3[("Raw Data Lake (S3)<br/>bronze/data/<source>/<table>/<br/>Partitioned: year/month/day")]
        B_CATALOG[("Glue Data Catalog<br/>raw_tbl_<source>_<table>")]
    end

    subgraph SilverLayer["Silver Layer (Conformed Iceberg)"]
        S_JOB["AWS Glue Spark Job<br/>(uax_silver_etl.py)"]
        S_STATE[("S3 Watermarks<br/>metadata/silver/...")]
        S_ICEBERG[("Apache Iceberg Tables (S3)<br/>silver/data/<source>/<table>/")]
        S_CATALOG[("Glue Data Catalog<br/>tbl_<source>_<table>")]
    end

    subgraph GoldLayer["Gold Layer (Marts & Multi-Engine Serving)"]
        G_JOB["AWS Glue Spark Job<br/>(gold_layer_manager.py)"]
        G_INIT[("Historical CSV/Parquet<br/>gold/initial_exports/...")]
        G_SQL["Mart SQL Definitions<br/>gold/query/<source>/*.sql"]
        G_ICEBERG[("Athena Iceberg Marts<br/>gold/data/<source>/<table>/")]
        G_CATALOG[("Glue Data Catalog<br/>gold_<source>_<table>")]
    end

    subgraph Downstream["Downstream Analytics & Serving"]
        D_ATHENA["Amazon Athena<br/>(Ad-hoc SQL & BI Queries)"]
        D_AURORA["Amazon Aurora / RDS MySQL<br/>(Zero-Downtime Power BI Feed)"]
        D_REDSHIFT["Amazon Redshift Spectrum<br/>(External Iceberg Schema)"]
        D_SNOWFLAKE["Snowflake<br/>(External Iceberg / Shared DB)"]
        D_DATABRICKS["Databricks<br/>(Delta Lake Unified Catalog)"]
    end

    %% Flow connections
    S_REST -->|Authenticated Ingestion| B_JOB
    S_DB -->|JDBC Extraction| B_JOB
    S_S3 -->|S3 Event / Batch Read| B_JOB

    B_JOB <--> B_STATE
    B_JOB -->|Append Parquet + Snappy| B_S3
    B_JOB -->|Sync Metadata| B_CATALOG

    B_S3 -->|Incremental Delta Read| S_JOB
    S_JOB <--> S_STATE
    S_JOB -->|ACID Upsert / SCD1 & SCD2| S_ICEBERG
    S_JOB -->|Schema Evolution| S_CATALOG

    S_ICEBERG -->|Spark SQL Mart Query| G_JOB
    G_INIT -.->|Run 1 Historical Load| G_JOB
    G_SQL -->|Business Logic| G_JOB
    G_JOB -->|Iceberg MERGE INTO| G_ICEBERG
    G_JOB -->|Register Tables| G_CATALOG

    G_ICEBERG --> D_ATHENA
    G_JOB -->|Direct JDBC PK Upsert| D_AURORA
    G_CATALOG --> D_REDSHIFT
    G_ICEBERG --> D_SNOWFLAKE
    G_ICEBERG --> D_DATABRICKS
```

---

## 2. Layer-by-Layer Responsibilities

| Layer | Storage Engine | Format | Commit Pattern | Primary Purpose |
|---|---|---|---|---|
| **Bronze** | Amazon S3 | Apache Parquet (Snappy) | Append-Only with Hive partitioning (`year=YYYY/month=MM/day=DD`) | Ingest raw payload exactly as received from source systems; preserve lineage with zero data loss. |
| **Silver** | Amazon S3 + Apache Iceberg | Apache Iceberg v2 | ACID Upsert (`MERGE INTO`), SCD Type 1 & Type 2 | Enforce conformed types, natural key deduplication, schema evolution, and row-level updates. |
| **Gold** | Amazon S3 + Apache Iceberg | Apache Iceberg v2 + RDBMS | Idempotent Upsert + Dual-Write Serving | Aggregate dimensional models, materializing physical tables in Athena and syncing to MySQL/Redshift/Snowflake. |

---

## 3. End-to-End Execution Flow

```mermaid
sequenceDiagram
    autonumber
    participant SRC as External Sources
    participant BZ as Bronze Job (uax_bronze_load.py)
    participant S3B as S3 Bronze Bucket
    participant SV as Silver Job (uax_silver_etl.py)
    participant S3S as S3 Silver Iceberg
    participant GD as Gold Job (gold_layer_manager.py)
    participant S3G as S3 Gold Iceberg
    participant RDBMS as Aurora MySQL

    Note over BZ: Step 1: Raw Extraction
    BZ->>BZ: Resolve watermark (S3 state file / raw_tbl_watermarks)
    BZ->>SRC: Authenticate (OAuth2 / API Key / Basic) & fetch delta records
    BZ->>BZ: Flatten JSON & inject audit columns (_ingested_at, _source_system, etc.)
    BZ->>S3B: Write Snappy Parquet (partitioned by year/month/day)
    BZ->>BZ: Commit new watermark to S3 state file

    Note over SV: Step 2: Conformed Iceberg Transformation
    SV->>SV: Resolve lower bound watermark from metadata/silver/...
    SV->>S3B: Read incremental Bronze batch (_ingested_at > watermark)
    SV->>SV: Apply custom transform hook (if present) & deduplicate by nkey
    SV->>SV: Apply SCD Type 1 or Type 2 tracking with technical columns
    SV->>S3S: Execute Spark SQL MERGE INTO Apache Iceberg table
    SV->>SV: Update Silver watermark state file

    Note over GD: Step 3: Analytical Aggregation & Serving
    GD->>GD: Discover and dependency-sort SQL queries (gold/query/<source>/*.sql)
    opt First Run Historical Ingestion
        GD->>GD: Load initial export CSV/Parquet from gold/initial_exports/...
        GD->>S3G: Materialize initial Iceberg table & archive source CSV to _archived/
    end
    GD->>S3S: Execute SQL query over Silver Iceberg tables
    GD->>GD: Filter incremental delta (WHERE _updated_at > max(gold._updated_at))
    GD->>GD: Execute custom transform / API / LLM enrichment (if configured)
    GD->>S3G: Upsert into Athena Iceberg table via Spark SQL MERGE INTO
    opt MySQL Target Configured
        GD->>RDBMS: Write delta to staging table via JDBC (gold_<table>_staging)
        GD->>RDBMS: Execute zero-downtime primary key UPSERT into physical table
    end
```

---

## 4. Script Architecture & Linking Rationale

The data pipeline code is intentionally modularized into dedicated execution scripts and config loaders per layer:

### Why Standalone Python Scripts?
1. **AWS Glue Job Isolation**: Each hop runs as an independent AWS Glue Spark job (`uax_bronze_load.py`, `uax_silver_etl.py`, `gold_layer_manager.py`). Failures in Gold serving never block Bronze ingestion.
2. **Dedicated Resource Scaling**:
   - Bronze runs lightweight standard Python/Spark workers (`G.1X` or `Standard`).
   - Silver runs compute-intensive Iceberg compaction and merge workers (`G.2X`).
   - Gold runs memory-intensive multi-target broadcast and JDBC serving workers.
3. **Decoupled Configuration**:
   - `config_loader.py` (Bronze), `silver_config_loader.py` (Silver), and `gold_config_loader.py` (Gold) allow complete schema and parameter adjustments without changing execution code.
4. **Custom Transform Hooks**:
   - Complex business rules (e.g. LLM transcript summarization, entity resolution) live in `custom_transforms/` modules, loaded dynamically at runtime via Python introspection without altering the core pipeline engines.

---

## 5. Security & Authentication Architecture

All credentials, access tokens, and API keys are strictly forbidden from source code and JSON configuration files.

```mermaid
flowchart LR
    Job["AWS Glue Job<br/>(Spark Context)"] -->|Get Secret| SM["AWS Secrets Manager<br/>(Encrypted via KMS)"]
    SM -->|Return JSON Payload| Job
    Job -->|Build Auth Header| AuthLogic{"HTTP Client<br/>Auth Factory"}

    AuthLogic -->|auth_type = 'oauth'| OAuth["OAuth 2.0 Client<br/>(Token URL + Client Credentials)<br/>In-memory Cached Token"]
    AuthLogic -->|auth_type = 'basic'| Basic["Basic Auth<br/>(Base64 username:password)"]
    AuthLogic -->|auth_type = 'api_key'| ApiKey["API Key<br/>(Header: x-api-key)"]

    OAuth --> Endpoint["Target API Endpoint"]
    Basic --> Endpoint
    ApiKey --> Endpoint
```

### Authentication Strategies:
1. **OAuth 2.0 (`client_credentials`)**: Used for modern SaaS integrations (Genesys Cloud, Moveworks, Workday). The pipeline requests an access token from the Identity Provider token endpoint, caches it in-memory, and automatically refreshes it 60 seconds before expiration.
2. **Basic Authentication**: Used for legacy enterprise platforms (e.g. ServiceNow service accounts) utilizing base64-encoded credentials over TLS 1.3.
3. **API Key**: Direct header injection (e.g. `x-api-key`, `Authorization: Bearer <key>`) for cloud microservices.
4. **RDS / JDBC Authentication**: Injects database host, port, username, and password dynamically into Spark JDBC connection strings with SSL enforced (`useSSL=true&requireSSL=true`).

---

## 6. Directory Structure & Documentation Navigation

```text
Data-pipeline/
├── docs/                                  # Centralized Enterprise Documentation
│   ├── ARCHITECTURE.md                    # Platform architecture (this document)
│   ├── ONBOARDING_GUIDE.md                # Step-by-step developer onboarding & runbook
│   ├── bronze/
│   │   ├── BRONZE_LAYER.md                # Bronze engine architecture & low-level internals
│   │   └── CONFIG_BLUEPRINT.md            # bronze_config.json blueprint & parameters
│   ├── silver/
│   │   ├── SILVER_LAYER.md                # Silver Iceberg engine architecture & SCD rules
│   │   └── CONFIG_BLUEPRINT.md            # silver_config.json blueprint & parameters
│   └── gold/
│       ├── GOLD_LAYER.md                  # Gold multi-engine serving & mart lifecycle
│       └── CONFIG_BLUEPRINT.md            # gold_config.json blueprint & parameters
├── bronze/                                # Bronze Layer Implementation
│   ├── script/
│   │   ├── uax_bronze_load.py             # Main Bronze PySpark engine
│   │   ├── config_loader.py               # Hierarchical config parser
│   │   ├── config/bronze_config.json      # Bronze ingestion config
│   │   └── connectors/                    # Ingestion connectors (REST, S3, JDBC)
├── silver/                                # Silver Layer Implementation
│   ├── script/
│   │   ├── uax_silver_etl.py              # Main Silver Iceberg PySpark engine
│   │   ├── silver_config_loader.py        # Silver config parser
│   │   ├── transformer.py                 # Core transformation utilities
│   │   ├── custom_transforms/             # Table-specific transformation hooks
│   │   └── config/silver_config.json      # Silver transformation config
├── gold/                                  # Gold Layer Implementation
│   ├── script/
│   │   ├── gold_layer_manager.py          # Main Gold multi-target serving engine
│   │   ├── gold_config_loader.py          # Gold config parser
│   │   ├── gold_initial_load.py           # Historical backfill & CSV reconciliation engine
│   │   ├── custom_transforms/             # Table-specific transformation & API hooks
│   │   └── config/gold_config.json        # Gold serving config
│   └── query/                             # Mart Spark SQL query definitions
│       ├── genesys/                       # Genesys dimensional views
│       └── moveworks/                     # Moveworks dimensional views
└── README.md                              # Root repository overview
```
