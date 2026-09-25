# Enterprise Medallion Data Lakehouse Platform

An enterprise-grade, config-driven, multi-target Medallion Lakehouse platform built on **AWS Glue**, **Apache Iceberg (v2)**, **AWS Secrets Manager**, and **Amazon Athena**, providing high-throughput incremental ingestion, conformed dimensional transformation, and multi-engine serving across **Amazon Aurora MySQL**, **Amazon Redshift Spectrum**, **Snowflake**, and **Databricks**.

---

## 🏗️ Architectural Overview

The platform implements the **Medallion Lakehouse Architecture** (`Bronze` $\to$ `Silver` $\to$ `Gold (Athena Iceberg Table)` $\to$ `Reporting Layer`):

```mermaid
flowchart LR
    subgraph Sources ["Upstream Operational Systems"]
        REST["REST APIs\n(ServiceNow, Genesys, Moveworks)"]
        DB["Relational Databases\n(PostgreSQL, MySQL)"]
        S3F["Object Storage\n(Vendor S3 Feeds)"]
    end

    subgraph Bronze ["Bronze Layer (Raw Lake)"]
        BLoad["uax_bronze_load.py\n(BronzeLoadManager)"]
        BParquet[("Raw Parquet Batches\ns3://{bucket}/bronze/data/...")]
        BState[("State Watermarks\nmetadata/bronze/...")]
    end

    subgraph Silver ["Silver Layer (Conformed Lakehouse)"]
        SETL["uax_silver_etl.py\n(SilverETLManager)"]
        SIceberg[("Apache Iceberg v2 Tables\ns3://{bucket}/silver/data/...")]
        SWatermark[("Silver Watermarks\nmetadata/silver/...")]
    end

    subgraph Gold ["Gold Layer (Authoritative Marts)"]
        GMgr["gold_layer_manager.py\n(GoldLayerManager)"]
        GIceberg[("Athena Iceberg Table\n(Authoritative Source of Truth)\ns3://{bucket}/gold/data/...")]
    end

    subgraph Reporting ["Reporting Layer (Downstream Serving)"]
        Aurora[("Amazon Aurora MySQL\n(enterprise_reporting tables)")]
        Databricks[("Databricks Federated Layer\n(Mirrors Glue Catalog Tables directly)")]
        Redshift[("Amazon Redshift Spectrum\n(External Catalog)")]
        Snowflake[("Snowflake\n(Iceberg Tables)")]
    end

    REST --> BLoad
    DB --> BLoad
    S3F --> BLoad
    BLoad --> BParquet
    BLoad -.-> BState

    BParquet --> SETL
    SETL --> SIceberg
    SETL -.-> SWatermark

    SIceberg --> GMgr
    GMgr --> GIceberg

    GIceberg --> Aurora
    GIceberg --> Databricks
    GIceberg --> Redshift
    GIceberg --> Snowflake
```

---

## 📚 Comprehensive Documentation Suite

For granular engineering specifications, execution lifecycles, configuration blueprints, and developer guides, refer to the documentation suite in [`docs/`](docs/):

| Layer / Topic | Documentation Link | Description |
| :--- | :--- | :--- |
| **System Architecture** | [Architecture Blueprint](docs/ARCHITECTURE.md) | Universal architectural specification, execution lifecycle, and cross-layer security. |
| **Bronze Layer** | [Bronze Low-Level Guide](docs/bronze/BRONZE_LAYER.md) | Ingestion mechanics, connector protocols, OAuth2 grant types, and S3 state watermarks. |
| **Bronze Config** | [Bronze Config Blueprint](docs/bronze/CONFIG_BLUEPRINT.md) | Line-by-line configuration parameter blueprint and source connection templates. |
| **Silver Layer** | [Silver Low-Level Guide](docs/silver/SILVER_LAYER.md) | Apache Iceberg v2 storage, deduplication windowing, SCD1, SCD2, and schema evolution. |
| **Silver Config** | [Silver Config Blueprint](docs/silver/CONFIG_BLUEPRINT.md) | Declarative transformation, deduplication, and Iceberg table configuration reference. |
| **Gold Layer** | [Gold Low-Level Guide](docs/gold/GOLD_LAYER.md) | Multi-target serving, Aurora MySQL zero-downtime PK upsert, Athena Iceberg schema alignment, and initial CSV migration. |
| **Gold Config** | [Gold Config Blueprint](docs/gold/CONFIG_BLUEPRINT.md) | Business mart definition, SQL linkage, and downstream adapter configuration reference. |
| **Developer Onboarding** | [Onboarding & Operations Guide](docs/ONBOARDING_GUIDE.md) | Environment setup, CLI reference, Secrets Manager schemas, and step-by-step entity addition. |

---

## 📁 Repository Directory Structure

```text
Data-pipeline/
├── bronze/                             # Bronze Ingestion Engine
│   └── script/
│       ├── config/
│       │   └── bronze_config.json      # Ingestion configuration blueprint
│       ├── config_loader.py            # Centralized config parser & {env} interpolator
│       ├── connectors/                 # Modular upstream connectors
│       │   ├── __init__.py             # Connector registry & CONNECTOR_MAP
│       │   ├── database.py             # Relational database streaming connector
│       │   ├── genesys.py              # Genesys Cloud Analytics API connector
│       │   ├── http_client.py          # Resilient HTTP client with retry & rate limiting
│       │   ├── moveworks.py            # Moveworks Enterprise API connector
│       │   ├── oauth.py                # OAuth2 client (client_credentials, password, refresh)
│       │   ├── s3_file.py              # S3 file feed connector (CSV, JSON, Parquet)
│       │   └── servicenow.py           # ServiceNow REST Table API connector
│       └── uax_bronze_load.py          # Main Bronze execution script
├── silver/                             # Silver Conformation & Iceberg Engine
│   └── script/
│       ├── config/
│       │   └── silver_config.json      # Silver transformation & Iceberg merge configuration
│       ├── custom_transforms/          # Pluggable PySpark transformation scripts
│       ├── silver_config_loader.py     # Silver config parser & cache manager
│       ├── transformer.py              # Declarative transformation rules engine
│       └── uax_silver_etl.py           # Main Silver PySpark Iceberg execution script
├── gold/                               # Gold Analytics Marts & Multi-Target Serving Engine
│   ├── initial_exports/                # Historical CSV data migration drop directory
│   ├── query/                          # Business SQL transformation queries
│   │   ├── genesys/
│   │   │   └── conversations.sql       # Genesys conversation dimensional mart query
│   │   └── moveworks/
│   │       └── interactions.sql        # Moveworks interaction mart query
│   └── script/
│       ├── adapters/                   # Multi-target serving database connectors
│       │   ├── aurora.py               # Aurora MySQL isolated staging & PK UPSERT adapter
│       │   ├── databricks.py           # Databricks Unity Catalog Iceberg adapter
│       │   ├── redshift.py             # Amazon Redshift Spectrum external catalog adapter
│       │   └── snowflake.py            # Snowflake external Iceberg table adapter
│       ├── config/
│       │   └── gold_config.json        # Multi-target serving configuration blueprint
│       ├── custom_transforms/          # Domain-specific PySpark transformation hooks
│       ├── gold_config_loader.py       # Gold configuration loader & cache manager
│       ├── gold_initial_load.py        # Historical CSV parser, validator & auto-archiver
│       └── gold_layer_manager.py       # Master Gold orchestration engine
└── docs/                               # Enterprise Documentation Suite
│   ├── ARCHITECTURE.md                 # System-wide architectural blueprint
│   ├── ONBOARDING_GUIDE.md             # Developer setup, Glue job args & onboarding recipe
│   ├── bronze/
│   │   ├── BRONZE_LAYER.md             # Bronze low-level execution & auth guide
│   │   └── CONFIG_BLUEPRINT.md         # Bronze configuration parameter reference
│   ├── silver/
│   │   ├── SILVER_LAYER.md             # Silver low-level execution & Iceberg guide
│   │   └── CONFIG_BLUEPRINT.md         # Silver configuration parameter reference
│   └── gold/
│       ├── GOLD_LAYER.md               # Gold low-level execution & multi-target guide
│       └── CONFIG_BLUEPRINT.md         # Gold configuration parameter reference
```

---

## 🚀 AWS Glue Job Execution & Arguments

Each pipeline layer runs as an **AWS Glue Job** orchestrated by AWS Step Functions or triggered via AWS CLI / Glue Job Run calls. Parameters follow a strict hierarchy: **Glue Job Arguments (`--KEY value`) take top priority; if omitted, the job resolves parameters from its JSON configuration.**

### 1. Ingest Raw Data (Bronze — AWS Glue Python Shell)
```bash
aws glue start-job-run \
  --job-name "glue-bronze-servicenow-dev" \
  --arguments '{
    "--JOB_NAME": "glue-bronze-servicenow-dev",
    "--SOURCE_SYSTEM": "servicenow",
    "--ENV": "dev",
    "--SOURCE_TABLE_NAME": "incident",
    "--CONFIG_S3_PATH": "s3://uax-datalake-config-dev/bronze/config/bronze_config.json"
  }'
```
* **Mandatory Glue Arguments:** `--JOB_NAME`, `--SOURCE_SYSTEM`.
* **Optional Glue Arguments (Fallback to `bronze_config.json`):** `--ENV` (default: `dev`), `--CONFIG_S3_PATH`, `--SOURCE_TABLE_NAME` (all tables under source if omitted), `--BRONZE_BUCKET`, `--STATE_BUCKET`, `--SECRET_NAME`, `--BATCH_SIZE`, `--INITIAL_LOAD_DATE`, `--UPPER_BOUND`, `--FLATTEN_NESTED_JSON`, `--ERROR_HANDLING_MODE`.

---

### 2. Conform & Merge to Apache Iceberg (Silver — AWS Glue PySpark)
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
* **Mandatory Glue Arguments:** `--JOB_NAME`, `--SOURCE_SYSTEM`.
* **Optional Glue Arguments (Fallback to `silver_config.json`):** `--ENV` (default: `dev`), `--CONFIG_S3_PATH`, `--SOURCE_TABLE_NAME` (all tables under source if omitted), `--PROCESS_LAYER` (`silver`, `gold`, or `both`), `--DATA_LAKE_BUCKET`, `--GLUE_DATABASE`, `--TABLE_PREFIX`, `--BRONZE_DATA_PREFIX`, `--SILVER_DATA_PREFIX`, `--FULL_REFRESH`, `--INCREMENTAL`.

---

### 3. Build Marts & Sync Serving Targets (Gold — AWS Glue PySpark)
```bash
aws glue start-job-run \
  --job-name "glue-gold-moveworks-dev" \
  --arguments '{
    "--JOB_NAME": "glue-gold-moveworks-dev",
    "--SOURCE_SYSTEM": "moveworks",
    "--ENV": "dev",
    "--TABLE_NAME": "interactions",
    "--GOLD_SCHEMA": "enterprise_reporting",
    "--RDS_SECRET_NAME": "uax-datalake/aurora-credentials-dev"
  }'
```
* **Mandatory Glue Arguments:** `--JOB_NAME`, `--SOURCE_SYSTEM`.
* **Required for Aurora MySQL Serving:** `--GOLD_SCHEMA` (or `aurora.schema` in `gold_config.json` — zero fallback allowed by policy).
* **Optional Glue Arguments (Fallback to `gold_config.json`):** `--ENV` (default: `dev`), `--GOLD_CONFIG_S3_PATH`, `--TABLE_NAME` (all marts under source if omitted), `--DATA_LAKE_BUCKET`, `--GLUE_DATABASE`, `--RDS_SECRET_NAME`, `--GOLD_TARGETS`, `--FULL_REFRESH`, `--INCREMENTAL`.

For complete configuration blueprints and parameter definitions, consult the [Onboarding & Operations Guide](docs/ONBOARDING_GUIDE.md).
