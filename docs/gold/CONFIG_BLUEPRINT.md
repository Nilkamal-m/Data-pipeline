# Gold Layer — Configuration Blueprint Reference

This document provides the definitive configuration blueprint for `gold_config.json`. Every parameter accepted by the Gold Layer multi-target transformation and serving framework is documented below with its required/optional status, default value, and exact usage in the code.

---

## 1. Complete Blueprint Template

```json
{
  "_comment": "Gold Layer Multi-Target Transformation & Serving Blueprint",
  "pipeline_defaults": {
    "gold_bucket": "uax-datalake-gold-{env}",
    "gold_data_prefix": "gold/data",
    "target_format": "iceberg",
    "table_prefix": "gold_{source}_",
    "merge_strategy": "upsert",
    "glue_catalog": {
      "enabled": true,
      "database_name": "uax_datalake_db_{env}"
    },
    "technical_columns": {
      "inserted_at_column": "_inserted_at",
      "updated_at_column": "_updated_at"
    },
    "default_target_engines": [
      "athena"
    ],
    "initial_load": {
      "template": "s3://{bucket}/gold/initial_exports/{source}/{table}.csv",
      "format": "csv",
      "delimiter": ",",
      "has_header": true
    },
    "encryption": {
      "enabled": false,
      "lambda_arn": "",
      "batch_size": 500,
      "secret_name": "uax/protegrity-credentials-{env}",
      "user_secret_key": "protegrity_user_secret",
      "default_data_element": "AES256",
      "default_encoding": "utf8",
      "max_retries": 5
    }
  },
  "source_systems": {
    "genesys": {
      "target_engines": [
        "athena",
        "aurora"
      ],
      "tables": {
        "conversations": {
          "nkey": [
            "conversation_id"
          ],
          "incremental": true,
          "initial_load": {
            "path": "s3://{bucket}/gold/initial_exports/genesys/conversations.csv",
            "format": "csv",
            "delimiter": ",",
            "has_header": true
          },
          "encryption_columns": {
            "customer_name": "EMP_MEMBERNAME_ALPHANUM_LP",
            "customer_email": "EMP_MEMBER_EMAIL_LP"
          },
          "custom_transform_script": "gold/script/custom_transforms/genesys_conversations.py",
          "api_secret_name": "uax-datalake/genesys-credentials-{env}",
          "aurora": {
            "schema": "enterprise_reporting",
            "table_name": "gold_genesys_conversations"
          },
          "redshift": {
            "schema": "gold_spectrum_schema",
            "table_name": "gold_genesys_conversations"
          },
          "snowflake": {
            "database": "UAX_ANALYTICS_DB",
            "schema": "GOLD_MARTS",
            "table_name": "gold_genesys_conversations"
          },
          "databricks": {
            "catalog": "main",
            "schema": "gold",
            "table_name": "gold_genesys_conversations"
          }
        }
      }
    }
  }
}
```

---

## 2. Parameter Reference & Line-by-Line Annotations

### 2.1 `pipeline_defaults` Block

| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `gold_bucket` | string | **Required** (Config or Glue Argument) | *Used in `GoldLayerManager.run()`*: Target S3 bucket for Gold Iceberg marts and metadata storage. |
| `gold_data_prefix` | string | Optional (Default: `gold/data`) | *Used in Iceberg table location*: S3 prefix where Gold Iceberg data files are written. |
| `target_format` | string | Optional (Default: `iceberg`) | *Used in Spark DDL generation*: Target storage format for primary marts (`iceberg`). |
| `table_prefix` | string | Optional (Default: `gold_{source}_`) | *Used in table naming convention*: Uniform prefix prepended across all serving engines. |
| `merge_strategy` | string | Optional (Default: `upsert`) | *Used in `_upsert_iceberg_table`*: Default merge strategy (`upsert`, `append`, `overwrite`). |
| `glue_catalog.enabled` | boolean | Optional (Default: `true`) | *Used in Glue client*: Whether to register/synchronize the mart table in AWS Glue Data Catalog. |
| `glue_catalog.database_name` | string | Optional | *Used in Spark catalog queries*: Glue Database name for marts (e.g., `uax_datalake_db_{env}`). |
| `technical_columns.inserted_at_column` | string | Optional (Default: `_inserted_at`) | *Used in DataFrame enrichment*: Column name tracking record insertion timestamp. |
| `technical_columns.updated_at_column` | string | Optional (Default: `_updated_at`) | *Used in DataFrame enrichment*: Column name tracking record modification timestamp. |
| `default_target_engines` | array | Optional (Default: `["athena"]`) | *Used in serving router*: Fallback list of downstream engines when not specified at source/table level. |
| `initial_load.template` | string | Optional | *Used in `GoldInitialLoader`*: Default S3 URI template for historical data imports (`s3://{bucket}/...`). |
| `initial_load.format` | string | Optional (Default: `csv`) | *Used in Spark CSV reader*: Historical export file format (`csv`). |
| `initial_load.delimiter` | string | Optional (Default: `,`) | *Used in Spark CSV reader*: Delimiter character for parsing historical files. |
| `initial_load.has_header` | boolean | Optional (Default: `true`) | *Used in Spark CSV reader*: Flag indicating if the CSV contains a header row. |
| `encryption.enabled` | boolean | Optional (Default: `false`) | Master switch for Protegrity PII tokenization during historical export cold-start ingestion. |
| `encryption.lambda_arn` | string | **Mandatory if encryption used** | ARN of the Protegrity Protector Lambda service. Throws `ValueError` if empty when encryption columns are present. |
| `encryption.batch_size` | integer | Optional (Default: `500`) | Number of records per synchronous Lambda invocation to stay well below the 6MB limit. |
| `encryption.secret_name` | string | Optional | AWS Secrets Manager secret storing Protegrity user identity credentials. |
| `encryption.user_secret_key` | string | Optional (Default: `protegrity_user_secret`) | Key inside Secrets Manager secret JSON containing the authorization GUID. |
| `encryption.default_data_element` | string | Optional (Default: `AES256`) | Fallback Protegrity protection policy if not specified per column. |
| `encryption.default_encoding` | string | Optional (Default: `utf8`) | Character encoding passed in the Protegrity payload (`utf8`). |
| `encryption.max_retries` | integer | Optional (Default: `5`) | Maximum retry attempts for transient Lambda invocation failures with exponential backoff and jitter. |

---

### 2.2 `source_systems.<source>` Block

| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `target_engines` | array | Optional | *Used in `GoldLayerManager.run()`*: List of serving targets for all tables under this source (e.g., `["athena", "aurora"]`). |

---

### 2.3 `tables.<table>` Block

| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `nkey` | array | **Required** | *Used in `_upsert_iceberg_table` and MySQL PK upsert*: Primary / unique business key(s) identifying a distinct entity. |
| `incremental` | boolean | Optional (Default: `true`) | *Used in SQL query builder*: If `true`, filters incoming Silver data using `WHERE _updated_at > MAX(gold._updated_at)`. If `false`, full-refresh: all Silver records re-materialized. |
| `max_target_partitions` | integer | Optional (Default: none) | *Used in Spark writer before Iceberg MERGE*: Controls `coalesce(N)` applied to the mart DataFrame before writing. Limits small-file proliferation for high-volume tables. Omit to let Spark decide automatically. |
| `custom_transform_script` | string | Optional | *Used in `GoldLayerManager`*: Relative path to Python transformation script extending business logic. |
| `api_secret_name` | string | Optional | *Used in custom transforms*: Secrets Manager key name for third-party APIs (e.g. LLM scoring endpoints). |
| `initial_load.path` | string | Optional | *Used in `GoldInitialLoader`*: Specific S3 URI pointing to the historical CSV file for this table. |
| `encryption_columns` | object / array | Optional | Defines sensitive columns to encrypt and their Protegrity data element (e.g. `{"customer_name": "EMP_MEMBERNAME_ALPHANUM_LP", "customer_email": "EMP_MEMBER_EMAIL_LP"}`). Used in `gold_initial_load.py` for cold-start CSV exports. |

> [!TIP]
> **Immediate Short-Circuit Rule for Encryption**:
> If `encryption_columns` is omitted or empty on a table, the Gold initial load engine (`gold_initial_load.py`) **immediately skips** all encryption processing. It does not check `encryption.enabled: true` and does not check or validate `lambda_arn`. Only when `encryption_columns` contains one or more non-empty column mappings does it validate `lambda_arn` (raising an explicit `ValueError` if missing) and encrypt the raw historical DataFrame via `mapInPandas` micro-batches.

---

### 2.4 Multi-Target Serving Engine Blocks

The config keys below tell `gold_layer_manager.py` **where** to write for each downstream engine. All serving is handled by inline routing methods inside `GoldLayerManager` — see [ENHANCEMENT_GUIDE.md § 10](ENHANCEMENT_GUIDE.md#10-adding-a-new-downstream-target-engine) for how to add a new target.

#### Amazon Aurora MySQL (`aurora`)
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `schema` | string | **Required** | *Used in `_serve_to_mysql()`*: Target MySQL database/schema name (e.g., `enterprise_reporting`). Mandatory per zero-fallback policy. |
| `table_name` | string | **Required** | *Used in `_serve_to_mysql()`*: Target physical MySQL table name (e.g., `gold_moveworks_interactions`). |

##### AWS Secrets Manager Credentials Contract:
Credentials for Aurora MySQL are fetched via the secret referenced by `--RDS_SECRET_NAME` (or `secret_name` in config). The secret JSON payload must provide:

| Secret Key | Permitted Aliases | Description |
| :--- | :--- | :--- |
| `host` | `HOST` | Cluster writer endpoint URL for Aurora MySQL or RDS instance. |
| `port` | `PORT` | MySQL connection port (`3306` default). |
| `username` | `user`, `USERNAME` | Database application username with DDL/DML privileges. |
| `password` | `PASSWORD`, `pwd`, `db_password` | Database password (raises `ValueError` if missing). |
| `engine` | `db_type` | Database engine type (`mysql`). |

> **Flow:** Gold Glue job materializes the Iceberg mart first, then reads it back via `spark.table()` and writes to Aurora using a staging-swap pattern. Aurora is **downstream of Iceberg**, not a parallel write target.

#### Amazon Redshift Spectrum (`redshift`)
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `schema` | string | **Required** | *Used in Redshift sync routing*: Redshift external schema name referencing the Glue Data Catalog. |
| `table_name` | string | **Required** | *Used in Redshift sync routing*: Physical external table name exposed to BI queries. |

#### Snowflake (`snowflake`)
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `database` | string | **Required** | *Used in Snowflake sync routing*: Target Snowflake database name. |
| `schema` | string | **Required** | *Used in Snowflake sync routing*: Target Snowflake schema name. |
| `table_name` | string | **Required** | *Used in Snowflake sync routing*: Snowflake external Iceberg table identifier. |

#### Databricks (`databricks`)
| Parameter | Type | Required / Optional | Code Usage & Description |
| :--- | :--- | :--- | :--- |
| `catalog` | string | **Required** | *Used in Databricks sync routing*: Databricks Unity Catalog name. |
| `schema` | string | **Required** | *Used in Databricks sync routing*: Databricks schema name. |
| `table_name` | string | **Required** | *Used in Databricks sync routing*: Unity Catalog registered table name. |
