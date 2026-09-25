# Bronze Layer — Enhancement Guide

This guide explains how the Bronze layer code is structured and how to safely extend it — adding new sources, new connectors, or modifying ingestion behavior.

---

## 1. Code Structure at a Glance

```
bronze/script/
├── uax_bronze_load.py          # Main entry point (CLI → orchestration loop)
├── config_loader.py            # JSON config reader, {env} interpolation, watermark helpers
├── config/
│   └── bronze_config.json      # All source and table definitions
└── connectors/
    ├── __init__.py             # CONNECTOR_MAP — maps source type to connector class
    ├── base_connector.py       # Abstract BaseConnector interface
    ├── http_client.py          # Shared HTTP session, retry, backoff
    ├── oauth.py                # OAuth2 multi-grant token manager
    ├── servicenow_connector.py # ServiceNow REST Table API
    ├── genesys_connector.py    # Genesys Cloud Analytics API
    ├── moveworks_connector.py  # Moveworks Enterprise API
    ├── database_connector.py   # Relational DB JDBC streaming
    └── s3_file.py              # S3 file drop ingestion
```

---

## 2. How the Main Job Works (`uax_bronze_load.py`)

The entry point follows a single, linear execution loop:

```
parse_cli_args()
    └─► ConfigLoader.load_config()          # Loads and interpolates bronze_config.json
        └─► for each table in source_system:
                _get_watermark()            # Read S3 state file (last_load_date)
                └─► route_connector()       # Instantiate the right connector from CONNECTOR_MAP
                    └─► connector.fetch_delta()  # Pull records incrementally
                        └─► _write_parquet()     # Write Snappy Parquet to S3 (_ingested_at=<ISO8601_TIMESTAMP>/)
                            └─► _commit_watermark()   # Write updated watermark.json to S3
```

**Key design choices:**
- The watermark is read **before** extraction and written **after** a successful write.
- If a table fails, the watermark stays at its old value — the next run will re-extract the same window.
- `error_handling_mode = CONTINUE_ON_ERROR` lets the loop skip a failing table and continue with the rest.

---

## 3. How the Connector Routing Works (`connectors/__init__.py`)

`CONNECTOR_MAP` maps a `type` key from config to a connector class:

```python
CONNECTOR_MAP = {
    "rest":       GenesysConnector,      # default for REST APIs
    "servicenow": ServiceNowConnector,
    "genesys":    GenesysConnector,
    "moveworks":  MoveworksConnector,
    "database":   DatabaseConnector,
    "s3_file":    S3FileConnector,
}
```

The main job calls `get_connector(source_config)` which inspects `source_config["type"]` or `source_config["connection_type"]` and returns the matching class. This means adding a new source type requires only:
1. Writing the connector class.
2. Adding one entry to `CONNECTOR_MAP`.

---

## 4. Connector Interface (`base_connector.py`)

Every connector must implement:

```python
class BaseConnector:
    @classmethod
    def fetch_delta(
        cls,
        last_load_date: str,
        secret_dict: dict,
        table_name: str,
        source_config: dict,
        custom_query: str | None,
        on_chunk_callback: callable,
        s3_chunk_size: int,
    ) -> None:
        ...
```

- `last_load_date`: The watermark string (`YYYY-MM-DD HH:MM:SS`). The connector uses this to filter records.
- `secret_dict`: Decrypted Secrets Manager payload (credentials, API base URLs).
- `on_chunk_callback(records: list[dict], part: int)`: The connector calls this for each batch of records. The main job's callback flattens and writes a Parquet partition.
- The connector **does not write to S3 directly** — it only yields record batches through the callback.

---

## 5. The S3 File Connector (`s3_file.py`)

Used when `"type": "s3_file"` is set in config. Key behaviors:

| Feature | How It Works |
|---|---|
| **File discovery** | Paginates `list_objects_v2` on `source_bucket` + `file_prefix` |
| **Pattern matching** | If `file_pattern` is set (e.g. `conversations_*.csv`), uses `fnmatch` to filter keys |
| **fetch_mode=all** | Returns all files whose `LastModified > watermark` |
| **fetch_mode=latest** | Returns only the single most recently modified file, ignoring watermark |
| **{env} interpolation** | If `source_bucket` contains `{env}`, it is replaced at runtime from config or `ENV` env var |
| **Cross-account** | If `secret_dict` contains `aws_access_key_id`, a dedicated boto3 client is built |
| **multiLine parsing** | Default `true`: uses `io.StringIO` and dynamic field limits to handle multi-paragraph text fields with embedded newlines |
| **escape option** | Default `\\`: handles escaped quotes (e.g. `\"`) and characters within CSV records without corrupting rows |

**Per-table path resolution priority (highest to lowest):**
1. `tables.<table_name>.file_path`
2. `table_paths.<table_name>` (source-level map)
3. `file_prefix` (source-level default)

---

## 6. Config Loader & `{env}` Interpolation (`config_loader.py`)

`ConfigLoader.load_config(s3_path, env)`:
1. Reads JSON from S3 or local file.
2. Walks every string value recursively and replaces `{env}` with the runtime environment name (`dev`, `prod`, etc.).
3. Returns the merged config dict with `pipeline_defaults` merged into each source table config.

**Watermark helpers:**
- `get_watermark(state_bucket, state_prefix, source, table)` — reads `watermark.json` from S3; returns `default_initial_load_date` if missing.
- `commit_watermark(state_bucket, state_prefix, source, table, payload)` — atomically puts the updated JSON to S3.

---

## 7. Adding a New REST Source

### Step 1 — Add config entry
```json
"source_systems": {
  "my_new_api": {
    "type": "rest",
    "base_url": "https://api.vendor.com",
    "api_endpoint_template": "/v1/{table_name}",
    "default_delta_filter": "updated_at>={last_load_date}",
    "response_records_key": "data",
    "batch_size": 500,
    "tables": {
      "orders": {
        "initial_load_date": "2025-01-01 00:00:00"
      }
    }
  }
}
```

### Step 2 — Point to an existing connector or create a new one
If the API uses standard OAuth2 + pagination, reuse `GenesysConnector` or `MoveworksConnector` as a template.

If it needs custom protocol handling:
1. Create `bronze/script/connectors/my_new_api_connector.py`.
2. Implement `fetch_delta(...)` following the `BaseConnector` interface.
3. Register: `CONNECTOR_MAP["my_new_api"] = MyNewApiConnector`.

### Step 3 — Add Secrets Manager entry
```json
{
  "auth_type": "oauth",
  "token_url": "https://auth.vendor.com/oauth/token",
  "client_id": "...",
  "client_secret": "...",
  "grant_type": "client_credentials",
  "api_base_url": "https://api.vendor.com"
}
```
Secret name: `uax-datalake/my-new-api-credentials-{env}`

---

## 8. Adding a New S3 File Source

```json
"source_systems": {
  "hr_feed": {
    "type": "s3_file",
    "source_bucket": "uax-datalake-{env}-vendor-drops",
    "file_prefix": "hr/daily/",
    "file_format": "csv",
    "has_header": true,
    "fetch_mode": "latest",
    "tables": {
      "employees": {
        "initial_load_date": "2025-01-01 00:00:00",
        "file_path": "hr/daily/employees/",
        "fetch_mode": "latest"
      }
    }
  }
}
```

No code change is required — `S3FileConnector` handles this automatically.

---

## 9. Audit Columns Injected by Bronze

Every record written to the Bronze Parquet layer is enriched with:

| Column | Description |
|---|---|
| `_ingested_at` | UTC timestamp of when this record was written to Bronze |
| `_source_system` | Source name from config (e.g., `genesys`) |
| `_table_name` | Table name from config (e.g., `conversations`) |
| `_execution_id` | Unique Glue job run ID for lineage tracing |

These columns are used by the Silver layer to filter incremental batches (`_ingested_at > watermark`).

---

## 10. Testing Connectors Locally

Each connector can be unit-tested without a Glue context:

```python
from bronze.script.connectors.s3_file import S3FileConnector

records = []
def capture(chunk, part):
    records.extend(chunk)

S3FileConnector.fetch_delta(
    last_load_date="2025-01-01 00:00:00",
    secret_dict={},
    table_name="employees",
    source_config={
        "source_bucket": "my-test-bucket",
        "file_prefix": "hr/daily/employees/",
        "file_format": "csv",
        "has_header": True,
        "fetch_mode": "all",
    },
    on_chunk_callback=capture,
    s3_chunk_size=1000,
)
print(f"Captured {len(records)} records")
```
