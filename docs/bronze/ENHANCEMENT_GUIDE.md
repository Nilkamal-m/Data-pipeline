# Bronze Layer — Enhancement Guide

This guide explains how the Bronze layer code is structured and how to safely extend it — adding new sources, new connectors, or modifying ingestion behavior.

---

## 1. Code Structure at a Glance

```
bronze/script/
├── uax_bronze_load.py          # Main entry point (AWS Glue arguments → orchestration loop)
├── config_loader.py            # JSON config reader, {env} interpolation, watermark helpers
├── config/
│   └── bronze_config.json      # All source and table definitions
└── connectors/
    ├── __init__.py             # CONNECTOR_MAP — maps source type to connector class
    ├── database.py             # Relational DB JDBC streaming
    ├── genesys.py              # Genesys Cloud Analytics API
    ├── http_client.py          # Shared HTTP session, retry, backoff
    ├── moveworks.py            # Moveworks Enterprise API
    ├── oauth.py                # OAuth2 multi-grant token manager
    ├── s3_file.py              # S3 file drop ingestion
    └── servicenow.py           # ServiceNow REST Table API
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

## 4. Connector Contract & Onboarding Class Pattern

To onboard a new upstream system, connectors follow a modular, duck-typed class pattern. **No base class inheritance is used**—any class implementing the standard `@classmethod def fetch_delta(...)` can be plugged directly into the pipeline.

### 4.1 Production Boilerplate Connector Template

Save new connectors under `bronze/script/connectors/<source_name>.py`:

```python
import logging
from typing import Dict, Any, Optional, Callable, List
from .http_client import HttpClient
from .oauth import OAuthHandler

logger = logging.getLogger(__name__)

class CustomApiConnector:
    """
    Modular upstream connector for Custom REST API.
    Handles authentication, incremental pagination, response envelope extraction,
    and streams batches to Bronze storage via on_chunk_callback.
    """

    @classmethod
    def fetch_delta(
        cls,
        last_load_date: str,
        secret_dict: Dict[str, Any],
        table_name: str,
        source_config: Dict[str, Any],
        custom_query: Optional[str],
        on_chunk_callback: Callable[[List[Dict[str, Any]], int], None],
        s3_chunk_size: int,
        upper_bound: Optional[str] = None,
    ) -> int:
        """
        Extracts records from source API and streams chunks to Bronze via on_chunk_callback.

        Args:
            last_load_date: Watermark timestamp boundary ('YYYY-MM-DD HH:MM:SS').
            secret_dict: Decrypted credentials from AWS Secrets Manager.
            table_name: Logical table name being extracted.
            source_config: Merged table and source configuration dictionary.
            custom_query: Optional custom filter override.
            on_chunk_callback: Callback function to flatten and write Parquet chunk to S3.
            s3_chunk_size: Chunk threshold (records) before triggering on_chunk_callback.
            upper_bound: Optional upper timestamp boundary.

        Returns:
            int: Total count of records ingested for this table.
        """
        # 1. Resolve response envelope key (Mandatory for REST APIs)
        response_key = source_config.get('response_records_key')
        if not response_key:
            raise ValueError(
                f"CustomApiConnector for '{table_name}': 'response_records_key' is not set. "
                f"Add 'response_records_key' (e.g. 'result', 'entities', or 'value') to bronze_config.json."
            )

        # 2. Initialize authenticated HTTP session
        base_url = secret_dict.get('api_base_url') or source_config.get('base_url')
        token = OAuthHandler.get_token(secret_dict) if secret_dict.get('auth_type') == 'oauth' else None
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        client = HttpClient.get_session(retries=3, backoff_factor=1.5)

        # 3. Construct endpoint and pagination parameters
        endpoint_template = source_config.get('api_endpoint_template', '/api/v1/{table_name}')
        url = f"{base_url.rstrip('/')}/{endpoint_template.format(table_name=table_name).lstrip('/')}"
        
        batch_size = source_config.get('batch_size', s3_chunk_size)
        offset = 0
        total_records = 0
        part_num = 1
        current_chunk = []

        logger.info(f"Starting extraction for '{table_name}' from '{url}' with watermark > '{last_load_date}'...")

        # 4. Ingestion / Pagination Loop
        while True:
            params = {
                "limit": batch_size,
                "offset": offset,
                "updated_after": last_load_date
            }
            if upper_bound:
                params["updated_before"] = upper_bound

            resp = client.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            # 5. Extract records using response_records_key
            records = data.get(response_key, [])
            if not records:
                break

            current_chunk.extend(records)
            total_records += len(records)

            # 6. Stream chunk to S3 when batch size threshold is reached
            if len(current_chunk) >= s3_chunk_size:
                on_chunk_callback(current_chunk, part_num)
                part_num += 1
                current_chunk = []

            if len(records) < batch_size:
                break  # Last page reached

            offset += len(records)

        # 7. Flush any remaining records in final chunk
        if current_chunk:
            on_chunk_callback(current_chunk, part_num)

        logger.info(f"Completed extraction for '{table_name}': {total_records} record(s) ingested.")
        return total_records
```

### 4.2 Registering the Connector in `connectors/__init__.py`

To activate the connector, import and register it in `bronze/script/connectors/__init__.py`:

```python
from .custom_api import CustomApiConnector

CONNECTOR_MAP = {
    "custom_api": CustomApiConnector,
    ...
}
```

Now any table in `bronze_config.json` with `"type": "custom_api"` or `"connection_type": "custom_api"` will automatically route to `CustomApiConnector`.

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
1. Create `bronze/script/connectors/my_new_api.py`.
2. Implement `fetch_delta(...)` following the connector protocol.
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
