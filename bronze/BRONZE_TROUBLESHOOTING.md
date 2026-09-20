# Bronze Layer — Troubleshooting Guide

> This guide catalogs every known error, warning, and silent failure in the Bronze pipeline with **exact cause** and **precise fix**.  
> Errors are grouped by phase: Startup → Config → Connector → S3 → Glue Catalog.

---

## Error Catalog

### Startup Errors (Job Won't Start)

---

#### E01 — `ValueError: Missing required argument '--SOURCE_SYSTEM'`

**When**: `SOURCE_SYSTEM` CLI argument is not set.

**Fix**: Always pass `--SOURCE_SYSTEM <name>` in the Glue job arguments. Valid values: `moveworks`, `servicenow`, `genesys`, `postgresql`, `mysql`, `mariadb`, `sqlite`, `s3_file`, or any custom vendor source registered in `bronze_config.json`.

---

#### E02 — `ValueError: 'BRONZE_BUCKET' is required`

**When**: `BRONZE_BUCKET` is not set via CLI, `pipeline_defaults.bronze_bucket` in config, or `BRONZE_BUCKET` environment variable.

**Fix** (choose one):
- CLI: `--BRONZE_BUCKET my-datalake-bucket`
- Config: `"pipeline_defaults": { "bronze_bucket": "my-datalake-bucket" }`
- Env: set `BRONZE_BUCKET` in the Glue job environment.

---

#### E03 — `ValueError: Unsupported source system: 'xyz'`

**When**: `SOURCE_SYSTEM` value is not in `CONNECTOR_MAP` and the source config block has no `type` key.

**Fix** (choose one):
- Use a registered source name: `moveworks`, `servicenow`, `genesys`, `postgresql`, `mysql`, `mariadb`, `sqlite`, `s3_file`.
- For a custom vendor source: add `"type": "s3_file"` (or another valid type) to its block in `bronze_config.json`.
- For a new connector: implement `fetch_delta()`, add to `connectors/__init__.py` CONNECTOR_MAP, add config block.

---

#### E04 — `FileNotFoundError: Bronze configuration file 'bronze_config.json' not found`

**When**: `config_loader.py` cannot find `bronze_config.json` locally and no `--CONFIG_S3_PATH` was given.

**Fix**:
- Verify `script/config/bronze_config.json` exists.
- Or pass `--CONFIG_S3_PATH s3://<bucket>/path/to/bronze_config.json`.
- When running in Glue: ensure `bronze_config.json` is uploaded as a Glue job file or is reachable at the S3 path.

---

#### E05 — `CRITICAL CONFIG ERROR: 'database_name' is missing`

**When**: `GLUE_CATALOG_ENABLED=true` but `pipeline_defaults.glue_catalog.database_name` is empty or missing.

**Fix**:
```json
"pipeline_defaults": {
  "glue_catalog": {
    "database_name": "uax_datalake_db_{env}"
  }
}
```
Or pass `--GLUE_DATABASE uax_datalake_db_{env}` (or `--ENV dev`) via CLI.

---

#### E06 — `CRITICAL CONFIG ERROR: 'table_prefix' is missing`

**When**: `GLUE_CATALOG_ENABLED=true` but `glue_catalog.table_prefix` is empty.

**Fix**:
```json
"glue_catalog": { "table_prefix": "raw_tbl_" }
```

---

#### E07 — `CRITICAL CONFIG ERROR: 'watermark_table_name' is missing`

**When**: `SYNC_WATERMARK_TABLE=true` but `glue_catalog.watermark_table_name` is empty.

**Fix**:
```json
"glue_catalog": { "watermark_table_name": "raw_tbl_watermarks" }
```

---

### Configuration Errors (Job Starts, Fails on First Table)

---

#### E10 — `CRITICAL ERROR: No initial load date specified for table '<table>'`

**When**: Table has no S3 watermark (first run) and no initial load date is configured.

**Fix** (choose one, in priority order):
1. CLI: `--INITIAL_LOAD_DATE "2024-01-01 00:00:00"` (applies to all tables in this run).
2. Config per-table under `tables.<table_name>`:
   ```json
   "tables": {
     "incident": {
       "initial_load_date": "2024-01-01 00:00:00"
     }
   }
   ```
3. Config global fallback (use with caution):
   ```json
   "pipeline_defaults": { "default_initial_load_date": "2024-01-01 00:00:00" }
   ```

---

#### E11 — `No tables configured for source '<source>'`

**When**: `--SOURCE_TABLE_NAME` was not passed via CLI and the source has no tables configured under `tables` (or legacy `default_tables`).

**Fix**: Add table definitions to `tables` in the source block:
```json
"moveworks": {
  "tables": {
    "conversations": { "initial_load_date": "2024-01-01 00:00:00" },
    "interactions": { "initial_load_date": "2024-01-01 00:00:00" },
    "users": { "initial_load_date": "2024-01-01 00:00:00" }
  }
}
```

---

### Connector Errors (Table-Level)

---

#### E20 — `ValueError: '<source> connector for '<table>': API base URL is not configured`

**When**: `api_base_url` is absent from both Secrets Manager and `base_url` in `bronze_config.json`.

**Fix** (choose one):
- Add `api_base_url` to the Secrets Manager secret.
- Add `base_url` to the source config block:
  ```json
  "moveworks": { "base_url": "https://api.moveworks.ai" }
  ```

---

#### E21 — `ValueError: '<source> connector for '<table>': 'response_records_key' is not set`

**When**: `response_records_key` missing from source config block.

**Fix**: Add the correct JSON key name:
```json
"moveworks":   { "response_records_key": "value" }
"servicenow":  { "response_records_key": "result" }
"genesys":     { "response_records_key": "entities" }
```

---

#### E22 — `ValueError: '<source> connector for '<table>': 'batch_size' is not configured`

**When**: `batch_size` missing from both Secrets Manager and source config block.

**Fix**: Add `batch_size` to the source config block:
```json
"moveworks":  { "batch_size": 500 }
"servicenow": { "batch_size": 1000 }
"genesys":    { "batch_size": 100 }
```
Note: Moveworks enforces a maximum of 500 — any higher value is silently capped.

---

#### E23 — `ValueError: Database connector: 'port' is required in Secrets Manager`

**When**: `port` key is absent from the Secrets Manager secret for a database source.

**Fix**: Add `port` to your Secrets Manager secret:
```json
{ "port": "5432" }   // PostgreSQL
{ "port": "3306" }   // MySQL / MariaDB
```

The `port_reference` value in `bronze_config.json` is **documentation only** and is never read by the connector.

---

#### E24 — `ValueError: Database connector: 'db_type' is required`

**When**: `db_type` absent from both Secrets Manager and source config block.

**Fix**: Set `db_type` in Secrets Manager: `"db_type": "postgresql"`.  
Or in config: `"source_systems.postgresql": { "db_type": "postgresql" }`.

---

#### E25 — `ValueError: Database connector: unsupported 'db_type': '<value>'`

**When**: `db_type` value is not one of `postgresql`, `postgres`, `mysql`, `mariadb`, `sqlite`.

**Fix**: Use an exact supported value. Note: both `postgresql` and `postgres` map to the same driver.

---

#### E26 — `KeyError: Response key '<key>' not found or not a list`

**When**: The API response does not contain `response_records_key` at top level, or its value is not a list.

**Diagnosis**: Check the API response schema for the endpoint. The log line will show available keys:
```
Available keys: ['@odata.context', '@odata.nextLink', 'content', ...]
```

**Fix**: Update `response_records_key` in `bronze_config.json` to the correct key name.  
Or add a `custom_table_endpoints` entry if the endpoint itself is wrong.

---

#### E27 — `ValueError: S3FileConnector for '<table>': 'source_bucket' is not configured`

**When**: `source_bucket` absent from the S3 source config block.

**Fix**:
```json
"vendor_a_s3": { "source_bucket": "vendor-a-incoming-bucket" }
```

---

#### E28 — `ValueError: S3FileConnector for '<table>': invalid 'fetch_mode' = '<value>'`

**When**: `fetch_mode` is set to a value other than `all` or `latest`.

**Fix**: Use only `"fetch_mode": "all"` or `"fetch_mode": "latest"`.

---

#### E29 — `RuntimeError: [<table>] N of M shard(s) failed. Failed windows: [(lb, ub), ...]`

**When**: One or more parallel Moveworks shards failed (network error, API 5xx, or timeout).

**Context**: Successfully fetched shards are already staged; they are NOT rolled back.

**Fix**:
1. Check CloudWatch logs for the per-shard error detail (look for `[<table>] Shard N/M FAILED`).
2. If transient network/API issue: re-run the job. The watermark was not updated, so the full range will be re-extracted. Previously staged data will be overwritten.
3. If `max_workers` is too high causing API throttling: reduce `parallel_processing.max_workers` in config.
4. If shard window is too wide: reduce `shard_window_days` (e.g. `7` instead of `15`).

---

### OAuth / Auth Errors

---

#### E30 — `ValueError: OAuth 2.0 error: 'token_url' is missing in Secrets Manager`

**When**: `auth_type: oauth` but `token_url` not in Secrets Manager.

**Fix**: Add `token_url` to the secret:
```json
{
  "auth_type": "oauth",
  "token_url": "https://api.moveworks.ai/oauth/v1/token",
  "grant_type": "client_credentials",
  "client_id": "...",
  "client_secret": "..."
}
```

---

#### E31 — `RuntimeError: OAuth 2.0 token request to '<url>' failed`

**When**: The token endpoint returned an HTTP error or network failure.

**Fix**:
- Verify `token_url` is correct.
- Verify `client_id` and `client_secret` are valid.
- Check if the Glue IAM role can reach the OAuth endpoint (check VPC/security group if running in a VPC).

---

#### E32 — `ValueError: auth_type=basic requires 'username' / 'password' in Secrets Manager`

**When**: `auth_type: basic` but `username` or `password` not in secret.

**Fix**: Add `username` and `password` to the Secrets Manager secret.

---

#### E33 — `HTTPError 401` — repeated after token refresh

**When**: OAuth token is acquired but API returns 401. Happens once per request (connector retries with refreshed token). If 401 persists after refresh, the connector raises.

**Fix**:
- Verify `client_id` / `client_secret` have the correct scopes for the requested endpoint.
- For Moveworks: confirm the `assistant_name` header matches the configured assistant.
- Check token expiry settings on the API server side.

---

### S3 / Staging Errors

---

#### E40 — `ClientError: Access Denied` on `s3.put_object` to staging

**When**: Glue execution role lacks write permission on `<bronze_bucket>/_staging/` prefix.

**Fix**: Attach an IAM policy granting `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject`, `s3:ListBucket` on `arn:aws:s3:::<bronze_bucket>/*`.

---

#### E41 — Staging files not cleaned up after failure (orphan files)

**When**: `cleanup_failed_staging()` was interrupted (e.g. Glue job timeout), leaving `_staging/exec_<id>/` files.

**Diagnosis**: Check for `_staging/` prefixes older than 24h in your bronze bucket.

**Fix**: Manually delete via AWS Console or:
```bash
aws s3 rm s3://<bronze_bucket>/_staging/ --recursive
```
Or create an S3 lifecycle rule to auto-expire `_staging/` objects after 1 day.

---

#### E42 — `ClientError: NoSuchBucket` on state file read

**When**: `STATE_BUCKET` value is incorrect or the bucket does not exist in the Glue job's region.

**Fix**: Verify `STATE_BUCKET` or `BRONZE_BUCKET` name. Check AWS region.

---

#### E43 — Extraction Returns 0 Records Due to Inverted Window (`upper_bound < lower_bound`)

**When**: An explicit `--UPPER_BOUND` or table-wise `table_upper_bounds` is earlier than the table's current watermark (`last_load_date`). Example: `lower_bound = '2024-05-01 00:00:00'` and `--UPPER_BOUND '2024-03-01 00:00:00'`.

**Cause**: The filter expression `WHERE updated_at >= '2024-05-01 00:00:00' and updated_at <= '2024-03-01 00:00:00'` evaluates to an empty set.

**Fix**: Ensure `--UPPER_BOUND` or `table_upper_bounds` is strictly greater than `last_load_date`. If backfilling an earlier historical period, also supply `--INITIAL_LOAD_DATE` to reset `lower_bound` for the backfill run:
```bash
--INITIAL_LOAD_DATE "2024-01-01 00:00:00" --UPPER_BOUND "2024-03-01 00:00:00"
```

---

#### E44 — Unregistered Source System During New Source Onboarding

**When**: A newly developed connector fails with:
`ValueError: Unsupported source system: 'new_source'. Not found in CONNECTOR_MAP and no valid 'type' found in config.`

**Cause**: The connector class was created but not mapped in `bronze/script/connectors/__init__.py`.

**Fix**: Open `bronze/script/connectors/__init__.py` and add the mapping:
```python
from connectors.new_source import NewSourceConnector
CONNECTOR_MAP['new_source'] = NewSourceConnector
```

---

#### E45 — Missing Required Secrets Manager Keys During New Source Onboarding

**When**: The new connector raises `KeyError: 'api_key'` or `ValueError: Missing required secret credentials`.

**Cause**: The secret created in AWS Secrets Manager does not contain all keys expected by the connector.

**Fix**: Verify the secret payload in AWS Secrets Manager matches the connector's required fields:
```bash
aws secretsmanager get-secret-value --secret-id prod/new_source/api_credentials --query SecretString --output text
```

---

#### E46 — Unparseable Timestamp String in `upper_bound` or `initial_load_date`

**When**: S3FileConnector or Moveworks sharding raises `ValueError: Invalid isoformat string` or `strptime` error.

**Cause**: The timestamp passed via `--UPPER_BOUND` or `--INITIAL_LOAD_DATE` is in an unrecognized date format.

**Fix**: Use standard database timestamp format `"YYYY-MM-DD HH:MM:SS"` (e.g. `"9999-01-01 00:00:00"`) or standard ISO 8601 UTC format `"YYYY-MM-DDTHH:MM:SSZ"` (e.g. `"2024-03-01T00:00:00Z"`). Both formats are automatically supported and parsed by all Bronze connectors. When `"9999-01-01 00:00:00"` is used, the pipeline automatically records the current run time into the watermark state to prevent state corruption.


---

### Glue Catalog Errors

---

#### E50 — `WARNING: Failed to create Glue Catalog table '<table>'`

**When**: IAM role lacks `glue:CreateTable` permission.

**Impact**: Data is still written to S3. Athena queries will not work until the catalog is fixed.

**Fix**: Attach `glue:CreateTable`, `glue:GetTable`, `glue:CreatePartition`, `glue:UpdatePartition`, `glue:GetDatabase`, `glue:CreateDatabase` to the Glue execution role.

---

#### E51 — Athena query returns 0 rows despite data in S3

**Causes**:
1. `glue_catalog.enabled: false` — catalog not synced.
2. Partition `_ingested_at=<TS>` not registered (log shows `WARNING: Could not register partition`).
3. SerDe mismatch (table was created as JSON but data is Parquet).

**Fix**:
1. Ensure `glue_catalog.enabled: true`.
2. Check IAM for `glue:CreatePartition`.
3. If SerDe mismatch: delete the Glue catalog table and let the job recreate it on the next run.

---

#### E52 — Glue Crawler shows `STOPPING` and never completing

**When**: The crawler `trigger_crawler_name` points to a crawler that does not exist.

**Impact**: Glue logs `WARNING: Could not trigger Glue Crawler`. Data is still queryable (partitions registered via API).

**Fix**: Either provision the crawler in AWS Glue or set `trigger_crawler: false` in config.

---

### Performance Issues

---

#### P01 — Job runs slower than expected with many tables

**Diagnosis**: Tables are processed serially. CloudWatch `IngestionDurationSeconds` will show which table is slowest.

**Fix options**:
- For Moveworks: enable `parallel_processing` with appropriate `max_workers` and `shard_window_days`.
- For other sources: reduce `batch_size` if API throttling, or increase `fetch_size` for databases.
- Split large tables into separate Glue jobs and run concurrently.

---

#### P02 — `MemoryError` or Glue OOM on a large table

**Cause**: `s3_chunk_size` too large — too many records held in `records_buffer` before flush.

**Fix**: Reduce `s3_chunk_size` via CLI or config. Start with `5000` and reduce if OOM persists:
```bash
--S3_CHUNK_SIZE 2000
```

---

#### P03 — Moveworks extraction is slow despite parallel enabled

**Diagnosis**: Check if `users` table is included — it has a mandatory 2-second inter-page delay (per Moveworks API requirements) and cannot be parallelized within its own shard.

**Fix**: Run `users` as a separate job from `interactions`/`conversations` so they don't block each other.

---

### Silent Failures (No Exception, Wrong Behavior)

---

#### S01 — Watermark advances but 0 records ingested for all subsequent runs

**Cause**: `lower_bound` (`last_load_date`) equals `upper_bound` — no time window to extract.

**When**: Can happen if the job is re-run within the same second as the watermark timestamp.

**Fix**: Wait at least 1 second between re-runs. For testing, reset the watermark by deleting:
```bash
aws s3 rm s3://<state_bucket>/metadata/bronze/<source>/<table>/watermark.json
```

---

#### S02 — Same records appear in multiple S3 partitions

**Cause**: Job ran, wrote staging, failed before writing watermark, was re-run. Re-run creates a new `_ingested_at` partition with the same records.

**This is expected behavior.** Bronze is a raw landing zone — duplicates across partitions are resolved in Silver/Gold layers.

**Prevention**: Use `HALT_ON_ERROR` mode and alert on failures to avoid repeated re-runs without investigation.

---

#### S03 — `response_records_key` check passes but column names are wrong

**Cause**: API returned records successfully but the field names in the response changed (e.g., API version upgrade).

**Diagnosis**: Enable `flatten_nested_json: true` and check Parquet column names via Athena:
```sql
SELECT * FROM uax_datalake_db_dev.raw_tbl_incident LIMIT 1;
```

**Fix**: No code change needed. Bronze stores raw records as-is. Schema drift in Bronze is expected — Silver handles normalization.

---

#### S04 — S3 file source picks up wrong files

**Cause**: `file_prefix` path is too broad or `fetch_mode` is `all` when `latest` was intended.

**Fix**:
```json
"table_paths": { "employee_feed": "hr/employees/2026/" },
"table_fetch_modes": { "employee_feed": "latest" }
```

---

## Quick Reference — Required Secrets Manager Keys

| Source | Required Keys |
|--------|--------------|
| Moveworks | `auth_type`, `token_url`, `client_id`, `client_secret`, `grant_type`, `api_base_url`, `batch_size` |
| ServiceNow (basic) | `auth_type`, `api_base_url`, `username`, `password`, `batch_size` |
| ServiceNow (oauth) | `auth_type`, `token_url`, `client_id`, `client_secret`, `grant_type`, `api_base_url`, `batch_size` |
| Genesys | `auth_type`, `token_url`, `client_id`, `client_secret`, `grant_type`, `api_base_url`, `batch_size` |
| PostgreSQL | `db_type`, `host`, `dbname`, `username`, `password`, `port` |
| MySQL/MariaDB | `db_type`, `host`, `dbname`, `username`, `password`, `port` |
| SQLite | `db_type`, `dbname` (file path) |
| S3File (same account) | None required — IAM role used |
| S3File (cross-account) | `aws_access_key_id`, `aws_secret_access_key`, optional: `aws_session_token` |
