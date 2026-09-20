# 🚀 Architecture & Implementation Plan: Parallel Ingestion Engine for Moveworks Data API

## 1. Executive Summary & Root-Cause Diagnosis

### Problem Summary
The current Bronze layer ingestion job runs sequentially against the Moveworks REST API. When attempting to ingest **~1.7 million records** at a batch size of 500 records/hit, the Glue job ran for **over 20 hours** before ultimately timing out or failing.

### Key Insights from Official Moveworks Documentation
Based on the official [Moveworks Data API Documentation: How to build raw interactions table using Data API](https://docs.moveworks.com/ai-assistant/data-api/how-to-build-raw-interactions-table-using-data-api#python-script):

1. **Date-Range Filtering is Natively Supported**:
   - Moveworks supports bounded OData date filtering:
     ```text
     $filter=last_updated_time ge 'YYYY-MM-DDTHH:MM:SS.fffZ' and last_updated_time le 'YYYY-MM-DDTHH:MM:SS.fffZ'
     ```
   - **Significance**: This unlocks **deterministic time-slice sharding**, allowing 1.7M records to be partitioned into independent, non-overlapping date buckets that can be processed in parallel.

2. **The 10-Second Sleep Was an Artificial Bottleneck**:
   - In the current code (`moveworks.py`), a hardcoded `time.sleep(10)` runs after **every single page** of 500 records.
   - For 1.7M records (3,400 pages), `3,400 * 10s = 34,000s` (~**9.44 hours**) was spent purely in Python `sleep()`!
   - Official Moveworks guidance:
     - Standard endpoints (`interactions`, `conversations`, `plugin-calls`, `plugin-resources`): **No delay between requests when receiving HTTP 200 OK**. Only pause for **60 seconds upon HTTP 429**.
     - `users` endpoint: 2-second sleep between requests; 90-second pause upon HTTP 429.

3. **Multi-Endpoint Normalization**:
   - The "Raw Interactions" view is composed of 5 endpoints: `interactions`, `conversations`, `plugin-calls`, `plugin-resources`, and `users`.
   - Bronze layer must ingest all 5 entities with uniform date partition alignment, which Silver layer joins together.

---

## 2. Parallel Processing Architectural Terminology & Concepts

To transform the ingestion from a 20-hour sequential bottleneck into a 45–90 minute high-throughput pipeline, we employ standard distributed data processing patterns:

```
                      [ Moveworks Ingestion Coordinator ]
                                      │
                   ┌──────────────────┴──────────────────┐
                   ▼                                     ▼
        [ Table-Level Fan-Out ]             [ Time-Range Sharding ]
      (interactions, conversations,         (Shard 1: Jan 1 - Jan 15)
       plugin-calls, plugin-res)            (Shard 2: Jan 16 - Jan 31)
                   │                                     │
                   └──────────────────┬──────────────────┘
                                      ▼
                        [ Worker Thread Pool / Tasks ]
                      ┌───────────────┼───────────────┐
                      ▼               ▼               ▼
                  Worker 1        Worker 2        Worker 3
                 (Shard 1)       (Shard 2)       (Shard 3)
                      │               │               │
                      └───────┬───────┴───────┬───────┘
                              ▼               ▼
                 [ Global Token-Bucket Rate Limiter ]
                 (Backoff: 60s on 429, 2**n on 5xx)
                              │
                              ▼
                 [ S3 Streaming Chunks (_staging/) ]
                              │
                              ▼
                 [ Two-Phase Commit & Watermarking ]
```

### Core Concepts

1. **Temporal Sharding (Time-Window Slicing)**:
   - Partitioning the total extraction interval $[T_{\text{start}}, T_{\text{end}})$ into $N$ discrete, non-overlapping sub-intervals:
     $$S_i = [t_i, t_{i+1}) \quad \text{where } t_0 = T_{\text{start}}, t_N = T_{\text{end}}$$
   - Each shard $S_i$ is an autonomous unit of work fetched via:
     `$filter=last_updated_time ge '{t_i}' and last_updated_time lt '{t_{i+1}}'`

2. **Concurrent Worker Pool (Bounded ThreadPoolExecutor)**:
   - A managed pool of worker threads ($W \in [3, 8]$) that pulls shards from a thread-safe task queue.
   - Prevents overwhelming the Moveworks API or Glue Python runtime memory.

3. **Global Adaptive Rate Limiter (Token Bucket / Circuit Breaker)**:
   - Replaces static per-page delays with an event-driven backoff:
     - **HTTP 200 OK**: 0–250ms inter-request pacing.
     - **HTTP 429 (Too Many Requests)**: Global pause (60s for interactions, 90s for users) signaling all workers to hold.
     - **HTTP 5xx**: Exponential backoff ($2^{\text{retry}}$ seconds, max 5 attempts).

4. **Isolated Staging & Two-Phase Commit**:
   - Each shard writes chunk Parquet files to an isolated S3 path:
     `s3://{bucket}/_staging/{execution_id}/{table_name}/shard_{shard_id}/part_*.parquet`
   - On successful shard completion, files are atomically promoted to the final partition path:
     `s3://{bucket}/bronze/data/moveworks/{table_name}/_ingested_at={timestamp}/`
   - Partial shard failures clean up their own staging prefix without corrupting other shards.

5. **Shard-Level Checkpointing & Resumability**:
   - Persists a `checkpoint_{shard_id}.json` containing the current `@odata.nextLink` / `$skip`.
   - If Shard 3 of 10 fails, the job can retry **only Shard 3** rather than restarting the entire 1.7 million record load.

---

## 3. High-Level System Design & Component Changes

### Component 1: `bronze_config.json` (Configuration Updates)
Add parallel sharding configuration specifically tailored to Moveworks:

```json
{
  "moveworks": {
    "base_url": "https://api.moveworks.ai",
    "api_endpoint_template": "/export/v1beta2/records/{table_name}",
    "default_tables": ["interactions", "conversations", "plugin-calls", "plugin-resources", "users"],
    "response_records_key": "value",
    "orderby": "id desc",
    "batch_size": 500,
    "parallel_processing": {
      "enabled": true,
      "max_workers": 5,
      "shard_strategy": "time_window",
      "shard_window_days": 15,
      "inter_page_delay_seconds": 0.1,
      "users_endpoint_delay_seconds": 2.0,
      "rate_limit_429_sleep_seconds": 60,
      "users_429_sleep_seconds": 90,
      "max_retries": 5
    }
  }
}
```

---

### Component 2: `connectors/moveworks.py` (Refactoring)
Upgrade connector to support both single-stream (delta) and multi-threaded sharded extraction:

#### Key Changes:
1. **Remove Unconditional 10s Sleep**:
   - Remove `time.sleep(10)` after 200 OK.
   - For `users` endpoint, enforce 2.0s delay as specified in documentation.
   - For other endpoints, only pause when HTTP 429 or 5xx is encountered.
2. **Implement `fetch_shard_window()`**:
   - Accepts `(start_time, end_time, shard_id, on_chunk_callback)`.
   - Traverses `@odata.nextLink` until the window is fully drained.
   - Respects `clean_illegal_chars` and `detail` flattening.
3. **Implement `fetch_parallel_shards()`**:
   - Calculates time windows from `initial_load_date` to `execution_start_time`.
   - Dispatches windows across a `ThreadPoolExecutor(max_workers=5)`.
   - Coordinates chunk writing into isolated staging directories per shard.

---

### Component 3: `uax_bronze_load.py` (Staging, Cataloging & Orchestration)
1. **Dynamic Shard Slicing**:
   - Given `initial_load_date = "2024-01-01T00:00:00Z"` and `current_time = "2024-07-01T00:00:00Z"`, calculate 15-day chunks:
     - Shard 1: `2024-01-01 -> 2024-01-16`
     - Shard 2: `2024-01-16 -> 2024-01-31`
     - ...
     - Shard 12: `2024-06-15 -> 2024-07-01`
2. **Concurrent Extraction Execution**:
   - Process shards concurrently or in batches of $N$ threads.
3. **Safe Two-Phase Staging Promotion**:
   - Collect parts across all shards and promote atomically to Bronze.
4. **Watermark State Update**:
   - Update `watermark.json` with execution start time once all shards succeed.

---

## 4. Performance & Execution Time Projections

| Metric | Current Implementation (Sequential) | Parallel Engine (5 Workers + Time Sharding) |
| :--- | :--- | :--- |
| **Total Records** | ~1,700,000 | ~1,700,000 |
| **Total Pages ($top=500)** | 3,400 pages | 3,400 pages distributed across shards |
| **Sleep Overhead** | 3,400 × 10s = **9.4 hours** | 0s (interactions) / 2s (users) = **~0.1 hours** |
| **API Concurrency** | 1 page at a time | 4–5 concurrent shard requests |
| **Network & I/O Time** | ~10 hours | ~1.5–2 hours |
| **Total Job Duration** | **20+ hours (Failed)** | **~1 to 2 hours (Success)** |
| **Failure Recovery** | Restarts from page 0 (all 20h lost) | Resumes only the failed shard |

---

## 5. Step-by-Step Implementation Roadmap

### Phase 1: Connector Modernization (`connectors/moveworks.py`)
- [ ] Align base URL and endpoint templates to `/export/v1beta2/records/{table_name}`.
- [ ] Remove hardcoded 10-second sleep after 200 OK.
- [ ] Add endpoint-specific throttling (2s for `users`, 0s for `interactions`/`conversations`/`plugins`).
- [ ] Implement bounded date-range query builder (`last_updated_time ge ... and last_updated_time le ...`).
- [ ] Add `@odata.nextLink` traversal with exponential backoff on 5xx and 60s/90s backoff on 429.

### Phase 2: Time-Window Sharding Engine
- [ ] Implement `DateRangeSharder` utility to divide arbitrary historical periods into $K$-day windows (default: 15 days).
- [ ] Implement `ThreadPoolExecutor` worker loop with safe callback-to-S3 chunk flushing.
- [ ] Add thread-safe shard metric collection for CloudWatch.

### Phase 3: Resilient Staging & Fault Isolation
- [ ] Partition staging directories by `_staging/{execution_id}/{table}/shard_{id}/`.
- [ ] Ensure shard failures clean up their respective directory and report specific failed windows.
- [ ] Implement shard checkpointing in S3 metadata.

### Phase 4: Verification & Silver Layer Raw Interactions Alignment
- [ ] Test with narrow 1-day range for all 5 Moveworks endpoints.
- [ ] Verify Parquet file schemas, column casing, and `detail_` flattening.
- [ ] Confirm Silver layer model matches the official Moveworks Raw Interactions join logic.

---

## 6. Risk Mitigation & Guardrails

| Potential Risk | Impact | Mitigation Strategy |
| :--- | :--- | :--- |
| **Moveworks 429 Throttling** | Temporary request pausing | Respect 60s/90s backoff; limit worker threads to max 5. |
| **Glue Driver Memory Pressure** | Out of Memory (OOM) | Memory-safe S3 chunk streaming flushes every 10,000 records to disk/S3 immediately. |
| **Partial Extraction Failure** | Inconsistent Bronze dataset | Staging area isolation + two-phase commit ensures Bronze only receives complete runs. |
| **Duplicate Records on Boundaries** | Duplicate interactions | Shards use strictly non-overlapping intervals $[t_i, t_{i+1})$, and Silver layer deduplicates on primary key `id`. |
