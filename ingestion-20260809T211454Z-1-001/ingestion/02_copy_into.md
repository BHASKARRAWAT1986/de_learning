# COPY INTO — Complete FAANG Interview Guide
## Idempotent Batch Ingestion for Delta Tables

---

## 1. What Is COPY INTO?

`COPY INTO` is a SQL command in Databricks that **idempotently loads files from cloud storage into a Delta table**. It tracks which files have already been loaded (via the Delta transaction log) and skips them on re-runs.

### The Core Value Proposition
- **Idempotent**: Run it 100 times — data loads exactly once
- **Simple**: One SQL statement, no streaming infrastructure
- **Self-tracking**: No external state management needed
- **Transactional**: Either all files in a batch succeed or none commit

---

## 2. Syntax — Complete Reference

### Basic Syntax
```sql
COPY INTO target_table
FROM 'source_path'
FILEFORMAT = format
[FORMAT_OPTIONS (key = 'value', ...)]
[COPY_OPTIONS (key = 'value', ...)]
```

### Full Examples

```sql
-- ─── JSON Files ───────────────────────────────────────────────────────
COPY INTO bronze.orders_raw
FROM 's3://my-bucket/landing/orders/'
FILEFORMAT = JSON
FORMAT_OPTIONS (
  'inferSchema' = 'true',
  'multiLine' = 'false'              -- true for multi-line JSON objects
)
COPY_OPTIONS (
  'mergeSchema' = 'true'             -- allow new columns
);

-- ─── CSV Files with Options ───────────────────────────────────────────
COPY INTO bronze.customers
FROM 's3://bucket/customers/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true',
  'sep' = ',',
  'quote' = '"',
  'escape' = '\\',
  'encoding' = 'UTF-8',
  'nullValue' = 'NULL',
  'timestampFormat' = 'yyyy-MM-dd HH:mm:ss',
  'ignoreLeadingWhiteSpace' = 'true'
)
COPY_OPTIONS (
  'mergeSchema' = 'true'
);

-- ─── Parquet with Specific File Pattern ───────────────────────────────
COPY INTO bronze.transactions
FROM (
  SELECT * FROM 's3://bucket/transactions/'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS (
  'pathGlobFilter' = '*.parquet'
)
COPY_OPTIONS (
  'mergeSchema' = 'false'
);

-- ─── Avro with Schema Registry ────────────────────────────────────────
COPY INTO bronze.events
FROM 's3://bucket/avro-events/'
FILEFORMAT = AVRO;

-- ─── Load Specific Files (not a prefix) ───────────────────────────────
COPY INTO bronze.orders_raw
FROM (
  SELECT * FROM 's3://bucket/orders/'
  WHERE _metadata.file_modification_time > '2024-06-01'
)
FILEFORMAT = JSON;

-- ─── With Explicit Column Mapping ─────────────────────────────────────
COPY INTO bronze.orders_raw (order_id, customer_id, amount, created_at)
FROM (
  SELECT id, cust_id, total, ts FROM 's3://bucket/orders/'
)
FILEFORMAT = JSON;
```

### All FORMAT_OPTIONS by Format

```
JSON:  inferSchema, multiLine, timestampFormat, dateFormat, primitivesAsString
CSV:   header, inferSchema, sep, quote, escape, encoding, nullValue, 
       timestampFormat, dateFormat, ignoreLeadingWhiteSpace, ignoreTrailingWhiteSpace
PARQUET: mergeSchema, pathGlobFilter
AVRO:  pathGlobFilter
TEXT:  wholeText (true = one row per file)
BINARYFILE: pathGlobFilter
ORC:   mergeSchema
```

### COPY_OPTIONS
```
mergeSchema    = 'true'/'false'   — allow schema evolution
force          = 'true'/'false'   — reprocess already-loaded files (use for reloads)
```

---

## 3. How COPY INTO Tracks State (Internals)

```
_delta_log/
  ├── 00000000000000000001.json   ← initial table creation
  ├── 00000000000000000002.json   ← COPY INTO run 1
  │     Contains: {"add": {...}, "cdc": {...}}
  │     AND: copyInto metadata: list of files loaded
  ├── 00000000000000000003.json   ← COPY INTO run 2
  │     Files already in run 1 → SKIPPED automatically
  └── ...
```

The list of processed files is stored **inside the Delta transaction log** as part of the commit metadata. When you run `COPY INTO` again:
1. It lists the source prefix
2. Looks up all file paths against the Delta log's `copyInto` history
3. Only submits NEW files to Spark for reading
4. Commits new files atomically

### See What's Been Loaded
```sql
-- View COPY INTO history for a table
DESCRIBE HISTORY bronze.orders_raw;

-- Opertion column will show "COPY INTO" entries
-- operationMetrics will show: numCopiedRows, numSkippedCorruptFiles, numFiles

-- Detailed copy history
SELECT 
  timestamp,
  operation,
  operationMetrics.numCopiedRows,
  operationMetrics.numFiles,
  operationMetrics.numSkippedCorruptFiles
FROM (DESCRIBE HISTORY bronze.orders_raw)
WHERE operation = 'COPY INTO';
```

---

## 4. Force Reload (Re-process Files)

```sql
-- FORCE option: ignore already-loaded state, reprocess everything
COPY INTO bronze.orders_raw
FROM 's3://bucket/orders/'
FILEFORMAT = JSON
COPY_OPTIONS ('force' = 'true');
-- WARNING: This will duplicate data if the table has no dedup logic
-- Use ONLY with a downstream dedup or on a truncated/empty table
```

---

## 5. PySpark API (Programmatic Execution)

```python
# Execute COPY INTO from Python
spark.sql("""
    COPY INTO bronze.orders_raw
    FROM 's3://bucket/orders/'
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('inferSchema' = 'true', 'multiLine' = 'false')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

# Parameterized (safe string interpolation for paths only, never for SQL values)
source_path = "s3://bucket/orders/date=2024-06-15/"
target_table = "bronze.orders_raw"

spark.sql(f"""
    COPY INTO {target_table}
    FROM '{source_path}'
    FILEFORMAT = JSON
    FORMAT_OPTIONS ('inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
```

### Scheduled via Databricks Jobs
```python
# In a notebook job cell:
from datetime import datetime, timedelta

# Dynamic path for daily load
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
source_path = f"s3://prod-landing/orders/date={yesterday}/"

result = spark.sql(f"""
    COPY INTO prod.bronze_orders
    FROM '{source_path}'
    FILEFORMAT = PARQUET
    COPY_OPTIONS ('mergeSchema' = 'true')
""")

# Check result
print(result.collect())
# [Row(num_affected_rows=15234, num_inserted_rows=15234)]
```

---

## 6. Return Value

```sql
COPY INTO bronze.orders_raw FROM 's3://bucket/orders/' FILEFORMAT = JSON;
-- Returns a DataFrame with one row:
-- num_affected_rows | num_inserted_rows
-- 15234             | 15234

-- If all files already loaded:
-- num_affected_rows = 0
-- num_inserted_rows = 0
```

---

## 7. Pros and Cons

### Pros
| Benefit | Detail |
|---------|--------|
| Idempotent by default | Re-run safe — no duplicates without `force` |
| Zero infrastructure | No streaming cluster, no checkpoint management |
| Simple SQL | One command, works in notebooks, jobs, and SQL warehouse |
| Transactional | Atomic commit — partial failures don't corrupt target |
| Schema inference | Automatic for JSON/CSV |
| Schema evolution | `mergeSchema = true` for additive changes |
| Works with SQL Warehouses | Can run on serverless SQL compute |
| Cost-effective for batch | No always-on cluster needed |

### Cons
| Limitation | Detail |
|------------|--------|
| Doesn't scale to billions of files | File listing is O(n) — full prefix scan every run |
| Not streaming | No micro-batch — it's a single batch command |
| State in Delta log | If you drop and recreate the table, state is lost → duplicates on rerun |
| No backpressure | Loads ALL new files in one batch — can OOM with huge drops |
| Schema change requires manual handling | `failOnNewColumns` not supported — just `mergeSchema` |
| No file-level metadata columns | Unlike Auto Loader, no `_metadata.file_path` |
| Cannot handle deletes | Only appends — no CDC support |

---

## 8. Trade-offs

### COPY INTO vs Auto Loader
| Dimension | COPY INTO | Auto Loader |
|-----------|-----------|-------------|
| Scale | ~1M files max | Billions of files |
| Latency | Batch (scheduled) | Near-real-time (streaming) |
| State management | Delta log (auto) | Checkpoint + SQS (auto) |
| Infrastructure | None | Streaming cluster required |
| Schema evolution | mergeSchema only | Full evolution modes |
| File metadata | Not available | `_metadata.*` columns |
| SQL Warehouse support | Yes | No (requires streaming cluster) |
| **Choose when** | Scheduled bulk loads | Continuous or high-volume |

### COPY INTO vs Manual Spark Read + Write
| Dimension | COPY INTO | Manual Spark |
|-----------|-----------|--------------|
| De-duplication | Automatic | Manual (must track seen files) |
| Simplicity | One SQL statement | Code + logic |
| Flexibility | Limited options | Full Spark API |
| **Choose when** | Standard formats, simple transforms | Complex transforms needed |

---

## 9. Common Patterns

### Pattern 1: Daily Partitioned Load
```sql
-- Run as a daily job with date parameter
COPY INTO prod.bronze_sales
FROM 's3://landing/sales/date=${run_date}/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');
```

### Pattern 2: Initial Bulk Load (One-Time)
```sql
-- Load 3 years of historical data in one shot
COPY INTO prod.bronze_historical_orders
FROM 's3://archive/orders/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true', 'multiLine' = 'false')
COPY_OPTIONS ('mergeSchema' = 'true');
-- Idempotent: safe to re-run if it fails midway
```

### Pattern 3: Multi-Directory Load
```sql
-- Load from multiple paths in one statement
COPY INTO bronze.events
FROM (
  SELECT * FROM 's3://bucket/events/2024-06/' 
  UNION ALL
  SELECT * FROM 's3://bucket/events/2024-07/'
)
FILEFORMAT = PARQUET;
```

### Pattern 4: Landing → Bronze → Silver Pipeline
```sql
-- Step 1: COPY INTO Bronze (raw, no transforms)
COPY INTO bronze.raw_orders
FROM 's3://landing/orders/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Step 2: Transform to Silver (batch job after COPY INTO)
INSERT INTO silver.orders
SELECT
  order_id,
  customer_id,
  CAST(amount AS DECIMAL(10,2)) AS amount,
  CAST(created_at AS TIMESTAMP) AS created_at,
  current_timestamp() AS _processed_at
FROM bronze.raw_orders
WHERE _processed_at IS NULL;  -- or use watermark logic
```

---

## 10. STAR Answers for FAANG

### Q1: "How do you build an idempotent data loading pipeline?"

**Situation:** Our data team was running a nightly ETL job that read files from an S3 bucket and wrote to a Delta table. The Spark job had no idempotency logic — if the job failed at 3 AM and was restarted, it would re-read all files and create duplicates. We were dealing with ~5 duplicate incidents per month.

**Task:** Make the ingestion pipeline fully idempotent so re-runs never produce duplicates, without adding complex deduplication logic.

**Action:**
1. Replaced the `spark.read.json() + df.write.delta()` pattern with `COPY INTO`
2. COPY INTO stores loaded file paths inside the Delta transaction log — re-runs automatically skip already-processed files
3. For the schema variation we saw monthly, added `COPY_OPTIONS ('mergeSchema' = 'true')`
4. Wrapped the command in a Databricks Job with retry = 3 — safe because COPY INTO is idempotent
5. Added a post-job validation query: if `num_affected_rows = 0` AND source bucket has new files → alert (indicates a state tracking issue)

**Result:** Zero duplicate incidents in 6 months following the change. Job failure + retry became a non-event instead of a 1-hour incident. On-call burden reduced by ~4 hours/month.

---

### Q2: "Walk me through how you'd do a one-time historical data migration to Delta Lake"

**Situation:** The company acquired a smaller firm. They had 4 years of order history as Parquet files in an S3 bucket (~800GB, ~2M files across yearly/monthly partitions). We needed to migrate this into our Delta Lake Bronze layer before the integration deadline.

**Task:** Load all 2M historical files into a Delta table reliably, with no data loss and the ability to resume if the job failed mid-way.

**Action:**
1. Created the target Delta table with `USING DELTA` and the expected schema
2. Used `COPY INTO` with a wildcard path: `FROM 's3://acquired-co-bucket/orders/**'` with `FILEFORMAT = PARQUET`
3. Added `FORMAT_OPTIONS ('pathGlobFilter' = '*.parquet')` to skip non-Parquet files (README, manifests)
4. The first run loaded 1.8M files successfully in ~6 hours before timing out
5. Re-ran the same COPY INTO command — it picked up exactly from where it left off (790K files were tracked in Delta log as loaded, remaining 200K were loaded in second run)
6. Ran row count validation: `SELECT COUNT(*) FROM bronze.acquired_orders` vs. manifest count provided by acquired company

**Result:** Full 4-year historical data loaded in 2 runs, zero duplicates, zero data loss. The self-tracking nature of COPY INTO meant no custom "progress file" or external state was needed. Migration completed 3 days before deadline.

---

### Q3: "What are the limitations of COPY INTO and when would you choose something else?"

**Situation:** An IoT platform team came to us asking to use `COPY INTO` for their sensor data pipeline. They had 500 IoT devices each writing a new file to S3 every 30 seconds — that's ~1,000 files/minute, ~1.4M files/day.

**Task:** Evaluate whether COPY INTO was the right tool, and recommend an alternative if not.

**Action:**
1. **Identified the scale problem**: COPY INTO does a full prefix listing on every run. At 1.4M files/day × 30 days = 42M files, listing time would become the bottleneck (S3 List API returns 1,000 objects per call → 42,000 API calls just to list). Listing alone would take 20-30 minutes on a large run.
2. **Identified the latency problem**: The team needed data available within 2 minutes of sensor readings. COPY INTO is a batch command — it doesn't support micro-batch/streaming semantics.
3. **Recommended Auto Loader instead**: With `cloudFiles.useNotifications = true`, file discovery is event-driven (SQS/SNS) — no directory listing. Latency is under 1 minute. Scales to billions of files with no degradation.
4. **Trade-off acknowledged**: Auto Loader requires an always-on streaming cluster (~$200/month vs COPY INTO on a SQL Warehouse which scales to zero). Justified by the latency requirement.

**Result:** Recommended and implemented Auto Loader for the IoT pipeline. The IoT team got <90-second latency. Also proposed a cost optimization: use Spot instances for the streaming cluster and set cluster auto-termination at 10 minutes idle. Final cost was ~$120/month, well within budget.

---

## 11. Key Internals to Mention in Interviews

1. **State is in the Delta log, not external**: If you run `DROP TABLE` + recreate, the COPY INTO history is gone — use `COPY_OPTIONS ('force' = 'false')` only works if the table exists with history
2. **COPY INTO uses Spark under the hood**: It's not a metadata-only operation — it actually reads files with Spark executors
3. **`num_affected_rows = 0` is a success case**: It means all files were already loaded — not an error
4. **`force = true` is a reprocessing escape hatch**: Only use on empty table or with downstream dedup
5. **COPY INTO supports Unity Catalog**: Target can be a UC managed or external table
6. **Works in SQL Warehouses**: Unlike Auto Loader, can run on serverless SQL compute — great for cost optimization on batch workloads
