# Auto Loader — Complete FAANG Interview Guide
## Databricks cloudFiles — Scalable Incremental File Ingestion

---

## 1. What Is Auto Loader?

Auto Loader is Databricks' structured streaming source (`cloudFiles`) that **incrementally and efficiently processes new files** as they arrive in cloud object storage (S3, ADLS Gen2, GCS).

### The Problem It Solves
Without Auto Loader, to ingest new files you either:
- **Re-scan the entire prefix** every run (expensive, O(n) with file count)
- **Maintain your own "seen files" state** (brittle, operational burden)
- **Use COPY INTO** (doesn't scale to billions of files)

Auto Loader solves this by offloading file discovery to cloud-native event services.

---

## 2. Architecture — How It Works Internally

### Mode 1: File Notification Mode (Default, Recommended)
```
S3 Bucket
    │
    │ (new file arrives)
    ▼
S3 Event Notification ──► SQS Queue (auto-created by Databricks)
                                │
                                ▼
                         Auto Loader Driver
                         (reads SQS events)
                                │
                         Checkpoint (Delta log)
                                │
                                ▼
                         Executor Workers
                         (read & parse files)
                                │
                                ▼
                         Delta Target Table
```

**Key points:**
- Databricks **auto-creates** the SNS topic + SQS queue on first run
- Each new file triggers an SQS event — no directory listing at all
- Latency: **< 1 minute** from file land to processing start
- Scales to **billions of files** — queue depth, not prefix scan

### Mode 2: Incremental Listing Mode (Fallback)
```
Auto Loader Driver
    │
    ├── Checkpoint stores: last processed file path (lexicographic)
    └── On each trigger: list files AFTER last checkpoint offset
```

- Used when event services are unavailable or disabled
- Still much better than full scan — only lists new files since last run
- Relies on **lexicographic ordering** of file names (timestamps in name = works perfectly)
- Latency: depends on trigger interval (typically 1-5 min)

---

## 3. Complete Configuration Reference

### Basic Setup
```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")           # File format: json, csv, parquet, avro, text, binaryFile
    .option("cloudFiles.schemaLocation", "/path/to/schema/checkpoint")  # REQUIRED: where to store inferred schema
    .load("/mnt/landing/orders/")
)

df.writeStream
  .format("delta")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("append")
  .trigger(processingTime="2 minutes")
  .table("bronze.orders_raw")
```

### Full Options Cheat Sheet

```python
# ─── Discovery Mode ───────────────────────────────────────────────
.option("cloudFiles.useNotifications", "true")     # Default: true for S3/ADLS (notification mode)
                                                   # Set false to force incremental listing

# ─── File Format Options ──────────────────────────────────────────
.option("cloudFiles.format", "json")               # json | csv | parquet | avro | text | binaryFile | orc
.option("cloudFiles.includeExistingFiles", "true") # Process files already in path on first run (default: true)

# ─── Schema Inference & Evolution ─────────────────────────────────
.option("cloudFiles.schemaLocation", "/schema/")  # REQUIRED: persists schema between runs
.option("cloudFiles.inferColumnTypes", "true")    # Infer int/long/double (default: false → all strings)
.option("cloudFiles.schemaHints", "id LONG, amount DECIMAL(10,2)")  # Override specific columns
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# Modes:
#   addNewColumns  → new columns added to table (default)
#   rescue         → unknown cols go to _rescued_data column
#   failOnNewColumns → pipeline fails if schema changes
#   none           → schema is fixed, new columns ignored

# ─── File Filtering ───────────────────────────────────────────────
.option("cloudFiles.maxFilesPerTrigger", 1000)    # Limit files per micro-batch (backpressure)
.option("cloudFiles.maxBytesPerTrigger", "1g")    # Limit bytes per micro-batch
.option("pathGlobFilter", "*.json")               # Only process matching files
.option("modifiedAfter", "2024-01-01 00:00:00")  # Ignore files older than this timestamp
.option("ignoreCorruptFiles", "true")             # Skip corrupt files instead of failing

# ─── AWS-Specific (File Notification Mode) ────────────────────────
.option("cloudFiles.region", "us-east-1")
.option("cloudFiles.useNotifications", "true")

# ─── Azure-Specific ───────────────────────────────────────────────
.option("cloudFiles.connectionString", dbutils.secrets.get("kv", "adls-connection-string"))

# ─── Metadata Columns (auto-added) ────────────────────────────────
# _metadata.file_path      → full path of source file
# _metadata.file_name      → filename only
# _metadata.file_size      → bytes
# _metadata.file_modification_time → timestamp
# _metadata.file_block_start
# _metadata.file_block_length
```

### Production Pattern — Bronze Ingest with Metadata
```python
from pyspark.sql import functions as F

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/orders_schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxFilesPerTrigger", 5000)
    .option("pathGlobFilter", "*.json")
    .load("/mnt/landing/orders/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_file_ts", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_year", F.year(F.col("_metadata.file_modification_time")))
    .withColumn("_month", F.month(F.col("_metadata.file_modification_time")))
)

(
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/orders_bronze")
    .option("mergeSchema", "true")
    .outputMode("append")
    .partitionBy("_year", "_month")
    .trigger(processingTime="5 minutes")
    .table("bronze.orders_raw")
)
```

---

## 4. Schema Evolution Deep Dive

### The Schema Inference Problem
First run: Auto Loader sees `{"id": 1, "amount": 99.9}` → infers schema  
Week 3: New field added: `{"id": 2, "amount": 10.0, "discount": 5.0}`

Without schema evolution config → **pipeline fails** with AnalysisException

### Schema Evolution Modes

```python
# Mode 1: addNewColumns (recommended for bronze)
# New columns automatically added to Delta table
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

# Mode 2: rescue (recommended for strict silver)
# Unknown columns captured in _rescued_data JSON string
.option("cloudFiles.schemaEvolutionMode", "rescue")

# Then in silver:
df.withColumn("discount", F.get_json_object("_rescued_data", "$.discount").cast("double"))

# Mode 3: failOnNewColumns (recommended for gold/production critical)
# Pipeline alerts on any schema change — forces human review
.option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
```

### Force Schema Refresh (after intentional schema change)
```python
# Option 1: Delete schema checkpoint location and restart
dbutils.fs.rm("/mnt/checkpoints/orders_schema", recurse=True)

# Option 2: Use dbutils.fs.rm only for schema, keep streaming checkpoint intact
# (schema checkpoint and streaming checkpoint are SEPARATE locations)
```

---

## 5. Monitoring Auto Loader

### Via Spark UI
- Go to **Streaming** tab → see batch duration, input rows, processing rate
- Look for **"Input Rate"** vs **"Processing Rate"** — if input > processing, you're falling behind

### Via Delta Table Metrics
```python
# See what files have been processed
%sql
SELECT * FROM cloud_files_state('/mnt/checkpoints/orders_bronze')
-- Returns: path, size, modification_time, discovered_time
```

### Via Programmatic Health Check
```python
query = bronze_df.writeStream.start()

# Check status
print(query.status)
# {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}

# Recent progress
import json
print(json.dumps(query.recentProgress[-1], indent=2))
# Look for: numInputRows, inputRowsPerSecond, processedRowsPerSecond, batchDuration
```

---

## 6. Pros and Cons

### Pros
| Benefit | Detail |
|---------|--------|
| Scalable to billions of files | Cloud events = no directory listing overhead |
| Schema inference + evolution | Auto-handles new columns, type changes |
| Exactly-once semantics | Checkpointing + Delta idempotency |
| Built-in metadata columns | `_metadata.file_path` for lineage |
| No custom state management | Databricks manages the seen-file state |
| Works with DLT | Native integration as DLT source |
| Backpressure controls | `maxFilesPerTrigger` prevents OOM |
| Multiple file formats | JSON, CSV, Parquet, Avro, Binary |

### Cons
| Limitation | Detail |
|------------|--------|
| AWS setup complexity | First run auto-creates SNS/SQS — needs IAM permissions |
| Not real-time | File notification latency ~30-60s (not sub-second) |
| Schema location required | Must manage a second checkpoint path |
| Cost: SQS/SNS charges | High-volume landing zones accumulate queue messages |
| No file deletion detection | Does NOT process file deletes/overwrites |
| Ordering not guaranteed | Files processed in discovery order, not necessarily timestamp order |
| Debugging is harder | "Why wasn't this file processed?" requires inspecting `cloud_files_state` |

---

## 7. Trade-offs

### Auto Loader vs COPY INTO
| Dimension | Auto Loader | COPY INTO |
|-----------|-------------|-----------|
| Scale | Billions of files | ~1M files max |
| Latency | Near-real-time | Batch only |
| State storage | Cloud events / checkpoint | Delta log (`_copy_history`) |
| Schema evolution | Built-in | Manual |
| Operational overhead | Medium (IAM, SQS setup) | Low |
| Use case | Continuous streaming | Scheduled bulk load |
| **Choose when** | New files arrive frequently | Periodic large bulk loads |

### Auto Loader vs Kafka
| Dimension | Auto Loader | Kafka |
|-----------|-------------|-------|
| Source type | Files (S3/ADLS) | Event stream (topics) |
| Latency | 30-60 seconds | Sub-second |
| Ordering | Best-effort | Partition-ordered |
| Replay | Re-process files | Offset-based replay |
| Use case | File-based landing zone | Real-time event ingestion |
| **They are complementary**: Kafka → Auto Loader → Delta is a valid pattern |

### Auto Loader Notification vs Listing Mode
| Dimension | Notification Mode | Listing Mode |
|-----------|-------------------|--------------|
| Latency | < 1 min | Depends on trigger |
| Cloud infra needed | SNS + SQS (AWS) | None |
| Ordering | Event order | Lexicographic |
| Best for | High-volume, low-latency | Regulated environments, simpler IAM |

---

## 8. STAR Answers for FAANG

### Q1: "Tell me about a time you built a scalable data ingestion pipeline"

**Situation:** We were ingesting IoT sensor data from 200+ devices. Each device wrote a JSON file to S3 every minute — that's ~300,000 files per day. Initially, we used a scheduled Spark job that did `spark.read.json("s3://bucket/sensors/")` every 30 minutes.

**Task:** The job was taking 45 minutes to run because Spark was listing ALL 2.3 million accumulated files on every run. We needed to reduce latency to under 5 minutes and eliminate the listing bottleneck.

**Action:**
1. Migrated to Auto Loader with file notification mode — Databricks auto-created SNS/SQS
2. Set `cloudFiles.maxFilesPerTrigger = 2000` to control batch size
3. Set `cloudFiles.schemaEvolutionMode = "rescue"` to capture schema drift in `_rescued_data`
4. Added `_metadata.file_path` and `_metadata.file_modification_time` columns for lineage
5. Partitioned the Bronze table by `device_id` and `date` for query efficiency

**Result:** Latency dropped from 45 minutes to under 3 minutes. The listing cost went from O(2.3M files) to O(new files since last trigger) — effectively zero listing cost. File processing throughput went from 6,000 files/run to 300,000 files/day sustained. The pipeline ran for 8 months with zero maintenance interventions on the discovery mechanism.

---

### Q2: "How do you handle schema changes in a streaming pipeline without downtime?"

**Situation:** Our Bronze layer Auto Loader pipeline was ingesting e-commerce order events from S3. The upstream team notified us they were adding 3 new fields to the JSON payload in a weekend deployment.

**Task:** Handle the schema change without stopping the pipeline or losing data. Downtime was not acceptable — orders were being processed 24/7.

**Action:**
1. Changed `cloudFiles.schemaEvolutionMode` from `"addNewColumns"` to `"rescue"` on the Bronze layer — new/unknown fields go to `_rescued_data` JSON column
2. No pipeline restart needed — this is a schema checkpoint config change that takes effect on next trigger
3. In the Silver layer, added explicit parsing: `F.get_json_object("_rescued_data", "$.new_field")`
4. After the upstream deployment, monitored `_rescued_data IS NOT NULL` count to confirm new fields were arriving
5. After one week of stable data, ran `ALTER TABLE` to add the new columns formally and deleted `schemaHints` override

**Result:** Zero downtime during the schema migration. The `_rescued_data` rescue column acted as a buffer. The new fields were available in Silver within minutes of the upstream deployment without any pipeline restart. Established this as our standard schema migration playbook.

---

### Q3: "How do you ensure exactly-once delivery with Auto Loader?"

**Situation:** Finance team flagged that our revenue aggregation showed occasional double-counting. The Auto Loader pipeline was writing to a Bronze Delta table, and they suspected duplicates.

**Task:** Investigate and prove (or fix) exactly-once semantics in the pipeline.

**Action:**
1. **Diagnosed the issue**: Found that a team member had manually restarted the pipeline without preserving the checkpoint location — it had been moved. Auto Loader re-processed files from scratch.
2. **Explained the guarantee**: Auto Loader provides exactly-once IF (a) checkpoint is preserved AND (b) the sink is idempotent (Delta). The checkpoint stores the offset (list of processed files). If you delete the checkpoint, Auto Loader treats it as a fresh start.
3. **Fixed the issue**: Restored the checkpoint from backup, identified the duplicate window, ran a dedup job on Bronze using `ROW_NUMBER() OVER (PARTITION BY file_path, record_id ORDER BY _ingested_at)`
4. **Prevented recurrence**: Stored checkpoint in ADLS with soft-delete enabled (30-day retention). Added a pre-restart runbook check: "Does checkpoint exist? If not, set `cloudFiles.includeExistingFiles = false` and set `modifiedAfter` to last successful run timestamp."

**Result:** Eliminated the double-counting. Established a checkpoint governance policy: checkpoints stored in a separate protected storage account with delete protection. Added a monitoring alert: if `cloud_files_state()` row count drops by >80% in a single trigger, alert the on-call engineer (indicates possible checkpoint loss/reset).

---

## 9. Advanced Patterns

### Pattern: Auto Loader → DLT Integration
```python
import dlt

@dlt.table(
    name="bronze_orders",
    comment="Raw order events from S3 landing zone"
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schema/orders")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/mnt/landing/orders/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

### Pattern: Multi-Format Landing Zone
```python
# Different file types in same landing zone
for fmt in ["json", "csv", "parquet"]:
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("pathGlobFilter", f"*.{fmt}")
        .option("cloudFiles.schemaLocation", f"/schema/{fmt}")
        .load("/mnt/landing/mixed/")
    )
    df.writeStream.table(f"bronze.raw_{fmt}")
```

### Pattern: Replay From Specific Date (Backfill)
```python
# Re-process only files modified after a specific time
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("modifiedAfter", "2024-06-01 00:00:00")
    .option("modifiedBefore", "2024-06-30 23:59:59")
    .option("cloudFiles.schemaLocation", "/schema/backfill")
    .load("/mnt/landing/orders/")
)
```

---

## 10. Key Internals to Mention in Interviews

1. **Checkpoint = 2 things**: (a) streaming checkpoint (offsets, WAL) in `checkpointLocation` and (b) schema checkpoint in `cloudFiles.schemaLocation` — they're separate and BOTH needed
2. **SQS queue visibility timeout**: If a batch takes longer than the SQS visibility timeout, messages reappear — Auto Loader handles this idempotently via the Delta checkpoint
3. **`cloud_files_state()` SQL function**: The internal catalog of all files seen — useful for debugging "why wasn't this file processed"
4. **Incremental listing uses S3 List API pagination with a cursor** — much cheaper than full listing
5. **Binary file mode**: `cloudFiles.format = "binaryFile"` — useful for ML feature stores, image/audio pipelines
