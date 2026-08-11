# Auto Loader — Real-World Deep Dive
## 50,000 Files × 70MB with maxFilesPerTrigger = 5,000
## Batch Mechanics, Stages, Delta Commits, Hiccups & FAANG Answers

---

## 1. The Scenario — Numbers First

```
Source:           S3 bucket /landing/orders/
Files:            50,000 JSON files
Size per file:    70 MB
Total data:       50,000 × 70 MB = 3,500 GB = 3.5 TB
File format:      JSON (one event per line, JSONL)
maxFilesPerTrigger: 5,000
Target:           Delta table  bronze.orders_raw
Cluster:          8 workers, each with 8 cores, 32 GB RAM
Trigger:          processingTime = "5 minutes"
```

### Derived Numbers
```
Total batches needed:    50,000 ÷ 5,000 = 10 batches
Data per batch:          5,000 × 70 MB  = 350 GB per batch
Tasks per batch Stage 0: 5,000 tasks    (1 task per file, since 70MB < 128MB HDFS block)
Total cores available:   8 workers × 8 cores = 64 cores (64 parallel tasks at a time)
Waves of tasks per batch: 5,000 ÷ 64 = ~79 waves (rounds of 64 tasks)
```

---

## 2. The Pipeline Code

```python
from pyspark.sql import functions as F

# The streaming query
query = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/schema/orders")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxFilesPerTrigger", 5000)          # KEY: 5,000 files per batch
    .option("cloudFiles.useNotifications", "true")          # SQS/SNS mode
    .option("pathGlobFilter", "*.json")
    .load("/mnt/landing/orders/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_file_size_bytes", F.col("_metadata.file_size"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_year",  F.year(F.col("_metadata.file_modification_time")))
    .withColumn("_month", F.month(F.col("_metadata.file_modification_time")))
    .writeStream
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

## 3. What Happens Before Batch 1 Even Starts

### Step 1: Driver connects to SQS (File Notification Mode)
```
Auto Loader startup:
  1. SparkSession starts the streaming query
  2. cloudFiles source connects to the SQS queue
     (Databricks auto-created this SNS → SQS pipeline when the bucket was configured)
  3. SQS has 50,000 messages (one per file that arrived in S3)
  4. Auto Loader driver reads the FIRST 5,000 SQS messages
     (not the file data — just the S3 event notifications: bucket + key + size + timestamp)
  5. Driver writes these 5,000 file paths to the WAL (checkpoint/offsets/0)
     BEFORE reading a single byte of actual data
  6. Driver marks the SQS messages as "in-flight" (visibility timeout ~30 min)
```

### Why WAL First?
```
If the cluster dies after Step 5 but before data is read:
  → On restart, driver reads checkpoint/offsets/0
  → Knows exactly which 5,000 files to process
  → Replays the batch from scratch
  → Exactly-once: if the Delta write already committed → skip
  → If Delta write did NOT commit → re-read and re-write

This is the foundation of exactly-once delivery.
```

---

## 4. Batch-by-Batch Breakdown — How Each Batch Executes

### Batch 0 (Files 1–5,000)

```
TRIGGER FIRES at T+0:00

Phase 1: File Discovery (Driver, ~5 seconds)
  ├── Read 5,000 SQS events (S3 file paths)
  ├── Write paths to WAL: checkpoint/offsets/0
  └── Plan: "I need to read these 5,000 files"

Phase 2: Spark Job Creation
  ├── DAGScheduler builds the physical plan
  └── Job 0 created with:
       Stage 0: Read 5,000 files → parse JSON → add metadata columns
       Stage 1: Shuffle by (_year, _month) → write partitioned Parquet to Delta

STAGE 0 — File Read + Transform
  Tasks:     5,000 (one task per file)
  Each task:
    1. Reads its 70MB JSON file from S3
    2. Parses each line as JSON (from_json internally)
    3. Adds _source_file, _ingested_at, _year, _month columns
    4. Produces output rows in memory
  Parallelism: 64 cores → 79 waves of 64 tasks
  Data read:  5,000 × 70 MB = 350 GB from S3

  [SHUFFLE BOUNDARY — because of partitionBy("_year", "_month")]

STAGE 1 — Shuffle + Delta Write
  Tasks:     spark.sql.shuffle.partitions (default 200, or AQE may reduce)
  Each task:
    1. Fetches its shuffle partition data from Stage 0 executors
    2. Writes one Parquet file to S3 under the Delta table path
       e.g., s3://delta-table/bronze/orders/_year=2024/_month=6/part-xxxxx.parquet
  Data written: ~350 GB → ~32 GB Parquet (Snappy compression, ~11x ratio for JSON→Parquet)

Phase 3: Delta Commit (Driver, atomic)
  ├── Driver writes to _delta_log/00000000000000000001.json:
  │     {add: [list of new Parquet files written]}
  │     {stats: {min/max per column for each file}}
  ├── This commit is ATOMIC: either all 5,000 files' data is committed or none
  └── Data is now PERMANENTLY visible in the Delta table ✅

Phase 4: Checkpoint Update
  ├── Driver writes to checkpoint/commits/0 → "batch 0 complete"
  ├── SQS messages for files 1-5,000 DELETED (acknowledged)
  └── Auto Loader internal state updated: "processed up to offset 5,000"

TOTAL TIME FOR BATCH 0: ~8-12 minutes (350GB at ~40-50 GB/min S3 throughput)
```

### Batch 1 (Files 5,001–10,000)

```
TRIGGER fires at T+5:00 (but batch 0 may still be running!)

If batch 0 is still running when T+5:00 fires:
  → Spark does NOT start batch 1
  → Trigger is SKIPPED
  → Next trigger check at T+10:00
  → If batch 0 finishes at T+9:00 → batch 1 starts at T+9:00

If batch 0 finished before T+5:00:
  → Batch 1 starts immediately at T+5:00

Batch 1 is IDENTICAL to Batch 0 but for files 5,001-10,000:
  Same stage structure:
    Stage 0: 5,000 tasks reading files 5,001-10,000
    Stage 1: Shuffle + write to Delta
  Same Delta commit pattern
  
  Delta log after Batch 1:
    _delta_log/00000000000000000002.json ← Batch 1 commit
    (new Parquet files added, existing files untouched)
```

### All 10 Batches Timeline

```
Batch 0:  Files     1 –  5,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 1:  Files 5,001 – 10,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 2:  Files 10,001– 15,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 3:  Files 15,001– 20,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 4:  Files 20,001– 25,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 5:  Files 25,001– 30,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 6:  Files 30,001– 35,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 7:  Files 35,001– 40,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 8:  Files 40,001– 45,000  │ 350 GB read │ ~32 GB written to Delta │
Batch 9:  Files 45,001– 50,000  │ 350 GB read │ ~32 GB written to Delta │
─────────────────────────────────────────────────────────────────
TOTAL:   50,000 files            │ 3.5 TB read │ ~320 GB in Delta        │
         10 Delta commits
         10 transaction log entries
```

---

## 5. Stages Per Batch — Detailed Analysis

### Case A: Simple Append, No partitionBy (1 Stage)
```python
.writeStream.format("delta").outputMode("append").table("bronze.orders_raw")
# No partitionBy → no shuffle needed

Physical Plan:
  Stage 0: cloudFiles read → parse → withColumn → Delta write
    Tasks: 5,000
    Each task reads its file AND writes its Parquet file directly
    No data movement between executors
    
Spark UI shows: 1 stage, 5,000 tasks
```

### Case B: With partitionBy (2 Stages) — Most Common
```python
.writeStream.partitionBy("_year", "_month").format("delta").table("bronze.orders_raw")
# partitionBy triggers a shuffle: same (_year, _month) rows must go to same executor

Physical Plan:
  Stage 0: cloudFiles read → parse → withColumn
    Tasks: 5,000 (one per file)
    Output: rows in memory + shuffle write by (_year, _month)
    
  [SHUFFLE]
  
  Stage 1: shuffle read → Delta write (one Parquet file per shuffle partition per partition key)
    Tasks: spark.sql.shuffle.partitions = 200 (or AQE-coalesced down)
    Output: Parquet files written to partitioned Delta table

Spark UI shows: 2 stages
  Stage 0: 5,000 tasks
  Stage 1: 200 tasks (or fewer with AQE)
```

### Case C: With Silver Transform (groupBy or join) → More Stages
```python
# If you add a groupBy for pre-aggregation:
.groupBy("order_date", "region").agg(F.sum("amount"))
# This adds another shuffle stage

Physical Plan:
  Stage 0: read → parse → partial HashAgg (5,000 tasks)
  [SHUFFLE]
  Stage 1: final HashAgg (200 tasks)
  [SHUFFLE for partitionBy]
  Stage 2: Delta write (200 tasks)
  
Spark UI: 3 stages
```

### AQE Impact on Stage 1 Task Count
```
After Stage 0, AQE inspects the shuffle output:
  Total shuffle data = 350 GB (JSON) → Parquet ~32 GB
  Target partition size = 128 MB (spark.sql.adaptive.advisoryPartitionSizeInBytes)
  
  Ideal partitions = 32 GB ÷ 128 MB = 256 partitions
  → AQE keeps ~256 tasks for Stage 1 (instead of default 200)
  
  OR if the data is heavily skewed to 2 months:
  → Most data in 2 partitions (_year=2024, _month=6) and (_year=2024, _month=7)
  → AQE might coalesce 200 shuffle buckets down to 50
```

---

## 6. Does Data Persist After Each Batch? (Critical Question)

### YES — Each Batch = One Atomic Delta Commit

```
After Batch 0 completes:
  _delta_log/00000000000000000001.json ← committed ✅
  
  SELECT COUNT(*) FROM bronze.orders_raw;
  → Returns rows from files 1-5,000 immediately
  → Data is PERMANENT — even if the cluster dies now, this data is safe
  → VACUUM cannot remove these files (they're referenced in the active log)

After Batch 1:
  _delta_log/00000000000000000002.json ← committed ✅
  SELECT COUNT(*) → returns rows from files 1-10,000

...and so on.

The Delta table grows incrementally — each batch appends new Parquet files
and adds a new commit to the transaction log.

If the cluster dies mid-Batch 3:
  → Batch 3 was partially written (some Parquet files exist on S3)
  → But the Delta commit (log entry) was NEVER written
  → Those partial Parquet files are "orphaned" — not referenced by any log entry
  → Delta table still shows only data from Batches 0, 1, 2 (clean state)
  → On restart: checkpoint says "offset = 10,000 processed" (after Batch 1)
    Wait — checkpoint/commits/ shows Batch 2 completed.
    → Restart replays Batch 3 from scratch (reads files 10,001-15,000 again)
    → Parquet files from the partial Batch 3 run are orphaned
    → They will be cleaned by VACUUM after the retention period
```

### Visibility Timeline

```
T=0:00  Batch 0 starts — data NOT yet visible
T=9:00  Batch 0 Delta commit → data VISIBLE for files 1-5,000
T=9:00  Batch 1 starts — new data NOT yet visible
T=18:00 Batch 1 Delta commit → data VISIBLE for files 1-10,000
...
```

### Can Someone Query While Batch Is Running?

```
YES — Delta's snapshot isolation allows concurrent reads and writes.

Reader at T=13:00 (during Batch 1):
  → Reads the latest committed snapshot (after Batch 0)
  → Sees files 1-5,000 only
  → Does NOT see partial Batch 1 data
  → No dirty reads, no locks on the reader

This is Delta's ACID guarantee: readers never see partial writes.
```

---

## 7. Memory and Executor Sizing Analysis

### Per-Task Memory Requirement
```
Each task reads one 70MB JSON file and parses it in memory.

Raw JSON:                 70 MB
After parsing to rows:    ~140 MB (JVM object overhead ≈ 2x raw bytes)
After columnar encoding:  ~100 MB (schema struct uses less than raw strings)
Shuffle write buffer:     ~32 KB per output partition

Per-task peak memory: ~200-250 MB

With 8 cores per executor, 32 GB RAM:
  Memory per task slot = 32 GB ÷ 8 = 4 GB
  Peak usage per task = 250 MB
  → 250 MB / 4 GB = ~6% memory utilization per task slot
  → This executor sizing is more than enough ✅

BUT: if you have 64 tasks running simultaneously (all cores active):
  64 × 250 MB = 16 GB peak across all executors
  Total available = 8 × 32 GB × 0.6 (Spark fraction) = 153 GB
  → 16 GB / 153 GB = fine ✅
```

### S3 Throughput Bottleneck
```
Real bottleneck is S3 → Executor network bandwidth, not memory.

Per-executor S3 throughput:    ~500 MB/s (AWS enhanced networking)
Data per executor per batch:   350 GB ÷ 8 executors = 43.75 GB
Time to read per executor:     43.75 GB ÷ 500 MB/s = ~87 seconds

But tasks run in 79 waves of 64 tasks:
  Per wave: 64 files × 70 MB = 4.48 GB
  Time per wave: 4.48 GB ÷ (8 executors × 500 MB/s) = ~1.1 seconds
  
  Total Stage 0 time: 79 waves × ~1.1 s + overhead ≈ 2-3 minutes

Stage 1 (shuffle write to Delta S3):
  32 GB of Parquet to write at ~200 MB/s = ~2.7 minutes

Total batch time: ~5-8 minutes (excluding cluster startup)
```

---

## 8. Hiccups — What Goes Wrong in Production

### Hiccup 1: Batch Takes Longer Than Trigger Interval
```
PROBLEM:
  trigger = processingTime("5 minutes")
  Batch 0 takes 9 minutes
  → Trigger at T=5 fires → Spark: "batch 0 still running, skip"
  → Trigger at T=10 fires → batch 0 finishes at T=9 → batch 1 starts at T=10
  → Effective throughput: 1 batch per ~10 min instead of ~5 min
  → 10 batches × 10 min = 100 min actual time vs 50 min expected

DETECTION:
  Spark UI → Streaming tab → look for "skipped triggers" pattern
  query.recentProgress[-1]["batchDuration"] > trigger interval

FIX:
  Option 1: Increase cluster size (more executors = faster per batch)
  Option 2: Reduce maxFilesPerTrigger to 2,500 (2 batches instead of 1 per interval)
  Option 3: Change trigger to availableNow=True (process all, stop — no trigger timing issue)
  Option 4: Increase trigger interval to "15 minutes" to match actual batch time
```

### Hiccup 2: Schema Mismatch Mid-Run
```
PROBLEM:
  Files 1-25,000: {"order_id": 1, "amount": 99.9}
  Files 25,001-50,000: {"order_id": 1, "amount": 99.9, "discount": 5.0}  ← new field
  
  Batch 5 (files 25,001-30,000):
  IF schemaEvolutionMode = "failOnNewColumns":
    → Pipeline FAILS with AnalysisException
    → checkpoint marks batch 5 as failed
    → All data from files 25,001-30,000 is lost until pipeline is fixed

DETECTION: 
  DLT Event Log or streaming exception in logs

FIX:
  Use schemaEvolutionMode = "addNewColumns" (recommended for bronze)
  Files before the new field: "discount" column is NULL ← acceptable for bronze
  
  OR use schemaEvolutionMode = "rescue":
  New field goes to _rescued_data JSON column
  Silver layer parses it explicitly
```

### Hiccup 3: SQS Queue Depth Overwhelm
```
PROBLEM:
  50,000 files arrive in 10 minutes (burst load)
  Auto Loader in notification mode: 50,000 SQS messages queued
  
  Default SQS visibility timeout: 30 minutes
  If Batch 0 takes 35 minutes (unexpected slowdown):
    → SQS visibility timeout expires for batch 0's messages
    → Messages become VISIBLE again in queue
    → Auto Loader processes them again in a later batch
    → DUPLICATE DATA RISK if Delta write failed but SQS messages reappeared

REALITY CHECK:
  Delta idempotency: each batch has a unique batch ID
  Auto Loader tracks processed files in checkpoint
  Duplicate file paths in SQS → Auto Loader deduplicates via checkpoint state
  → NOT a real duplicate risk, but causes confusing logs

FIX:
  Increase SQS visibility timeout to 2x expected batch duration
  Or reduce maxFilesPerTrigger to ensure batches finish well within timeout
```

### Hiccup 4: Corrupted or Empty Files
```
PROBLEM:
  File 3,721 is a 0-byte file (upstream write failed)
  File 8,204 is truncated (upload interrupted)
  
  Task reading file 3,721:
    → org.apache.spark.sql.execution.datasources.FileNotFoundException
    OR → empty DataFrame (0 rows) — not an error, just skipped
  
  Task reading file 8,204 (truncated JSON):
    → JsonParseException → task fails
    → Spark retries task (spark.task.maxFailures = 4)
    → All 4 retries fail → stage fails → job fails → batch fails
    → Checkpoint NOT updated → same batch replays next trigger

FIX:
  .option("ignoreCorruptFiles", "true")   ← skip corrupt files, log them
  .option("ignoreMissingFiles", "true")   ← skip files that disappeared (S3 eventual consistency)
  
  PLUS: write corrupt file paths to an alert table:
  .option("badRecordsPath", "/mnt/quarantine/autoloader_errors/")
```

### Hiccup 5: Small File Explosion in Delta Table
```
PROBLEM:
  10 batches × Stage 1 with 200 shuffle partitions × 12 months partitions
  = 10 × 200 × 12 = 24,000 small Parquet files in Delta table
  
  Each file ≈ 32 GB ÷ (200 × 12) ≈ 13 MB
  → Way below the target 128 MB Parquet file size
  → Future queries on this table suffer: 24,000 files to open, list, check stats
  → Query planning time grows linearly with file count

DETECTION:
  DESCRIBE DETAIL bronze.orders_raw;
  → numFiles = 24,000 ← problem
  
  Or: SELECT COUNT(*) FROM (DESCRIBE DETAIL bronze.orders_raw) 
  shows numFiles exploding over time

FIX: Run OPTIMIZE after all batches complete (or on a schedule)
  OPTIMIZE bronze.orders_raw;
  → Consolidates 24,000 files into ~250 files (128 MB each for 32 GB data)
  → Future queries 95x faster file listing
  
  OR: Enable Auto Optimize in table properties
  ALTER TABLE bronze.orders_raw SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',   ← coalesce during write
    'delta.autoOptimize.autoCompact' = 'true'       ← compact after write
  );
```

### Hiccup 6: Checkpoint Location Deleted
```
PROBLEM:
  Someone ran: dbutils.fs.rm("/mnt/checkpoints/orders_bronze", recurse=True)
  (maybe trying to "fix" a different issue)
  
  Auto Loader restarts:
    → No checkpoint → treat as fresh start
    → cloudFiles.includeExistingFiles = true (default)
    → ALL 50,000 files re-processed
    → DUPLICATE DATA in Delta table (50,000 files × 2 = 100,000 files worth of data)

DETECTION:
  SELECT COUNT(*) FROM bronze.orders_raw;  ← suddenly 2x what it should be
  DESCRIBE HISTORY bronze.orders_raw;  ← see "STREAMING UPDATE" with abnormally high numOutputRows

FIX (if it happens):
  1. Stop the pipeline immediately
  2. Restore the Delta table:
     RESTORE TABLE bronze.orders_raw TO VERSION BEFORE DUPLICATE_COMMIT;
  3. Re-instate checkpoint from backup (or set up backup)
  4. If no backup: restart with cloudFiles.includeExistingFiles = false
     AND set modifiedAfter = last successful run timestamp
  
PREVENTION:
  Store checkpoint in ADLS with soft-delete enabled (30-day retention)
  Never delete checkpoint unless you want to reprocess everything
  Add runbook: "Check if checkpoint exists BEFORE any restart"
```

### Hiccup 7: Data Skew in Stage 1
```
PROBLEM:
  50,000 files are all from one month: 2024-06
  partitionBy("_year", "_month") → all data goes to ONE Delta partition
  
  Stage 1 (200 shuffle tasks):
    199 tasks: empty shuffle buckets (0 bytes) → finish instantly
    1 task: ALL 32 GB of data for _year=2024, _month=6 → takes 15 minutes
  
  Result: Stage 1 is serialized (1 task does all the work)
  
DETECTION:
  Spark UI → Stage 1 tasks → 1 task has 32GB shuffle read, others have 0 bytes

FIX:
  Option 1: Add a sub-partition column
    .partitionBy("_year", "_month", "_day")  ← 30 partitions instead of 1
  
  Option 2: Use Liquid Clustering instead of Hive partitioning
    ALTER TABLE bronze.orders_raw CLUSTER BY (order_date, region)
    → No directory partitioning, Spark manages co-location internally
  
  Option 3: Enable AQE skew join (doesn't directly fix partition skew, 
    but AQE coalescing handles small/large partition imbalance)
```

---

## 9. Trade-offs

### maxFilesPerTrigger: 5,000 vs Other Values

| Value | Batch Size | Batch Duration | Latency | Risk |
|-------|-----------|----------------|---------|------|
| **500** | 35 GB | ~1 min | Lower | 100 batches, 100 Delta commits, small file risk |
| **2,500** | 175 GB | ~4 min | Medium | 20 batches, balanced |
| **5,000** | 350 GB | ~8 min | Medium-High | 10 batches, efficient |
| **10,000** | 700 GB | ~15 min | High | 5 batches, OOM risk, SQS timeout risk |
| **No limit** | 3.5 TB | ~90 min | Very High | 1 batch, massive OOM risk |

**Rule of thumb**: `maxFilesPerTrigger` should result in batches that finish in **2-3x the trigger interval**. If trigger = 5 min and batches take 8 min, set trigger to 10 min OR reduce to 2,500 files.

### Notification Mode vs Incremental Listing

| Dimension | Notification Mode (SQS) | Incremental Listing |
|-----------|------------------------|---------------------|
| File discovery | Event-driven (instant) | Lexicographic scan from last offset |
| Latency | < 1 minute | Depends on trigger interval |
| For 50,000 files | 50,000 SQS events queued | Lists only files after last processed path |
| AWS setup | SNS + SQS auto-created | Nothing extra needed |
| IAM complexity | Higher | Lower |
| Ordering | Event arrival order | Alphabetical by file path |
| Best for this scenario | ✅ Yes (event-driven, no full scan) | Works but less efficient |

### Triggered (`availableNow`) vs `processingTime` for This Scenario

```
processingTime = "5 minutes" (streaming):
  ✅ Processes new files continuously as they arrive
  ✅ Low latency for ongoing ingestion after backfill
  ❌ Cluster must stay running ($$$)
  ❌ Timing complexity (what if batch > trigger interval?)
  
availableNow = True (one-shot batch):
  ✅ Processes all 50,000 files in optimized multi-batch run
  ✅ Cluster terminates after completion (cost-efficient for one-time load)
  ✅ No timing complexity — just runs until done
  ❌ No ongoing ingestion — must re-trigger for new files
  
FOR THIS SCENARIO (initial 50k file load):
  → Use availableNow=True for the initial backfill
  → Switch to processingTime="5 minutes" for ongoing ingestion
```

---

## 10. Business Impact

### Before Auto Loader (Legacy Batch Job)
```
Old approach:
  1. Scheduled Spark job runs every hour
  2. Lists ALL files in s3://bucket/landing/ on every run
  3. After 3 months: 450,000 accumulated files
  4. S3 List API: 450,000 ÷ 1,000 per call = 450 API calls just to list
  5. Listing takes 4-5 minutes before any data is processed
  6. Job runtime: 4 min listing + 45 min processing = 49 minutes
  7. Data latency: up to 1 hour + 49 min = ~2 hours
  8. At 12 months: 1.8M files → 30 min listing → job nearly unusable
```

### After Auto Loader
```
  1. SQS event-driven → no directory listing at all (O(1) not O(n))
  2. Data latency: < 3 minutes (SQS notification + batch processing)
  3. Scales to 1 billion files with NO performance degradation
  4. Exactly-once delivery → finance team trusts the numbers
  5. Schema evolution handled automatically → no weekend deployments for upstream changes
```

### Quantified Business Benefits (Real Numbers)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Data latency | 2 hours | 3 minutes | 40x faster |
| Engineering time on pipeline maintenance | 8 hrs/month | 0.5 hrs/month | 94% reduction |
| Failed pipeline incidents | 5/month | 0.2/month | 96% fewer |
| Max sustainable file count | ~500K | 1 billion+ | ∞ scale |
| Duplicate data incidents | 3/month | 0/month | Eliminated |

---

## 11. Best Practices for This Scenario

### 1. Right-Size maxFilesPerTrigger
```python
# Target: batch duration = 2-3x trigger interval (buffer for variance)
# 5,000 files × 70 MB = 350 GB → ~8 min on this cluster
# Trigger: 10 minutes → 8 min batch finishes before next trigger

# Monitor and tune:
batch_duration = query.recentProgress[-1]["batchDuration"] / 1000  # ms → seconds
trigger_interval = 600  # 10 minutes
if batch_duration > trigger_interval * 0.8:
    print("WARNING: Reduce maxFilesPerTrigger or increase cluster/trigger interval")
```

### 2. Separate Schema and Streaming Checkpoints
```python
# Two DIFFERENT paths — common mistake is using the same path
.option("cloudFiles.schemaLocation",  "/mnt/schema/orders")        # Schema only
.option("checkpointLocation",         "/mnt/checkpoints/orders")   # Streaming state only

# If you delete schemaLocation but keep checkpointLocation:
#   → Schema re-inferred from current files → might change types → pipeline breaks
# If you delete checkpointLocation but keep schemaLocation:
#   → ALL files reprocessed → duplicates
# Keep BOTH safe
```

### 3. Always Add Source File Metadata
```python
.withColumn("_source_file", F.col("_metadata.file_path"))
.withColumn("_source_file_ts", F.col("_metadata.file_modification_time"))
.withColumn("_source_file_size", F.col("_metadata.file_size"))
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_batch_id", F.spark_partition_id())  # useful for debugging
```
Why: Audit trail, debugging "which batch wrote this row?", GDPR deletion scoping.

### 4. Use `availableNow` for Initial Backfill, then Switch
```python
# Phase 1: Backfill (50,000 files)
query_backfill = df.writeStream \
    .trigger(availableNow=True) \            # Process all, then stop
    .option("checkpointLocation", "/mnt/checkpoints/orders") \
    .table("bronze.orders_raw")
query_backfill.awaitTermination()

# Phase 2: Ongoing (after backfill completes, same checkpoint!)
query_live = df.writeStream \
    .trigger(processingTime="5 minutes") \   # Reuse same checkpoint
    .option("checkpointLocation", "/mnt/checkpoints/orders") \  # SAME PATH
    .table("bronze.orders_raw")
# Auto Loader knows files 1-50,000 are done → only processes new files
```

### 5. Run OPTIMIZE After Backfill
```python
# After all 10 batches complete → compact the small files
spark.sql("OPTIMIZE bronze.orders_raw")
# OR with Z-ORDER for future query patterns:
spark.sql("OPTIMIZE bronze.orders_raw ZORDER BY (order_date, customer_id)")

# Schedule: run OPTIMIZE nightly (not after every batch)
```

### 6. Monitor Batch Health Programmatically
```python
import json

def monitor_streaming(query, alert_threshold_seconds=600):
    """Alert if any batch takes longer than threshold."""
    for progress in query.recentProgress:
        batch_duration_s = progress.get("batchDuration", 0) / 1000
        num_input_rows = progress.get("numInputRows", 0)
        
        if batch_duration_s > alert_threshold_seconds:
            print(f"ALERT: Batch {progress['batchId']} took {batch_duration_s:.0f}s")
        
        if num_input_rows == 0 and progress.get("sources"):
            # Check if source has data but pipeline processed 0 rows (stuck?)
            print(f"WARNING: Batch {progress['batchId']} processed 0 rows")
            
        print(f"Batch {progress['batchId']}: "
              f"{num_input_rows:,} rows in {batch_duration_s:.1f}s "
              f"({progress.get('processedRowsPerSecond', 0):,.0f} rows/sec)")
```

### 7. Protect the Checkpoint
```python
# Store checkpoint on ADLS with soft-delete
# Azure: enable soft-delete on the storage account (30-day retention)
# AWS: enable S3 versioning on the checkpoint bucket

# Pre-restart checklist (add to runbook):
checkpoint_path = "/mnt/checkpoints/orders"
if not dbutils.fs.ls(checkpoint_path):
    raise Exception("CHECKPOINT MISSING! Set includeExistingFiles=false + modifiedAfter before restart")
```

---

## 12. STAR Answers for FAANG

### Q1: "Walk me through a large-scale file ingestion problem you solved"

**Situation:** We had an initial data migration: 50,000 JSON order files (~70 MB each, totaling 3.5 TB) sitting in S3 from a legacy system. The business needed this data in Delta Lake for a reporting go-live in 72 hours. The legacy migration script used `spark.read.json("s3://bucket/")` — listing 50,000 files took 18 minutes alone, and the whole job kept timing out after 4 hours without completing.

**Task:** Ingest all 50,000 files reliably into a Delta Bronze table within 72 hours, with exactly-once guarantees and no data loss.

**Action:**
1. **Chose Auto Loader with `availableNow` trigger** — `availableNow` processes all available files in controlled micro-batches, then stops. Unlike the bulk `spark.read`, it doesn't try to read all 3.5 TB in one Spark job.
2. **Set `maxFilesPerTrigger = 5,000`** — each batch reads 350 GB (5,000 × 70 MB). With the 8-node cluster (64 cores), each batch took ~8 minutes. 10 batches total.
3. **Used `schemaEvolutionMode = "addNewColumns"`** — the legacy files had 3 different schema versions across the 50K files. Without this, the pipeline would fail when it encountered the first schema variant.
4. **Added source file metadata columns** (`_source_file`, `_ingested_at`, `_file_size_bytes`) — the compliance team needed to trace which file each row came from for audit purposes.
5. **Ran OPTIMIZE after all 10 batches** — 10 batches × 200 shuffle partitions = 2,000 small Parquet files → compacted to 250 files of ~128 MB each.
6. **Protected checkpoint on ADLS with soft-delete** — the checkpoint was the only thing preventing re-ingestion of all 50,000 files.

**Result:** All 50,000 files ingested in ~90 minutes (10 batches × 8-9 minutes each). Zero duplicates confirmed via `SELECT COUNT(DISTINCT _source_file) FROM bronze.orders_raw` matching exactly 50,000. The reporting go-live happened on schedule. After the initial load, switched the same query to `processingTime = "5 minutes"` for ongoing ingestion, reusing the same checkpoint — new files were picked up automatically.

---

### Q2: "How do you ensure exactly-once delivery in Auto Loader?"

**Situation:** Post-ingestion, our Finance team ran a revenue reconciliation between the legacy system (50,000 source files) and the new Delta table. They found the row count was 97,412,851 in Delta vs 97,290,000 in the source — 122,851 extra rows. This was a potential duplicate problem.

**Task:** Investigate the discrepancy, identify the root cause, and prevent it from happening again.

**Action:**
1. **Diagnosed the issue**: Ran `DESCRIBE HISTORY bronze.orders_raw` — found 11 commits instead of 10. One batch had committed TWICE.
2. **Root cause**: During Batch 4, the cluster hit a spot instance preemption mid-write. The Delta commit had already been made (log entry written), but the checkpoint for batch 4 had NOT been updated yet (the WAL offset write succeeded, but the commit confirmation write failed). On restart, Auto Loader re-ran batch 4, creating duplicate data.
3. **This is a known edge case**: Auto Loader + Delta provides **exactly-once** when the checkpoint and Delta log are both consistent. If they get out of sync (rare infrastructure failure), you can get a duplicate commit.
4. **Fix for the duplicate**: Used Delta time travel to find the pre-duplicate state:
   ```python
   RESTORE TABLE bronze.orders_raw TO VERSION AS OF 9;  # Before the double commit
   # Re-ran batch 4 manually with the original files
   ```
5. **Prevention**: 
   - Stored checkpoint on multi-AZ ADLS (not single-AZ S3) for resilience
   - Added a post-batch validation: `assert batch_row_count == expected_rows_per_batch`
   - Added a daily reconciliation job: file count in S3 vs distinct `_source_file` values in Delta

**Result:** Fixed the 122,851 duplicate rows. Established a "trust but verify" monitoring pattern: Auto Loader provides exactly-once by design, but we run a nightly reconciliation as a safety net. The reconciliation job has caught 0 anomalies in 8 months since implementation.

---

### Q3: "How would you design Auto Loader for a production pipeline with 1 million files/day?"

**Situation:** System design round — "Your company processes clickstream data. 1 million JSON files, 5 MB each, arrive in S3 every day (about 700 files/minute). Design the ingestion layer."

**Task:** Design a scalable, fault-tolerant, cost-effective ingestion pipeline.

**Action (design answer):**

"First, key numbers: 1M files × 5 MB = 5 TB/day → ~55 GB/minute continuous throughput needed.

**Source configuration**:
- `cloudFiles.useNotifications = true` — at 700 files/minute, incremental listing would quickly fall behind. SQS event-driven discovery is mandatory at this scale.
- `maxFilesPerTrigger = 10,000` — 10,000 × 5 MB = 50 GB per batch. With a 32-node cluster (256 cores), each batch takes ~3 minutes. Trigger interval: 5 minutes.
- `maxBytesPerTrigger = "50g"` as a safety cap alongside maxFilesPerTrigger — whichever limit hits first stops the batch.

**Cluster design**:
- 32 workers × 8 cores = 256 core slots
- 10,000 tasks per batch ÷ 256 cores = ~40 waves → acceptable
- Use Spot instances for executor nodes (streaming is auto-restartable, so preemption is recoverable) → 60-70% cost reduction

**Schema handling**:
- `schemaEvolutionMode = "rescue"` — at this scale, you cannot afford pipeline failures from schema changes. Unknown fields go to `_rescued_data`, Silver layer handles them explicitly.

**Delta table design**:
- `partitionBy("date", "hour")` — not by `minute` (too many small partitions)
- Enable `autoOptimize.optimizeWrite = true` — coalesces small writes automatically
- Run `OPTIMIZE` every 4 hours to prevent file count explosion
- Enable Liquid Clustering on `user_id, event_type` for downstream queries

**Monitoring**:
- Alert if `batchDuration > 240s` (batch taking > 4 min means we're falling behind)
- Alert if `numInputRows = 0` for 3 consecutive batches (file discovery may be broken)
- Daily reconciliation: S3 manifest file count vs distinct `_source_file` count in Delta

**Cost**: At 5 TB/day, this pipeline runs 24/7. With Spot instances and Databricks Enhanced Autoscaling (scales down to 4 nodes during off-peak hours like 2-6 AM), estimated cost: ~$800/month vs ~$2,200/month for reserved on-demand."

**Result:** The interviewer extended the conversation into trade-offs between Auto Loader and Kafka for this volume — I noted that at 700 files/minute with 3-minute batch latency, Auto Loader is appropriate. If sub-minute latency was needed, the upstream service should write to Kafka directly and we'd use Structured Streaming. The choice depends on the latency SLA.
