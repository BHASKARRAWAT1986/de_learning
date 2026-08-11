# Structured Streaming — Complete FAANG Interview Guide
## Apache Spark's Scalable, Fault-Tolerant Stream Processing Engine

---

## 1. What Is Structured Streaming?

Structured Streaming is a **scalable, fault-tolerant stream processing engine** built on the Spark SQL engine. It treats a live data stream as an **unbounded table** — new data appends as new rows, and queries are run continuously or in micro-batches.

### The Mental Model
```
Streaming DataFrame = Unbounded Table

Time ──────────────────────────────────────────►
      [batch 0] [batch 1] [batch 2] [batch 3] ...
          │         │         │         │
          ▼         ▼         ▼         ▼
      Results   Results   Results   Results
          │         │         │         │
          └─────────┴─────────┴─────────┴──► Output Table (Delta)
```

---

## 2. Architecture Internals

### Execution Model
```
Source (Kafka/S3/Delta) 
    │
    ├── Micro-batch trigger fires (e.g., every 30s)
    │
    ├── Driver: reads new offsets from source
    │   (not data — just metadata like Kafka partition offsets)
    │
    ├── Driver: writes offsets to WAL (Write-Ahead Log) in checkpoint
    │
    ├── Executors: read actual data for the offset range
    │
    ├── Executors: execute transformations (filter, join, agg)
    │
    ├── Executors: write results to sink (Delta table)
    │
    └── Driver: commits offsets to checkpoint (batch complete)
         
Checkpoint:
  ├── offsets/      ← what has been read (WAL)
  ├── commits/      ← what has been processed and committed
  └── state/        ← stateful aggregation state (for groupBy, joins)
```

### Exactly-Once Guarantee
1. **Offset tracking**: Driver writes offsets to WAL BEFORE processing — if it crashes, replay from last committed offset
2. **Idempotent writes**: Delta supports idempotent writes via transaction log
3. **Result**: Even if a batch re-executes, the output is identical → exactly-once end-to-end

---

## 3. Sources — Complete Reference

### Kafka (Most Common)
```python
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders")                      # Single topic
    # OR .option("subscribePattern", "orders_.*")       # Regex pattern
    # OR .option("assign", '{"orders":[0,1,2]}')        # Specific partitions
    .option("startingOffsets", "latest")                # latest | earliest | {"topic":{"part":offset}}
    .option("endingOffsets", "latest")                  # Only for batch, not streaming
    .option("kafka.group.id", "databricks-silver")      # Consumer group
    .option("failOnDataLoss", "false")                  # Don't fail if Kafka topic is compacted
    .option("maxOffsetsPerTrigger", 100000)             # Backpressure: max records per batch
    .option("minPartitions", 10)                        # Spark partitions (not Kafka partitions)
    .option("kafka.security.protocol", "SASL_SSL")      # Auth
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config",
            dbutils.secrets.get("kv", "kafka-jaas-config"))
    .load()
)

# Kafka schema: key, value, topic, partition, offset, timestamp, timestampType
# Value is binary — must deserialize:
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, LongType

schema = StructType() \
    .add("order_id", LongType()) \
    .add("customer_id", LongType()) \
    .add("amount", StringType())

df_parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp"),
    col("partition"),
    col("offset")
).select("data.*", "kafka_timestamp", "partition", "offset")
```

### Delta Table as Source (Delta Change Data Feed)
```python
df = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")                   # CDF: get insert/update/delete rows
    .option("startingVersion", 0)                       # OR startingTimestamp
    .table("silver.orders")
)
# CDF adds: _change_type (insert/update_preimage/update_postimage/delete), _commit_version, _commit_timestamp
```

### Auto Loader (Files)
```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .load("/mnt/landing/")
# See 01_autoloader.md for full reference
```

### Rate Source (Testing Only)
```python
df = spark.readStream.format("rate").option("rowsPerSecond", 1000).load()
# Generates: timestamp, value (monotonically increasing long)
```

---

## 4. Triggers — When Batches Execute

```python
# Micro-batch: process every N seconds/minutes
.trigger(processingTime="30 seconds")    # default: as fast as possible
.trigger(processingTime="5 minutes")

# One-shot: process all available data, then stop (like a batch job)
.trigger(availableNow=True)              # DBR 10.1+ — replaces once=True
.trigger(once=True)                      # Deprecated but still works

# Continuous processing (experimental, sub-millisecond latency)
.trigger(continuous="1 second")          # Checkpoint every 1 second (epoch-based)
```

### `availableNow` vs `processingTime`

| Trigger | Behavior | Use Case |
|---------|----------|----------|
| `processingTime="30s"` | Run every 30s indefinitely | Continuous low-latency |
| `availableNow=True` | Process all backlog in multiple batches, stop | Scheduled job, backfill |
| `once=True` | Process all backlog in ONE batch, stop | Simple scheduled job (less efficient than availableNow) |
| `continuous="1s"` | Sub-millisecond, epoch checkpoints | Ultra-low latency (limited ops support) |

---

## 5. Output Modes

```python
# append — only new rows since last trigger (default for most sources)
.outputMode("append")
# Use for: no aggregations, or append-only operations

# complete — entire result table rewritten every trigger
.outputMode("complete")
# Use for: aggregations without watermark (counts, sums)
# WARNING: rewrites entire table every batch — expensive

# update — only rows that changed since last trigger
.outputMode("update")
# Use for: aggregations with watermark
# NOT supported by Delta as sink (use append instead)
```

---

## 6. Sinks — Where to Write

### Delta Table (Recommended)
```python
query = (
    df_parsed.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/silver_orders")
    .option("mergeSchema", "true")
    .outputMode("append")
    .partitionBy("date")                                # Optional
    .trigger(processingTime="1 minute")
    .table("silver.orders")                            # Unity Catalog compatible
)
```

### Kafka (Write Back)
```python
df_result.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("topic", "enriched_orders") \
    .option("checkpointLocation", "/checkpoint/kafka-out") \
    .start()
# DataFrame must have: value column (binary or string)
# Optional: key, topic, partition columns
```

### foreachBatch (Custom Sink — Most Flexible)
```python
def process_batch(batch_df, batch_id):
    """Custom logic per micro-batch. Exactly-once requires idempotent writes."""
    # batch_df is a static DataFrame — use all static Spark APIs
    
    # Example: MERGE (upsert) to Delta
    batch_df.createOrReplaceTempView("batch_updates")
    batch_df._jdf.sparkSession().sql("""
        MERGE INTO silver.orders AS target
        USING batch_updates AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # Example: Write to multiple tables
    batch_df.filter("status = 'COMPLETED'").write.mode("append").table("silver.completed_orders")
    batch_df.filter("status = 'FAILED'").write.mode("append").table("silver.failed_orders")

query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/checkpoint/foreachbatch") \
    .trigger(processingTime="2 minutes") \
    .start()
```

---

## 7. Stateful Operations

### Aggregations with Watermark (Group By + Time Window)
```python
from pyspark.sql.functions import window, sum, count

# Watermark: how late can data arrive and still be included?
# Here: data up to 10 minutes late is accepted. Older data is dropped.
windowed_agg = (
    df_parsed
    .withWatermark("event_time", "10 minutes")         # REQUIRED for stateful agg in streaming
    .groupBy(
        window(col("event_time"), "5 minutes"),         # 5-minute tumbling window
        col("product_id")
    )
    .agg(
        sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count")
    )
)

windowed_agg.writeStream \
    .outputMode("append")                              # Use append with watermark
    .option("checkpointLocation", "/checkpoint/agg") \
    .table("gold.revenue_5min")
```

### Window Types
```python
from pyspark.sql.functions import window

# Tumbling window: non-overlapping, fixed size
window(col("event_time"), "5 minutes")

# Sliding window: overlapping
window(col("event_time"), "10 minutes", "5 minutes")  # 10-min window, slides every 5 min

# Session window: gap-based (DBR 10.5+)
from pyspark.sql.functions import session_window
session_window(col("event_time"), "30 minutes")       # New session if 30-min gap
```

### Stream-Static Join
```python
# Static dimension table (customer profile)
customers_df = spark.read.table("silver.customers")   # Static read

# Stream-static join (no state management needed)
enriched_orders = orders_stream.join(
    customers_df,
    orders_stream.customer_id == customers_df.customer_id,
    "left"
)
# NOTE: Static table is re-read at start of each batch
# Use broadcast for small dimension tables
```

### Stream-Stream Join (Stateful)
```python
# Both sides are streams — requires watermark on both
orders_stream = orders_stream.withWatermark("order_time", "1 hour")
payments_stream = payments_stream.withWatermark("payment_time", "1 hour")

joined = orders_stream.join(
    payments_stream,
    expr("""
        order_id = payment_order_id AND
        payment_time BETWEEN order_time AND order_time + INTERVAL 1 HOUR
    """),
    "leftOuter"
)
# State is kept for the watermark duration (1 hour)
# Records outside watermark are matched and emitted or dropped
```

---

## 8. Checkpoint — Deep Dive

```
checkpoint/
  ├── metadata           ← stream ID, Spark version
  ├── offsets/
  │     ├── 0            ← offsets at batch 0 start (WAL entry)
  │     ├── 1            ← offsets at batch 1 start
  │     └── ...
  ├── commits/
  │     ├── 0            ← batch 0 committed successfully
  │     ├── 1
  │     └── ...
  └── state/
        └── 0/           ← stateful operator state (for groupBy, joins)
              ├── 0.delta ← state delta files (RocksDB in streaming)
              └── ...
```

### Checkpoint Recovery
- On restart, driver reads `offsets/` to find last committed offset
- If `commits/N` exists → batch N completed → start from N+1
- If `commits/N` missing → batch N may be partial → re-execute batch N (idempotent with Delta sink)

### When to Delete Checkpoint (and When NOT to)
```python
# DELETE CHECKPOINT ONLY IF:
# 1. You intentionally want to reprocess from the beginning
# 2. You changed the query schema incompatibly
# 3. You changed the source (new Kafka topic, new S3 path)

# NEVER DELETE CHECKPOINT if:
# - Pipeline crashed and you're restarting (checkpoint enables recovery)
# - You're doing a rolling deploy of code that's schema-compatible

# Force-reset checkpoint:
dbutils.fs.rm("/checkpoint/silver_orders", recurse=True)
# Then restart with: startingOffsets = "latest" or specific offset
```

---

## 9. Common Configurations

```python
# ─── Performance Tuning ───────────────────────────────────────────
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")  # Max records/sec/partition from Kafka
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")  # Better state store
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")

# ─── Checkpointing ───────────────────────────────────────────────
spark.conf.set("spark.sql.streaming.checkpointFileManagerClass",
               "com.databricks.sql.streaming.WritePreciselyOnceFileManager")

# ─── Watermark / Late Data ────────────────────────────────────────
spark.conf.set("spark.sql.streaming.multipleWatermarkPolicy", "min")  # or "max"

# ─── Schema Evolution ─────────────────────────────────────────────
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ─── Async Progress Tracking ──────────────────────────────────────
spark.conf.set("spark.sql.streaming.asyncProgressTrackingEnabled", "true")  # DBR 12+
spark.conf.set("spark.sql.streaming.asyncProgressTrackingCheckpointIntervalMs", "1000")
```

---

## 10. Monitoring

### Query Progress Object
```python
query = df.writeStream.start()

# Current status
print(query.status)
# {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}

# Last N batches performance
import json
for progress in query.recentProgress[-5:]:
    print(json.dumps({
        "batchId": progress["batchId"],
        "inputRows": progress["numInputRows"],
        "inputRowsPerSecond": progress["inputRowsPerSecond"],
        "processedRowsPerSecond": progress["processedRowsPerSecond"],
        "batchDurationMs": progress["batchDuration"],
        "sources": progress["sources"]
    }, indent=2))
```

### Detect Consumer Lag (Kafka)
```python
# From recentProgress
latest = query.recentProgress[-1]
for source in latest["sources"]:
    if "endOffset" in source and "startOffset" in source:
        # Lag = messages in Kafka - messages processed this batch
        print(f"Topic lag: {source.get('latestOffset', 'N/A')}")
```

### Spark UI for Streaming
- **Structured Streaming tab** → batch timeline, input/output rate
- Look for: batches taking longer than trigger interval → processing rate < input rate → falling behind

---

## 11. Pros and Cons

### Pros
| Benefit | Detail |
|---------|--------|
| Exactly-once end-to-end | Checkpoint + Delta = no duplicates, no data loss |
| Unified API | Same DataFrame API for batch and stream |
| Fault-tolerant | Checkpoint enables seamless restart from last committed offset |
| Stateful operations | Watermark-based windowed aggregations, stream-stream joins |
| Multiple sources | Kafka, Kinesis, Event Hubs, Delta CDF, Files, Rate |
| backpressure | `maxOffsetsPerTrigger` prevents OOM on data spikes |
| `availableNow` trigger | Process backlog like batch, then stop — cost-efficient |
| RocksDB state store | Much faster than in-memory for large state (DBR 9.0+) |

### Cons
| Limitation | Detail |
|------------|--------|
| Always-on cluster cost | Streaming cluster must be running (even during idle periods) |
| Operational complexity | Checkpoint management, schema evolution, state size monitoring |
| Watermark tuning is hard | Too tight = data loss; too loose = state explosion + memory |
| State can grow unbounded | Stateful joins/agg without watermark → OOM eventually |
| No native UI for lag | Must use Kafka CLI or kafka-exporter for consumer lag metrics |
| Schema changes need restart | Changing query structure requires new checkpoint |
| Debugging is harder | No query plan for a streaming batch until it executes |
| `complete` output mode is expensive | Rewrites entire result table every batch |

---

## 12. Trade-offs

### Structured Streaming vs DLT
| Dimension | Structured Streaming | DLT |
|-----------|---------------------|-----|
| Code style | Imperative (you define HOW) | Declarative (you define WHAT) |
| Data quality | Manual (filter + write to quarantine) | Built-in `expect` / `expect_or_drop` |
| Retry/recovery | Manual (restart, checkpoint) | Automatic (Databricks manages) |
| Observability | Spark UI, query.recentProgress | DLT Event Log, built-in dashboard |
| Multi-table dependencies | Manual ordering | Automatic DAG resolution |
| CDC support | Manual MERGE logic | `APPLY CHANGES INTO` built-in |
| Control | Full | Limited (Databricks abstracts infra) |
| **Choose when** | Need full control or custom sink | Want managed pipeline with quality gates |

### Structured Streaming vs Batch
| Dimension | Structured Streaming | Batch |
|-----------|---------------------|-------|
| Latency | Seconds to minutes | Minutes to hours |
| Cost | Higher (always-on) | Lower (on-demand) |
| Complexity | Higher | Lower |
| State management | Required for stateful ops | Not needed |
| **Choose when** | Near-realtime required | Hourly/daily SLA is fine |

---

## 13. STAR Answers for FAANG

### Q1: "Describe a time you built a fault-tolerant streaming pipeline"

**Situation:** We were building a real-time fraud detection pipeline. Payment events came from Kafka at ~50K events/minute. The pipeline needed to join each payment event with the customer's 30-day spend history to compute a fraud score. A previous attempt using simple batch jobs had 15-minute latency — too slow for fraud decisioning.

**Task:** Build a streaming pipeline with <2 minute latency, fault-tolerant (no data loss on crash), and capable of stateful join with historical spend data.

**Action:**
1. **Source**: Kafka `payment_events` topic, `maxOffsetsPerTrigger = 100000`, trigger `processingTime = "1 minute"`
2. **Stream-static join**: Read `customer_spend_30d` table (updated hourly) as a static DataFrame, broadcast-joined to enrich each payment event
3. **Fraud scoring**: Applied a UDF wrapping a pre-trained scikit-learn model loaded via MLflow
4. **Sink**: `foreachBatch` with a MERGE to Delta table `silver.payment_fraud_scores` — idempotent upsert by `payment_id`
5. **Checkpoint**: Stored on ADLS with soft-delete (30-day retention) to prevent accidental deletion
6. **Recovery testing**: Deliberately killed the cluster mid-batch, restarted — verified via DESCRIBE HISTORY that no duplicate commits occurred

**Result:** Pipeline achieved 45-second end-to-end latency. Zero data loss across 4 planned and 2 accidental restarts over 3 months. Fraud detection rate improved from 3.2% (batch model) to 4.8% (real-time model) because decisions were made in seconds, not minutes.

---

### Q2: "How do you handle late-arriving data in a streaming pipeline?"

**Situation:** Our e-commerce order analytics pipeline aggregated order events into 5-minute revenue windows. We noticed that ~3% of mobile app events arrived 8-12 minutes late (due to poor connectivity), causing those events to be dropped and revenue figures to be understated.

**Task:** Handle late data without holding state indefinitely (which would OOM the streaming cluster).

**Action:**
1. **Added watermark**: `.withWatermark("event_time", "15 minutes")` — events up to 15 minutes late are accepted; after that, state for that window is released
2. **Changed output mode** from `complete` to `append` — with watermark, append mode emits a window's result only after the watermark passes the window's end time (i.e., once we're confident no more late data will arrive)
3. **Added a "corrections" stream**: For events >15 minutes late (e.g., users who were offline for hours), we wrote them to a separate `corrections` Delta table. A daily micro-batch job patched the gold aggregation tables with these corrections.
4. **Monitored watermark lag**: Added a metric alert when `max(event_time) - current_time > 20 minutes` — indicates the pipeline is processing stale data.

**Result:** Late data coverage improved from 97% to 99.97%. Revenue figures were accurate within 15 minutes for 99.97% of events. The corrections job handled the remaining 0.03% (typically offline-sync events) with a next-day SLA, which was acceptable to the business.

---

### Q3: "How do you handle schema evolution in a streaming pipeline?"

**Situation:** Our Kafka-based order pipeline was running for 6 months. The upstream service team announced they were adding 5 new fields to the order event schema. They couldn't give us advance warning because it was a rolling deployment — new and old schema events would coexist in the Kafka topic for ~1 hour.

**Task:** Handle the schema migration with zero pipeline downtime and no data loss for either old or new schema events.

**Action:**
1. **Changed deserialization**: Replaced `from_json(value, hardcoded_schema)` with a dynamic approach — parsed value as a JSON string first, then used `schema_of_json` sampling + `from_json` with `"permissive"` mode (unknown fields go to `_corrupt_record` or are silently ignored)
2. **Added `_rescued_data` pattern**: Any fields not in the base schema were captured in a JSON string column `_extra_fields` using `get_json_object` for known new fields
3. **Enabled `mergeSchema`** on the Delta write: `.option("mergeSchema", "true")` — new columns in the DataFrame automatically extend the Delta table schema
4. **No checkpoint deletion**: Schema-compatible changes don't require checkpoint reset. Only added columns, no renames or type changes.
5. **Post-migration**: After 1 week of stable new schema, ran `ALTER TABLE` to rename `_extra_fields`-derived columns to proper names and dropped the `_extra_fields` column.

**Result:** Zero pipeline downtime during the 1-hour mixed-schema window. Both old and new schema events were correctly parsed. The new fields were available in Silver tables within 5 minutes of the first new-schema event arriving in Kafka. Established this as the standard schema migration runbook for the team.

---

## 14. Key Internals to Memorize for Interviews

1. **Micro-batch vs Continuous**: Micro-batch (default) = process a range of offsets per trigger; Continuous = record-by-record with epoch-based checkpointing (sub-millisecond but limited operator support)
2. **WAL before process**: Offsets written to WAL *before* reading data — guarantees at-least-once on crash, idempotent sink provides exactly-once
3. **State is in RocksDB**: Stateful ops (groupBy, stream-stream join) store state in RocksDB on executor local disk, checkpointed to ADLS/S3
4. **`foreachBatch` = static API**: Inside foreachBatch, `batch_df` is a static DataFrame — use `.write.mode("append")` not `.writeStream`
5. **Watermark = two things**: (a) how late data can be and still be processed, AND (b) when state is cleared (state kept until watermark passes window end)
6. **`availableNow` > `once`**: `availableNow` processes backlog in multiple optimized batches; `once` does it in one potentially huge batch
