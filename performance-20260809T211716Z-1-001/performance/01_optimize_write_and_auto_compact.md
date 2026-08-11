# Optimize Write & Auto Compact — Complete Guide

---

## 1. What Is Optimize Write?

**Optimize Write** is a Databricks feature that intelligently merges small files **at write time** before committing them to the Delta table.  
Without it, each task in a Spark job writes its own file — leading to thousands of tiny files (the "small file problem").

> It does NOT change the data layout, it only consolidates file sizes.

### How It Works Internally
```
Without Optimize Write:
  200 Spark tasks × 1MB each = 200 × 1MB files → SLOW reads later

With Optimize Write:
  200 Spark tasks → shuffle + merge → ~10 × 20MB files → FAST reads
```

The engine analyzes the output file count and size. If files would be too small, it triggers an additional **shuffle phase** to redistribute and merge data before writing.

### Enable Optimize Write
```sql
-- Table-level (recommended)
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Session-level
SET spark.databricks.delta.optimizeWrite.enabled = true;

-- At write time (DataFrame API)
df.write.option("optimizeWrite", "true").format("delta").save("/path/to/table")

-- In CREATE TABLE
CREATE TABLE events (
  event_id BIGINT,
  user_id  STRING,
  ts       TIMESTAMP
)
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
```

### Cost of Optimize Write
- Adds a **shuffle step** — increases CPU and network
- Worth it when read latency matters more than write latency
- NOT recommended for write-heavy, real-time pipelines where write speed is critical

---

## 2. What Is Auto Compact?

**Auto Compact** runs a lightweight OPTIMIZE automatically after every write operation, merging small files that were recently written.

Unlike full `OPTIMIZE`, Auto Compact:
- Only compacts **recently written files** (not the entire table)
- Targets a smaller file size (~128MB vs 1GB for full OPTIMIZE)
- Runs **asynchronously** after the write commits — the write itself returns to the caller immediately

### How It Works Internally
```
Write commits to Delta log
        ↓
Auto Compact checks: "Are there many small files from recent writes?"
        ↓
If yes: merges those recent small files into larger ones (~128MB)
        ↓
Delta log updated with compacted file list
        ↓
Old small files marked for deletion (cleaned by VACUUM later)
```

### Enable Auto Compact
```sql
-- Table-level
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Session-level
SET spark.databricks.delta.autoCompact.enabled = true;

-- Both together (standard production setup)
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
```

### Configure Target File Size for Auto Compact
```sql
-- Default is 128MB; tune based on workload
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.autoCompact.minNumFiles' = '5',  -- trigger if ≥5 small files
  'delta.targetFileSize' = '134217728'                 -- 128MB target
);
```

---

## 3. Optimize Write vs Auto Compact — Side by Side

| Property | Optimize Write | Auto Compact |
|---|---|---|
| **When it runs** | During write (pre-commit) | After write (post-commit, async) |
| **What it does** | Merges task outputs before writing | Merges recently written small files |
| **Scope** | Current write batch only | Recent files in the table |
| **Target file size** | ~1GB (full size) | ~128MB (lighter) |
| **Write latency impact** | **Yes** — adds shuffle | **Minimal** — async |
| **Read improvement** | Immediate | Near-immediate |
| **Best for** | Batch writes with many small tasks | Streaming / frequent incremental writes |

---

## 4. Full OPTIMIZE vs Auto Compact

| Property | Full OPTIMIZE | Auto Compact |
|---|---|---|
| **Scope** | Entire table (or WHERE clause) | Only recently written files |
| **Target file size** | ~1GB | ~128MB |
| **Runtime** | Minutes to hours | Seconds to minutes |
| **Trigger** | Manual / scheduled | Automatic after write |
| **With Liquid Clustering** | Required (incremental) | Supplements it |

---

## 5. Pros and Cons

### Optimize Write
| Pros | Cons |
|---|---|
| Eliminates small file problem at source | Extra shuffle = higher write latency |
| Reduces number of files immediately | Increased network I/O during write |
| Better downstream read performance | Not suitable for latency-sensitive pipelines |

### Auto Compact
| Pros | Cons |
|---|---|
| Async — no write latency penalty | Slightly delayed — files may be small briefly |
| Automatic — no manual OPTIMIZE needed | Does not cluster data (just compacts) |
| Great for streaming workloads | Smaller target (128MB) may need full OPTIMIZE later |

---

## 6. Real-World Streaming Example

```python
# Structured Streaming with both features enabled
(spark.readStream
  .format("delta")
  .load("/bronze/raw_events")
  .writeStream
  .format("delta")
  .option("checkpointLocation", "/checkpoints/silver_events")
  .trigger(processingTime="2 minutes")
  .toTable("silver_events")   # table has optimizeWrite + autoCompact enabled
  .start())
```

With this setup:
- Each micro-batch's small files are merged by **Optimize Write** before commit
- **Auto Compact** merges any remaining stragglers asynchronously
- Full **OPTIMIZE** can be run weekly to do a deep compaction pass

---

## 7. Decision Guide

```
Is your workload streaming or frequent incremental loads?
  YES → Enable Auto Compact (async, no latency hit)
  
Do your batch writes produce many small task outputs?
  YES → Enable Optimize Write

Do you need the absolute best read performance?
  YES → Enable both + schedule periodic full OPTIMIZE
  
Is write latency your primary concern?
  YES → Skip Optimize Write; use Auto Compact only
```

---

## 8. Quick Reference

```sql
-- Check current table properties
SHOW TBLPROPERTIES my_table;

-- Enable both
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- Disable both
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact'   = 'false'
);
```
