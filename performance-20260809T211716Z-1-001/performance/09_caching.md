# Caching in Databricks / Spark — Complete Guide

---

## 1. Types of Caching in Databricks

| Cache Type | What Is Cached | Where | Persistence |
|---|---|---|---|
| **Spark DataFrame cache** | DataFrame in Spark memory | Executor JVM heap/off-heap | Until unpersist() or cluster restart |
| **Delta Cache (Disk Cache)** | Raw Parquet bytes | NVMe SSD on worker nodes | Until cache eviction or node restart |
| **Result Cache** | Query result set | Driver memory | Per-session, cleared on restart |
| **Broadcast Variables** | Small lookup data | All executor JVMs | Until unpersisted |

---

## 2. Spark DataFrame / RDD Cache

### How It Works
```
First access:
  Read from storage → process → cache result in executor memory
  
Subsequent accesses:
  Read directly from executor memory (no storage I/O)
```

### Cache a DataFrame
```python
# Cache (lazy — not loaded until first action)
df = spark.table("orders").filter("year = 2025")
df.cache()

# Force materialization NOW (not lazy)
df.count()   # triggers cache population

# Or use persist with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)  # spill to disk if memory full

# Check if cached
print(df.is_cached)  # True

# Uncache explicitly
df.unpersist()
```

### Storage Levels
```python
from pyspark import StorageLevel

StorageLevel.MEMORY_ONLY          # JVM heap only; dropped if no space
StorageLevel.MEMORY_AND_DISK      # heap first; spill to disk
StorageLevel.DISK_ONLY            # disk only (slower but doesn't use heap)
StorageLevel.MEMORY_ONLY_SER      # serialized in heap (smaller, slower to read)
StorageLevel.MEMORY_AND_DISK_SER  # serialized, spills to disk
StorageLevel.OFF_HEAP              # off-heap memory (requires off-heap config)
```

### SQL Cache
```sql
-- Cache a table (SQL equivalent of df.cache())
CACHE TABLE orders;

-- Cache a filtered subset
CACHE TABLE recent_orders AS SELECT * FROM orders WHERE year = 2025;

-- Uncache
UNCACHE TABLE orders;

-- Check what's cached
SELECT * FROM spark_catalog.default.orders;  -- Spark UI shows cached marker
```

---

## 3. Delta Cache (Databricks Disk Cache)

**Delta Cache** is a Databricks-specific feature that caches **raw Parquet bytes** on fast NVMe SSDs attached to worker nodes.

### Key Difference: Delta Cache vs Spark Cache
| | Delta Cache | Spark DataFrame Cache |
|---|---|---|
| **What's stored** | Raw Parquet bytes (compressed) | Deserialized Spark rows |
| **Where stored** | Worker NVMe SSD | JVM heap / off-heap |
| **Memory usage** | Minimal (uses SSD) | High (uses executor RAM) |
| **Shared** | Across all queries and sessions | Per-DataFrame reference |
| **Persistence** | Survives query completion | Until unpersist() |
| **Automatic** | YES (reads automatically cached) | NO (must call cache()) |
| **Cloud cost** | Requires SSD-enabled instances | Any instance |

### How Delta Cache Works
```
First read of a file:
  Read from cloud storage (S3/ADLS/GCS) → cache on worker NVMe SSD → process

Subsequent reads of same file (same or different query):
  Read from local NVMe SSD → process
  (10-100x faster than cloud storage read)
```

### Enable/Configure Delta Cache
```python
# Enable (default: enabled on SSD instances in Databricks)
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# Configure cache size (% of SSD used)
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
spark.conf.set("spark.databricks.io.cache.maxMetaDataCache", "1g")
```

```sql
-- Check delta cache status
SET spark.databricks.io.cache.enabled;

-- Enable for session
SET spark.databricks.io.cache.enabled = true;
```

### Pre-warm Delta Cache (Proactive Caching)
```sql
-- Pre-load files into SSD cache before queries run
-- Useful before reports run in the morning
CACHE SELECT * FROM orders WHERE year = 2025;
-- This reads the data now and caches on SSD; subsequent queries hit SSD
```

---

## 4. Broadcast Variables (Small Lookup Tables)

For lookup tables / dimension tables that are used in JOIN or UDF lookups, broadcast them once to all executors.

```python
# Broadcast a small lookup table
lookup_df = spark.table("dim_region")  # 500 rows

# Method 1: Automatic broadcast (if table < autoBroadcastJoinThreshold)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50MB
# Spark automatically broadcasts tables smaller than this threshold

# Method 2: Explicit broadcast hint
from pyspark.sql import functions as F
result = large_df.join(
    F.broadcast(lookup_df),  # force broadcast
    on="region_id"
)

# Method 3: Broadcast a Python dict in UDF
lookup_dict = {row.id: row.name for row in lookup_df.collect()}
lookup_bc = spark.sparkContext.broadcast(lookup_dict)

@F.udf("string")
def lookup_name(id):
    return lookup_bc.value.get(id, "UNKNOWN")

df.withColumn("name", lookup_name("id"))
```

---

## 5. When to Cache — Decision Guide

```
Should I cache this DataFrame?
  
  Is it used more than once in the same job?
    NO  → Don't cache (cache has overhead)
    YES → Continue...
  
  Is it large (>1GB)?
    YES and memory-heavy → Use MEMORY_AND_DISK or Delta Cache
    NO → MEMORY_ONLY is fine
  
  Is it a dimension/lookup table used in JOINs?
    YES and small (<50MB) → Broadcast instead of cache
    YES and large (>50MB) → Cache with MEMORY_AND_DISK
  
  Is it the same data read by multiple users/queries?
    YES → Use Delta Cache (shared across sessions)
    NO  → Use Spark DataFrame cache
```

---

## 6. Cache Invalidation

```python
# Spark cache is stale if the underlying table is updated
df = spark.table("orders")
df.cache()
df.count()  # caches 100 rows

# Someone inserts new rows into 'orders' table
spark.sql("INSERT INTO orders VALUES (...)")

# df still shows 100 rows — STALE CACHE
df.count()  # returns 100 (wrong!)

# Fix: unpersist and re-cache
df.unpersist()
df = spark.table("orders")  # re-read fresh
df.cache()
df.count()  # now correct
```

For Delta Tables, use `refreshTable` to invalidate cached metadata:
```python
spark.catalog.refreshTable("orders")
```

---

## 7. Memory Management for Caching

```python
# Spark memory is split between:
# - Execution memory (for tasks: shuffles, joins, sorts)
# - Storage memory (for caching)
# Unified Memory Manager allows borrowing between them

# Default memory fractions
spark.conf.set("spark.memory.fraction", "0.6")          # 60% of executor heap for Spark
spark.conf.set("spark.memory.storageFraction", "0.5")   # 50% of that for cache

# For cache-heavy workloads: increase storage fraction
spark.conf.set("spark.memory.storageFraction", "0.7")
```

---

## 8. Pros and Cons

### Spark DataFrame Cache
| Pros | Cons |
|---|---|
| Eliminates repeated storage reads | Uses executor heap memory |
| Great for iterative algorithms | Stale after table updates |
| Supports custom storage levels | Per-DataFrame, not shared |
| Works with any data source | Not useful for one-time reads |

### Delta Cache (SSD)
| Pros | Cons |
|---|---|
| Shared across all queries and users | Requires SSD-enabled instance type |
| Survives query completion | Databricks-specific (not open source) |
| Doesn't use executor heap | SSD cache can be evicted under pressure |
| Transparent — no code changes | Not useful if data changes frequently |
| 10-100x faster than cloud I/O | |

---

## 9. Best Practices

1. **Cache DataFrames used multiple times** within the same job
2. **Use Delta Cache** for tables queried repeatedly across jobs and users
3. **Broadcast** lookup/dimension tables < 50MB instead of caching
4. **Unpersist explicitly** — don't rely on garbage collection
5. **Use MEMORY_AND_DISK** for large DataFrames to prevent OOM
6. **Pre-warm Delta Cache** before scheduled report runs
7. **Don't cache streaming DataFrames** — they're unbounded
8. **Cache early in the DAG** — cache the filtered/joined result, not the raw table

```python
# Anti-pattern: cache too early, wasting memory
full_table = spark.table("events")  # 10TB
full_table.cache()  # WRONG — caches 10TB
result = full_table.filter("year = 2025").groupBy("user_id").count()

# Correct: cache only what you need multiple times
filtered = spark.table("events").filter("year = 2025")  # 50GB
filtered.cache()
count_result = filtered.groupBy("user_id").count()
agg_result   = filtered.groupBy("region").agg(F.sum("amount"))
filtered.unpersist()
```
