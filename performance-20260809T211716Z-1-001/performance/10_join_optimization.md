# Join Optimization Strategies — Complete Guide

---

## 1. Join Types in Spark

Understanding which join algorithm Spark uses is critical for performance.

| Strategy | Abbrev | When Used | Cost |
|---|---|---|---|
| Broadcast Hash Join | BHJ | One side < broadcast threshold | LOW — no shuffle of large table |
| Sort-Merge Join | SMJ | Both sides large | HIGH — shuffle + sort both sides |
| Shuffle Hash Join | SHJ | One side medium-sized | MEDIUM — shuffle both, hash one |
| Broadcast Nested Loop | BNL | Non-equi joins | VERY HIGH — cartesian-like |
| Cartesian Product | CP | Cross joins | EXTREME — avoid |

---

## 2. Broadcast Hash Join (BHJ)

### How It Works
```
Small table (dim_region: 500 rows) is serialized and sent to ALL executors
Large table (fact_sales: 10B rows) is NOT shuffled — read locally on each executor

Each executor:
  1. Has full copy of dim_region in memory (broadcast variable)
  2. Reads its partition of fact_sales
  3. For each row, looks up matching row in in-memory hash table of dim_region
  4. Emits joined result

Result: ZERO shuffle of fact_sales → huge performance win
```

### Configure BHJ
```sql
-- Set broadcast threshold (default 10MB; increase for larger dims)
SET spark.sql.autoBroadcastJoinThreshold = 52428800;  -- 50MB

-- Disable auto-broadcast (for testing)
SET spark.sql.autoBroadcastJoinThreshold = -1;
```

```python
# Force broadcast with hint (overrides threshold)
from pyspark.sql import functions as F

result = fact_sales.join(
    F.broadcast(dim_region),
    on="region_id",
    how="inner"
)

# SQL hint
spark.sql("""
  SELECT /*+ BROADCAST(d) */ f.*, d.region_name
  FROM fact_sales f
  JOIN dim_region d ON f.region_id = d.region_id
""")
```

---

## 3. Sort-Merge Join (SMJ)

### How It Works
```
Both tables must be:
  1. Shuffled so matching keys go to the same executor partition
  2. Sorted by join key within each partition
  3. Merged (like merge-sort) to find matches

Cost:
  - 2 full shuffles (one per table)
  - 2 sorts
  - Merge pass
  
Unavoidable when both tables are large (>broadcast threshold)
```

### Optimize SMJ
```python
# Ensure enough shuffle partitions for large SMJ
spark.conf.set("spark.sql.shuffle.partitions", "400")
# Too few → each partition is huge → task OOM
# Too many → too many small tasks → scheduler overhead
# AQE coalesces automatically — set high and let AQE reduce

# Avoid SMJ by increasing broadcast threshold if possible
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB
```

---

## 4. Skew in Joins

### Problem
```
JOIN on customer_id:
  customer_id='BIG_CORP'  → 100M rows on one partition
  All other customers     → 1K rows average per partition

Task for 'BIG_CORP':  takes 3 hours
All other tasks:      take 30 seconds
Job duration:         3 hours (limited by the skewed task)
```

### Solution 1: AQE Skew Join (Automatic)
```sql
-- Enable AQE skew handling
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456;
```

### Solution 2: Salting (Manual)
```python
import random
from pyspark.sql import functions as F

# Add salt to the large skewed table
SALT_FACTOR = 10
skewed_df = large_table.withColumn(
    "salted_key",
    F.concat(F.col("customer_id"), F.lit("_"), (F.rand() * SALT_FACTOR).cast("int"))
)

# Explode the small table to match all salt values
small_df_exploded = small_table.withColumn(
    "salt",
    F.explode(F.array([F.lit(i) for i in range(SALT_FACTOR)]))
).withColumn(
    "salted_key",
    F.concat(F.col("customer_id"), F.lit("_"), F.col("salt"))
)

# Join on salted key
result = skewed_df.join(small_df_exploded, on="salted_key")
```

---

## 5. Bucket Join (Pre-Shuffle)

**Bucketing** pre-sorts and pre-partitions data on disk so that JOIN operations don't need to shuffle.

```python
# Write bucketed table
orders.write \
  .format("delta") \
  .bucketBy(50, "customer_id") \   # 50 buckets on customer_id
  .sortBy("customer_id") \
  .saveAsTable("orders_bucketed")

customers.write \
  .format("delta") \
  .bucketBy(50, "customer_id") \   # SAME number of buckets, SAME key
  .sortBy("customer_id") \
  .saveAsTable("customers_bucketed")

# JOIN requires NO SHUFFLE — both tables already co-partitioned
result = spark.table("orders_bucketed") \
    .join(spark.table("customers_bucketed"), on="customer_id")
```

**Limitation:** Bucketing is not natively supported in Delta Lake on Databricks in the same way as Hive managed tables. Use Liquid Clustering as an alternative.

---

## 6. Partition-Aware Joins

If both tables are partitioned on the same column, Spark can do a **partition-aware join** (co-located join):

```python
# Both tables partitioned by dt
orders   = spark.table("orders")    # partitioned by dt
returns  = spark.table("returns")   # partitioned by dt

# Spark reads matching partitions together — no full shuffle needed
result = orders.join(returns, on=["order_id", "dt"])
# Spark prunes: joins only dt=2025-03-01 from orders with dt=2025-03-01 from returns
```

---

## 7. Join Hints Summary

```sql
-- Broadcast (BHJ)
SELECT /*+ BROADCAST(small_table) */ ...
SELECT /*+ BROADCASTJOIN(small_table) */ ...

-- Sort-Merge Join
SELECT /*+ MERGE(t1, t2) */ ...
SELECT /*+ MERGEJOIN(t1, t2) */ ...

-- Shuffle Hash Join
SELECT /*+ SHUFFLE_HASH(t1, t2) */ ...

-- Shuffle Replicate Nested Loop (for cross joins)
SELECT /*+ SHUFFLE_REPLICATE_NL(t1, t2) */ ...
```

---

## 8. Range Joins

Spark 10.4+ and Databricks support **range joins** efficiently:

```sql
-- Equi join: fast
JOIN ON t1.id = t2.id

-- Range join: slower (no hash table possible)
JOIN ON t1.start_date <= t2.event_date AND t2.event_date <= t1.end_date

-- Databricks range join hint: bin the range to enable a bucket-based range join
SELECT /*+ RANGE_JOIN(t1, 86400) */ *   -- 86400 seconds = 1 day bin size
FROM events t2
JOIN time_windows t1 
ON t2.ts BETWEEN t1.start_ts AND t1.end_ts;
```

---

## 9. Best Practices

1. **Broadcast dimension tables** — any table < 50MB should be broadcast
2. **Enable AQE** for automatic skew handling and join strategy switching
3. **Run ANALYZE** on join columns to help Catalyst choose the right strategy
4. **Avoid joining without keys** (cross/cartesian joins) unless intentional
5. **Filter before join** — reduce both sides as much as possible before the join
6. **Co-locate join keys** — use partitioning or Liquid Clustering on join keys for large-large joins
7. **Salt skewed joins** when AQE cannot handle the level of skew
8. **Monitor Spark UI** — look for disproportionate task times = skew

```python
# Anti-pattern: filter AFTER join (wastes join resources)
result = fact.join(dim, on="id").filter("region = 'US'")

# Correct: filter BEFORE join
result = fact.filter("region = 'US'").join(dim, on="id")
```
