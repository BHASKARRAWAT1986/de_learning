# Adaptive Query Execution (AQE) — Complete Guide

---

## 1. What Is AQE?

**Adaptive Query Execution (AQE)** is a runtime query optimization framework in Spark 3.0+ (enabled by default in Databricks Runtime) that re-optimizes query plans **after each shuffle stage completes**, using actual runtime statistics instead of estimated statistics.

Traditional Spark query planning is **static** — the plan is fixed before execution begins, based on potentially inaccurate row count estimates. AQE makes query execution **dynamic**.

```
Static Planning:                     AQE:
  Estimate stats at parse time         Estimate stats at parse time
  Build query plan                     Build initial plan
  Execute (plan may be wrong)          Execute Stage 1
                                       Read ACTUAL stats from Stage 1 output
                                       Re-optimize remaining plan
                                       Execute Stage 2
                                       Re-optimize again...
                                       Continue until done
```

---

## 2. Enable / Disable AQE

```sql
-- Check current setting
SET spark.sql.adaptive.enabled;

-- Enable (default: true in DBR 8.0+)
SET spark.sql.adaptive.enabled = true;

-- Disable (for debugging only)
SET spark.sql.adaptive.enabled = false;
```

```python
# Enable in SparkSession config
spark = SparkSession.builder \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

---

## 3. AQE Feature 1: Dynamic Partition Coalescing

### The Problem
When a shuffle produces many small partitions (e.g., after filtering heavily), Spark launches one task per partition — thousands of tiny tasks waste scheduler overhead.

```
After shuffle: 2000 output partitions
  Partition 1:  200 bytes   (tiny)
  Partition 2:  500 bytes   (tiny)
  ...
  Partition 1999: 2MB
  Partition 2000: 1.5MB
  
Without AQE: 2000 tasks launched (most do microseconds of work)
```

### AQE Solution: Merge Small Partitions
```
After shuffle stage completes, AQE reads actual partition sizes:
  Partitions 1-50: each <100KB → merge into 1 task
  Partitions 51-80: each <100KB → merge into 1 task
  ...
  Result: 2000 partitions → 40 coalesced tasks
  
With AQE: 40 tasks (50x reduction in task overhead)
```

### Configuration
```sql
-- Target size per coalesced partition (default 64MB)
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 67108864;  -- 64MB

-- Minimum number of partitions to keep (prevents over-coalescing)
SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;

-- Initial shuffle partitions (before AQE coalescing)
SET spark.sql.shuffle.partitions = 200;  -- AQE will reduce this dynamically
```

---

## 4. AQE Feature 2: Dynamic Join Strategy Switching

### The Problem
Spark must choose a join strategy **before** execution. If it guesses wrong, performance suffers.

| Join Strategy | When Used | Cost |
|---|---|---|
| Sort-Merge Join (SMJ) | Both tables large | High — shuffle both sides |
| Broadcast Hash Join (BHJ) | One side small | Low — broadcast small table |
| Shuffle Hash Join | One side medium | Medium |

The classic mistake: Spark estimates Table B has 10M rows but at runtime (after filtering) it has only 5000 rows → should broadcast but plan uses SMJ.

### AQE Solution: Switch to Broadcast at Runtime
```
Query: SELECT * FROM large_table l JOIN filtered_table f ON l.id = f.id
WHERE f.status = 'ACTIVE'  ← heavy filter

Catalyst estimate (wrong): filtered_table has 10M rows → plan Sort-Merge Join

AQE at runtime: 
  After filter stage: filtered_table actually has 8000 rows → fits in broadcast limit
  AQE switches plan: Sort-Merge Join → Broadcast Hash Join
  
Result: No shuffle of filtered_table → 10x speedup on this join
```

### Configuration
```sql
-- Broadcast threshold (default 10MB)
SET spark.sql.autoBroadcastJoinThreshold = 10485760;  -- 10MB

-- Allow AQE to change join strategy (default true)
SET spark.sql.adaptive.localShuffleReader.enabled = true;
```

---

## 5. AQE Feature 3: Dynamic Skew Join Handling

### The Problem
Data skew: one JOIN key value has millions of rows while others have a few hundred.

```
JOIN on customer_id:
  customer_id = 'AMAZON'  → 50,000,000 rows  ← HOT PARTITION (task takes 10 hours)
  customer_id = 'GOOGLE'  → 200,000 rows
  customer_id = 'SMALL_CO' → 500 rows
  
Without AQE: One task handles 50M rows while others finish in seconds
→ Job stalls waiting for the skewed task
```

### AQE Solution: Split Skewed Partitions
```
AQE detects: partition for 'AMAZON' is 100x larger than median
AQE splits 'AMAZON' partition into 5 sub-partitions
Reads the matching side of the join multiple times (5x) for 'AMAZON'

Result: 
  'AMAZON' handled by 5 tasks (10M rows each)
  Other partitions: 1 task each
  Total time: ~10M rows per task max → balanced parallelism
```

### Configuration
```sql
-- Enable skew join handling (default true when AQE is on)
SET spark.sql.adaptive.skewJoin.enabled = true;

-- A partition is skewed if its size exceeds this factor × median partition size
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;

-- And its size exceeds this absolute threshold
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456; -- 256MB
```

---

## 6. AQE Feature 4: Dynamic Partition Pruning (DPP)

**Note:** DPP is related to AQE but is a separate feature.

```sql
-- Star schema query: fact + dimension
SELECT f.revenue, d.region_name
FROM fact_sales f
JOIN dim_region d ON f.region_id = d.region_id
WHERE d.country = 'USA';

-- Without DPP: Scan entire fact_sales table
-- With DPP: 
--   1. Run dim_region filter → get list of region_ids where country='USA'
--   2. Use that list to prune fact_sales partitions/files at scan time
--   3. Only read fact_sales rows for matching region_ids
```

```sql
-- Enable DPP (default true in DBR)
SET spark.sql.optimizer.dynamicPartitionPruning.enabled = true;
```

---

## 7. AQE vs Static Optimizer — Full Comparison

| Aspect | Static (no AQE) | AQE |
|---|---|---|
| When plan is built | Before execution | Rebuilt after each shuffle stage |
| Row count estimates | Catalog statistics (often wrong) | Actual runtime stats |
| Shuffle partitions | Fixed (spark.sql.shuffle.partitions) | Dynamically coalesced |
| Join strategy | Fixed at plan time | Can switch broadcast ↔ SMJ at runtime |
| Skew handling | None (job stalls) | Automatic partition splitting |
| Plan quality | Depends on statistics freshness | Always accurate (uses real data) |

---

## 8. AQE Internals — Materialized Query Stages

AQE works by introducing **materialized query stages** at shuffle boundaries.

```
Query Plan:
  Stage 1: TableScan + Filter
  ===== SHUFFLE BARRIER =====   ← AQE re-plans here using Stage 1's output stats
  Stage 2: Join + Aggregate
  ===== SHUFFLE BARRIER =====   ← AQE re-plans again
  Stage 3: Final output

At each barrier, Spark:
  1. Completes the current stage and writes shuffle files
  2. Reads actual partition sizes / row counts from shuffle metadata
  3. Re-optimizes the remaining plan using real stats
  4. Continues execution with the improved plan
```

---

## 9. Monitoring AQE in Action

```python
# Enable SQL UI extended display
spark.conf.set("spark.sql.ui.explainMode", "extended")

# In Spark UI: Job Details → SQL tab → View query plan
# AQE changes show as "AdaptiveSparkPlan" nodes
# Hover over nodes to see: "isFinalPlan: true/false"

# Python: check if AQE was used
df = spark.sql("SELECT * FROM fact_sales f JOIN dim_region d ON f.region_id = d.region_id")
print(df._jdf.queryExecution().toString())
# Look for: AdaptiveSparkPlan, BroadcastHashJoin (switched from SortMergeJoin)
```

---

## 10. Common AQE Issues and Fixes

### Issue 1: AQE Not Coalescing Partitions
```sql
-- Problem: partitions not being merged
-- Fix: lower the advisory size
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 33554432; -- 32MB

-- Also ensure coalescing is enabled
SET spark.sql.adaptive.coalescePartitions.enabled = true;
```

### Issue 2: Skew Not Being Handled
```sql
-- Lower the factor threshold
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 3;
-- Lower the absolute threshold
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 67108864; -- 64MB
```

### Issue 3: Join Not Switching to Broadcast
```sql
-- Increase broadcast threshold
SET spark.sql.autoBroadcastJoinThreshold = 52428800; -- 50MB
```

---

## 11. Pros and Cons

| Pros | Cons |
|---|---|
| Works on real runtime data — always accurate | Cannot help with Stage 1 (the first scan) |
| Handles skew automatically | Slight overhead from re-planning between stages |
| No manual tuning required for most cases | May occasionally choose suboptimal intermediate plans |
| Transparent — no query changes needed | Complex debugging (plan changes at runtime) |
| Default ON in Databricks Runtime | Cannot fix data quality issues (nulls, duplicates) |

---

## 12. AQE Best Practices

1. **Leave AQE enabled** — it almost always helps, rarely hurts
2. **Set initial shuffle partitions high** (200-2000) and let AQE coalesce down
3. **Set skew thresholds** to match your data distribution (lower factor for heavily skewed data)
4. **Use ANALYZE TABLE** to give Catalyst better starting estimates (AQE improves from there)
5. **Monitor via Spark UI** — look for AdaptiveSparkPlan nodes and broadcast switches
6. **Combine with DPP** for star schema / dimension-fact join patterns

```sql
-- Production-recommended AQE config
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 67108864;   -- 64MB
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 268435456; -- 256MB
SET spark.sql.optimizer.dynamicPartitionPruning.enabled = true;
```
