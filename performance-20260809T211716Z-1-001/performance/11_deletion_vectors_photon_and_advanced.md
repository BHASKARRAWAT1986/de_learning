# Deletion Vectors, Photon Engine & Other Advanced Optimizations

---

# PART 1: DELETION VECTORS

## What Are Deletion Vectors?

**Deletion Vectors (DVs)** are a Delta Lake feature (DBR 12.0+) that represents deleted or updated rows as a **bitmap file** instead of rewriting the entire Parquet file.

### Before Deletion Vectors (Copy-on-Write)
```
Table has file_001.parquet with 1M rows
DELETE FROM orders WHERE order_id = 42

Without DVs:
  1. Read all 1M rows from file_001.parquet
  2. Remove row where order_id=42
  3. Write new file_002.parquet with 999,999 rows
  4. Mark file_001.parquet as deleted in Delta log
  
Cost: Read 1M rows + Write 1M rows for deleting ONE row → huge write amplification
```

### With Deletion Vectors
```
DELETE FROM orders WHERE order_id = 42

With DVs:
  1. Find the row position of order_id=42 in file_001.parquet
  2. Write a tiny DV bitmap file: "row position 12345 is deleted"
  3. file_001.parquet remains unchanged
  
Cost: Write ~100 bytes of bitmap data for deleting ONE row → near-zero write amplification
```

### How DVs Work at Read Time
```
Query: SELECT * FROM orders

Spark reads file_001.parquet AND its associated DV bitmap
For each row: "Is this row position flagged in the DV?" 
  YES → skip this row
  NO  → include this row

Result: Reader sees 999,999 rows; row 42 is invisible
Physical: file_001.parquet still has 1M rows on disk
```

### Enable Deletion Vectors
```sql
-- Enable on a table
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true'
);

-- Or create with DVs enabled
CREATE TABLE orders (...) 
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
```

### DV Compaction
DVs accumulate over time. `OPTIMIZE` compacts them:
```sql
-- OPTIMIZE applies DVs (removes flagged rows) and rewrites clean files
OPTIMIZE orders;
-- After OPTIMIZE: deleted rows are physically gone, DV files are removed
```

### DVs + Liquid Clustering
The perfect combo:
- **DVs**: near-zero cost for deletes/updates (no file rewrite)
- **Liquid Clustering**: incremental file layout optimization
- **OPTIMIZE**: periodically applies DVs and reclusters, all in one pass

---

# PART 2: PHOTON ENGINE

## What Is Photon?

**Photon** is Databricks' native vectorized query engine written in **C++**, replacing the JVM-based Spark execution engine for certain operations.

### JVM Spark vs Photon
| Aspect | JVM Spark | Photon |
|---|---|---|
| Language | Java/Scala JVM bytecode | C++ (native code) |
| Vectorization | Row-at-a-time (traditional) or vectorized JVM | SIMD vectorized operations |
| Memory | JVM heap + GC pauses | Off-heap native memory |
| CPU efficiency | Good | Excellent (near hardware speed) |
| Speedup typical | Baseline | 2-10x faster for scan-heavy queries |

### What Photon Accelerates
- Table scans (reading Parquet/Delta files)
- Aggregations (GROUP BY, window functions)
- Hash joins
- Sort operations
- String operations
- Complex expressions and UDFs (limited)

### What Photon Does NOT Accelerate
- Python/Pandas UDFs (these still go to JVM)
- Structured Streaming complex stateful operations
- Some unsupported SQL functions (falls back to Spark)

### Enable Photon
```sql
-- Photon is enabled by default on Databricks Runtime 9.1+
-- Check if cluster has Photon enabled: cluster config → "Photon Acceleration" toggle

-- Force disable (for debugging/comparison)
SET spark.databricks.photon.enabled = false;

-- Re-enable
SET spark.databricks.photon.enabled = true;
```

### Verify Photon Is Being Used
```sql
EXPLAIN SELECT SUM(amount) FROM orders GROUP BY region;
-- Look for: PhotonGroupingAgg, PhotonScan, PhotonHashJoin in the plan
-- If you see: HashAggregateExec → Photon fallback to JVM for that node
```

---

# PART 3: PREDICTIVE OPTIMIZATION

## What Is Predictive Optimization?

**Predictive Optimization** (Databricks-only) automatically determines when and which tables need `OPTIMIZE`, `VACUUM`, and `ANALYZE` and runs them without manual scheduling.

### Enable
```sql
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.predictiveOptimization' = 'enable'
);

-- Or enable for entire catalog (Unity Catalog)
ALTER CATALOG my_catalog SET DBPROPERTIES (
  'delta.predictiveOptimization' = 'enable'
);
```

### What It Does Automatically
1. Monitors table write patterns and clustering scores
2. Triggers `OPTIMIZE` when clustering score drops below threshold
3. Triggers `VACUUM` based on retention and storage cost signals
4. Runs `ANALYZE` after significant data changes
5. All runs are tracked in system tables for auditability

---

# PART 4: SKEW HANDLING TECHNIQUES

## 1. Key Salting (for uneven JOIN distributions)
(See Join Optimization guide for details)

## 2. Repartition & Coalesce

```python
# Repartition: full shuffle to exact N partitions
df = df.repartition(200)                    # shuffle to 200 partitions
df = df.repartition(200, "customer_id")     # shuffle + distribute by key
df = df.repartition("region", "dt")        # distribute by these columns

# Coalesce: reduce partition count WITHOUT shuffle (merge local partitions)
df = df.coalesce(50)   # reduce 200 → 50 partitions, no shuffle
# Use coalesce to reduce partitions, repartition to increase or rebalance
```

## 3. Skew Hints
```sql
SELECT /*+ SKEW('orders', 'customer_id', ('BIG_CORP', 'MEGA_CORP')) */ *
FROM orders o JOIN customers c ON o.customer_id = c.customer_id;
-- Tells Spark to split these specific skewed values
```

---

# PART 5: WRITE OPTIMIZATIONS

## 1. Minimize Small Files at Write Time

```python
# Option 1: Repartition before write
df.repartition(100).write.format("delta").save("/path")

# Option 2: Use Optimize Write (see guide 01)
df.write.option("optimizeWrite", "true").format("delta").save("/path")

# Option 3: Control file size via targetFileSize
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.targetFileSize' = '536870912'  -- 512MB
);
```

## 2. Idempotent Writes (Exactly-Once Semantics)

```python
# Use transaction IDs for idempotent writes (deduplication)
df.write \
  .format("delta") \
  .option("txnAppId", "my_pipeline_job_id") \
  .option("txnVersion", batch_id) \
  .mode("append") \
  .save("/path/to/table")
# If this write is retried with the same txnVersion → Delta deduplicates automatically
```

---

# PART 6: FULL OPTIMIZE COMMAND

## OPTIMIZE — The Swiss Army Knife

```sql
-- Standard OPTIMIZE: compact small files to ~1GB
OPTIMIZE my_table;

-- OPTIMIZE with WHERE: only compact a date partition
OPTIMIZE my_table WHERE dt = '2025-03-01';

-- OPTIMIZE with ZORDER (legacy)
OPTIMIZE my_table ZORDER BY (customer_id, dt);

-- OPTIMIZE for Liquid Clustered tables (incremental only)
OPTIMIZE my_table;  -- automatically detects liquid clustering; no ZORDER needed
```

### What OPTIMIZE Does
1. Reads all small files (below target size) in scope
2. Merges their data
3. Applies Z-order or Hilbert curve sort (if configured)
4. Writes larger output files (~1GB default)
5. Marks old small files as removed in Delta log
6. Updates file-level statistics in Delta log

---

# PART 7: COMPLETE OPTIMIZATION DECISION MATRIX

| Scenario | Best Technique |
|---|---|
| Small files from streaming writes | Auto Compact + Optimize Write |
| Multi-column query filtering (new table) | Liquid Clustering |
| Multi-column query filtering (legacy table) | ZORDER BY |
| Low-cardinality filter columns | Partitioning |
| High-cardinality ID lookups | Bloom Filter Index |
| Query uses non-first-32 columns for filter | Increase `dataSkippingNumIndexedCols` |
| Frequent deletes/updates on large table | Deletion Vectors |
| Large JOIN with small dimension | Broadcast Hash Join |
| Large JOIN both tables | AQE + Sort-Merge Join |
| Skewed JOIN | AQE Skew Join or Salting |
| Slow aggregation on large scan | Photon + Partitioning/Clustering |
| Repeated queries on same dataset | Delta Cache (SSD) |
| Repeated computation within a job | DataFrame cache |
| Bad join plans / wrong row estimates | ANALYZE + AQE |
| Storage cost growing uncontrollably | VACUUM |
| Want fully hands-off maintenance | Predictive Optimization |
