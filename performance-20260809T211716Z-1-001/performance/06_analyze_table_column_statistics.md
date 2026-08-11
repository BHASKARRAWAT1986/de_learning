# ANALYZE — Table & Column Statistics — Complete Guide

---

## 1. What Is ANALYZE?

`ANALYZE TABLE` collects **table-level and column-level statistics** and stores them in the Hive Metastore (or Unity Catalog).  
These statistics are used by Spark's **Catalyst query optimizer** to make better query plan decisions — choosing join strategies, estimating cardinality, and detecting skew.

> Without ANALYZE: Spark makes rough estimates based on file sizes alone.  
> With ANALYZE: Spark uses actual row counts, cardinality, and value distributions.

---

## 2. Types of Statistics Collected

### Table-Level Statistics
- **Row count** (`rowCount`)
- **Total size in bytes** (`sizeInBytes`)

### Column-Level Statistics
- **Distinct count** (approximate)
- **Min / Max values**
- **Null count**
- **Average length** (for strings)
- **Max length** (for strings)
- **Histogram** (value frequency distribution — for skew detection)

---

## 3. Syntax

### Collect Table-Level Stats Only
```sql
ANALYZE TABLE orders COMPUTE STATISTICS;
-- Collects: rowCount, sizeInBytes
```

### Collect Column-Level Stats
```sql
-- Specific columns
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_date, amount;

-- All columns (expensive on wide tables)
ANALYZE TABLE orders COMPUTE STATISTICS FOR ALL COLUMNS;
```

### Collect With Histograms (Best for Skew Detection)
```sql
-- Histograms give Spark distribution information, not just min/max
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, amount
WITH HISTOGRAMS;
```

### For Partitioned Tables — Per-Partition Stats
```sql
-- Analyze all partitions
ANALYZE TABLE orders PARTITION (year, month) COMPUTE STATISTICS;

-- Analyze specific partition
ANALYZE TABLE orders PARTITION (year=2025, month=3) COMPUTE STATISTICS;

-- With columns for a specific partition
ANALYZE TABLE orders PARTITION (year=2025) 
COMPUTE STATISTICS FOR COLUMNS customer_id, amount;
```

---

## 4. How Statistics Improve Query Planning

### Example 1: Better Join Strategy Selection

```sql
-- Without stats: Spark estimates both tables are large → Sort-Merge Join
-- With stats: Spark knows dim_region has 500 rows → Broadcast Hash Join

ANALYZE TABLE dim_region COMPUTE STATISTICS;
-- rowCount = 500 → well within broadcast threshold → BHJ chosen automatically

SELECT f.revenue, d.region_name
FROM fact_sales f
JOIN dim_region d ON f.region_id = d.region_id;
-- Now automatically uses BroadcastHashJoin (was SortMergeJoin before)
```

### Example 2: Better Cardinality Estimates for GROUP BY
```sql
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id;
-- distinctCount(customer_id) = 50,000

-- Spark now knows the GROUP BY will produce ~50,000 groups
-- → chooses appropriate partition count, memory allocation
SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;
```

### Example 3: Null Count for Join Optimizations
```sql
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS product_id;
-- nullCount(product_id) = 200,000 (20% nulls)

-- Spark filters NULLs earlier in the plan (NULLs can never match a JOIN)
-- Reduces data shuffled for the JOIN
```

---

## 5. View Collected Statistics

```sql
-- View table-level stats
DESCRIBE EXTENDED orders;
-- Look for: Statistics section at the bottom

-- View column-level stats
DESCRIBE EXTENDED orders customer_id;
-- Shows: distinctCount, min, max, nullCount, avgLen, maxLen

-- SQL query against metastore (advanced)
SELECT * FROM default.orders TABLESAMPLE (100 PERCENT)
-- Not ideal; use DESCRIBE EXTENDED instead
```

```python
# Programmatic access
spark.sql("DESCRIBE EXTENDED orders").show(truncate=False)

# Check stats in the catalog
catalog = spark.catalog
print(catalog.listTables())

# For Delta tables: stats are in Delta log AND metastore
detail = spark.sql("DESCRIBE DETAIL orders").first()
print(detail['numFiles'], detail['sizeInBytes'])
```

---

## 6. File-Level Stats vs Table-Level Stats vs Column-Level Stats

| Level | What | Where Stored | Used For |
|---|---|---|---|
| **File-level stats** | min/max/nullCount per column per file | Delta log JSON | Data skipping (skip entire files) |
| **Table-level stats** | row count, byte size | Hive Metastore | Join strategy, memory estimation |
| **Column-level stats** | distinct count, histogram | Hive Metastore | Cardinality estimates, join ordering |

### Key Difference: Delta vs Non-Delta
- **Delta tables**: File-level stats are automatic (written on every OPTIMIZE/write). Table-level stats via ANALYZE are optional but improve Catalyst planning.
- **Non-Delta tables** (Parquet, ORC): No automatic file stats. ANALYZE is the ONLY way to give Spark accurate statistics.

---

## 7. When to Run ANALYZE

```
Run ANALYZE when:
  ✓ After bulk loading a table for the first time
  ✓ After a major data change (>20% rows added/deleted)
  ✓ Before joining large tables without partition pruning
  ✓ When you see Sort-Merge Join where BroadcastHashJoin is expected
  ✓ For non-Delta tables (Parquet, CSV, ORC)
  ✓ When AQE is disabled and you rely on static planning

DO NOT run ANALYZE for:
  ✗ Every micro-batch in streaming (too expensive)
  ✗ Tables smaller than 1GB (Spark estimates are fine)
  ✗ Delta tables with frequent writes (stats go stale quickly)
  ✗ Very wide tables with FOR ALL COLUMNS (prohibitively expensive)
```

---

## 8. Automating ANALYZE in Pipelines

```python
# Post-load analysis as part of ETL
def load_and_analyze(df, table_name, analyze_cols):
    # Write data
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    
    # Collect statistics
    cols = ", ".join(analyze_cols)
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR COLUMNS {cols}")
    
    print(f"Stats collected for {table_name}")

# Usage
load_and_analyze(
    df=orders_df,
    table_name="orders",
    analyze_cols=["customer_id", "order_date", "region", "amount"]
)
```

---

## 9. Statistics Staleness

Statistics become stale after data changes. Spark does NOT automatically refresh them.

```
Table loaded: 10M rows → ANALYZE → rowCount=10M
Next day: 2M rows appended → rowCount still shows 10M (stale!)
Spark still plans based on 10M → could be suboptimal for joins
```

### Solutions:
```sql
-- Rerun ANALYZE periodically (nightly for critical tables)
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_date;

-- Or use AQE to compensate for stale stats at runtime
SET spark.sql.adaptive.enabled = true;
-- AQE uses actual runtime stats regardless of metastore stats staleness
```

---

## 10. Pros and Cons

| Pros | Cons |
|---|---|
| Significantly improves join strategy selection | Stats go stale after writes |
| Enables accurate cardinality estimation | Expensive to run on large tables |
| One-time cost with long-lasting benefit | Must be re-run after significant data changes |
| Critical for non-Delta formats | FOR ALL COLUMNS is very slow on wide tables |
| Enables better skew detection (histograms) | Not useful for streaming pipelines |

---

## 11. ANALYZE vs AQE — They Are Complementary

| Scenario | ANALYZE helps? | AQE helps? |
|---|---|---|
| First stage table scan planning | YES | NO (AQE can't re-plan before Stage 1) |
| Join strategy after heavy filter | NO (stats pre-filter) | YES (runtime stats post-filter) |
| Cardinality of final GROUP BY | YES (pre-execution) | YES (runtime) |
| Data skew in shuffle | Partially (histograms) | YES (always) |

**Best practice: Use both.**  
ANALYZE gives Catalyst a good starting plan. AQE corrects mistakes at runtime.

---

## 12. Quick Reference

```sql
-- Basic stats
ANALYZE TABLE my_table COMPUTE STATISTICS;

-- Column stats (most useful)
ANALYZE TABLE my_table COMPUTE STATISTICS 
FOR COLUMNS col1, col2, col3;

-- Full stats with histograms
ANALYZE TABLE my_table COMPUTE STATISTICS 
FOR ALL COLUMNS WITH HISTOGRAMS;

-- Per partition
ANALYZE TABLE my_table PARTITION (dt='2025-03-01') 
COMPUTE STATISTICS FOR COLUMNS id, amount;

-- View stats
DESCRIBE EXTENDED my_table;
DESCRIBE EXTENDED my_table col_name;
```
