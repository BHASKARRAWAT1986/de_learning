# Data Skipping, File Pruning & Predicate Pushdown — Complete Guide

---

## 1. What Is Data Skipping?

**Data skipping** is Delta Lake's ability to skip entire Parquet files during a query without opening them, based on **file-level statistics** stored in the Delta transaction log.

When you write data to a Delta table, Delta automatically collects:
- `min` value for each column
- `max` value for each column
- `null_count` for each column
- `num_records` per file

These are stored in the `_delta_log/*.json` checkpoint files — NOT inside the Parquet files.

### How It Works
```
Query: SELECT * FROM orders WHERE order_date = '2025-03-15'

Delta Log has file stats:
  file_001.parquet → min_order_date=2025-01-01, max_order_date=2025-02-28  ← SKIP
  file_002.parquet → min_order_date=2025-03-01, max_order_date=2025-03-31  ← READ
  file_003.parquet → min_order_date=2025-04-01, max_order_date=2025-04-30  ← SKIP

Result: Only 1 of 3 files opened → 66% I/O reduction
```

### Statistics Collected Per File
```sql
-- View file-level stats stored in Delta log
SELECT 
  path,
  stats:numRecords,
  stats:minValues.order_date,
  stats:maxValues.order_date,
  stats:nullCount.order_date
FROM delta.`/path/to/orders`._delta_log
```

---

## 2. File-Level Statistics

Delta collects stats for the **first 32 columns** by default (configurable).

### Configure Number of Stats Columns
```sql
-- Increase stats columns (e.g., for wide tables where filter columns are beyond col 32)
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.dataSkippingNumIndexedCols' = '64'
);
```

### Recompute Stats on Existing Table
```python
# After changing stats columns or for older tables without stats
spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR ALL COLUMNS")

# Or via Delta OPTIMIZE which recomputes stats on rewritten files
spark.sql("OPTIMIZE orders")
```

### Check Stats Effectiveness
```sql
-- Run with stats info
EXPLAIN COST SELECT * FROM orders WHERE order_date = '2025-03-15';
-- Look for: "numFiles=1" vs "numFiles=100" — shows skipping in action
```

---

## 3. What Is File Pruning?

File pruning is the broader term for eliminating files from the scan plan.  
Delta Lake does pruning at two levels:

### Level 1: Partition Pruning (Folder-level)
For partitioned tables, eliminates entire **directories** before reading any file.

```sql
-- Table partitioned by year/month
CREATE TABLE events PARTITIONED BY (year INT, month INT);

-- Query filters on partition column → entire folders skipped
SELECT * FROM events WHERE year = 2025 AND month = 3;
-- Only reads: /events/year=2025/month=3/
-- Skips: all other year/month folders
```

### Level 2: File Pruning (Delta Statistics)
For non-partitioned or Liquid Clustered tables, uses min/max stats.

```sql
-- Even without partitions, data skipping prunes files
SELECT * FROM events_liquid_clustered WHERE user_id = 'U12345';
-- Delta reads file stats → skips files where min_user_id > 'U12345' or max_user_id < 'U12345'
```

### Level 3: Row-Group Pruning (Parquet)
Inside each file, Parquet stores **row group statistics** (min/max per column per row group, typically 128MB chunks).  
Spark/Databricks applies predicate pushdown to skip row groups within a file.

```
File: file_002.parquet (256MB)
  Row Group 1: user_id min=A, max=M  → includes 'U12345'? No → SKIP row group
  Row Group 2: user_id min=N, max=Z  → includes 'U12345'? No → SKIP row group
  Row Group 3: user_id min=T, max=W  → includes 'U12345'? No → SKIP row group
```

---

## 4. Predicate Pushdown

**Predicate pushdown** means filter conditions are pushed down as close to the data source as possible — down to the file reader, not evaluated in Spark memory.

### Three Levels of Pushdown

```
SQL Query: WHERE region = 'US' AND amount > 1000

Level 1 — Delta (file skip):    Skip files where min_region > 'US' or max_region < 'US'
                                  ↓ (files not matching eliminated)
Level 2 — Parquet row group:    Skip row groups inside surviving files
                                  ↓ (row groups not matching eliminated)
Level 3 — Parquet page level:   Skip pages within row groups (dictionary encoding)
                                  ↓ (pages not matching eliminated)
Final scan: Only matching rows loaded into Spark memory
```

### Verify Pushdown Is Working
```python
# Check the physical plan
df = spark.table("orders").filter("region = 'US' AND amount > 1000")
df.explain(True)

# Look for in the physical plan:
# PushedFilters: [IsNotNull(region), EqualTo(region,US), GreaterThan(amount,1000.0)]
# This confirms filters are pushed to the file reader
```

---

## 5. What Breaks Data Skipping?

### Anti-Patterns That Disable Skipping
```python
# BAD: Function on filter column → stats can't be used
df.filter(F.year("order_date") == 2025)              # skipping disabled on order_date
df.filter(F.lower("region") == "us")                 # skipping disabled on region
df.filter(F.col("amount") * 0.9 > 100)               # skipping disabled on amount

# GOOD: Direct column comparison → stats used
df.filter(F.col("order_date").between("2025-01-01", "2025-12-31"))
df.filter(F.col("region") == "US")
df.filter(F.col("amount") > 111)
```

### Why Functions Break Skipping
Delta's stats store the **raw column value** min/max.  
If you apply a function (`YEAR()`, `LOWER()`) before comparing, Delta cannot know what the transformed value would be — so it must read everything.

---

## 6. Bloom Filter Indexes

For high-cardinality columns with equality predicates (e.g., `WHERE transaction_id = 'X'`), min/max stats are useless because min='A' and max='Z' tells you nothing.

**Bloom Filters** are a probabilistic data structure that can answer "Does this file DEFINITELY NOT contain value X?" very efficiently.

### Create Bloom Filter
```sql
-- Add bloom filter on high-cardinality column
CREATE BLOOMFILTER INDEX ON TABLE orders
FOR COLUMNS(transaction_id OPTIONS (fpp=0.1, numItems=10000000));

-- fpp = false positive probability (0.1 = 10% false positives allowed)
-- numItems = expected distinct values in the indexed column
```

### How Bloom Filter Works
```
Query: WHERE transaction_id = 'TXN_XYZ_999'

For each file:
  1. Check bloom filter: "Is TXN_XYZ_999 possibly in this file?"
  2. If bloom filter says NO → skip file with 100% certainty
  3. If bloom filter says YES → may or may not be there (false positive possible)
  4. Only read files where bloom filter says YES
```

### When to Use Bloom Filters
| Use Case | Good? |
|---|---|
| High cardinality equality predicates (`WHERE id = ?`) | YES |
| Low cardinality columns (`WHERE status = 'ACTIVE'`) | No (min/max is sufficient) |
| Range predicates (`WHERE date > ?`) | No (min/max is better) |
| LIKE predicates | No |

---

## 7. Pros and Cons

### Data Skipping
| Pros | Cons |
|---|---|
| Automatic — no code changes needed | Only works on first 32 columns (default) |
| Works on any Delta table | Stats collected only at write/OPTIMIZE time |
| Orders of magnitude I/O reduction | Effectiveness depends on data clustering |
| Works with partitioning AND liquid clustering | Function-wrapped predicates bypass it |

### Bloom Filters
| Pros | Cons |
|---|---|
| Excellent for UUID/ID equality lookups | Adds storage overhead per file |
| Very fast (bit array lookup) | False positives possible |
| Complements min/max stats | Need to set fpp and numItems correctly |

---

## 8. Summary — Hierarchy of Pruning

```
Query executes
   │
   ├─ 1. Partition pruning    → eliminates folders (physical partitions)
   │
   ├─ 2. Delta file skipping  → eliminates files (min/max + bloom filter)
   │
   ├─ 3. Parquet row groups   → eliminates row groups within files
   │
   └─ 4. Spark filter eval    → eliminates rows in memory
```

The earlier the pruning, the better. Aim to maximize steps 1–3 through good data layout.

---

## 9. Best Practices

1. Always filter on clustering / partition columns
2. Avoid functions on filter columns in WHERE clauses
3. Put high-selectivity filter columns within the first 32 columns of the schema, or increase `dataSkippingNumIndexedCols`
4. Add Bloom Filters for UUID / ID columns with equality lookups
5. Run `OPTIMIZE` regularly to compact files and refresh stats
6. Use `EXPLAIN COST` to validate that file skipping is occurring
7. For Liquid Clustered tables, ensure `OPTIMIZE` runs regularly so new data is clustered and stats are current
