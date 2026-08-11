# Z-Order (ZORDER BY) — Complete Guide

---

## 1. What Is Z-Order?

**Z-Order** is a multi-dimensional data ordering technique used in Delta Lake to co-locate related data within the same set of files.  
It maps multiple column values to a single **Z-curve** (Morton code) index, then sorts all data by that index before writing files.

The result: rows with similar values across the Z-ordered columns are stored close together in the same files — enabling aggressive **data skipping** at query time.

---

## 2. The Z-Curve (Morton Code) Explained

A Z-curve maps N-dimensional coordinates to a 1D index by **interleaving the bits** of each dimension.

```
Example: 2D → 1D mapping
  Point (x=3, y=5):
    x in binary: 011
    y in binary: 101
    Interleaved:  100111  ← Z-index
    
Points close in 2D space → similar Z-index → stored in same files
```

This allows a single range scan on the Z-index to cover a box in N-dimensional space — meaning fewer files need to be opened for multi-column queries.

---

## 3. How to Use Z-Order

### Basic Syntax
```sql
-- Reorganize table with Z-order on two columns
OPTIMIZE orders ZORDER BY (customer_id, order_date);

-- Z-order on a subset of data (partition filter)
OPTIMIZE orders WHERE order_date >= '2025-01-01' ZORDER BY (customer_id, order_date);
```

```python
# PySpark equivalent
spark.sql("OPTIMIZE orders ZORDER BY (customer_id, order_date)")

# With partition filter
spark.sql("""
  OPTIMIZE orders 
  WHERE order_date >= '2025-01-01' 
  ZORDER BY (customer_id, order_date)
""")
```

### Check Z-Order History
```sql
DESCRIBE HISTORY orders;
-- Look for operationName = 'OPTIMIZE' with operationParameters.zOrderBy
```

---

## 4. How Z-Order Improves Query Performance

### Without Z-Order (Random Layout)
```
100 files, each containing rows from ALL customers
Query: WHERE customer_id = 'C100'
→ Must scan ALL 100 files to find rows for C100
→ 100% file scan
```

### With Z-Order on customer_id
```
100 files, rows sorted by Z-curve of (customer_id, order_date)
Query: WHERE customer_id = 'C100'
→ File stats: min_customer='C001', max_customer='C200' for some files
→ Only ~5 files contain rows for C100
→ 95% file skip → 20x I/O reduction
```

---

## 5. Z-Order vs Liquid Clustering — Detailed Comparison

| Property | Z-Order | Liquid Clustering |
|---|---|---|
| **Algorithm** | Z-curve (Morton code) | Hilbert curve |
| **Locality preservation** | Good | **Better** (fewer jumps) |
| **Incremental** | **NO** — full table rewrite | YES — only new files |
| **Persists after new writes** | NO — must rerun manually | YES — auto-managed |
| **Compatible with streaming** | Poorly | **Fully** |
| **Change ordering columns** | Full rewrite required | Metadata-only change |
| **Auto Optimize compatible** | NO | YES |
| **DBR version** | Any | 13.3+ |
| **Write amplification** | Very HIGH (always full) | LOW (incremental) |
| **Best for** | Static / batch tables | All workloads |

### Key Interview Point
> Z-Order is a **one-time, manual, full-table operation**. Every time new data arrives, the table is no longer Z-ordered — you must rerun `OPTIMIZE ZORDER BY` on the entire table to restore ordering. This makes it expensive for frequently updated tables.  
> **Liquid Clustering solves this** by being incremental.

---

## 6. Z-Order Best Practices

### Column Selection
```sql
-- GOOD: Columns frequently used together in WHERE clauses
OPTIMIZE orders ZORDER BY (customer_id, order_date);

-- BAD: Too many columns — effectiveness diminishes rapidly after 3-4
OPTIMIZE orders ZORDER BY (col1, col2, col3, col4, col5, col6);
-- Z-curve in high dimensions provides poor locality

-- BAD: Low-cardinality columns (2-3 unique values)
OPTIMIZE orders ZORDER BY (status);   -- min/max stats already handles this

-- GOOD: Medium-to-high cardinality columns
OPTIMIZE orders ZORDER BY (customer_id, product_category);
```

### Column Cardinality Guidelines
| Column Type | Good for Z-Order? | Reason |
|---|---|---|
| UUID / high cardinality ID | YES | Skipping very effective |
| Date / timestamp | YES | Range queries benefit greatly |
| Category (100s of values) | YES | Good clustering |
| Boolean / status (2-5 values) | NO | Min/max stats sufficient |
| Free-text | NO | No meaningful ordering |

---

## 7. Multi-Column Z-Order Effectiveness

Z-Order is most effective for the **first 1-2 columns**. Adding more provides diminishing returns because the Z-curve's locality degrades in high dimensions.

```
Effectiveness (approximate):
  ZORDER BY (col1)         → ~80% file skip on col1 queries
  ZORDER BY (col1, col2)   → ~70% skip on col1, ~60% skip on col2
  ZORDER BY (col1, col2, col3) → ~60%/50%/40% respectively
  ZORDER BY (4+ columns)   → effectiveness drops off significantly
```

---

## 8. When Z-Order Runs (Full Rewrite!)

```
OPTIMIZE ZORDER BY (col1, col2) runs:
  1. Read ALL files in the table
  2. Compute Z-index for each row
  3. Sort ALL rows by Z-index globally
  4. Write new Parquet files (sized ~1GB)
  5. Mark old files as deleted in Delta log
  6. VACUUM later removes old physical files

For a 10TB table → reads 10TB + writes 10TB = 20TB I/O per OPTIMIZE run
```

This is why Z-Order is expensive for large, frequently updated tables.

---

## 9. Pros and Cons

| Pros | Cons |
|---|---|
| Significantly improves multi-column query skipping | Full table rewrite every time |
| Available in all Delta Lake versions | Not incremental — re-run after every major load |
| Simple to use | Write amplification is very high |
| Effective for read-heavy analytical workloads | Doesn't persist for new data |
| Can be combined with partitioning | Degraded effectiveness with >3 columns |

---

## 10. Z-Order + Partitioning Pattern (Legacy Best Practice)

Before Liquid Clustering, the standard approach was:

```sql
-- Partition by low-cardinality date, Z-order by high-cardinality ID
CREATE TABLE orders
PARTITIONED BY (order_year INT, order_month INT);

-- Then OPTIMIZE per partition
OPTIMIZE orders WHERE order_year = 2025 AND order_month = 3
ZORDER BY (customer_id, product_id);
```

This limited the Z-order scope to one partition at a time — making it more manageable.  
**Liquid Clustering replaces this entire pattern.**

---

## 11. Practical Example with Performance Comparison

```python
# Before Z-Order
df = spark.sql("""
  SELECT * FROM orders 
  WHERE customer_id = 'CUST_5000' 
    AND order_date BETWEEN '2025-01-01' AND '2025-03-31'
""")
df.explain()
# numFiles scanned: 500 (all files)

# Run Z-Order
spark.sql("OPTIMIZE orders ZORDER BY (customer_id, order_date)")

# After Z-Order
df = spark.sql("""
  SELECT * FROM orders 
  WHERE customer_id = 'CUST_5000' 
    AND order_date BETWEEN '2025-01-01' AND '2025-03-31'
""")
df.explain()
# numFiles scanned: 12 (97.6% reduction)
```

---

## 12. Summary

> Use Z-Order for **static or infrequently updated** batch tables on Databricks runtimes before 13.3.  
> For any table with continuous writes, streaming, or frequent MERGE operations — **switch to Liquid Clustering** instead.
