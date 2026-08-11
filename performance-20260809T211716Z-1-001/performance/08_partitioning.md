# Partitioning in Delta Lake — Complete Guide

---

## 1. What Is Partitioning?

**Partitioning** divides a Delta table's data into separate **physical subdirectories** on storage, one per unique combination of partition column values.

```
Table: orders PARTITIONED BY (year, month)
Storage layout:
  /orders/year=2024/month=11/part-0001.parquet
  /orders/year=2024/month=12/part-0001.parquet
  /orders/year=2025/month=01/part-0002.parquet
  /orders/year=2025/month=02/part-0001.parquet
```

When a query filters on `year` and/or `month`, Spark reads **only the matching directories** — skipping others entirely without even opening them. This is **partition pruning**.

---

## 2. How Partition Pruning Works

```
Query: SELECT * FROM orders WHERE year = 2025 AND month = 3

Without partitioning: Spark reads all files in all directories
With partitioning:    Spark reads ONLY /orders/year=2025/month=3/
                      All other directories: SKIPPED at the filesystem level
```

This is the cheapest form of data skipping — it happens at the **directory level** before any file is opened.

---

## 3. Create a Partitioned Table

```sql
-- SQL
CREATE TABLE orders (
  order_id   BIGINT,
  customer_id STRING,
  amount      DOUBLE,
  region      STRING,
  order_date  DATE,
  year        INT GENERATED ALWAYS AS (YEAR(order_date)),
  month       INT GENERATED ALWAYS AS (MONTH(order_date))
)
PARTITIONED BY (year, month);

-- Or simpler: partition on an existing column
CREATE TABLE events (
  event_id   BIGINT,
  user_id    STRING,
  event_type STRING,
  ts         TIMESTAMP,
  dt         DATE   -- partition column
)
PARTITIONED BY (dt);
```

```python
# PySpark
df.write \
  .format("delta") \
  .partitionBy("year", "month") \
  .mode("overwrite") \
  .save("/path/to/orders")

# Or save to table
df.write \
  .format("delta") \
  .partitionBy("dt") \
  .mode("append") \
  .saveAsTable("events")
```

---

## 4. Partition Column Selection — Critical Rules

### Rule 1: Low Cardinality Only
```
GOOD partition columns:
  year       → ~5-10 unique values
  month      → 12 unique values  
  country    → ~200 unique values
  status     → 3-5 unique values
  dt (date)  → ~365/year → borderline (depends on table size)

BAD partition columns:
  customer_id → millions of unique values → MILLIONS of directories (over-partitioning)
  user_id     → millions of unique values → file explosion
  timestamp   → infinite unique values → catastrophic
  order_id    → unique per row → one directory per row!
```

### Rule 2: Must Be in WHERE Clauses
```sql
-- Only partition on columns you actually filter on
-- If you never query WHERE region = 'US', don't partition by region
SELECT * FROM orders WHERE year = 2025 AND month = 3;  -- year, month are good partition cols
```

### Rule 3: Target File Size ≥ 1GB Per Partition
```
Table: 100GB, partitioned by country (200 countries)
Average per partition: 100GB / 200 = 500MB ← acceptable

Table: 100GB, partitioned by customer_id (1M customers)
Average per partition: 100GB / 1M = 100KB ← catastrophic small files!
```

---

## 5. Over-Partitioning: The Biggest Mistake

Over-partitioning on high-cardinality columns creates the **small file problem**:

```
Table: 10GB of data, partitioned by user_id (500K unique users)
  Average file size: 10GB / 500K = 20KB per file

Problems:
  - 500,000 directories on object storage
  - Each Spark task reads one 20KB file → massive task scheduling overhead
  - Spark driver overwhelmed listing hundreds of thousands of files
  - Metadata operations slow to a crawl
  - Delta log grows enormous tracking all these files
  - VACUUM takes hours scanning all directories
```

**Solution:** Use Liquid Clustering instead for high-cardinality columns.

---

## 6. Partition Overwrite

```python
# Overwrite a specific partition without touching others
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Only overwrites year=2025/month=3, leaves other partitions intact
df.write \
  .format("delta") \
  .mode("overwrite") \
  .partitionBy("year", "month") \
  .save("/path/to/orders")
```

```sql
-- SQL: insert overwrite specific partition
INSERT OVERWRITE orders PARTITION (year=2025, month=3)
SELECT * FROM orders_staging WHERE year = 2025 AND month = 3;
```

---

## 7. Manage Partitions

```sql
-- List all partitions
SHOW PARTITIONS orders;

-- Add partition (for external tables)
ALTER TABLE orders ADD PARTITION (year=2025, month=4);

-- Drop a partition (deletes data)
ALTER TABLE orders DROP PARTITION (year=2023, month=1);

-- Repair/refresh partition metadata
MSCK REPAIR TABLE orders;         -- Hive metastore sync
ALTER TABLE orders RECOVER PARTITIONS;  -- Delta equivalent
```

---

## 8. Partitioning vs Liquid Clustering vs ZORDER

| Feature | Partitioning | ZORDER | Liquid Clustering |
|---|---|---|---|
| **Pruning level** | Directory (folder) | File (statistics) | File (statistics) |
| **Best column type** | Low cardinality | Medium-high cardinality | Any cardinality |
| **Incremental** | N/A (static layout) | NO (full rewrite) | YES |
| **Small file risk** | HIGH if over-partitioned | Low | Low (auto-managed) |
| **Change layout** | Requires rewrite | Requires rewrite | Metadata-only |
| **Streaming friendly** | Partially | No | YES |
| **DBR requirement** | Any | Any | 13.3+ |

### Combined Pattern (Pre-Liquid Clustering Era)
```sql
-- Classic best practice: partition on date + Z-order on ID
CREATE TABLE events PARTITIONED BY (dt DATE);
OPTIMIZE events WHERE dt = '2025-03-01' ZORDER BY (user_id, event_type);
```

### Modern Best Practice (Liquid Clustering Era)
```sql
-- For most new tables: skip partitioning, use liquid clustering
CREATE TABLE events (
  event_id   BIGINT,
  user_id    STRING,
  event_type STRING,
  ts         TIMESTAMP,
  dt         DATE
)
CLUSTER BY (user_id, dt);  -- handles both high and low cardinality
```

---

## 9. When to Still Use Partitioning (Even in 2025+)

| Scenario | Use Partitioning? | Reason |
|---|---|---|
| Regulatory data retention (delete by month) | YES | `DROP PARTITION` for compliance |
| External tools that expect partition paths | YES | BI tools, Hive, EMR |
| DBR < 13.3 (no Liquid Clustering) | YES | No alternative |
| Predictable filter on low-cardinality column | YES | Zero-cost directory-level skip |
| Multi-cloud/multi-tool tables | YES | Maximum compatibility |
| Tables > 10TB partitioned by date | YES | Partition pruning at massive scale |

---

## 10. Pros and Cons

| Pros | Cons |
|---|---|
| Zero-cost directory-level pruning | Over-partitioning kills performance |
| Simple to understand and implement | Rigid — changing partition keys requires full rewrite |
| Compatible with all tools (Hive, Spark, etc.) | Small file problem on high-cardinality columns |
| Enables partition-level maintenance (DROP) | Doesn't help with multi-column filters on non-partition columns |
| Works with DBR < 13.3 | Skew issues on uneven distributions |

---

## 11. Best Practices

1. **Target partition size ≥ 1GB** — if partitions are smaller, consider coarser partitioning
2. **Use date/year/month for time-series tables** — the most natural partition key
3. **Avoid high-cardinality columns** — use Liquid Clustering instead
4. **Combine with Z-Order or Liquid Clustering** for multi-column pruning within partitions
5. **Use dynamic partition overwrite** for safe incremental loads
6. **Run OPTIMIZE per partition** to compact small files within each partition
7. **Monitor file counts per partition** — alert if any partition exceeds 10,000 files

```python
# Monitor partition health
spark.sql("""
  SELECT year, month, COUNT(*) as file_count, SUM(size) as total_bytes
  FROM (
    SELECT year, month, size
    FROM delta.`/path/to/orders`._delta_log
    -- This is pseudocode; use DESCRIBE DETAIL or sys tables in practice
  )
  GROUP BY year, month
  ORDER BY file_count DESC
""")
```
