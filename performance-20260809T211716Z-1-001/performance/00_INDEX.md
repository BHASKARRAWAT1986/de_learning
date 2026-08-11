# Databricks Performance Optimization — Master Index

> Complete reference for FAANG-level Data Engineering interviews.  
> All techniques with examples, pros/cons, internals, and decision guides.

---

## Files in This Directory

| # | File | Topics Covered |
|---|---|---|
| 00 | `liquid clustering` | Liquid Clustering — Hilbert curve, incremental OPTIMIZE, streaming |
| 01 | `01_optimize_write_and_auto_compact.md` | Optimize Write, Auto Compact, file size tuning |
| 02 | `02_data_skipping_file_pruning_predicate_pushdown.md` | Data Skipping, File Pruning, Bloom Filters, Predicate Pushdown |
| 03 | `03_zorder.md` | ZORDER, Z-curve, vs Liquid Clustering |
| 04 | `04_column_pruning.md` | Column Pruning, Projection Pushdown, Nested types |
| 05 | `05_AQE_adaptive_query_execution.md` | AQE, Dynamic Partitioning, Skew Join, Dynamic Broadcast |
| 06 | `06_analyze_table_column_statistics.md` | ANALYZE, Table Stats, Column Stats, Histograms |
| 07 | `07_vacuum.md` | VACUUM, retention, time travel, shallow clone risks |
| 08 | `08_partitioning.md` | Hive Partitioning, over-partitioning, partition pruning |
| 09 | `09_caching.md` | Spark cache, Delta Cache, Broadcast Variables |
| 10 | `10_join_optimization.md` | BHJ, SMJ, Skew, Salting, Range Join |
| 11 | `11_deletion_vectors_photon_and_advanced.md` | DVs, Photon, Predictive Optimization, full matrix |

---

## Quick Decision Guide

```
My query is SLOW. What do I check?

STEP 1 — Is data layout the problem?
  → Check: Is data clustered on filter columns?
  → Fix: Liquid Clustering (new) or ZORDER (legacy) + OPTIMIZE

STEP 2 — Is file count the problem?
  → Check: DESCRIBE DETAIL → numFiles very high?
  → Fix: OPTIMIZE (full compaction) + Auto Compact + Optimize Write

STEP 3 — Is the JOIN strategy wrong?
  → Check: EXPLAIN → SortMergeJoin when one table is small?
  → Fix: BROADCAST hint or increase autoBroadcastJoinThreshold

STEP 4 — Is there data skew?
  → Check: Spark UI → one task much longer than others?
  → Fix: AQE skewJoin or manual salting

STEP 5 — Are file-level stats missing/stale?
  → Fix: OPTIMIZE (refreshes stats on rewritten files)
       ANALYZE TABLE (updates metastore stats)

STEP 6 — Is storage growing out of control?
  → Fix: VACUUM with appropriate retention period

STEP 7 — Is column I/O excessive?
  → Check: SELECT * anywhere in pipeline?
  → Fix: Explicit column selection (column pruning)

STEP 8 — Is AQE enabled?
  → SET spark.sql.adaptive.enabled = true (default ON in DBR)
```

---

## The Hierarchy of Data Skipping (Best to Worst I/O)

```
1. Partition Pruning        → eliminates entire directories (cheapest)
2. Delta Data Skipping      → eliminates files via min/max stats
3. Bloom Filter             → eliminates files via probabilistic check (equality)
4. Parquet Row Group Stats  → eliminates row groups within files
5. Parquet Page-level       → eliminates pages via dictionary encoding
6. Spark Filter Eval        → eliminates rows in memory (most expensive)
```

**Goal:** Push as much filtering to levels 1–4 as possible.

---

## The Write → Read → Maintain Lifecycle

```
WRITE PHASE:
  Optimize Write → compact task outputs before commit
  Auto Compact   → merge small files after commit (async)

READ PHASE:
  Data Skipping        → skip files via Delta log stats
  Column Pruning       → skip columns in Parquet
  AQE                  → re-optimize plan at runtime
  Predicate Pushdown   → push filters to file reader
  Caching              → serve from SSD/memory on repeat reads

MAINTAIN PHASE:
  OPTIMIZE             → compact + recluster files
  ANALYZE              → refresh metastore statistics  
  VACUUM               → delete obsolete physical files
  Predictive Opt.      → automate all of the above
```

---

## FAANG Interview One-Liners

| Concept | One-Line Answer |
|---|---|
| **Liquid Clustering** | Incremental, partition-free data layout using Hilbert curve for data skipping |
| **Data Skipping** | Delta reads file-level min/max stats from log to skip non-matching files before opening them |
| **ZORDER** | Full-table Z-curve reorder for multi-column co-location; not incremental — replaced by Liquid Clustering |
| **Optimize Write** | Pre-write shuffle to merge small task outputs into larger files at commit time |
| **Auto Compact** | Async post-write small file merger targeting ~128MB files |
| **AQE** | Runtime re-optimization of Spark plans using actual shuffle statistics after each stage |
| **Column Pruning** | Parquet columnar format enables reading only referenced columns — 0 I/O for unused columns |
| **ANALYZE** | Collects row count and column cardinality statistics for Catalyst join strategy selection |
| **VACUUM** | Physically deletes Delta files removed from log beyond the retention period |
| **Partitioning** | Physical folder-per-value layout enabling directory-level pruning for low-cardinality columns |
| **Deletion Vectors** | Bitmap file marking deleted rows to avoid full file rewrite on DELETE/UPDATE |
| **Photon** | Databricks C++ vectorized engine replacing JVM Spark for 2-10x faster scans/aggregations |
| **Bloom Filter** | Probabilistic index per file for high-cardinality equality lookups; O(1) check per file |
| **Broadcast Join** | Send small table to all executors → zero shuffle of large table |
| **Skew Join (AQE)** | Auto-detect and split skewed partitions into sub-tasks for balanced parallelism |
| **Delta Cache** | Cache raw Parquet bytes on worker NVMe SSD — shared across queries and sessions |
