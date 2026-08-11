# File Formats — Complete FAANG Interview Guide
## CSV, JSON, Parquet, ORC, Avro, Delta Lake — When to Use What and Why

---

## 1. The Core Problem File Formats Solve

When you store 1 billion rows of data, how you lay the bytes on disk determines:
- How fast queries run (seconds vs hours)
- How much storage you pay for (GB vs TB)
- Whether filters can skip data (read 1% vs read 100%)
- Whether schema changes break pipelines
- Whether you can do concurrent reads and writes safely

The wrong format is not a minor performance issue — it can make a query **100x slower**.

---

## 2. CSV (Comma-Separated Values)

### What It Is
Plain text. Every row is a line. Every field is separated by a delimiter.
```
order_id,customer_id,amount,country,created_at
1001,5001,99.99,US,2024-01-15
1002,5002,149.50,UK,2024-01-15
```

### Internal Layout
```
FILE ON DISK (row-oriented, text):
[row 1 bytes][row 2 bytes][row 3 bytes]...[row N bytes]

To read "amount" column only:
  → Must read the ENTIRE file (every row, every column)
  → Parse each line as text
  → Extract the Nth field by counting commas

To filter WHERE amount > 100:
  → Must read every row
  → Parse every row
  → Evaluate the filter AFTER parsing
  → No skipping possible — zero embedded metadata
```

### What Works and What Doesn't

| Feature | Works? | Why |
|---------|--------|-----|
| Column pruning | ❌ NO | Row-based text — must parse whole line to get any field |
| Predicate pushdown (row skip) | ❌ NO | No min/max stats embedded — every row must be read |
| Predicate pushdown (filter during parse) | ✅ Partial | Filter applied while reading row-by-row (saves CPU, not I/O) |
| Compression | ✅ YES | gzip, bzip2, snappy (but gzip is not splittable — 1 task per file!) |
| Schema | ❌ Weak | Everything is a string — types must be inferred or specified |
| Schema evolution | ❌ Fragile | Column reorder breaks pipelines. New column = all downstream breaks |
| Splittable | ✅ YES (uncompressed/bzip2) ❌ NO (gzip) | gzip file cannot be split across multiple tasks |
| Human readable | ✅ YES | Can open in Excel |

### Compression Gotcha
```python
# gzip CSV = NOT splittable → 1 task reads the ENTIRE file
# This is a silent killer — no parallelism, even if file is 10GB
spark.read.option("compression", "gzip").csv("s3://bucket/data.csv.gz")
# → 1 task, 1 executor, no parallelism

# Use bzip2 (splittable) or just write multiple smaller CSV files
# Or better — use Parquet
```

### When to Use CSV
| Use Case | Reason |
|----------|--------|
| Exporting data to non-technical users / Excel | Human readable |
| Receiving data from external partners or legacy systems | Universal format, no tooling needed |
| Small lookup tables (< 1MB) | Overhead of columnar format not worth it |
| One-time data exchange | Not building a persistent table |
| Regulatory exports that must be text | Compliance requirement |

### When NOT to Use CSV
- As your persistent storage format for analytics (use Parquet/Delta instead)
- For data > 1GB that will be queried repeatedly
- When you need schema enforcement
- When you need fast filtered reads

---

## 3. JSON (JavaScript Object Notation)

### What It Is
Text-based, row-oriented. Every row is a JSON object (one per line in JSONL/NDJSON format, or a JSON array).

```json
{"order_id": 1001, "customer_id": 5001, "amount": 99.99, "tags": ["flash_sale", "mobile"]}
{"order_id": 1002, "customer_id": 5002, "amount": 149.50, "address": {"city": "London", "zip": "EC1A"}}
```

### Internal Layout
```
FILE ON DISK (row-oriented, text, with schema flexibility):
{row1 JSON}\n
{row2 JSON}\n
{row3 JSON}\n   ← row 3 may have DIFFERENT fields than row 1

To read "amount" only:
  → Must parse EVERY row's full JSON object
  → Extract the "amount" key from each parsed object
  → No column skipping (unlike Parquet)

Nested structures: natively supported
  → {"address": {"city": "London"}} → nested without extra joins
```

### What Works and What Doesn't

| Feature | Works? | Why |
|---------|--------|-----|
| Column pruning | ❌ NO | Row-based text — full JSON object must be parsed |
| Predicate pushdown (skip) | ❌ NO | No embedded stats |
| Schema evolution | ✅ GOOD | New keys in JSON are silently ignored or captured |
| Nested/semi-structured data | ✅ EXCELLENT | First-class nested object support |
| Splittable | ✅ (JSONL) ❌ (JSON array) | JSONL = one JSON per line, splittable. JSON array = one big object, not splittable |
| Human readable | ✅ YES | Readable with any text editor |
| Type safety | ❌ Weak | Types inferred from values; can be inconsistent |

### JSON vs JSONL
```python
# JSONL (newline-delimited JSON) — use this for Spark
{"id": 1, "name": "Alice"}\n
{"id": 2, "name": "Bob"}\n
# Each line = one record. Spark can split this file → parallelism

# JSON array — avoid for big data
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob"}
]
# Entire file = one record. Cannot split. 1 task reads everything.

spark.read.json("path/")               # default: JSONL
spark.read.option("multiLine", "true").json("path/")  # JSON array per file
```

### When to Use JSON
| Use Case | Reason |
|----------|--------|
| Kafka/event stream messages | Standard format for CDC, clickstream, IoT events |
| REST API payloads | Natural format for web services |
| Semi-structured / schema-less data | Fields vary per record |
| Config files and small metadata | Human readable, flexible |
| Bronze layer raw landing | Preserve original structure before enforcing schema |

### When NOT to Use JSON
- For Silver/Gold analytics tables (too slow, too large — use Parquet/Delta)
- When you need columnar access patterns
- Very large datasets with fixed schema (Parquet is 5-10x faster + smaller)

---

## 4. Parquet — The Analytics Workhorse

### What It Is
**Columnar binary format**. Data is stored column-by-column, not row-by-row. Designed for analytical queries.

### Internal Layout
```
PARQUET FILE STRUCTURE:

┌──────────────────────────────────────────────────────┐
│                   Parquet File                        │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │              Row Group 1 (~128MB)            │     │
│  │  ┌──────────┐ ┌──────────┐ ┌─────────────┐  │     │
│  │  │ Col: id  │ │Col:amount│ │Col: country │  │     │
│  │  │ 1,2,3,4  │ │99,149,50 │ │US,UK,US,FR  │  │     │
│  │  │ ...      │ │...       │ │...          │  │     │
│  │  │ min: 1   │ │min: 10   │ │             │  │     │
│  │  │ max: 1000│ │max: 9999 │ │             │  │     │
│  │  │ nulls: 0 │ │nulls: 2  │ │             │  │     │
│  │  └──────────┘ └──────────┘ └─────────────┘  │     │
│  │  [Column Chunk Stats embedded per row group] │     │
│  └─────────────────────────────────────────────┘     │
│  ┌─────────────────────────────────────────────┐     │
│  │              Row Group 2 (~128MB)            │     │
│  │  ...                                         │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │           FILE FOOTER (metadata)             │     │
│  │  - Schema (column names + types)             │     │
│  │  - Row group byte offsets                    │     │
│  │  - Column stats per row group (min/max/null) │     │
│  │  - Encoding info (dictionary, RLE, etc.)     │     │
│  └─────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────┘
```

### How Column Pruning Works in Parquet
```
Query: SELECT amount FROM orders

Without column pruning (CSV): read ALL columns, discard the rest
  → Read: id + amount + country + created_at + ... (100% of data)

With column pruning (Parquet): read ONLY the "amount" column chunk
  → Read: only the amount bytes from each row group (5-20% of data)
  → Skips id, country, created_at column chunks entirely

This is why SELECT * is a crime in Parquet queries —
you defeat the entire point of the columnar layout.
```

### How Predicate Pushdown Works in Parquet
```
Query: SELECT * FROM orders WHERE amount > 10000

Row Group 1: stats say amount min=10, max=9999
  → 10000 > max(9999) → this entire row group CANNOT have matching rows
  → SKIP Row Group 1 — don't even read it from disk

Row Group 2: stats say amount min=5000, max=50000
  → 10000 is within [5000, 50000] — might have matches
  → READ Row Group 2, apply filter

Row Group 3: stats say amount min=1, max=100
  → SKIP

Result: 2 out of 3 row groups skipped → 67% less I/O
This is REAL predicate pushdown — actual bytes skipped from disk.
```

### Parquet Encodings (Why It's Smaller)
```
Dictionary Encoding: for low-cardinality columns
  country = [US, UK, US, US, FR, UK, US, ...]
  → Dictionary: {0: "US", 1: "UK", 2: "FR"}
  → Stored as: [0, 1, 0, 0, 2, 1, 0, ...]  ← ints instead of strings
  → "US" (2 bytes) stored as 0 (1 byte) → ~50% reduction on string columns

RLE (Run-Length Encoding): for sorted/repeated values
  [US, US, US, US, UK, UK, UK, ...] → "4x US, 3x UK"
  
Bit Packing: for integer columns with small range
  age values 0-127 → need only 7 bits per value (not 64-bit long)

Delta Encoding: for timestamps/monotonic integers
  [1000, 1001, 1002, 1003] → [1000, +1, +1, +1] → tiny delta values
```

### What Works and What Doesn't

| Feature | Works? | Why |
|---------|--------|-----|
| Column pruning | ✅ EXCELLENT | Columnar layout — read only needed columns |
| Predicate pushdown (row group skip) | ✅ EXCELLENT | Min/max stats in footer per row group |
| Bloom filters | ✅ (optional) | For high-cardinality columns where min/max is weak |
| Compression | ✅ EXCELLENT | Snappy (fast), Gzip (smaller), ZSTD (best of both) |
| Schema | ✅ STRONG | Typed schema embedded in file footer |
| Nested structures | ✅ GOOD | Dremel encoding for nested/repeated fields |
| Schema evolution | ✅ Good (additive) | New columns fine; renames/drops are fragile |
| Splittable | ✅ YES | Row group boundaries = natural split points |
| Human readable | ❌ NO | Binary format |
| ACID transactions | ❌ NO | Plain Parquet — no transaction support |

### When to Use Parquet
| Use Case | Reason |
|----------|--------|
| Analytics tables (Silver/Gold) | Column pruning + predicate pushdown = fast aggregation queries |
| Data lake storage | Industry standard, universally supported (Spark, Hive, Presto, Athena, BigQuery) |
| Wide tables (100+ columns) | Column pruning makes SELECT of 5 cols cheap even with 200 col schema |
| Tables queried frequently with filters | Row group skipping reduces I/O dramatically |
| ML feature stores | Fast columnar reads for feature extraction |
| Long-term archival with query needs | Better compression than CSV + queryable |

---

## 5. ORC (Optimized Row Columnar)

### What It Is
Columnar binary format created by Hive/Hortonworks. Direct competitor to Parquet. More optimized for the Hive ecosystem.

### Internal Layout
```
ORC FILE STRUCTURE:

┌───────────────────────────────────────────┐
│                ORC File                    │
│                                           │
│  ┌─────────────────────────────────────┐  │
│  │           Stripe 1 (~250MB)          │  │
│  │  ┌──────────────┐ ┌───────────────┐  │  │
│  │  │ Index Data   │ │ Column Data   │  │  │
│  │  │ (per 10K rows│ │ (columnar,    │  │  │
│  │  │  min/max/    │ │  compressed)  │  │  │
│  │  │  bloom fltr) │ │               │  │  │
│  │  └──────────────┘ └───────────────┘  │  │
│  └─────────────────────────────────────┘  │
│  ┌─────────────────────────────────────┐  │
│  │           Stripe 2 (~250MB)          │  │
│  └─────────────────────────────────────┘  │
│                                           │
│  ┌─────────────────────────────────────┐  │
│  │         FILE FOOTER + POSTSCRIPT    │  │
│  │  - Schema + stripe stats            │  │
│  │  - Bloom filters (built-in default) │  │
│  │  - Compression codec                │  │
│  └─────────────────────────────────────┘  │
└───────────────────────────────────────────┘
```

### Parquet vs ORC Comparison

| Dimension | Parquet | ORC |
|-----------|---------|-----|
| Primary ecosystem | Spark, Impala, Presto, BigQuery | Hive, Spark |
| Row group size | 128MB default | 250MB stripes |
| Nested data | Excellent (Dremel encoding) | Good (but less efficient for deep nesting) |
| Bloom filters | Optional, manual | Built-in, automatic |
| Predicate pushdown granularity | Row group (128MB) | Row index entry (10K rows) — MORE granular |
| Hive integration | Good | Better (native Hive format) |
| Compression | Snappy/Gzip/ZSTD | Zlib/Snappy/LZO |
| ACID support | No (raw Parquet) | Yes (ORC ACID with Hive) |
| Industry standard | ✅ More widely adopted | ✅ Dominant in Hive ecosystem |
| **Default in Databricks** | ✅ YES | No |
| **Default in AWS Athena** | ✅ YES (recommended) | Supported |

### When to Use ORC vs Parquet
```
Use ORC when:
  - Your primary query engine is Hive
  - You need ORC ACID transactions (without Delta Lake)
  - Your queries heavily use bloom filters on high-cardinality columns
  - Migrating from a Hive-based architecture

Use Parquet when:
  - Your primary engine is Spark, Databricks, Presto, Athena, BigQuery
  - You need maximum ecosystem compatibility
  - You're building on Delta Lake (Delta wraps Parquet internally)
  - Starting a new project from scratch in 2024+
```

---

## 6. Avro

### What It Is
**Row-based binary format** designed for Kafka, Hadoop serialization, and schema evolution. Not columnar — optimized for WRITE throughput and schema evolution, not analytical reads.

### Internal Layout
```
AVRO FILE STRUCTURE:

┌─────────────────────────────────────┐
│           AVRO File                  │
│                                     │
│  ┌──────────────────────────────┐   │
│  │  HEADER                      │   │
│  │  - Magic bytes ("Obj\x01")   │   │
│  │  - Schema (full JSON schema) │   │ ← Schema embedded in file!
│  │  - Codec (null/deflate/snappy│   │
│  └──────────────────────────────┘   │
│                                     │
│  ┌──────────────────────────────┐   │
│  │  DATA BLOCK 1                │   │
│  │  [row1][row2][row3]...       │   │ ← Row-oriented binary
│  │  (encoded per schema)        │   │
│  └──────────────────────────────┘   │
│  ┌──────────────────────────────┐   │
│  │  DATA BLOCK 2                │   │
│  └──────────────────────────────┘   │
└─────────────────────────────────────┘
```

### Schema Evolution — Avro's Superpower
```json
// Schema v1
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "long"},
    {"name": "amount",   "type": "double"}
  ]
}

// Schema v2 — added "discount" field with default
{
  "type": "record", 
  "name": "Order",
  "fields": [
    {"name": "order_id",  "type": "long"},
    {"name": "amount",    "type": "double"},
    {"name": "discount",  "type": ["null", "double"], "default": null}  ← backward compatible
  ]
}

// Rules for backward-compatible evolution:
// ✅ Add new field WITH a default value
// ✅ Remove a field that had a default
// ❌ Remove a required field (no default) — breaks old readers
// ❌ Change a field's type (int → long OK; string → int NOT OK)
// ❌ Rename a field — breaks all readers (use "aliases" instead)
```

### Avro with Kafka Schema Registry
```python
# Production pattern: Avro + Schema Registry = safe schema evolution
# Schema Registry stores schemas centrally, assigns schema IDs
# Message format: [magic byte][schema_id (4 bytes)][avro payload]

# Reading Avro from Kafka in Spark with Schema Registry
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "orders")
    .load()
)

# Deserialize with Confluent Schema Registry
from pyspark.sql.avro.functions import from_avro

# Get schema from registry
schema_str = requests.get(
    "http://schema-registry:8081/subjects/orders-value/versions/latest"
).json()["schema"]

df_parsed = df.select(
    from_avro(col("value"), schema_str).alias("data")
).select("data.*")
```

### What Works and What Doesn't

| Feature | Works? | Why |
|---------|--------|-----|
| Column pruning | ❌ NO | Row-based — full row decoded to get any field |
| Predicate pushdown | ❌ NO | No embedded stats |
| Schema evolution | ✅ EXCELLENT | Best-in-class with Schema Registry |
| Nested structures | ✅ EXCELLENT | First-class complex types (arrays, maps, unions) |
| Compression | ✅ YES | Deflate, Snappy, Bzip2 |
| Splittable | ✅ YES | Sync markers between blocks |
| Human readable | ❌ NO | Binary |
| Write speed | ✅ FAST | Row-based = fast sequential writes |
| Read speed for analytics | ❌ SLOW | Must decode all fields per row |

### When to Use Avro
| Use Case | Reason |
|----------|--------|
| Kafka event messages | Row-based = fast per-message serialization |
| Schema Registry integration | Best schema evolution support |
| Write-heavy pipelines (ETL landing zone) | Fast row writes |
| RPC / data serialization between services | Compact binary, schema-defined |
| Hadoop ecosystem input/output | HDFS, HBase, Hive interop |

### When NOT to Use Avro
- As your analytics storage format (use Parquet/Delta for reads)
- When you need fast aggregation queries
- When column pruning or predicate pushdown matters

---

## 7. Delta Lake — The Modern Standard

### What It Is
Delta Lake is NOT a different file format — it is **Parquet files + a transaction log**. Delta Lake adds ACID transactions, time travel, schema enforcement, and CDC capabilities on top of Parquet.

```
Delta Table on Disk:
  s3://bucket/my_table/
    ├── _delta_log/                ← The transaction log (what makes it "Delta")
    │     ├── 00000000000000000000.json  ← initial CREATE TABLE commit
    │     ├── 00000000000000000001.json  ← INSERT commit
    │     ├── 00000000000000000002.json  ← UPDATE commit (adds new files, marks old as removed)
    │     ├── 00000000000000000010.checkpoint.parquet  ← checkpoint (compacted log)
    │     └── _last_checkpoint
    │
    ├── part-00000-abc.snappy.parquet  ← actual data (regular Parquet files)
    ├── part-00001-def.snappy.parquet
    ├── part-00002-ghi.snappy.parquet
    └── (tombstoned files still present until VACUUM)
```

### What the Transaction Log Contains
```json
// 00000000000000000002.json  (a DELETE or UPDATE commit)
{
  "commitInfo": {
    "timestamp": 1706054400000,
    "operation": "DELETE",
    "operationParameters": {"predicate": "[\"(amount < 0)\"]"}
  },
  "remove": {
    "path": "part-00001-def.snappy.parquet",  ← mark old file as deleted
    "deletionTimestamp": 1706054400000,
    "dataChange": true
  },
  "add": {
    "path": "part-00001-new.snappy.parquet",   ← new file with rows removed
    "stats": "{\"numRecords\":9500,\"minValues\":{\"amount\":0.01},\"maxValues\":{\"amount\":9999.99}}",
    "dataChange": true
  }
}
```

### Delta Lake Capabilities

| Feature | How It Works |
|---------|-------------|
| **ACID Transactions** | Optimistic concurrency control via transaction log. Writers check for conflicts before committing. |
| **Time Travel** | Every version of the table is preserved in the log. `VERSION AS OF 5` reads the log at version 5 |
| **Schema Enforcement** | Write with wrong schema → rejected at commit time |
| **Schema Evolution** | `mergeSchema = true` → new columns added to log metadata |
| **Data Skipping** | Min/max stats for every column stored in transaction log JSON per file (not just Parquet footer) |
| **Z-Order / Liquid Clustering** | Co-locates related data in files → better data skipping |
| **Deletion Vectors** | Mark deleted rows as a bitmap file — no rewrite needed (DBR 12.1+) |
| **CDC / CDF** | Change Data Feed: tracks row-level changes (insert/update/delete) |
| **OPTIMIZE** | Compact small files into larger ones (solves "small file problem") |
| **VACUUM** | Delete old Parquet files no longer referenced by any version |

### Time Travel in Practice
```sql
-- Read table as it was 7 days ago
SELECT * FROM orders TIMESTAMP AS OF '2024-01-01';

-- Read table at a specific version
SELECT * FROM orders VERSION AS OF 42;

-- Restore table to a previous version (in-place rollback)
RESTORE TABLE orders TO VERSION AS OF 42;

-- See all history
DESCRIBE HISTORY orders;
-- Returns: version, timestamp, operation, operationMetrics, userMetadata
```

### Delta vs Plain Parquet

| Dimension | Delta Lake | Plain Parquet |
|-----------|------------|---------------|
| ACID | ✅ Full ACID | ❌ No |
| Concurrent writes | ✅ Optimistic concurrency | ❌ Last write wins / corruption |
| Schema enforcement | ✅ | ❌ |
| Time travel | ✅ | ❌ |
| MERGE/UPSERT | ✅ | ❌ (must rewrite files) |
| DELETE/UPDATE | ✅ | ❌ (must rewrite files) |
| Data skipping | ✅ Delta log stats (file-level) + Parquet stats (row group) | ✅ Parquet stats only |
| Small file problem | Solved by OPTIMIZE | Grows unbounded |
| Streaming + batch | ✅ Same table | ❌ Race conditions |
| Portability | Databricks, open source | Universal |
| **Choose when** | Always (for persistent tables) | One-time exports, cross-system interop |

### When to Use Delta Lake
| Use Case | Reason |
|----------|--------|
| All persistent tables in Databricks | The default — use Delta for everything |
| Tables with UPDATE/DELETE/MERGE | ACID writes without full rewrites |
| Tables read by streaming AND batch | Unified table, no race conditions |
| CDC target tables | APPLY CHANGES INTO / MERGE |
| Tables needing audit trail | Time travel = free audit log |
| Any Silver/Gold table | Data quality, schema enforcement |

---

## 8. Format Comparison Matrix

### Feature Comparison

| Feature | CSV | JSON | Parquet | ORC | Avro | Delta Lake |
|---------|-----|------|---------|-----|------|------------|
| Layout | Row text | Row text | Columnar binary | Columnar binary | Row binary | Parquet + TXN log |
| Column pruning | ❌ | ❌ | ✅ | ✅ | ❌ | ✅ |
| Predicate pushdown | ❌ | ❌ | ✅ | ✅ | ❌ | ✅✅ (file + row group) |
| Schema evolution | ❌ Fragile | ✅ | ✅ Additive | ✅ | ✅✅ Best | ✅✅ Enforced |
| Compression ratio | Low | Low | High | Highest | Medium | High (Parquet-based) |
| Nested data | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Human readable | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| Splittable | ✅* | ✅ (JSONL) | ✅ | ✅ | ✅ | ✅ |
| ACID | ❌ | ❌ | ❌ | ⚠️ ORC ACID | ❌ | ✅ |
| Time travel | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| Streaming + batch | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| Write speed | Fast | Fast | Medium | Medium | Very Fast | Medium |
| Read speed (analytics) | Slow | Slow | Very Fast | Very Fast | Slow | Very Fast |
| Storage size | Large | Large | Small | Smallest | Medium | Small |
| Ecosystem | Universal | Universal | Wide | Hive-heavy | Kafka/Hadoop | Databricks/OSS |

### Size Comparison (Same 100M Row Dataset)

```
CSV (uncompressed):   ~15 GB
CSV (gzip):           ~3 GB  (not splittable!)
JSON:                 ~18 GB (key names repeated per row)
Avro:                 ~8 GB
Parquet (snappy):     ~2 GB
ORC (zlib):           ~1.5 GB
Delta Lake (Parquet): ~2 GB  (+ ~1MB for _delta_log)
```

### Query Speed Comparison (SELECT country, SUM(amount) GROUP BY country on 1B rows)

```
Format          I/O Read    Query Time
CSV             100 GB      45 minutes  (read all, no skipping)
JSON            120 GB      55 minutes  (read all + JSON parsing overhead)
Avro            60 GB       20 minutes  (compressed but row-based)
Parquet         8 GB        3 minutes   (column pruning + row group skip)
ORC             6 GB        2.5 minutes (slightly better compression)
Delta Lake      8 GB        2 minutes   (Parquet + file-level stats from _delta_log)
```

---

## 9. Choosing the Right Format — Decision Tree

```
Is this data for EXCHANGE with external systems or people?
  ├── YES, needs to be human-readable or go to Excel/BI tools?
  │     → CSV
  ├── YES, going to/from Kafka or a streaming service?
  │     → Avro (with Schema Registry for schema evolution)
  ├── YES, going to a REST API or external JSON consumer?
  │     → JSON
  └── NO, this is internal storage you control?
        │
        ├── Is this a one-time export or temporary file?
        │     → Parquet (universal, fast, compact)
        │
        ├── Is this a persistent table that will be queried repeatedly?
        │     → Delta Lake (always — it's Parquet + transaction log)
        │
        ├── Is your primary query engine Hive (not Spark)?
        │     → ORC
        │
        └── Is this a raw Bronze landing zone receiving JSON/CSV from upstream?
              → Keep original format in Bronze (preserve raw)
              → Convert to Delta in Silver
```

---

## 10. Predicate Pushdown — What Really Happens Per Format

```
Query: SELECT * FROM big_table WHERE order_date = '2024-06-15' AND amount > 1000

CSV:
  - Reads 100% of file bytes
  - Parses every row as text
  - Applies filter after parsing
  - No skipping whatsoever

JSON:
  - Reads 100% of file bytes
  - Parses every row as JSON
  - Applies filter after parsing
  - No skipping

Avro:
  - Reads 100% of file bytes
  - Decodes each row per schema
  - Applies filter after decoding
  - No skipping

Parquet:
  - Reads file footer (kilobytes) first
  - Footer has: per-row-group min/max for order_date and amount
  - Row group where max(order_date) < '2024-06-15' → SKIP (don't even open it)
  - Row group where min(amount) > 1000 → SKIP
  - Only reads row groups that MIGHT have matching data
  - Within each selected row group, reads ONLY order_date + amount column chunks
  - Can skip 90% of I/O in a well-written dataset

ORC:
  - Same as Parquet, but index granularity is 10,000 rows (finer than Parquet's 128MB row groups)
  - Also has built-in bloom filters for point lookups

Delta Lake:
  - TWO LEVELS of skipping:
    Level 1: _delta_log JSON has file-level min/max per column
      → "This Parquet file has order_date from 2024-06-01 to 2024-06-10"
      → If querying 2024-06-15 → skip this entire Parquet file (don't even open it)
    Level 2: Within the Parquet files that survive file-level skipping:
      → Row group min/max stats (same as plain Parquet)
  - Most powerful: file-level + row-group-level skipping combined
```

---

## 11. Schema Evolution Detailed Comparison

```python
# ─── CSV: FRAGILE ─────────────────────────────────────────────────
# Original: id, name, amount
# New:      id, name, amount, discount
# Problem: column positions shift — all downstream code breaks

# ─── JSON: TOLERANT BUT UNCONTROLLED ──────────────────────────────
# Original: {"id": 1, "name": "Alice", "amount": 99.0}
# New:      {"id": 2, "name": "Bob",   "amount": 50.0, "discount": 5.0}
# Spark behavior: new "discount" field → NULL in old records (if using inferSchema)
# Problem: no enforcement → "disccount" typo silently creates a new column

# ─── AVRO: CONTROLLED EVOLUTION WITH RULES ────────────────────────
# Schema Registry ensures forward/backward compatibility
# Adding field with default → compatible
# Removing required field → incompatible → rejected by registry
# Most controlled schema evolution of any format

# ─── PARQUET: ADDITIVE ONLY (naturally) ──────────────────────────
# New column in Parquet file: old files just return NULL for that column
# Renamed column: breaks — Parquet uses column names not positions
# Type change: breaks

# ─── DELTA LAKE: ENFORCED + CONTROLLED ───────────────────────────
# Write a DataFrame with a new column:
df.write.option("mergeSchema", "true").mode("append").saveAsTable("silver.orders")
# → New column added to Delta schema in _delta_log
# → Old Parquet files return NULL for the new column (backward compatible)

# Write a DataFrame with a missing column:
df.write.mode("append").saveAsTable("silver.orders")
# → Missing column → filled with NULL automatically

# Write a DataFrame with WRONG TYPE:
# amount is DECIMAL in table, you write STRING
df.write.mode("append").saveAsTable("silver.orders")
# → AnalysisException: SCHEMA MISMATCH — Delta enforces schema!

# Change column type (e.g., INT → LONG):
spark.sql("ALTER TABLE silver.orders ALTER COLUMN amount TYPE BIGINT")
# → Works for widening casts (INT→LONG, FLOAT→DOUBLE)
# → Fails for narrowing (LONG→INT) — data loss risk
```

---

## 12. STAR Answers for FAANG

### Q1: "Why did you choose Parquet over CSV for your data lake?"

**Situation:** When I joined the team, all our Silver and Gold tables were stored as CSV files on ADLS. A daily revenue report that read 500GB of CSV took 45 minutes to run and was frequently timing out.

**Task:** Migrate the storage format to improve query performance without changing the business logic.

**Action:**
1. **Measured the problem**: Profiled the slow query with `explain("formatted")`. Saw `Scan csv` with no `PushedFilters` doing actual skipping — all 500GB was being read to answer a query filtered to one week of data.
2. **Migrated to Parquet**:
   ```python
   spark.read.csv("adls://container/silver/orders/") \
       .write.partitionBy("year", "month") \
       .parquet("adls://container/silver_v2/orders/")
   ```
3. **Enabled column pruning**: The report only used 8 of 65 columns — with Parquet, only those 8 column chunks were read.
4. **Row group skipping**: With `partitionBy("year","month")`, combined with Parquet min/max stats on `order_date`, ~92% of the file was skipped for a 7-day query.
5. **Applied Snappy compression**: 500GB CSV → 45GB Parquet (11x compression).

**Result:** Query dropped from 45 minutes to 3 minutes. Storage cost reduced by 91% (500GB → 45GB). Migrated 8 other tables in the following sprint using the same pattern. Established Parquet as the mandatory format for all Silver/Gold tables.

---

### Q2: "How do you handle schema evolution in a production pipeline?"

**Situation:** Our Bronze layer received raw JSON from an IoT platform. The upstream team pushed a firmware update that added 3 new sensor fields to the JSON payload — without coordinating with us. Our Silver pipeline failed at 3 AM because the schema mismatch broke a `from_json` call with a hardcoded schema.

**Task:** Fix the immediate failure and build a schema evolution strategy that handles future upstream changes without incidents.

**Action:**
1. **Immediate fix**: Changed `from_json` to use `schema_of_json` to infer schema from a sample, combined with `PERMISSIVE` mode — unknown fields were captured in `_corrupt_record` column.
2. **Short-term strategy**: Migrated Bronze to Auto Loader with `cloudFiles.schemaEvolutionMode = "rescue"` — new fields went to `_rescued_data` JSON column automatically. Silver read known fields directly; new fields extracted from `_rescued_data` after validation.
3. **Long-term strategy**: Introduced Avro + Confluent Schema Registry for the IoT Kafka topic. Any schema change must be registered as a new schema version. Schema Registry enforces backward compatibility — adding a field without a default is REJECTED before the producer can publish.
4. **Process change**: Added a Slack channel `#schema-changes` — Schema Registry webhook posts there on any schema registration. Silver team gets 48 hours notice before new schema version goes live.

**Result:** Zero schema-related pipeline failures in the 10 months following the changes. The Schema Registry rejection caught 3 attempted backward-incompatible changes by the upstream team before they reached production.

---

### Q3: "What is Delta Lake and why is it better than plain Parquet for production?"

**Situation:** Standard system design question — "Design a data lakehouse for a fintech company processing 50M transactions/day with strict audit requirements."

**Task:** Justify the format choices in the architecture.

**Action (the answer I gave):**

"I'd use Delta Lake at every layer — Bronze, Silver, and Gold. Here's why it's better than plain Parquet for this use case:

**ACID transactions**: With 50M transactions/day, multiple jobs will write to the same table. Plain Parquet has no concurrency control — two jobs writing simultaneously can corrupt the table or cause last-write-wins data loss. Delta uses optimistic concurrency with the transaction log as a serialization point.

**Audit requirements**: The transaction log is a built-in audit trail. Every write is a commit with a timestamp and operation type. `DESCRIBE HISTORY orders` shows every change ever made. Time travel (`VERSION AS OF 42`) lets auditors see exactly what the table contained at any point in history.

**Streaming + batch on the same table**: The reconciliation job (batch) and the real-time ingest job (streaming) can both write to the same Delta table safely. Plain Parquet would have race conditions.

**Data corrections**: In fintech, you WILL need to correct records. With plain Parquet, a DELETE requires rewriting entire files. Delta's Deletion Vectors mark deleted rows as a bitmap — near-zero write amplification.

**Two-level data skipping**: Delta's `_delta_log` stores file-level min/max stats for every column in every Parquet file. Before even opening a Parquet file, Spark checks if it can be skipped. This is in addition to Parquet's own row-group-level stats — two layers of skipping.

The only place I'd use plain Parquet is for cross-system exports where the receiving system doesn't understand Delta. Everything else is Delta."

**Result:** Got follow-up on Delta's transaction log mechanism and VACUUM retention — which I answered using the exact details of the `_delta_log` structure, leading to a deeper technical discussion that the interviewer said was "the best format answer I've heard."

---

## 13. Quick Reference — Format Decision Card

```
RECEIVING data from outside:
  Text/Excel/BI exports  →  CSV
  Kafka events           →  Avro + Schema Registry
  REST APIs / webhooks   →  JSON
  Partner data dumps     →  CSV or Parquet (ask them)

STORING data internally:
  Bronze raw layer       →  Keep original format OR Delta
  Silver/Gold tables     →  Delta Lake (always)
  Hive-only workloads    →  ORC
  One-time temp files    →  Parquet
  ML features            →  Parquet or Delta

SENDING data outside:
  To Excel/dashboards    →  CSV
  To external Kafka      →  Avro
  To another data team   →  Parquet (universal)
  To BigQuery/Athena     →  Parquet (native support)

REMEMBER:
  Delta = Parquet + transaction log (not a different format)
  CSV predicate pushdown = reads every row (NO actual skipping)
  Parquet predicate pushdown = skips entire row groups (REAL skipping)
  Delta predicate pushdown = skips entire FILES + row groups (BEST skipping)
  Avro = fastest writes, slowest analytical reads
```
