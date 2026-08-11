# Delta Live Tables (DLT) — Complete FAANG Interview Guide
## Declarative, Managed Pipeline Framework for Databricks

---

## 1. What Is DLT?

Delta Live Tables (DLT) is Databricks' **declarative pipeline framework** that automates infrastructure management, data quality enforcement, pipeline orchestration, and observability for both batch and streaming pipelines.

### The Core Value Proposition
Instead of writing imperative Spark code that says **"how"** to process data, DLT lets you declare **"what"** the data should look like — Databricks handles execution, retries, scaling, and quality enforcement.

### The Mental Model
```
Traditional Streaming:
  You write:  readStream → transform → writeStream → manage checkpoint → handle retries

DLT:
  You write:  @dlt.table (define schema + transformation)
  Databricks: manages cluster, checkpoint, retry, lineage, quality metrics
```

---

## 2. Core Concepts

### Table Types

| Type | Decorator | Storage | Use |
|------|-----------|---------|-----|
| Streaming Live Table | `@dlt.table` + `readStream` | Materialized Delta table | Append-only sources (Kafka, Auto Loader) |
| Live Table (Materialized View) | `@dlt.table` | Materialized Delta table | Batch transformations, joins |
| View | `@dlt.view` | Not persisted | Intermediate transforms, avoid duplication |

### Pipeline Modes

| Mode | Behavior | Cost | Use Case |
|------|----------|------|----------|
| **Triggered** | Runs once, processes all data, cluster terminates | Pay-per-run | Batch/scheduled pipelines |
| **Continuous** | Cluster stays on, processes continuously | Always-on | Near-real-time streaming |

---

## 3. Complete Syntax Reference

### Basic Table Definition
```python
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ─── Bronze: Raw Ingest from Auto Loader ───────────────────────────
@dlt.table(
    name="bronze_orders",
    comment="Raw order events from landing zone",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/pipeline/schema/orders")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/mnt/landing/orders/")
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

# ─── Bronze: Raw Ingest from Kafka ─────────────────────────────────
@dlt.table(name="bronze_events_kafka")
def bronze_events_kafka():
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("user_id", LongType()),
        StructField("event_type", StringType()),
        StructField("payload", StringType()),
        StructField("event_time", TimestampType())
    ])
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", spark.conf.get("kafka.bootstrap.servers"))
        .option("subscribe", "user_events")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50000)
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("d"), col("timestamp"))
        .select("d.*", col("timestamp").alias("kafka_ts"))
    )
```

### Silver with Data Quality Expectations
```python
# ─── Expectation Modes ─────────────────────────────────────────────
# @dlt.expect                 → track violations, but keep ALL rows
# @dlt.expect_or_drop         → drop violating rows, track count
# @dlt.expect_or_fail         → fail the entire pipeline on any violation
# @dlt.expect_all             → multiple expectations, WARN mode
# @dlt.expect_all_or_drop     → multiple expectations, DROP mode
# @dlt.expect_all_or_fail     → multiple expectations, FAIL mode

@dlt.table(
    name="silver_orders",
    comment="Validated and cleaned orders"
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("positive_amount", "amount > 0")
@dlt.expect_or_drop("valid_status", "status IN ('PENDING', 'COMPLETED', 'CANCELLED', 'REFUNDED')")
@dlt.expect_or_fail("no_null_customer", "customer_id IS NOT NULL")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")              # Read from Bronze (streaming)
        .select(
            col("order_id").cast("long"),
            col("customer_id").cast("long"),
            col("amount").cast("decimal(10,2)"),
            col("status"),
            col("created_at").cast("timestamp"),
            col("_source_file"),
            col("_ingested_at")
        )
        .filter(col("order_id").isNotNull())          # Pre-filter before expectations
    )

# ─── Multiple Expectations Dict ───────────────────────────────────
rules = {
    "valid_order_id": "order_id IS NOT NULL",
    "positive_amount": "amount > 0",
    "valid_status": "status IN ('PENDING', 'COMPLETED', 'CANCELLED', 'REFUNDED')",
    "recent_order": "created_at >= '2020-01-01'"
}

@dlt.table(name="silver_orders_v2")
@dlt.expect_all_or_drop(rules)
def silver_orders_v2():
    return dlt.read_stream("bronze_orders").select(...)
```

### Quarantine Pattern (Capture Bad Records)
```python
# Quarantine: use expect (warn) + filter downstream
@dlt.table(name="silver_orders_valid")
@dlt.expect("valid_amount", "amount > 0")
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
def silver_orders_valid():
    return (
        dlt.read_stream("bronze_orders")
        .filter("amount > 0 AND order_id IS NOT NULL")   # Only valid records
    )

@dlt.table(name="silver_orders_quarantine", comment="Records failing quality checks")
def silver_orders_quarantine():
    return (
        dlt.read_stream("bronze_orders")
        .filter("amount <= 0 OR order_id IS NULL")       # Only invalid records
        .withColumn("_quarantine_reason",
            when(col("order_id").isNull(), "null_order_id")
            .when(col("amount") <= 0, "non_positive_amount")
            .otherwise("unknown")
        )
    )
```

### CDC with APPLY CHANGES INTO
```python
# CDC: Process Debezium/DMS change events (insert/update/delete)
dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 1 — current state of customers"
)

dlt.apply_changes(
    target = "silver_customers",
    source = "bronze_customers_cdc",            # Source streaming table
    keys = ["customer_id"],                     # PK for MERGE
    sequence_by = col("cdc_timestamp"),         # How to order events (latest wins)
    apply_as_deletes = expr("op = 'D'"),        # Delete events
    apply_as_truncates = expr("op = 'T'"),      # Truncate events (optional)
    except_column_list = ["op", "cdc_timestamp", "_before"],  # Exclude CDC metadata cols
    stored_as_scd_type = "1"                    # SCD Type 1: current state only
    # stored_as_scd_type = "2"                  # SCD Type 2: full history (DBR 12.1+)
)

# SCD Type 2 — full history with effective dates
dlt.apply_changes(
    target = "silver_customers_history",
    source = "bronze_customers_cdc",
    keys = ["customer_id"],
    sequence_by = col("cdc_timestamp"),
    apply_as_deletes = expr("op = 'D'"),
    stored_as_scd_type = "2",
    track_history_column_list = ["email", "address", "tier"]  # Only track changes to these cols
)
```

### Gold Aggregation Table
```python
# Gold: Business-level aggregation
@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregated by product and region"
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")                       # Static read (batch)
        .filter("status = 'COMPLETED'")
        .groupBy(
            to_date("created_at").alias("order_date"),
            "product_category",
            "region"
        )
        .agg(
            sum("amount").alias("total_revenue"),
            count("order_id").alias("order_count"),
            avg("amount").alias("avg_order_value")
        )
    )
```

---

## 4. Pipeline Configuration (YAML / JSON)

### `pipeline.yml` (DLT Pipeline Settings)
```yaml
# Pipeline settings — configured in Databricks UI or via API
name: "ecommerce-ingestion-pipeline"
target: "ecommerce_dev"                         # Target schema

clusters:
  - label: default
    node_type_id: Standard_D8s_v3
    autoscale:
      min_workers: 2
      max_workers: 10
      mode: ENHANCED                             # ENHANCED (recommended) | LEGACY

libraries:
  - notebook:
      path: /Repos/team/pipeline/bronze_layer
  - notebook:
      path: /Repos/team/pipeline/silver_layer
  - notebook:
      path: /Repos/team/pipeline/gold_layer

configuration:
  kafka.bootstrap.servers: "broker1:9092,broker2:9092"
  spark.sql.shuffle.partitions: "200"
  spark.databricks.delta.schema.autoMerge.enabled: "true"

continuous: false                               # true = continuous mode
development: false                             # true = dev mode (no retries, verbose)
photon: true                                   # Enable Photon engine
```

### Access Pipeline Config in Notebook
```python
# Read pipeline parameters at runtime
bootstrap = spark.conf.get("kafka.bootstrap.servers")
env = spark.conf.get("env", "dev")             # default = dev if not set
```

---

## 5. DLT Event Log — Monitoring & Debugging

```sql
-- DLT Event Log is a Delta table auto-created per pipeline
-- Path: /pipelines/<pipeline_id>/system/events

-- Pipeline health overview (last 24h)
SELECT
  timestamp,
  event_type,
  level,
  details
FROM delta.`/pipelines/<pipeline_id>/system/events`
WHERE timestamp > current_timestamp() - INTERVAL 24 HOURS
ORDER BY timestamp DESC;

-- Data quality: expectation pass/fail rates
SELECT
  details:flow_progress.metrics.num_output_rows AS output_rows,
  details:flow_progress.data_quality.dropped_records AS dropped,
  details:flow_progress.data_quality.expectations[0].name AS expectation,
  details:flow_progress.data_quality.expectations[0].passed_records AS passed,
  details:flow_progress.data_quality.expectations[0].failed_records AS failed,
  timestamp
FROM delta.`/pipelines/<pipeline_id>/system/events`
WHERE event_type = 'flow_progress'
  AND details:flow_progress.status = 'COMPLETED'
ORDER BY timestamp DESC;

-- Failed pipelines
SELECT timestamp, details:error.exceptions[0].message AS error_msg
FROM delta.`/pipelines/<pipeline_id>/system/events`
WHERE level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 10;

-- Throughput per table per update
SELECT
  details:flow_progress.origin.flow_name AS table_name,
  details:flow_progress.metrics.num_output_rows AS rows_written,
  details:flow_progress.metrics.num_output_bytes AS bytes_written,
  timestamp
FROM delta.`/pipelines/<pipeline_id>/system/events`
WHERE event_type = 'flow_progress'
  AND details:flow_progress.status = 'COMPLETED'
ORDER BY timestamp DESC;
```

---

## 6. Key DLT Behaviors to Know

### `dlt.read()` vs `dlt.read_stream()`
```python
# dlt.read_stream() — streaming read (micro-batch or continuous)
# Use for: bronze/silver tables receiving new data continuously
@dlt.table
def silver_orders():
    return dlt.read_stream("bronze_orders")    # Incremental processing

# dlt.read() — batch/static read
# Use for: gold aggregations, dimension lookups, full table recompute
@dlt.table
def gold_daily_summary():
    return dlt.read("silver_orders")           # Reads entire table each update
```

### Table Properties for Performance
```python
@dlt.table(
    table_properties={
        # Auto-optimize (compaction + Z-ORDER)
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "customer_id,order_date",
        
        # Liquid clustering (preferred over Z-ORDER in DBR 13.3+)
        "delta.enableDeletionVectors": "true",
        
        # Bloom filter
        "delta.bloomFilter.enabled": "true",
        "delta.bloomFilter.columnNames": "customer_id,order_id",
        
        # Change data feed (so downstream can read changes)
        "delta.enableChangeDataFeed": "true"
    }
)
```

---

## 7. Pros and Cons

### Pros
| Benefit | Detail |
|---------|--------|
| Managed infrastructure | No cluster config, checkpoint management, retry logic needed |
| Declarative quality | `expect` / `expect_or_drop` / `expect_or_fail` built-in |
| CDC in one line | `APPLY CHANGES INTO` replaces complex MERGE logic |
| Automatic lineage | Full column-level lineage in Unity Catalog |
| Built-in observability | DLT Event Log, Databricks UI dashboard, quality metrics |
| Auto dependency resolution | DLT builds the DAG from `dlt.read()` references |
| Schema enforcement | Auto-enforces schema on write |
| `development` mode | No retries, verbose error display — fast iteration |
| Multi-task pipelines | One pipeline can have 100s of tables |
| Serverless option | DLT Serverless — zero cluster management |

### Cons
| Limitation | Detail |
|------------|--------|
| Less control | Cannot fully customize executor behavior, custom partitioners |
| Debugging harder | Errors in DLT event log, not always obvious |
| Cost premium | DLT adds ~10-20% compute overhead vs raw Structured Streaming |
| Cannot use arbitrary sinks | Only Delta tables as output (no Kafka sink, no custom writer) |
| No arbitrary Python in pipeline | Only functions decorated with `@dlt.table` / `@dlt.view` |
| SCD Type 2 is DBR 12.1+ | Older runtimes only support SCD Type 1 |
| Not portable | DLT is Databricks-only (not OSS) |
| `foreachBatch` not supported natively | Must use workarounds or use raw Structured Streaming |
| Triggered pipeline cold start | Cluster startup time (~5 min) added to every triggered run |

---

## 8. Trade-offs

### DLT vs Raw Structured Streaming
| Dimension | DLT | Structured Streaming |
|-----------|-----|---------------------|
| Setup complexity | Low (declarative) | High (manage checkpoint, retry, cluster) |
| Data quality | Built-in (`expect`) | Manual (filter + write to quarantine) |
| CDC | `APPLY CHANGES INTO` | Write MERGE logic in `foreachBatch` |
| Debugging | Event log, DLT UI | Spark UI, query.recentProgress |
| Output sinks | Delta only | Kafka, Delta, JDBC, custom |
| Control | Limited | Full |
| Portability | Databricks-only | Any Spark cluster |
| Cost | ~10-20% more | Baseline |
| **Choose when** | Managed pipelines, quality gates, CDC | Custom sink, full control, portability |

### DLT Triggered vs Continuous
| Dimension | Triggered | Continuous |
|-----------|-----------|------------|
| Cluster | Starts, processes, terminates | Always on |
| Cost | Pay per run | Always-on cost |
| Latency | Minutes (incl. cluster start) | Seconds |
| Use case | Hourly/daily SLA | Real-time SLA (<5 min) |
| **Rule of thumb** | If SLA > 15 min → Triggered | If SLA < 5 min → Continuous |

### DLT Serverless vs Classic
| Dimension | Serverless | Classic |
|-----------|------------|---------|
| Cluster management | Zero | You configure node type, size |
| Startup time | Faster | ~5 min cold start |
| Cost | Premium pricing | Standard DBU pricing |
| Control | Less | More |
| **Best for** | Dev/test, cost predictability | Production, cost optimization |

---

## 9. STAR Answers for FAANG

### Q1: "Describe a complex data pipeline you designed and built"

**Situation:** We were building a multi-source e-commerce data platform. Data arrived from: (1) MySQL CDC via Debezium/Kafka for transactional data, (2) S3 landing zone for batch partner feeds (CSV/JSON), (3) Snowflake for finance data (monthly). The platform needed Bronze/Silver/Gold layers with strict data quality SLAs — no invalid customer IDs in Gold, no negative amounts in Silver.

**Task:** Build a unified ingestion pipeline that handles all 3 source types, enforces quality at each layer, and provides end-to-end lineage for GDPR audit purposes.

**Action:**
1. **Chose DLT** as the framework — it auto-resolved the dependency DAG across all 3 source streams
2. **Bronze layer**: 3 DLT tables — `bronze_mysql_cdc` (Kafka source), `bronze_partner_feeds` (Auto Loader from S3), `bronze_finance` (COPY INTO scheduled monthly)
3. **Silver CDC**: Used `APPLY CHANGES INTO` with `stored_as_scd_type = "1"` for current state. Schema evolution handled via `cloudFiles.schemaEvolutionMode = "addNewColumns"`.
4. **Data quality**: Applied `@dlt.expect_or_drop` on Silver for structural rules (not-null, type checks). Applied `@dlt.expect_or_fail` on Gold for business-critical rules (customer must exist in customer master).
5. **Quarantine tables**: Any row dropped by Silver expectations was written to `silver_*_quarantine` tables with `_quarantine_reason` column. A Slack alert triggered when quarantine ingestion rate exceeded 1%.
6. **Lineage**: DLT + Unity Catalog gave automatic column-level lineage — during a GDPR deletion request, we traced all tables containing the customer's data in 2 minutes.

**Result:** Pipeline processed 8M events/day across 3 sources. Data quality violation rate was 0.03% (within SLA). GDPR deletion scope identification dropped from 4 hours (manual) to 3 minutes (Unity Catalog lineage). The pipeline ran for 12 months with 3 total incidents, all recovered automatically by DLT retry.

---

### Q2: "How did you implement CDC (Change Data Capture) with data quality enforcement?"

**Situation:** We needed to migrate a MySQL customer database (12M rows, active updates 24/7) into our Delta Lake Silver layer. The downstream Gold layer required current state only (SCD Type 1), but the compliance team needed 90-day change history (SCD Type 2) for audit.

**Task:** Implement CDC that maintained both SCD Type 1 (current state) and SCD Type 2 (90-day history) while enforcing data quality rules like valid email format and non-null customer IDs.

**Action:**
1. **Bronze**: Debezium → Kafka → DLT Bronze streaming table (`bronze_customers_cdc`) with `from_json` deserialization, raw CDC events stored as-is
2. **Quality on Bronze**: `@dlt.expect("valid_op", "op IN ('c', 'u', 'd', 'r')")` — fail fast on unexpected op codes
3. **Silver SCD Type 1** (current state for Gold consumption):
   ```python
   dlt.apply_changes(
       target="silver_customers",
       source="bronze_customers_cdc",
       keys=["customer_id"],
       sequence_by=col("cdc_timestamp"),
       apply_as_deletes=expr("op = 'd'"),
       stored_as_scd_type="1"
   )
   ```
4. **Silver SCD Type 2** (history for compliance):
   ```python
   dlt.apply_changes(
       target="silver_customers_history",
       source="bronze_customers_cdc",
       keys=["customer_id"],
       sequence_by=col("cdc_timestamp"),
       stored_as_scd_type="2",
       track_history_column_list=["email", "address", "tier", "status"]
   )
   ```
5. **Quality on Silver**: `@dlt.expect_or_drop("valid_email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")` — invalid emails dropped to quarantine

**Result:** Both SCD tables built from the same Bronze source — no duplicated processing. Compliance team had 90-day history available in `silver_customers_history`. Gold queries ran against `silver_customers` (current state only). CDC latency was <90 seconds from MySQL commit to Silver table. DLT `APPLY CHANGES INTO` replaced ~200 lines of custom MERGE logic.

---

### Q3: "How do you handle pipeline failures and ensure data quality in production?"

**Situation:** 6 months into production, our DLT pipeline started failing 2-3 times per week. The failures were non-deterministic — sometimes schema issues, sometimes Kafka connectivity, sometimes a bad upstream data batch with 1000s of rows failing validation. On-call engineers were spending 45 min each incident investigating, restarting, and verifying correctness.

**Task:** Reduce MTTR (Mean Time to Recovery) from 45 minutes to under 10 minutes, and prevent bad data from propagating to Gold tables.

**Action:**
1. **Failure classification**: Analyzed 3 months of DLT Event Log. Found 3 categories:
   - Schema drift (40%): new field in Kafka payload broke the `from_json` with hardcoded schema
   - Transient Kafka connectivity (35%): DLT retried and self-healed (we were over-alerting)
   - Bad data spikes (25%): 10K+ rows with null customer_id in one batch
2. **Schema drift fix**: Changed Bronze to use `schema_of_json` sampling + `from_json` in `PERMISSIVE` mode. New fields went to `_extra_fields` column. No more schema-change failures.
3. **Connectivity alert tuning**: Added `for: 10m` to the DLT failure alert — transient issues that recovered within 10 minutes no longer woke anyone up.
4. **Bad data protection**: Added `@dlt.expect_or_drop` on Silver for `customer_id IS NOT NULL`. Added a monitoring query on DLT Event Log: alert if `dropped_records / output_rows > 0.01` (>1% drop rate). This fires as a WARNING before it becomes a business problem.
5. **Runbook automation**: Created a Databricks notebook "incident-runbook" that queries DLT Event Log, shows last 10 errors with suggested actions, and has a single "restart pipeline" button. Posted in Slack channel on every alert.

**Result:** MTTR dropped from 45 minutes to 8 minutes. Alert noise reduced by 60% (transient connectivity no longer paged anyone). Zero bad data propagated to Gold in the 6 months following the changes. On-call engineers reported the runbook notebook cut investigation time from 30 minutes to 2 minutes.

---

## 10. DLT Pipeline Lifecycle (What Happens When You Click Run)

```
1. Pipeline Update Triggered
   │
2. Cluster Start (or reuse if Continuous mode)
   │  [~3-5 min for Triggered mode]
   │
3. Library Installation (if configured)
   │
4. Pipeline Graph Resolution
   │  DLT scans all @dlt.table functions
   │  Builds DAG from dlt.read() / dlt.read_stream() dependencies
   │  Validates no circular dependencies
   │
5. For each table in topological order:
   │  a. Check if table exists (create if not)
   │  b. Read from source (new data only, based on checkpoint)
   │  c. Apply transformations
   │  d. Evaluate expectations
   │  e. Write to Delta table (APPEND or MERGE for CDC)
   │  f. Commit checkpoint
   │  g. Log metrics to Event Log
   │
6. Pipeline Update Complete
   │
7. Cluster Terminates (Triggered mode only)
```

---

## 11. Key Internals to Memorize

1. **DLT stores pipeline state in `/pipelines/<id>/system/`** — event log, checkpoints, tables
2. **`dlt.read()` triggers a full recompute** every pipeline update (like a batch job)
3. **`dlt.read_stream()` is incremental** — only processes new data since last checkpoint
4. **`APPLY CHANGES INTO` uses a hidden `__apply_changes_storage_*` table** to track CDC sequences
5. **Expectations are evaluated on the Spark executor** — they're just SQL `WHERE` clauses internally
6. **DLT targets must be Delta tables** — you cannot write to Parquet, CSV, or Kafka from DLT
7. **Development mode**: pipeline runs with retries disabled and pipeline restart on code change — use for iteration, never in production
8. **Unity Catalog integration**: DLT tables in Unity Catalog have automatic column-level lineage visible in the Catalog Explorer
9. **Serverless DLT** uses a shared Photon cluster managed by Databricks — startup time ~30s vs ~5 min for classic
10. **`stored_as_scd_type = "2"` requires DBR 12.1+** — check runtime version before promising this feature
