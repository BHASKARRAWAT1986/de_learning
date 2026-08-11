# Spark Architecture — Complete Deep Dive
## Jobs, Stages, Tasks, Shuffle, Execution Plan — With Plain English Examples

---

## 1. The Big Picture — Spark Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        YOUR CODE (Driver JVM)                        │
│                                                                       │
│  SparkContext / SparkSession                                          │
│       │                                                               │
│       ├── DAGScheduler      (splits work into stages)                │
│       ├── TaskScheduler     (assigns tasks to executors)             │
│       └── BlockManager      (tracks where data lives)                │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ (sends tasks)
                                │
        ┌───────────────────────┼──────────────────────┐
        │                       │                      │
┌───────▼──────┐   ┌────────────▼─────┐   ┌───────────▼──────┐
│  Executor 1  │   │   Executor 2     │   │   Executor 3     │
│  (Worker JVM)│   │   (Worker JVM)   │   │   (Worker JVM)   │
│              │   │                  │   │                  │
│  ┌────────┐  │   │  ┌────────┐      │   │  ┌────────┐      │
│  │ Task 1 │  │   │  │ Task 2 │      │   │  │ Task 3 │      │
│  │ Task 4 │  │   │  │ Task 5 │      │   │  │ Task 6 │      │
│  └────────┘  │   │  └────────┘      │   │  └────────┘      │
│  BlockMgr    │   │  BlockMgr        │   │  BlockMgr        │
│  (local disk │   │  (local disk     │   │  (local disk     │
│   + memory)  │   │   + memory)      │   │   + memory)      │
└──────────────┘   └──────────────────┘   └──────────────────┘
```

### Key Components

| Component | Where It Lives | What It Does |
|-----------|---------------|--------------|
| **Driver** | Your laptop / cluster master node | Your main() program. Builds the plan, coordinates everything |
| **SparkContext** | Inside Driver | Entry point. Connects to cluster. Creates RDDs |
| **SparkSession** | Inside Driver | Modern entry point. Wraps SparkContext. Use this |
| **DAGScheduler** | Inside Driver | Converts your transformations into a DAG of stages |
| **TaskScheduler** | Inside Driver | Takes stages, creates tasks, sends them to executors |
| **Executor** | Worker node JVM | Runs your actual code. Has CPU cores + memory |
| **Task** | Inside Executor | Smallest unit of work. Processes ONE partition |
| **BlockManager** | Inside each Executor + Driver | Manages data blocks (cached RDDs, shuffle data, broadcast vars) |
| **Cluster Manager** | Separate service | YARN / Kubernetes / Databricks — allocates resources |

---

## 2. Lame Analogy — The Restaurant Kitchen

```
You (Driver)          = Head Chef who decides the menu and assigns work
Recipe (DAG)          = The plan for making a dish
Sous Chefs (Executors) = Workers in the kitchen who actually cook
One Dish (Task)       = One unit of work assigned to one sous chef
Course (Stage)        = All sous chefs working on the same course simultaneously
Full Meal (Job)       = Everything from start to finish for one ORDER (action)
Ingredients (Partition) = The data chunk each sous chef works on
```

---

## 3. How Your Code Becomes Execution (Step by Step)

```python
# Your code
df = spark.read.csv("s3://bucket/orders.csv")          # Transformation
df2 = df.filter("amount > 100")                         # Transformation
df3 = df2.groupBy("country").sum("amount")              # Transformation (wide — shuffle!)
df3.show()                                              # ACTION — triggers execution
```

### What Spark Does Internally

```
Step 1: df3.show() called → Spark starts planning

Step 2: SparkSession sends logical plan to Catalyst Optimizer
        Logical Plan:
          Show
           └── Aggregate (groupBy country, sum amount)
                └── Filter (amount > 100)
                     └── Scan CSV (orders.csv)

Step 3: Catalyst Optimizer transforms logical plan
        - Predicate pushdown: filter recorded in logical plan
          ⚠️  CSV has NO embedded statistics → Spark still reads EVERY row
              The filter is applied ROW-BY-ROW during reading, not skipped
              Predicate pushdown with actual row-skipping only works in
              Parquet, ORC, Delta (which embed min/max stats per row group)
        - Column pruning: ⚠️  Does NOT work for CSV — CSV is row-based
              (a row is one flat text line; you must parse the whole line
               to extract even one field)
              Column pruning DOES work for Parquet/ORC (columnar formats)
        - Cost-based optimization: picks join strategies

Step 4: Physical Plan created
        - Scan CSV → Filter → HashAggregate (partial) → Exchange (shuffle) → HashAggregate (final)

Step 5: DAGScheduler splits physical plan at shuffle boundaries
        Stage 0: Scan + Filter + HashAggregate (partial)    ← pre-shuffle
        Stage 1: Exchange (read shuffle) + HashAggregate (final)  ← post-shuffle

Step 6: TaskScheduler creates Tasks
        Stage 0: 8 tasks (one per CSV partition)
        Stage 1: 200 tasks (spark.sql.shuffle.partitions = 200 by default)

Step 7: Tasks sent to Executors, executed in parallel

Step 8: Results collected back to Driver → .show() prints output
```

---

## 4. Jobs, Stages, and Tasks — Exact Definitions

### Job
- Created by ONE **action** (`show()`, `collect()`, `count()`, `write()`, `save()`)
- A job is the ENTIRE computation needed to satisfy that one action
- One Spark application can have many jobs (e.g., 3 `.count()` calls = 3 jobs)

```python
df.count()    # Job 1
df.show()     # Job 2
df.write.save() # Job 3
```

### Stage
- A stage is a set of tasks that can run **without shuffling data between them**
- Stages are separated by **shuffle boundaries** (groupBy, join, repartition, sort)
- Within a stage, all operations are **pipelined** — run together on one pass through the data
- Stages run **sequentially** (Stage 1 must complete before Stage 2 can read shuffle output)

```
Lame example:
  Stage 0: Read file → filter → partial sum   (no data movement between executors)
  [SHUFFLE] — data moves between executors
  Stage 1: Read shuffle → final sum → output  (no data movement)
```

### Task
- A task is the SMALLEST unit of work
- ONE task processes ONE partition of data
- Tasks within a stage run **in parallel** (one per executor core)
- If you have 10 partitions in Stage 0 → Spark creates 10 tasks for Stage 0

```
Lame example:
  File has 8 CSV parts → 8 partitions → 8 tasks in Stage 0
  Each task: reads its CSV part → applies filter → computes partial sum

  spark.sql.shuffle.partitions = 200
  → After shuffle: 200 partitions → 200 tasks in Stage 1
  → Each task reads its shuffle bucket → computes final sum for its group of countries
```

### Visual Breakdown

```
ACTION: df.groupBy("country").sum("amount").show()

JOB 0
├── STAGE 0  (no shuffle needed within this stage)
│     ├── Task 0: partition_0.csv → filter → partial_agg
│     ├── Task 1: partition_1.csv → filter → partial_agg
│     ├── Task 2: partition_2.csv → filter → partial_agg
│     └── Task 3: partition_3.csv → filter → partial_agg
│     [ALL 4 TASKS RUN IN PARALLEL]
│
│     [SHUFFLE BOUNDARY — data written to disk, redistributed]
│
└── STAGE 1  (reads shuffle output)
      ├── Task 0: shuffle_bucket_0 → final_agg → output
      ├── Task 1: shuffle_bucket_1 → final_agg → output
      ├── ...
      └── Task 199: shuffle_bucket_199 → final_agg → output
      [ALL 200 TASKS RUN IN PARALLEL]
```

---

## 5. How Tasks Are Executed — Inside an Executor

```
Executor JVM
├── Memory Pool (Executor Memory)
│     ├── Storage Memory  (60% of usable)  ← cached RDDs/DataFrames
│     └── Execution Memory (40% of usable) ← shuffle buffers, sort, hash tables
│         (These two share a unified pool — one can borrow from the other)
│
├── CPU Cores (slots)
│     Each core = one task slot
│     4 cores → 4 tasks run simultaneously
│
└── Local Disk
      └── Shuffle Write Output (spilled sort/hash data)
```

### Task Execution Lifecycle

```
1. TaskScheduler sends serialized Task to Executor
   (Task contains: which partition to read, what operations to apply)

2. Executor deserializes the Task

3. Executor reads its partition data:
   - From HDFS/S3 (if reading original data)
   - From shuffle files on local disk (if post-shuffle stage)
   - From BlockManager cache (if data is cached)

4. Executor applies transformations:
   - Each row passes through the ENTIRE pipeline in one pass
   - Example: read row → filter → project → partial aggregate
   - This is called "volcano iterator model" or "pipeline execution"

5. If the stage ends before a shuffle:
   - Task writes shuffle output to local disk (shuffle write)
   - Driver's MapOutputTracker notes: "Task X wrote to disk at Executor Y"

6. If this is the final stage:
   - Task sends result back to Driver (for .collect() / .show())
   - Or writes to Delta/Parquet/etc (for .write())

7. Task reports success/failure to Driver
```

### Pipelined Execution (Important!)

```python
df.filter("amount > 100").select("country", "amount").groupBy("country").sum()

# Each ROW passes through all operations before the next row is processed:
Row 1: read → filter (pass) → select → partial_agg
Row 2: read → filter (FAIL — dropped)
Row 3: read → filter (pass) → select → partial_agg
...

# This is NOT:
# Step 1: read ALL rows
# Step 2: filter ALL rows  ← this would require storing the whole dataset
# Step 3: select ALL rows
```

---

## 6. Shuffle — The Most Expensive Operation in Spark

### What Is a Shuffle?

A shuffle happens when data needs to be **moved between executors** so that the right rows end up on the same executor.

**Why?** Because `groupBy("country")` needs ALL rows for "USA" on the SAME executor to compute the sum. But after reading the file, "USA" rows are scattered across all executors.

```
Before Shuffle (data is scattered):
  Executor 1: [USA, 50] [UK, 30] [USA, 80]
  Executor 2: [UK, 20] [FR, 40] [USA, 10]
  Executor 3: [FR, 60] [UK, 15] [USA, 25]

After Shuffle (same key goes to same executor):
  Executor 1: [USA, 50] [USA, 80] [USA, 10] [USA, 25]  ← all USA rows
  Executor 2: [UK, 30] [UK, 20] [UK, 15]               ← all UK rows
  Executor 3: [FR, 40] [FR, 60]                         ← all FR rows

Now each executor can compute its group's sum independently.
```

### Shuffle Internal Mechanism — Step by Step

```
SHUFFLE WRITE PHASE (happens at end of Stage 0 — "Map" side)

Each task (map-side):
1. Computes its partial aggregation
2. For each output row, determines the TARGET partition:
     target_partition = hash(groupBy_key) % numShufflePartitions
     e.g., hash("USA") % 200 = 47  → goes to bucket 47
          hash("UK")  % 200 = 123 → goes to bucket 123

3. Writes rows to LOCAL DISK files (shuffle files):
     /tmp/spark-xxxx/executor-0/shuffle_0_0_0.data  ← task 0, shuffle 0, map output 0
     /tmp/spark-xxxx/executor-0/shuffle_0_0_0.index ← byte offsets for each partition
                                                        in the .data file

4. Reports to Driver's MapOutputTracker:
     "I wrote shuffle output. My .data file is at executor-0:/tmp/spark-xxxx/..."
```

```
SHUFFLE READ PHASE (happens at start of Stage 1 — "Reduce" side)

Each task (reduce-side):
1. Asks Driver: "Where is shuffle data for partition 47?"
2. Driver: "Check executor-0 file X, executor-1 file Y, executor-2 file Z"
3. Task makes HTTP requests to BlockManagers on OTHER executors:
     executor-0:BlockManager → serves bytes 0-4096 of its shuffle file (partition 47)
     executor-1:BlockManager → serves bytes 512-1024 of its shuffle file (partition 47)
     executor-2:BlockManager → serves bytes 200-600 of its shuffle file (partition 47)
4. Task merges all incoming data → applies final aggregation → produces output
```

### Where Shuffle Data Is Written

```
Local disk of EACH executor:
  /local_dir/spark-<appId>/executor-<id>/
    blockmgr-<uuid>/
      shuffle_<shuffleId>_<mapId>_0.data    ← actual row data (binary)
      shuffle_<shuffleId>_<mapId>_0.index   ← partition byte offsets

On Databricks (DBFS / local SSD):
  /local_disk0/spark-<appId>/... (NVMe SSD on most node types)

In Databricks:
  spark.conf.get("spark.local.dir") → shows where shuffle files land
  Default: /local_disk0 (fast SSD — this is why Databricks shuffle is fast)
```

### Shuffle Implementations in Spark

| Shuffle Manager | How It Works | When Used |
|-----------------|-------------|-----------|
| **SortShuffleManager** (default) | Sorts by partition ID, writes one .data + .index file per map task | Always — this is the default |
| **BypassMergeSortShuffleManager** | Writes one file per output partition (no sort needed) | When ≤ 200 reduce partitions AND no map-side aggregation |
| **Tungsten Unsafe Shuffle** | Off-heap binary sort (faster, avoids GC) | Auto-selected when safe to do so |

### Memory During Shuffle

```
Shuffle Write (Map side):
  1. Each output partition has a write buffer in EXECUTION MEMORY
  2. When buffer full → SPILL to local disk (merge later)
  3. If too many spills → "shuffle spill" in Spark UI → increase executor memory
  
  Config: spark.shuffle.file.buffer (default: 32KB) — write buffer size

Shuffle Read (Reduce side):
  1. Incoming data from other executors stored in read buffer
  2. If > execution memory available → spill to local disk
  
  Config: spark.reducer.maxSizeInFlight (default: 48MB) — read buffer per reduce task
```

### What Operations Cause a Shuffle?

```python
# SHUFFLE TRIGGERS (always create a new stage)
df.groupBy("col").agg(...)        # Requires same-key data on same executor
df.join(other_df, "key")          # Requires matching keys on same executor (SortMergeJoin)
df.repartition(n)                 # Explicitly redistributes data
df.distinct()                     # Needs all copies of same row on same executor
df.sort("col") / df.orderBy("col") # Global sort requires shuffle
df.coalesce(n)                    # (narrow if reducing, but can cause uneven load)
window_functions_with_partitionBy # Each window partition needs co-location

# NO SHUFFLE (same stage, narrow dependencies)
df.filter("col > 5")              # Each row independently filtered
df.select("col1", "col2")         # Each row independently projected
df.withColumn("x", col("y") + 1) # Each row independently transformed
df.map(func)                      # Each row independently mapped
df.union(other_df)                # Just concatenates — no data movement
```

---

## 7. The Execution Plan — How to Read It

### Three Levels of Plans

```python
df = spark.read.csv("orders.csv") \
         .filter("amount > 100") \
         .groupBy("country") \
         .sum("amount")

# Level 1: Unresolved Logical Plan
df.explain(mode="extended")   # Shows ALL 4 plans

# Level 2: Quick physical plan (most commonly used)
df.explain()

# Level 3: Formatted (most readable)
df.explain("formatted")

# In SQL:
spark.sql("EXPLAIN FORMATTED SELECT country, SUM(amount) FROM orders WHERE amount > 100 GROUP BY country")
```

### Reading `df.explain()` Output

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[country#5], functions=[sum(amount#6)])     ← STAGE 1 (post-shuffle)
   +- Exchange hashpartitioning(country#5, 200), ENSURE_REQUIREMENTS, [id=#45]  ← SHUFFLE
      +- HashAggregate(keys=[country#5], functions=[partial_sum(amount#6)])      ← STAGE 0
         +- Project [country#5, amount#6]                                        ← column pruning
            +- Filter (isnotnull(amount#6) && (amount#6 > 100.0))               ← predicate pushdown
               +- FileScan csv [country#5, amount#6] ...                        ← only 2 cols read

HOW TO READ (bottom to top = execution order):
  1. FileScan → reads CSV, only columns "country" and "amount" (column pruning applied)
  2. Filter → drops rows where amount <= 100 (predicate pushdown — happens during scan)
  3. Project → selects only needed columns
  4. HashAggregate (partial) → partial sum per country within each partition
  5. Exchange → SHUFFLE — redistributes by country hash
  6. HashAggregate (final) → final sum per country after shuffle

BOTTOM = first to execute
TOP = last to execute
```

### `explain("formatted")` — Best for Interviews

```
== Physical Plan ==
* HashAggregate (7)
+- Exchange (6)
   +- * HashAggregate (5)
      +- * Project (4)
         +- * Filter (3)
            +- * Scan csv (2)

(2) Scan csv
Output [2]: [country#5, amount#6]
ReadSchema: struct<country:string,amount:double>               ← schema declared
PushedFilters: [IsNotNull(amount), GreaterThan(amount,100.0)]
⚠️  IMPORTANT — "PushedFilters" for CSV does NOT mean row-skipping:
    - CSV has no embedded statistics (no min/max per block)
    - Spark STILL reads every single row from the file
    - The filter is applied row-by-row during parsing (slightly faster
      than a separate post-scan Filter step, but NO I/O is saved)
    - Column pruning also does NOT work: CSV is row-based text,
      Spark must parse the entire line to extract any column
    Real predicate pushdown (actual I/O skipping) only works with:
      Parquet → skips row groups based on min/max stats in file footer
      ORC     → skips stripes based on bloom filters + column stats
      Delta   → skips files based on min/max in _delta_log JSON

(3) Filter
Condition : (isnotnull(amount#6) AND (amount#6 > 100.0))
Input count: 1000000 rows
Output count: 350000 rows    ← AQE provides actual counts after execution

(5) HashAggregate
Keys [country#5]
Functions [partial_sum(amount#6)]
Aggregate Attributes [sum#12]

(6) Exchange
Input [2]: [country#5, sum#12]
Arguments: hashpartitioning(country#5, 200)   ← 200 shuffle partitions
           ENSURE_REQUIREMENTS

(7) HashAggregate (final)
Keys [country#5]
Functions [sum(finalmerge_sum(sum#12))]
```

### Key Things to Look For in EXPLAIN

```
1. FileScan — check:
   ReadSchema → are only needed columns being read? (column pruning working?)
   PushedFilters → is your WHERE clause pushed to the scan? (predicate pushdown working?)
   PartitionFilters → is partition pruning happening? (partition skipping working?)

2. Exchange (Shuffle) — check:
   hashpartitioning(col, 200) → how many shuffle partitions?
   Arguments: ENSURE_REQUIREMENTS → normal
   Arguments: REBALANCE_PARTITIONS → AQE coalescing small partitions

3. HashAggregate — good! Spark chose hash-based aggregation
   SortAggregate — worse, uses sorting (more spill risk)

4. BroadcastHashJoin — good for small table join (no shuffle)
   SortMergeJoin — shuffle required (both sides sorted)
   BroadcastNestedLoopJoin — very bad (Cartesian-ish, appears when join keys missing)

5. * (asterisk before operator) → Whole-Stage Code Generation active (fast)
   No asterisk → interpreted execution (slower)

6. isFinalPlan=false → AQE is active and may re-plan mid-execution
   isFinalPlan=true → plan is finalized (after AQE re-planning)
```

---

## 8. Spark UI — Where to See All This Live

### Spark UI URL
- On Databricks: Click **Compute → your cluster → Spark UI**
- Standalone/YARN: `http://driver-host:4040`

### Key Tabs

#### Jobs Tab
```
Lists all jobs triggered by actions
Each job shows: duration, status, number of stages

Click a job → see its stages
```

#### Stages Tab
```
Lists all stages
Each stage shows:
  - Duration
  - Tasks (total, succeeded, failed, skipped)
  - Input: how much data was read
  - Output: how much data was written
  - Shuffle Read: how much data came in via shuffle
  - Shuffle Write: how much data was written to shuffle files

LOOK FOR:
  - "Shuffle Spill (Memory)" → data spilled from memory to disk during shuffle
  - "Shuffle Spill (Disk)" → data read back from disk (double the I/O cost)
  - Large "Shuffle Read" → expensive shuffle, consider AQE or broadcast join
  - Skewed task duration → data skew (one task takes 10x longer than others)
```

#### Tasks Tab (inside a Stage)
```
Shows every individual task
Columns:
  - Duration → how long each task ran
  - GC Time → garbage collection (if > 10% of duration, executor memory too small)
  - Shuffle Read Size → how much shuffle data this task received
  - Shuffle Write Size → how much shuffle data this task produced

LOOK FOR:
  - One task much slower than others → DATA SKEW
  - High GC time → need more executor memory
  - Many "killed" tasks → executor OOM or spot instance preemption
```

#### SQL / DataFrame Tab
```
Shows EXPLAIN plans with timing information for each operator
Most useful for understanding where time is spent

LOOK FOR:
  - "time in operator" next to each node → which operation is slow?
  - Row counts → did filter actually reduce data significantly?
  - "spilled to disk" annotation → shuffle or sort spilling
```

#### Storage Tab
```
Shows cached DataFrames/RDDs
Columns:
  - Fraction Cached: what % of partitions are in memory
  - Memory Size: how much memory the cache uses
  - Disk Size: how much spilled to disk

If Fraction Cached < 100% → not all partitions fit in cache memory
```

---

## 9. Whole-Stage Code Generation (Tungsten)

### What It Is
Instead of interpreting each row through multiple operator objects, Spark **generates custom JVM bytecode** for a chain of operators — as if you had written the loop yourself.

```
WITHOUT code gen (interpreted):
  for each row:
    FilterOperator.process(row) → HashAggOperator.process(row) → ...
    (multiple virtual dispatch calls per row → slow)

WITH code gen (generated bytecode):
  for each row:
    if (row.amount > 100) {         // filter inlined
      country = row.getString(0);   // project inlined
      partialSum[country] += row.getDouble(1);  // agg inlined
    }
    // ALL in one tight loop — fits in CPU cache → fast
```

### How to Verify It's Active
```
In explain() output:
  *(1) FileScan csv     ← asterisk = code gen active for this operator
  *(2) Filter
  *(3) Project
  *(4) HashAggregate    ← all in the SAME code-gen stage (WholeStageCodegen)

  Exchange              ← NO asterisk (shuffle boundary breaks code gen)
  
  *(5) HashAggregate    ← new code-gen stage starts after shuffle
```

---

## 10. Memory Model — Where Does Data Live?

```
Executor JVM Memory Layout:
┌─────────────────────────────────────────────────────┐
│                   JVM Heap                           │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │        Spark Managed Memory Pool            │     │
│  │        (spark.memory.fraction = 0.6)        │     │
│  │                                             │     │
│  │  ┌──────────────────┐ ┌──────────────────┐  │     │
│  │  │  Storage Memory  │ │ Execution Memory │  │     │
│  │  │  (cached data)   │ │ (shuffle, sort,  │  │     │
│  │  │                  │ │  hash tables)    │  │     │
│  │  │  ← can borrow → │ │ ← can borrow →  │  │     │
│  │  └──────────────────┘ └──────────────────┘  │     │
│  │  (unified pool — either can use the other's  │     │
│  │   space when the other is idle)              │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │     User Memory (spark.memory.fraction=0.4) │     │
│  │     (your UDFs, data structures in code)    │     │
│  └─────────────────────────────────────────────┘     │
│                                                       │
│  ┌─────────────────────────────────────────────┐     │
│  │  Reserved Memory (300MB hardcoded)          │     │
│  │  (Spark internal objects)                   │     │
│  └─────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────┘

OFF-HEAP (optional, configured separately):
  Project Tungsten off-heap storage
  Avoids JVM GC pressure for binary data
  spark.memory.offHeap.enabled = true
  spark.memory.offHeap.size = 2g
```

---

## 11. Broadcast Variables and Accumulators

### Broadcast Variables
```python
# Problem: a small lookup table joins with a huge table
# Without broadcast: both sides shuffle → expensive
# With broadcast: small table sent to EVERY executor once → no shuffle

# Small lookup table (< 10MB typically, default threshold 10MB auto-broadcast)
country_codes = {"US": "United States", "UK": "United Kingdom", "FR": "France"}

# Broadcast it
bc_countries = spark.sparkContext.broadcast(country_codes)

# Use inside a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def get_country_name(code):
    return bc_countries.value.get(code, "Unknown")

df.withColumn("country_name", get_country_name(col("country_code")))

# What happens internally:
# Driver serializes the dict → sends it to each executor ONCE
# Executor stores it in BlockManager → all tasks on that executor reuse it
# Without broadcast: the dict would be serialized and sent WITH EACH TASK (wasteful)
```

### Accumulators
```python
# Accumulator: a counter/sum that tasks can write to, driver reads
# Used for monitoring and debugging

# Custom accumulator
bad_records = spark.sparkContext.accumulator(0)

def process_row(row):
    global bad_records
    if row.amount < 0:
        bad_records += 1
    return row

df.foreach(process_row)
print(f"Bad records found: {bad_records.value}")

# WARNING: Accumulators in transformations (not actions) may be double-counted
# (if task re-executed due to failure, accumulator incremented twice)
# Safe pattern: only use accumulators inside actions (foreach, foreachPartition)
```

---

## 12. Partitions — The Unit of Parallelism

```python
# See how many partitions your DataFrame has
df.rdd.getNumPartitions()     # e.g., 8

# What determines initial partition count?
# 1. Reading files: number of file blocks (128MB default HDFS block = 1 partition)
# 2. After shuffle: spark.sql.shuffle.partitions (default: 200)
# 3. rdd.parallelize(data, numPartitions)

# Set shuffle partitions (most important tuning knob)
spark.conf.set("spark.sql.shuffle.partitions", "50")    # 50 instead of 200 for small data
# Rule: aim for 100MB–200MB of data per partition after shuffle

# Check partition sizes after a shuffle:
df.rdd.mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))]).collect()
# Output: [(0, 5000), (1, 4980), (2, 200), ...]
#          ↑ partitions 0 and 1 healthy   ↑ partition 2 tiny (skew?)

# Repartition (triggers shuffle) vs Coalesce (no shuffle, just merge)
df.repartition(100)           # Shuffle → evenly distribute
df.repartition(col("country")) # Shuffle → same-key rows on same partition
df.coalesce(10)               # No shuffle → merge existing partitions
                              # (can create uneven partitions if data was skewed)
```

---

## 13. Lame End-to-End Example — Tracing a Query Through Spark

```python
# The Query
result = (
    spark.read.parquet("s3://orders/")        # 16 partitions, 800MB total
    .filter("status = 'COMPLETED'")
    .join(
        spark.read.parquet("s3://customers/"), # 4 partitions, 50MB total
        "customer_id",
        "inner"
    )
    .groupBy("region")
    .agg(F.sum("amount").alias("revenue"))
    .orderBy("revenue", ascending=False)
)
result.show()
```

### What Spark Plans and Executes

```
CATALYST OPTIMIZER decisions:
  1. customers table is 50MB < 10MB threshold? NO → use SortMergeJoin
     Wait — AQE is on. At runtime, if customers is small after filter → broadcast it
  2. Column pruning: only read customer_id, amount, status, region from orders
                     only read customer_id, region from customers
  3. Predicate pushdown: status = 'COMPLETED' pushed into Parquet scan

PHYSICAL PLAN (simplified):
  Sort (orderBy revenue)                       ← Stage 3
    Exchange (shuffle for sort)
      HashAggregate final (groupBy region)     ← Stage 2
        Exchange (shuffle for groupBy)
          HashAggregate partial (groupBy region)  ← inside Stage 1
            SortMergeJoin (customer_id)
              Exchange (shuffle orders by customer_id)
                Filter + Scan orders parquet   ← Stage 0a
              Exchange (shuffle customers by customer_id)
                Scan customers parquet         ← Stage 0b

STAGES:
  Stage 0a: Read orders (16 tasks) → filter → column prune → shuffle write by customer_id
  Stage 0b: Read customers (4 tasks) → column prune → shuffle write by customer_id
  Stage 1: Read shuffle → SortMergeJoin → partial HashAgg → shuffle write by region
  Stage 2: Read shuffle → final HashAgg → shuffle write for sort
  Stage 3: Read shuffle → Sort → collect to driver → show()

TASK COUNT:
  Stage 0a: 16 tasks (16 parquet partitions)
  Stage 0b: 4 tasks  (4 parquet partitions)
  Stage 1:  200 tasks (default shuffle partitions)
  Stage 2:  200 tasks
  Stage 3:  200 tasks (AQE may coalesce down to 10 if data is small)

IF AQE IS ON:
  After Stage 0b runs, AQE sees customers shuffle output is only 20MB
  → AQE replaces SortMergeJoin with BroadcastHashJoin
  → Stage 0b and shuffle for customers are CANCELLED
  → customers broadcast to all executors
  → Stage 1 becomes: Read orders shuffle → BroadcastHashJoin → partial HashAgg
  → SAVES 200 shuffle read tasks + sort overhead for the join
```

---

## 14. Common Spark UI Signals and What They Mean

| Spark UI Signal | What It Means | Fix |
|-----------------|---------------|-----|
| One task 10x slower than others (Stages tab) | Data skew — one key has too many rows | Salting, AQE skew join handling |
| High "Shuffle Spill (Memory)" | Not enough execution memory for shuffle buffers | Increase executor memory or reduce `maxOffsetsPerTrigger` |
| "GC Time" > 10% of task time | JVM garbage collector overloaded | Increase executor memory, use off-heap |
| Stage takes 3x longer than expected | Speculative tasks not running | Enable `spark.speculation = true` |
| "Killed" tasks | Executor ran out of memory (OOM) | Increase `spark.executor.memory` |
| `BroadcastNestedLoopJoin` in plan | Join without a matching key — likely a bug | Add join condition or use cross join explicitly |
| `SortAggregate` instead of `HashAggregate` | groupBy on non-hashable types or fallback | Check data types, or this is normal for some ops |
| `isFinalPlan=false` after query completes | AQE re-planned mid-execution | Expected with AQE on — check what plan it chose |
| Stage 0 skipped entirely | Data was cached | Expected — Spark reused cached data |

---

## 15. Key Configurations to Know for Interviews

```python
# ─── Parallelism ──────────────────────────────────────────────────────
spark.conf.set("spark.sql.shuffle.partitions", "200")         # Default: 200 (too high for small data)
spark.conf.set("spark.default.parallelism", "200")            # Default for RDD operations

# ─── Adaptive Query Execution (AQE) ───────────────────────────────────
spark.conf.set("spark.sql.adaptive.enabled", "true")           # Default: true (DBR 8+)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# ─── Broadcast Join Threshold ─────────────────────────────────────────
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB default
# Increase if you have a 50MB lookup table you want auto-broadcast

# ─── Shuffle ──────────────────────────────────────────────────────────
spark.conf.set("spark.shuffle.compress", "true")               # Compress shuffle files
spark.conf.set("spark.shuffle.spill.compress", "true")         # Compress spilled data
spark.conf.set("spark.shuffle.file.buffer", "32k")             # Write buffer per shuffle output stream

# ─── Memory ───────────────────────────────────────────────────────────
spark.conf.set("spark.memory.fraction", "0.6")                 # Spark managed memory fraction
spark.conf.set("spark.memory.storageFraction", "0.5")          # Of managed memory, storage portion
spark.conf.set("spark.executor.memory", "8g")                  # Total executor heap
spark.conf.set("spark.executor.memoryOverhead", "1g")          # Off-heap overhead (containers, native libs)

# ─── Code Generation ──────────────────────────────────────────────────
spark.conf.set("spark.sql.codegen.wholeStage", "true")         # Default: true
spark.conf.set("spark.sql.codegen.factoryMode", "CODEGEN_ONLY") # Force code gen

# ─── Dynamic Resource Allocation ──────────────────────────────────────
spark.conf.set("spark.dynamicAllocation.enabled", "true")      # Auto scale executors
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
```

---

## 16. STAR Answers for FAANG

### Q1: "Explain how Spark executes a groupBy query internally"

**Situation:** Asked in a Meta data engineering interview to explain exactly what happens when you run `df.groupBy("country").sum("amount").show()`.

**Task:** Explain the end-to-end execution path from user code to output.

**Action (the answer):**
"When `.show()` is called, Spark's DAGScheduler kicks in. The query has a shuffle boundary at the `groupBy` — so Spark splits it into two stages.

Stage 0 runs as many tasks as there are partitions in the input data. Each task reads its chunk of data, applies the filter pushed down into the scan, and computes a **partial sum** per country within that partition. These partial sums are written to local disk shuffle files — one file per map task. Each row is routed to a shuffle bucket based on `hash(country) % 200`.

Once Stage 0 completes, Stage 1 starts. Spark creates 200 tasks (one per shuffle partition). Each task makes HTTP requests to the BlockManagers on other executors to fetch its slice of the shuffle files — this is the actual data movement across the network. Each task then merges all the partial sums it received for its assigned countries and produces the final total.

The shuffle is the most expensive part — it involves disk writes, network transfer, and disk reads. With AQE enabled, Spark might coalesce the 200 shuffle partitions down to 20 if the data is small, and might switch from SortMergeJoin to BroadcastHashJoin at runtime if a joined table turns out to be small."

**Result:** Got a follow-up question about AQE (which I'd just introduced), leading to a deeper discussion about adaptive planning — turned a simple question into a 15-minute conversation showing depth.

---

### Q2: "How would you diagnose and fix a slow Spark job?"

**Situation:** A production reporting job that normally ran in 8 minutes started taking 45 minutes. Business was waiting on a dashboard. I was the on-call engineer.

**Task:** Diagnose the root cause and fix it without a full code rewrite.

**Action:**
1. **Opened Spark UI** → Jobs tab → found the job was stuck in Stage 2 of 3 stages
2. **Stages tab** → Stage 2 had 200 tasks. 199 tasks finished in under 10 seconds. ONE task was running for 38 minutes.
3. **Tasks tab inside Stage 2** → that one task had "Shuffle Read Size = 15GB" while all others had "Shuffle Read Size = 50-100MB". Classic data skew.
4. **Identified the cause**: The data was partitioned by `user_id`. A new enterprise customer had been onboarded with 40M events under one user ID — all went to one shuffle partition.
5. **Immediate fix**: Enabled AQE skew join handling:
   ```python
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
   spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
   ```
6. **Permanent fix**: Applied salting — appended a random integer 0-9 to `user_id` to spread the skewed user's data across 10 partitions, then aggregated at the end.

**Result:** With AQE fix, the job dropped from 45 minutes to 11 minutes immediately (no code change). With the salting fix deployed the next day, back to 7 minutes. Added a monitoring alert: if any single task's shuffle read size > 5x the median, trigger an investigation.

---

### Q3: "What is the difference between a narrow and wide transformation?"

**Situation:** Standard Spark fundamentals question in Amazon data engineering interview.

**Task:** Explain the concept clearly with practical implications.

**Action (the answer):**
"A **narrow transformation** means each output partition depends on exactly ONE input partition — no data needs to move between executors. Examples: `filter`, `select`, `map`, `withColumn`, `union`. Spark pipelines these into the same stage — one task processes the entire chain on its partition in a single pass.

A **wide transformation** means each output partition depends on data from MULTIPLE input partitions — data must move between executors. Examples: `groupBy`, `join`, `sort`, `distinct`, `repartition`. This requires a shuffle, which creates a new stage boundary.

The practical difference: narrow transformations are basically free. Wide transformations are expensive because they involve disk writes, network transfer, and disk reads. If I see 5 narrow transformations in a row, they all run in one stage as one optimized loop. If I see one wide transformation, it breaks everything into a new stage.

The rule of thumb I use: count the number of shuffle operations in your query — that's roughly how many stage boundaries you have. Minimize shuffles by: (1) broadcasting small tables instead of shuffle-joining, (2) filtering data BEFORE joining, (3) using `coalesce` instead of `repartition` when reducing partition count, (4) pre-partitioning Delta tables by the join key so the data is already co-located."

**Result:** Interviewer followed up with "what if you have to do a sort?" — answered with range partitioning for large data vs rangePartitioner behavior, and discussed how `orderBy` vs `sortWithinPartitions` differ in whether they trigger a global shuffle.
