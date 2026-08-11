# ============================================================
# HOW DAG WORKS: BATCH vs STREAMING
# ============================================================

# -------------------------------------------------------
# BATCH DAG BEHAVIOR
# -------------------------------------------------------
# Each ACTION triggers a fresh DAG execution from scratch.
# By default, df.count() and df.write() will BOTH re-read the source.

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("s3://path/data/")  # Nothing happens yet — lazy evaluation

df.count()        # Action 1 → triggers full DAG: read → count
df.write.save()   # Action 2 → triggers full DAG AGAIN: read → write


# Fix: Use cache/persist to avoid re-reading the source
df_cached = spark.read.parquet("s3://path/data/").cache()

df_cached.count()      # reads from source, stores in memory/disk
df_cached.write.save() # reads from cache — no re-scan of source


# -------------------------------------------------------
# STREAMING DAG BEHAVIOR — Fundamentally Different
# -------------------------------------------------------
# The DAG is defined ONCE and Spark reuses it every micro-batch.
#
#   Trigger 1 (t=0s)  → DAG executes on batch of new data
#   Trigger 2 (t=5s)  → Same DAG executes on next batch of new data
#   Trigger 3 (t=10s) → Same DAG executes on next batch of new data
#
# There is only ONE terminal action: .writeStream.start()
# Spark internally handles the looping — you cannot call .count() or .write() separately.

df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "host:9092") \
    .option("subscribe", "topic_name") \
    .load()

# This single query plan is reused every micro-batch
query = df_stream.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/my_query") \
    .trigger(processingTime="10 seconds") \
    .start("s3://path/output/")


# -------------------------------------------------------
# HOW STREAMING AVOIDS RE-READING (Offsets + Checkpoints)
# -------------------------------------------------------
# Kafka offset 0-100   → micro-batch 1
# Kafka offset 101-200 → micro-batch 2  (doesn't re-read 0-100)
#
# The DAG structure is identical each micro-batch,
# but the INPUT RANGE shifts forward via tracked offsets.


# -------------------------------------------------------
# SIDE-BY-SIDE COMPARISON
# -------------------------------------------------------
# Aspect              | Batch                        | Streaming
# --------------------|------------------------------|---------------------------
# DAG trigger         | Each action (count, write)   | Each micro-batch, same plan
# Re-reads source     | Yes, per action (unless cached) | No — checkpoint tracks offsets
# Multiple actions    | Allowed, each re-executes    | Only one writeStream sink
# State               | Stateless per job            | Stateful via checkpointing
# Caching need        | High (avoid re-reads)        | Low (Spark manages internally)
