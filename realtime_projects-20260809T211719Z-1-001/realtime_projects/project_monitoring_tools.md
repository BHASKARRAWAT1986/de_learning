# Monitoring Tools for CDC Pipeline: Debezium + Kafka + Databricks
## Real Tools, Real Scenarios, Real Examples

---

## Architecture: Where Each Tool Plugs In

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        MONITORING STACK OVERVIEW                                 │
│                                                                                  │
│  MySQL ──► Debezium ──► Kafka ──► Databricks DLT ──► Silver/Bronze              │
│    │           │           │            │                                        │
│    │           │           │            │                                        │
│    ▼           ▼           ▼            ▼                                        │
│  ┌─────┐  ┌────────┐  ┌────────┐  ┌──────────┐                                  │
│  │MySQL│  │JMX/    │  │JMX/    │  │Spark UI  │                                  │
│  │slow │  │Prometh-│  │Prometh-│  │DLT Event │  ◄── PRIMARY METRICS SOURCES     │
│  │query│  │eus     │  │eus     │  │Log       │                                  │
│  │log  │  │Exporter│  │Exporter│  │Ganglia   │                                  │
│  └──┬──┘  └───┬────┘  └───┬────┘  └────┬─────┘                                 │
│     │         │           │            │                                        │
│     └─────────┴───────────┴────────────┘                                        │
│                           │                                                      │
│                    ┌──────▼──────┐                                               │
│                    │  Prometheus  │  ◄── METRICS AGGREGATION                    │
│                    │  (scrapes    │                                               │
│                    │   all above) │                                               │
│                    └──────┬──────┘                                               │
│                    ┌──────▼──────┐      ┌──────────────┐                        │
│                    │   Grafana   │      │ Alertmanager │  ──► PagerDuty/Slack   │
│                    │  Dashboards │      │ (alert rules)│                        │
│                    └─────────────┘      └──────────────┘                        │
│                                                                                  │
│  ┌────────────────────────────────────────┐                                      │
│  │      Log Aggregation (ELK / Loki)      │  ◄── ALL APPLICATION LOGS           │
│  │  Kafka Connect logs + DLT logs         │                                      │
│  └────────────────────────────────────────┘                                      │
│                                                                                  │
│  ┌────────────────────────────────────────┐                                      │
│  │  Confluent Control Center (if MSK/CC)  │  ◄── KAFKA-NATIVE UI               │
│  └────────────────────────────────────────┘                                      │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

# PART 1: PROMETHEUS + GRAFANA (Core Stack)

## What They Are

| Tool | Role | Analogy |
|---|---|---|
| **Prometheus** | Time-series metrics database — scrapes and stores metrics | The data collector |
| **Grafana** | Visualization and alerting on top of Prometheus | The dashboard & alarm |
| **JMX Exporter** | Translates Java JMX metrics → Prometheus format | The adapter |

Debezium, Kafka Connect, and Kafka brokers are all **JVM-based** → expose metrics via JMX → JMX Exporter translates them → Prometheus scrapes every 15 seconds.

---

## 1.1 Setting Up Prometheus to Monitor Debezium

### Step 1: Attach JMX Exporter to Kafka Connect (where Debezium runs)

```bash
# Download JMX exporter agent
wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar

# kafka-connect-start.sh — add JVM agent
export KAFKA_OPTS="-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent-0.19.0.jar=7071:/opt/jmx_exporter/kafka-connect.yml"
```

### Step 2: JMX Exporter Config (kafka-connect.yml)
```yaml
# /opt/jmx_exporter/kafka-connect.yml
# What JMX metrics to expose as Prometheus metrics

lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  # ── Debezium CDC Connector Metrics ─────────────────────────────
  - pattern: 'debezium.mysql<type=connector-metrics, context=snapshot, server=(.+)><>(.+)'
    name: debezium_mysql_snapshot_$2
    labels:
      server: "$1"

  - pattern: 'debezium.mysql<type=connector-metrics, context=streaming, server=(.+)><>(.+)'
    name: debezium_mysql_streaming_$2
    labels:
      server: "$1"

  # ── Kafka Connect Worker Metrics ───────────────────────────────
  - pattern: 'kafka.connect<type=connect-worker-metrics><>(.+)'
    name: kafka_connect_worker_$1

  - pattern: 'kafka.connect<type=connect-coordinator-metrics><>(.+)'
    name: kafka_connect_coordinator_$1
```

### Step 3: Prometheus Scrape Config
```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  # Scrape Debezium / Kafka Connect
  - job_name: 'kafka-connect-debezium'
    static_configs:
      - targets:
          - 'kafka-connect-1:7071'
          - 'kafka-connect-2:7071'
          - 'kafka-connect-3:7071'
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance

  # Scrape Kafka Brokers
  - job_name: 'kafka-brokers'
    static_configs:
      - targets:
          - 'kafka-broker-1:7072'
          - 'kafka-broker-2:7072'
          - 'kafka-broker-3:7072'

  # Kafka Lag via kafka-exporter (separate tool — see section 2)
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka-exporter:9308']

  # Databricks via custom pushgateway (see section 3)
  - job_name: 'databricks-metrics'
    static_configs:
      - targets: ['pushgateway:9091']
```

---

## 1.2 Key Prometheus Metrics — Debezium

After scraping, these are the critical metrics to query:

### Metric 1: `debezium_mysql_streaming_millisecondssinceelastevent`
**What it measures:** How many milliseconds since Debezium last received a binlog event.

```
# PromQL query
debezium_mysql_streaming_millisecondssinceelastevent{server="prod_mysql"}

# Normal value: < 5000 ms (5 seconds) during business hours
# Alert threshold: > 60000 ms (1 minute) AND MySQL is not idle
```

**Real Scenario:**  
At 2 AM on a Saturday, this metric shoots to 900,000 ms (15 minutes).  
Investigation reveals MySQL was completely idle — no writes happening.  
**Conclusion:** NOT a Debezium problem; MySQL had zero activity. No alert needed.  
The alert rule should be:
```
# Only alert if Debezium is behind AND MySQL is ALSO active
ALERT DebeziumStalled
  IF debezium_mysql_streaming_millisecondssinceelastevent > 60000
  AND mysql_global_status_queries > 10   # MySQL is active
  FOR 2m
  LABELS { severity = "critical" }
  ANNOTATIONS { summary = "Debezium is not reading binlog despite MySQL activity" }
```

---

### Metric 2: `debezium_mysql_streaming_totalnumberofeventseen`
**What it measures:** Cumulative total binlog events processed since connector started.

```
# Rate of events per second (over last 5 minutes)
rate(debezium_mysql_streaming_totalnumberofeventseen{server="prod_mysql"}[5m])

# Normal: 5-50 events/second during business hours
# Alert: rate drops to 0 while MySQL is active
```

**Real Scenario:**  
During a deploy of the e-commerce app, the event rate drops from 30/s to 0/s.  
The Grafana graph shows a flat line for 8 minutes.  
Investigation: a MySQL migration script held a table lock for 8 minutes, blocking writes.  
The alert fires → team investigates → discovers the migration was not batched correctly.

---

### Metric 3: `debezium_mysql_streaming_sourceeventposition` (binlog lag)
```
# This is a gauge with labels for binlog file and position
# Compare with MySQL master position to compute lag in bytes

# Grafana panel: Binlog Byte Lag
# MySQL current pos  - Debezium current pos = bytes behind
# (computed via the reconciliation job and pushed to Prometheus)
```

---

### Metric 4: `debezium_mysql_snapshot_*` (during initial snapshot)
```
# How many rows snapshotted so far
debezium_mysql_snapshot_totalnumberofeventseen

# How long the snapshot has been running
debezium_mysql_snapshot_snapshotdurationinSeconds

# Is snapshot complete?
debezium_mysql_snapshot_snapshotcompleted   # 1 = done, 0 = in progress
```

**Real Scenario:**  
Initial snapshot of the `orders` table (50M rows) begins at 9 AM.  
Grafana shows `snapshotcompleted = 0` and `totalnumberofeventseen` growing at 200K rows/min.  
Estimate: 50M / 200K = 250 minutes → snapshot will complete by ~1 PM.  
Team plans the go-live cutover for 2 PM (after snapshot + buffer).

---

## 1.3 Key Prometheus Metrics — Kafka Brokers

```yaml
# Key broker metrics (via JMX Exporter on broker pods)

# Messages per second being produced (from Debezium → Kafka)
kafka_server_brokertopicmetrics_messagesin_total{topic="prod_mysql.ecommerce.orders"}

# Bytes per second in / out
kafka_server_brokertopicmetrics_bytesin_total
kafka_server_brokertopicmetrics_bytesout_total

# Under-replicated partitions (0 = healthy, >0 = danger)
kafka_server_replicamanager_underreplicatedpartitions
# ALERT: any value > 0 for more than 30 seconds

# ISR (In-Sync Replica) shrink rate
kafka_server_replicamanager_isrshrinks_total
# ALERT: rate > 0 means replicas are falling behind → risk of data loss on broker failure

# Active controller count (must be exactly 1)
kafka_controller_kafkacontroller_activecontrollercount
# ALERT: != 1 means cluster is leaderless
```

**Real Scenario — Under-replicated Partitions:**  
At 3 PM, `kafka_server_replicamanager_underreplicatedpartitions = 3`.  
PagerDuty fires. Investigation shows broker-2 has high disk I/O — it's falling behind replication.  
Team immediately throttles the Debezium connector's produce rate:
```bash
# Throttle Debezium producer temporarily
curl -X PUT http://kafka-connect:8083/connectors/mysql-orders-connector/config \
  -H 'Content-Type: application/json' \
  -d '{"producer.override.max.block.ms": "5000",
       "producer.override.buffer.memory": "33554432"}'
```
Broker-2 catches up within 10 minutes. URP drops back to 0.

---

## 1.4 Grafana Dashboard Panels — Debezium

```
Panel 1: Connector Health Status (traffic light)
  Query: debezium_mysql_streaming_connected{server="prod_mysql"}
  Thresholds: 1 = GREEN (connected), 0 = RED (disconnected)

Panel 2: Events Per Second
  Query: rate(debezium_mysql_streaming_totalnumberofeventseen[1m])
  Type: Time series graph — shows CDC traffic over 24 hours

Panel 3: Milliseconds Since Last Event
  Query: debezium_mysql_streaming_millisecondssinceelastevent / 1000
  Display: Single stat (seconds)
  Thresholds: 0-30s GREEN, 30-60s YELLOW, >60s RED

Panel 4: Error Count
  Query: rate(debezium_mysql_streaming_numberofErrorsSeen[5m])
  Alert: > 0 for more than 1 minute → page on-call

Panel 5: Snapshot Progress (visible during initial load)
  Query: debezium_mysql_snapshot_totalnumberofeventseen / <expected_total_rows>
  Type: Gauge (0-100%)
```

---

# PART 2: KAFKA-SPECIFIC MONITORING TOOLS

## 2.1 kafka-exporter (Consumer Lag in Prometheus)

The JMX broker metrics don't expose per-consumer-group lag. Use **kafka-exporter**:

```bash
# Run kafka-exporter as a sidecar/deployment
docker run -d \
  --name kafka-exporter \
  -p 9308:9308 \
  danielqsj/kafka-exporter \
  --kafka.server=broker1:9092 \
  --kafka.server=broker2:9092 \
  --kafka.server=broker3:9092 \
  --sasl.enabled \
  --sasl.username=kafka-monitor \
  --sasl.password=*** \
  --sasl.mechanism=PLAIN \
  --tls.enabled
```

### Consumer Lag Metrics from kafka-exporter
```
# Total lag for a consumer group + topic
kafka_consumergroup_lag{
  consumergroup="dlt-mysql-cdc-consumer",
  topic="prod_mysql.ecommerce.orders",
  partition="0"
}

# Sum across all partitions
sum(kafka_consumergroup_lag{consumergroup="dlt-mysql-cdc-consumer",
                             topic="prod_mysql.ecommerce.orders"})

# Lag in seconds (requires message timestamp comparison)
kafka_consumergroup_lag_sum
```

### PromQL Alert Rules for Consumer Lag
```yaml
# alerting_rules.yml
groups:
  - name: kafka_cdc_alerts
    rules:

      # Alert when DLT consumer falls >5000 messages behind
      - alert: KafkaCDCConsumerHighLag
        expr: |
          sum by (topic) (
            kafka_consumergroup_lag{
              consumergroup="dlt-mysql-cdc-consumer"
            }
          ) > 5000
        for: 5m
        labels:
          severity: warning
          team: data-engineering
        annotations:
          summary: "DLT consumer lag is high on {{ $labels.topic }}"
          description: |
            Consumer group dlt-mysql-cdc-consumer has {{ $value }} messages of lag
            on topic {{ $labels.topic }}.
            This means DLT is ~{{ $value | humanizeDuration }} behind MySQL.

      # Alert when ANY partition has NO lag progress for 10 min (stuck consumer)
      - alert: KafkaCDCConsumerStuck
        expr: |
          delta(
            kafka_consumergroup_lag{consumergroup="dlt-mysql-cdc-consumer"}[10m]
          ) >= 0
          AND
          kafka_consumergroup_lag{consumergroup="dlt-mysql-cdc-consumer"} > 100
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "DLT consumer is STUCK on {{ $labels.topic }} partition {{ $labels.partition }}"
          description: |
            Lag has not decreased in 10 minutes. Lag = {{ $value }}.
            DLT pipeline may be down or Kafka partition may be unresponsive.

      # Alert on under-replicated partitions
      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka has {{ $value }} under-replicated partitions"
```

---

## 2.2 Confluent Control Center (if using Confluent Platform or Confluent Cloud)

**Control Center** is a web UI for Kafka — no code required.

```
URL: http://control-center:9021

Key panels:
  ├── Brokers:   health, throughput, disk usage
  ├── Topics:    message rate, consumer lag per topic
  ├── Consumers: per-group lag with timeline graph
  ├── Connect:   Debezium connector status, task count, errors
  └── ksqlDB:    stream processing queries (if used)
```

### What Control Center Shows That CLI Cannot
```
Visual consumer lag timeline:
  X-axis: time (last 24 hours)
  Y-axis: lag (messages)
  
  12:00 PM  ─────────────────────────────────────────── 0 lag (DLT keeping up)
  2:00 PM   ────────────────────────────────────/────── spike to 8000 (DLT batch slow)
  2:15 PM   ────────────────────────────────────\────── back to 0 (caught up)

Without Control Center: you'd only see the current lag, not the historical trend.
With Control Center: you can see the lag spike at 2 PM correlated with a deploy.
```

**Real Scenario:**  
Team notices Silver tables are not updating after a Databricks cluster restart.  
Control Center shows consumer group `dlt-mysql-cdc-consumer` has lag growing at 1000 messages/minute on orders topic.  
The consumer group status shows `EMPTY` (no active members).  
Conclusion: DLT pipeline cluster died but didn't restart automatically.  
Fix: Re-trigger DLT pipeline from Databricks UI.

---

## 2.3 Kafdrop (Lightweight Web UI — Open Source)

Simple UI to browse topics and messages without Confluent licensing.

```bash
docker run -d \
  -p 9000:9000 \
  -e KAFKA_BROKERCONNECT=broker1:9092,broker2:9092 \
  -e JVM_OPTS="-Xms32M -Xmx64M" \
  obsidiandynamics/kafdrop

# Access at http://localhost:9000
```

**What Kafdrop shows:**
- Topic list with partition count and replica status
- Browse individual messages (view the JSON CDC event)
- Consumer group offsets and lag per partition

**Real Scenario — Verify a Specific Record Reached Kafka:**
```
Developer reports: "Order #12345 was placed at 3:45 PM but I don't see it in Silver"

Step 1: Open Kafdrop → Topics → prod_mysql.ecommerce.orders
Step 2: Search messages by offset or time (3:44 PM - 3:46 PM)
Step 3: Found in partition 3, offset 94521:
  {"payload": {"op": "c", "after": {"id": 12345, "status": "PENDING", "amount": 99.99}}}

Conclusion: Debezium captured it, Kafka has it. Problem is in DLT Bronze or Silver.
→ Check DLT event log next.
```

---

## 2.4 kcat (kafkacat) — CLI Swiss Army Knife

```bash
# Install
apt-get install kafkacat

# Read last 10 messages from orders topic
kcat -b broker1:9092 \
     -t prod_mysql.ecommerce.orders \
     -C -o -10 -e \
     -X security.protocol=SASL_SSL \
     -X sasl.mechanisms=PLAIN \
     -X sasl.username=kafka-monitor \
     -X sasl.password=*** \
     | jq '{ts: .payload.ts_ms, op: .payload.op, id: .payload.after.id}'

# Output:
# {"ts": 1716912300000, "op": "c", "id": 10044}
# {"ts": 1716912301000, "op": "u", "id": 10041}
# ...

# Check offsets for all partitions
kcat -b broker1:9092 -t prod_mysql.ecommerce.orders -L
# Output:
# Metadata for prod_mysql.ecommerce.orders (3 brokers, 1 topics):
#   8 partitions:
#     partition 0, leader 1, offset 50000
#     partition 1, leader 2, offset 48500
#     ...
```

---

# PART 3: DATABRICKS PIPELINE MONITORING TOOLS

## 3.1 DLT Event Log (Built-in — Most Important)

Every DLT pipeline writes structured events to a Delta table at:
```
/pipelines/<pipeline-id>/system/events
```

This is your **first stop** when DLT behaves unexpectedly.

### Query DLT Events: Full Diagnostic Suite
```sql
-- ── A. Last 10 pipeline updates (status history) ──────────────────────────
SELECT
  timestamp,
  event_type,
  level,
  message,
  details:update_id      AS update_id,
  details:origin.flow_name AS flow
FROM delta.`/pipelines/<pipeline-id>/system/events`
WHERE level IN ('ERROR', 'WARN')
  AND timestamp > current_timestamp() - INTERVAL 24 HOURS
ORDER BY timestamp DESC
LIMIT 20;

-- ── B. Rows written per table per batch ───────────────────────────────────
SELECT
  timestamp,
  details:flow_name                                AS table_name,
  details:num_output_rows                          AS rows_written,
  details:num_output_bytes                         AS bytes_written,
  details:batch_id                                 AS batch_id,
  datediff(second,
    LAG(timestamp) OVER (
      PARTITION BY details:flow_name ORDER BY timestamp),
    timestamp)                                     AS batch_interval_secs
FROM delta.`/pipelines/<pipeline-id>/system/events`
WHERE event_type = 'flow_progress'
  AND details:status = 'COMPLETED'
  AND details:flow_name IS NOT NULL
ORDER BY timestamp DESC;

/*
Example output:
timestamp            | table_name          | rows_written | batch_interval_secs
2026-05-27 14:00:30  | bronze_orders_raw   | 1250         | 30
2026-05-27 14:00:00  | bronze_orders_raw   | 980          | 30
2026-05-27 13:59:30  | bronze_orders_raw   | 1100         | 30

A gap here (e.g., batch_interval_secs = 300) means DLT was stalled for 5 minutes.
*/

-- ── C. Data quality expectation failures ──────────────────────────────────
SELECT
  timestamp,
  details:flow_name                                           AS table_name,
  explode(from_json(details:expectations,
    'ARRAY<STRUCT<name:STRING, passed_records:LONG, failed_records:LONG>>'))
                                                             AS exp
FROM delta.`/pipelines/<pipeline-id>/system/events`
WHERE event_type = 'flow_progress'
  AND details:expectations IS NOT NULL
  AND timestamp > current_timestamp() - INTERVAL 1 HOUR
```

**Real Scenario — Expectation Spike:**
```
Normal: failed_records = 0-5 per batch (0.0% failure rate)
Alert:  failed_records = 4500 in one batch (30% failure rate!)

Investigation via DLT event log:
  table_name = "bronze_orders_raw"
  expectation_name = "valid_status"
  
Query Bronze quarantine table:
  SELECT status, COUNT(*) 
  FROM bronze_orders_quarantine
  WHERE quarantined_at > current_timestamp() - INTERVAL 1 HOUR
  GROUP BY status;
  
Result: status = 'REFUNDED' appears 4500 times
Root cause: Dev team added new order status in MySQL without updating
            the DLT expectation whitelist. Not a data problem — fix the rule.
```

---

## 3.2 Spark UI (for DLT)

DLT clusters expose the standard Spark UI at:
```
Databricks UI → Compute → DLT cluster → Spark UI link
```

### What to Look for in Spark UI

```
Tab: Jobs
  → Find the structured streaming job
  → Click into a batch → see Stages
  → Stage "writeStream" → Duration column
  
Normal: each stage completes in 5-15 seconds
Alert:  one stage takes 180 seconds → data skew or MERGE bottleneck

Tab: Streaming (for structured streaming jobs)
  → Input rate:     rows/second arriving from Kafka
  → Processing rate: rows/second DLT can handle
  → Batch duration: how long each micro-batch takes
  
RED FLAG: processing rate < input rate (DLT falling behind Kafka)
  → Need to scale up cluster or increase partitions

Tab: SQL
  → Find MERGE operations (from APPLY CHANGES INTO)
  → "number of files read/written" → high = table getting large
  → Look for "RowLevelOperationScan" nodes (means DV is being used)
  
Tab: Executors
  → Memory used vs memory available per executor
  → Spill to disk? → increase executor memory or reduce shuffle partitions
  → GC time > 10% of task time → JVM heap pressure
```

**Real Scenario — MERGE Slowdown:**
```
DLT batch duration grows from 15s → 45s → 90s over 2 weeks.

Spark UI → SQL tab → MERGE operations:
  Files read:    Week 1: 150 files
                 Week 3: 3,200 files  ← table has 3200 small files!

Root cause: OPTIMIZE has not been run on silver_orders in 3 weeks.
Fix:
  ALTER TABLE silver_orders SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableDeletionVectors' = 'true'
  );
  OPTIMIZE silver_orders;

After fix: batch duration back to 12 seconds.
```

---

## 3.3 Databricks SQL Dashboard (No-Code Monitoring)

Build a live monitoring dashboard using Databricks SQL + Auto-Refresh.

### Step 1: Create a Monitoring Schema
```sql
-- Create monitoring tables (written to by monitoring jobs)
CREATE SCHEMA IF NOT EXISTS prod_catalog.monitoring;

-- Pipeline health summary (written every 5 minutes by monitoring notebook)
CREATE TABLE IF NOT EXISTS prod_catalog.monitoring.pipeline_health (
  check_ts         TIMESTAMP,
  component        STRING,    -- 'debezium', 'kafka', 'dlt_bronze', 'dlt_silver', 'heartbeat'
  status           STRING,    -- 'HEALTHY', 'WARNING', 'CRITICAL'
  metric_name      STRING,
  metric_value     DOUBLE,
  detail_message   STRING
);

-- Consumer lag history (for trending)
CREATE TABLE IF NOT EXISTS prod_catalog.monitoring.kafka_lag_history (
  check_ts    TIMESTAMP,
  topic       STRING,
  partition   INT,
  lag         LONG
);

-- Reconciliation results
CREATE TABLE IF NOT EXISTS prod_catalog.monitoring.reconciliation (
  check_ts        TIMESTAMP,
  table_name      STRING,
  mysql_count     LONG,
  silver_count    LONG,
  diff            LONG,
  status          STRING
);
```

### Step 2: Monitoring Notebook (runs as Databricks Job every 5 min)
```python
# monitoring/pipeline_health_writer.py
import requests
from datetime import datetime
from pyspark.sql import Row

def write_health_metric(component, status, metric_name, metric_value, detail):
    row = Row(
        check_ts      = datetime.utcnow(),
        component     = component,
        status        = status,
        metric_name   = metric_name,
        metric_value  = float(metric_value),
        detail_message = detail
    )
    spark.createDataFrame([row]).write \
         .format("delta").mode("append") \
         .saveAsTable("prod_catalog.monitoring.pipeline_health")

# ── Check 1: Debezium Connector Status ────────────────────────────────────────
for connector in ["mysql-orders-connector", "mysql-users-connector"]:
    resp = requests.get(
        f"http://kafka-connect:8083/connectors/{connector}/status",
        timeout=5
    )
    data   = resp.json()
    state  = data["connector"]["state"]
    status = "HEALTHY" if state == "RUNNING" else "CRITICAL"
    write_health_metric("debezium", status, "connector_state",
                        1 if state == "RUNNING" else 0,
                        f"Connector: {connector} | State: {state}")

# ── Check 2: Kafka Consumer Lag ───────────────────────────────────────────────
# (via kafka-exporter Prometheus endpoint)
prom_url  = "http://kafka-exporter:9308/metrics"
prom_resp = requests.get(prom_url, timeout=5).text

import re
lags = re.findall(
    r'kafka_consumergroup_lag\{.*?topic="([^"]+)".*?partition="(\d+)".*?\} (\d+)',
    prom_resp
)
for topic, partition, lag in lags:
    lag_val = int(lag)
    status  = "HEALTHY" if lag_val < 1000 else ("WARNING" if lag_val < 5000 else "CRITICAL")
    spark.createDataFrame([Row(
        check_ts  = datetime.utcnow(),
        topic     = topic,
        partition = int(partition),
        lag       = lag_val
    )]).write.format("delta").mode("append") \
       .saveAsTable("prod_catalog.monitoring.kafka_lag_history")
    
    write_health_metric("kafka", status, f"consumer_lag_{topic}_p{partition}",
                        lag_val, f"Topic: {topic} | Partition: {partition} | Lag: {lag_val:,}")

# ── Check 3: DLT Last Batch Time ─────────────────────────────────────────────
last_batch = spark.sql("""
    SELECT
      details:flow_name                   AS flow,
      MAX(timestamp)                      AS last_batch_ts,
      datediff(second, MAX(timestamp),
               current_timestamp())       AS seconds_ago
    FROM delta.`/pipelines/<pipeline-id>/system/events`
    WHERE event_type = 'flow_progress'
      AND details:status = 'COMPLETED'
    GROUP BY details:flow_name
""").collect()

for row in last_batch:
    status = "HEALTHY" if row.seconds_ago < 90 else \
             ("WARNING" if row.seconds_ago < 180 else "CRITICAL")
    write_health_metric("dlt", status, f"batch_age_{row.flow}",
                        row.seconds_ago,
                        f"Flow: {row.flow} | Last batch: {row.seconds_ago}s ago")

# ── Check 4: Heartbeat ───────────────────────────────────────────────────────
heartbeat = spark.sql("""
    SELECT datediff(second, arrived_at, current_timestamp()) AS age_secs
    FROM prod_catalog.ecommerce.silver_heartbeat
""").first()

hb_age = heartbeat["age_secs"] if heartbeat else 9999
status = "HEALTHY" if hb_age < 90 else ("WARNING" if hb_age < 180 else "CRITICAL")
write_health_metric("heartbeat", status, "age_seconds", hb_age,
                    f"Last heartbeat: {hb_age}s ago")

print("Health metrics written successfully")
```

### Step 3: Databricks SQL Dashboard Queries
```sql
-- Widget 1: Current System Status (traffic light table)
SELECT
  component,
  status,
  metric_name,
  metric_value,
  detail_message,
  check_ts
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY component, metric_name ORDER BY check_ts DESC) AS rn
  FROM prod_catalog.monitoring.pipeline_health
)
WHERE rn = 1
ORDER BY
  CASE status WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END;

-- Widget 2: Consumer Lag Trend (last 6 hours — line chart)
SELECT
  date_trunc('minute', check_ts) AS minute,
  topic,
  SUM(lag) AS total_lag
FROM prod_catalog.monitoring.kafka_lag_history
WHERE check_ts > current_timestamp() - INTERVAL 6 HOURS
  AND topic = 'prod_mysql.ecommerce.orders'
GROUP BY minute, topic
ORDER BY minute;

-- Widget 3: DLT Batch Throughput (rows/batch over time)
SELECT
  timestamp,
  details:flow_name    AS flow,
  details:num_output_rows AS rows
FROM delta.`/pipelines/<pipeline-id>/system/events`
WHERE event_type = 'flow_progress'
  AND details:status = 'COMPLETED'
  AND timestamp > current_timestamp() - INTERVAL 24 HOURS
ORDER BY timestamp DESC;

-- Widget 4: Reconciliation Trend
SELECT
  date_trunc('hour', check_ts) AS hour,
  table_name,
  AVG(diff) AS avg_diff,
  MAX(diff) AS max_diff
FROM prod_catalog.monitoring.reconciliation
WHERE check_ts > current_timestamp() - INTERVAL 7 DAYS
GROUP BY hour, table_name
ORDER BY hour DESC;
```

---

## 3.4 Databricks Lakehouse Monitoring (System Tables — Unity Catalog)

Unity Catalog exposes built-in system tables for pipeline and query monitoring.

```sql
-- Query history: which queries are running against Silver tables?
SELECT
  statement_id,
  executed_by,
  statement_text,
  start_time,
  end_time,
  total_duration_ms,
  read_rows,
  produced_rows
FROM system.access.audit
WHERE service_name = 'databricksSQL'
  AND request_params:statementText LIKE '%silver_orders%'
  AND event_time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY total_duration_ms DESC
LIMIT 20;

-- DLT pipeline runs history
SELECT
  pipeline_id,
  update_id,
  state,
  start_time,
  end_time,
  datediff(second, start_time, end_time) AS duration_secs,
  cause
FROM system.lakeflow.pipeline_updates
WHERE pipeline_id = '<your-pipeline-id>'
  AND start_time > current_timestamp() - INTERVAL 7 DAYS
ORDER BY start_time DESC;

-- Cluster utilization for DLT cluster
SELECT
  cluster_id,
  event_time,
  type,        -- 'RUNNING', 'TERMINATED', 'RESIZING'
  details
FROM system.compute.clusters
WHERE cluster_name LIKE '%mysql-cdc%'
  AND event_time > current_timestamp() - INTERVAL 24 HOURS
ORDER BY event_time DESC;
```

---

## 3.5 Ganglia (Cluster-Level Metrics — Legacy but Still Used)

```
Databricks cluster → Spark UI → Ganglia Metrics link

Ganglia shows:
  CPU %        → high during MERGE operations → normal
  Memory %     → high during shuffle → may need larger executors
  Network I/O  → high during Kafka reads → expected
  Disk I/O     → high during OPTIMIZE → expected

Real Scenario:
  DLT processing time doubles every Monday morning.
  Ganglia shows: Memory = 95% on all executors at 8 AM Monday
  Root cause: Monday backlog from weekend — Kafka has 3M messages queued.
  DLT reads all at once → high shuffle memory → spill to disk → slow.
  Fix: Cap maxOffsetsPerTrigger = 50000 per batch to smooth the load.
```

---

# PART 4: LOG AGGREGATION — ELK STACK / GRAFANA LOKI

## 4.1 What Logs to Collect

```
Source System   → Log Type                   → What to Search For
─────────────────────────────────────────────────────────────────────
Kafka Connect   → kafka-connect.log          → ConnectException, FAILED task
Debezium        → embedded in Connect log    → "Could not find binlog", schema errors
Kafka Broker    → server.log                 → OfflineLogDir, ReplicaFetcherThread
Databricks DLT  → driver/executor logs       → StreamingQueryException, OOMError
MySQL           → error.log + slow_query.log → Lock timeouts, slow queries
```

## 4.2 Shipping Logs to ELK (Filebeat)

```yaml
# filebeat.yml on Kafka Connect hosts
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /var/log/kafka-connect/kafka-connect.log
    fields:
      service: kafka-connect
      environment: production
    multiline:
      pattern: '^\d{4}-\d{2}-\d{2}'   # Line starts with date = new log entry
      negate: true
      match: after

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "kafka-connect-%{+yyyy.MM.dd}"
```

## 4.3 Kibana / Elasticsearch Queries for Real Scenarios

### Scenario 1: Find What Caused Debezium to Fail
```
Kibana → Discover → index: kafka-connect-*
Filter: 
  service: kafka-connect
  level: ERROR
  time: last 24 hours

Query:
  "mysql-orders-connector" AND ("FAILED" OR "Exception" OR "ERROR")

Result logs:
  2026-05-27 14:23:45 ERROR Connector mysql-orders-connector encountered
  an exception: com.github.shyiko.mysql.binlog.network.ServerException:
  Could not find first log file name in binary log index file
  at Position{fileName='mysql-bin.000035', position=4}
  
Root cause: binlog file mysql-bin.000035 has been purged. Connector needs re-snapshot.
```

### Scenario 2: Track Schema Change Impact
```
Kibana search:
  "schema change" OR "SchemaChangeException" OR "incompatible schema"
  
Result:
  2026-05-27 09:15:22 WARN Detected schema change for table ecommerce.orders:
  new column 'discount_pct' (DOUBLE) added at position 8

Timeline: 
  09:15 — schema change detected
  09:15 — Bronze table schema evolved (mergeSchema = true)
  09:16 — Silver APPLY CHANGES picks up new column
  09:16 — 0 errors, pipeline continues normally ✓
```

---

# PART 5: ALERTMANAGER + PAGERDUTY INTEGRATION

## 5.1 Alertmanager Config

```yaml
# alertmanager.yml
global:
  resolve_timeout: 5m

route:
  group_by: ['alertname', 'severity']
  group_wait:      30s    # Wait 30s to group similar alerts
  group_interval:  5m     # Send summary every 5 min for ongoing alerts
  repeat_interval: 1h     # Re-alert every hour if not resolved

  receiver: 'default'

  routes:
    # CRITICAL alerts → PagerDuty (wakes someone up)
    - match:
        severity: critical
      receiver: pagerduty-critical
      continue: true   # Also send to Slack

    # WARNING alerts → Slack only (no wake-up)
    - match:
        severity: warning
      receiver: slack-warning

receivers:
  - name: 'pagerduty-critical'
    pagerduty_configs:
      - routing_key: '<pagerduty-integration-key>'
        description: '{{ .CommonAnnotations.summary }}'
        details:
          component: '{{ .CommonLabels.component }}'
          description: '{{ .CommonAnnotations.description }}'

  - name: 'slack-warning'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/xxx'
        channel: '#data-eng-alerts'
        title: ':warning: {{ .CommonAnnotations.summary }}'
        text: '{{ .CommonAnnotations.description }}'
        color: 'warning'

  - name: 'default'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/xxx'
        channel: '#data-eng-alerts'
```

---

# PART 6: COMPLETE REAL SCENARIO WALKTHROUGH

## Scenario: "Silver tables stopped updating — nobody noticed for 2 hours"

### The Problem
At 2:00 PM, Debezium silently moved to FAILED state.  
No alert fired. Silver tables stopped receiving updates.  
At 4:00 PM, a business analyst notices order statuses are stale.  
On-call engineer opens the monitoring dashboard.

### Step-by-Step Investigation

```
Step 1: Open Grafana Dashboard → "CDC Pipeline Health"
  ✗ Panel: "Debezium Events/sec"         → shows 0 since 14:00
  ✗ Panel: "Kafka Consumer Lag (orders)" → 120,000 messages behind (and growing)
  ✓ Panel: "Kafka Broker Health"         → all green (Kafka itself is fine)
  ✗ Panel: "Heartbeat Age"               → 7200 seconds (2 hours!)

  Conclusion: Debezium stopped at 14:00. Kafka has 2 hours of backlog.
              DLT is trying to consume but Debezium stopped producing.

Step 2: Check Debezium Status via REST API
  curl http://kafka-connect:8083/connectors/mysql-orders-connector/status | jq .
  
  Result:
  {
    "connector": {"state": "FAILED"},
    "tasks": [{
      "state": "FAILED",
      "trace": "com.mysql.cj.jdbc.exceptions.CommunicationsException: 
                Communications link failure... connect timed out"
    }]
  }
  
  Root cause identified: MySQL connection timed out.

Step 3: Investigate the MySQL Connection Timeout
  ELK search: "mysql-orders-connector" AND "timeout" last 3 hours
  
  Log found (14:00:15):
  "WARN io.debezium.connector.mysql.MySqlConnector: Connection to mysql-prod.internal:3306
   lost. Attempting reconnect..."
  "ERROR: Reconnect failed after 3 attempts. Marking connector FAILED."
  
  MySQL ops team checked: MySQL primary had a failover at 14:00!
  The primary moved from mysql-prod-1 to mysql-prod-2.
  Debezium was configured with a hardcoded hostname → didn't follow the failover.

Step 4: Fix — Update Debezium to use the VIP/DNS endpoint
  curl -X PUT http://kafka-connect:8083/connectors/mysql-orders-connector/config \
    -d '{"database.hostname": "mysql-vip.internal"}'  # Points to floating VIP
  
  Connector restarts. It picks up from its last committed binlog offset.

Step 5: Verify Recovery
  Grafana: "Debezium Events/sec" → immediately shows 800/sec (catching up on 2h backlog)
  Grafana: "Kafka Consumer Lag" → dropping rapidly: 120K → 80K → 40K → 5K → 0
  Heartbeat: arrives in Silver 55 seconds later. 
  Reconciliation job runs: diff = 0. All 2 hours of changes replayed.

Step 6: Prevention
  - Add Prometheus alert on debezium_mysql_streaming_connected = 0 for > 2 minutes
  - Test failover scenario in staging monthly
  - Use MySQL DNS VIP everywhere (not IPs or hardcoded hostnames)
```

---

# PART 7: MONITORING TOOLS SUMMARY TABLE

| Tool | Best For | Open Source? | Complexity |
|---|---|---|---|
| **Prometheus** | Metrics collection & alerting rules | YES | Medium |
| **Grafana** | Visualization, dashboards, alert routing | YES | Low-Medium |
| **kafka-exporter** | Consumer lag in Prometheus | YES | Low |
| **JMX Exporter** | Debezium/Kafka JVM metrics → Prometheus | YES | Low |
| **Confluent Control Center** | Kafka-native UI with lag timeline | NO (license) | Low |
| **Kafdrop** | Browse Kafka topics and messages | YES | Very Low |
| **kcat (kafkacat)** | CLI inspection of Kafka messages | YES | Low (CLI) |
| **Databricks SQL Dashboard** | DLT + Silver metrics, no-code | YES (DBR license) | Low |
| **DLT Event Log** | DLT batch metrics and expectations | Built-in DLT | Very Low |
| **Spark UI** | Task-level performance debugging | Built-in Spark | Medium |
| **ELK / Loki** | Log aggregation and searching | YES | High |
| **Alertmanager** | Alert routing to PagerDuty/Slack | YES | Medium |
| **PagerDuty** | On-call paging for critical alerts | NO (SaaS) | Low |
| **Databricks System Tables** | Pipeline + cluster audit history | Built-in UC | Low |

---

# PART 8: MINIMUM VIABLE MONITORING SETUP (If Starting From Scratch)

If you have limited time, implement in this priority order:

```
Priority 1 (Day 1): Basic alerts — prevents silent failures
  ✓ Debezium connector REST API health check → PagerDuty on FAILED state
  ✓ Kafka consumer lag alert via kafka-consumer-groups CLI → Slack if lag > 5000
  ✓ DLT pipeline email alert (built into Databricks — 5 minutes to set up)

Priority 2 (Week 1): Visibility — know what "normal" looks like
  ✓ Heartbeat table pattern (MySQL → Kafka → Silver)
  ✓ Databricks SQL dashboard with DLT event log queries
  ✓ Hourly reconciliation job (MySQL count vs Silver count)

Priority 3 (Month 1): Full observability — trending and root cause
  ✓ Prometheus + Grafana + kafka-exporter for lag trending
  ✓ JMX exporter on Kafka Connect for Debezium metrics
  ✓ ELK/Loki for log aggregation
  ✓ Alertmanager with routing rules (critical → PagerDuty, warning → Slack)
```
