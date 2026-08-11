# Databricks Ingestion Patterns — Master Index
## FAANG Interview Preparation Guide

---

## Files in This Directory

| # | File | Topic | When to Use |
|---|------|--------|-------------|
| 01 | `01_autoloader.md` | Auto Loader | Cloud object store (S3/ADLS/GCS) → Delta, incremental file discovery |
| 02 | `02_copy_into.md` | COPY INTO | Idempotent batch loads, one-time or scheduled bulk ingest |
| 03 | `03_structured_streaming.md` | Structured Streaming | Kafka/Kinesis/Event Hubs, micro-batch or continuous processing |
| 04 | `04_dlt_delta_live_tables.md` | Delta Live Tables (DLT) | Declarative pipeline framework, data quality, CDC, SLA enforcement |

---

## Quick Decision Matrix

```
New data arrives in S3/ADLS?
  ├── Batch (daily/hourly)?              → COPY INTO
  ├── Near-realtime (seconds/minutes)?  → Auto Loader
  └── CDC events from a message bus?    → Structured Streaming or DLT

Do you want a managed pipeline with:
  ├── Built-in data quality?            → DLT
  ├── Auto-scaling + retry?             → DLT
  └── Raw code control?                 → Structured Streaming

Scale of files:
  ├── < 1,000 files / load?             → COPY INTO (no state overhead)
  └── 1,000s – billions of files?       → Auto Loader (cloudFiles, scalable listing)
```

---

## One-Liner Answers for FAANG Screeners

| Question | Answer |
|----------|--------|
| Auto Loader vs COPY INTO? | Auto Loader scales to billions of files using cloud events/incremental listing; COPY INTO is simpler but rescans the full prefix |
| Auto Loader vs Kafka? | Auto Loader reads files already landed; Kafka is a live event bus — they're complementary, not competitors |
| DLT vs raw Structured Streaming? | DLT adds declarative quality expectations, automatic retry/backfill, lineage, and managed infrastructure on top of Structured Streaming |
| When does COPY INTO fail at scale? | When the source prefix accumulates millions of files — listing latency becomes O(n) |
| Exactly-once in Structured Streaming? | Checkpointing + idempotent sinks (Delta) + WAL = exactly-once end-to-end |
| DLT Bronze/Silver/Gold? | Bronze = raw append, Silver = validated/deduped, Gold = aggregated business metrics |

---

## Key Numbers to Memorize

- Auto Loader file notification mode latency: **< 1 minute**
- Auto Loader incremental listing: scans from **last checkpoint offset**, not full prefix
- COPY INTO: stores loaded files in **`_delta_log`** to prevent re-loading
- Structured Streaming checkpoint interval: **default = trigger interval**
- DLT pipeline modes: **Triggered** (batch) vs **Continuous** (streaming)
- Max DLT expectations per table: no hard limit, but 20-30 is practical
- Watermark default: **none** — must be set explicitly for late data handling
