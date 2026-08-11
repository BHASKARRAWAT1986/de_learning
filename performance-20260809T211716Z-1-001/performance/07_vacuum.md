# VACUUM — Complete Guide

---

## 1. What Is VACUUM?

`VACUUM` is a Delta Lake maintenance command that **permanently deletes old data files** that are no longer referenced by the current or recent versions of a Delta table.

Delta Lake uses a **copy-on-write** model: when files are updated or deleted, old files are marked as "removed" in the transaction log but remain physically on disk. Over time, these obsolete files accumulate and consume storage unnecessarily.

VACUUM cleans them up.

```
Before VACUUM:
  /delta/orders/
    part-000001.parquet  ← active (current version)
    part-000002.parquet  ← active
    part-000003.parquet  ← deleted (marked removed 30+ days ago)
    part-000004.parquet  ← deleted (from old OPTIMIZE run)
    part-000005.parquet  ← deleted (from MERGE operation)
    _delta_log/

After VACUUM:
  /delta/orders/
    part-000001.parquet  ← active
    part-000002.parquet  ← active
    _delta_log/          ← part-000003, 004, 005 physically deleted
```

---

## 2. Why Files Accumulate (The Write-on-Write Model)

Every Delta write operation produces new files and marks old ones as removed:

```
Operation     → Old Files Status   → New Files
─────────────────────────────────────────────────
INSERT        → (none removed)     → new files
UPDATE        → old files removed  → rewritten files
DELETE        → old files removed  → rewritten files (without deleted rows)
MERGE         → old files removed  → rewritten files
OPTIMIZE      → old files removed  → larger merged files
ZORDER        → old files removed  → reordered files
Schema change → may rewrite files  → new schema files
```

Without VACUUM, all removed files stay on disk forever.

---

## 3. Retention Period

VACUUM uses a **retention period** (default: **7 days**) to determine which files are safe to delete.  
Files removed from the Delta log MORE THAN the retention period ago are deleted by VACUUM.

```
Current time: 2025-05-27
Retention: 7 days

File removed from log on: 2025-05-15 → 12 days ago → DELETED by VACUUM
File removed from log on: 2025-05-25 → 2 days ago  → KEPT (within retention)
```

The retention period enables **Time Travel** — you can query older table versions as long as their files haven't been vacuumed.

```sql
-- Time travel works within retention window
SELECT * FROM orders VERSION AS OF 5;              -- OK if files still exist
SELECT * FROM orders TIMESTAMP AS OF '2025-05-20'; -- OK if within retention

-- After VACUUM removes old files → these queries fail with "file not found"
```

---

## 4. Syntax

### Basic VACUUM (uses default 7-day retention)
```sql
VACUUM orders;
```

### VACUUM with Custom Retention
```sql
-- Keep files from last 30 days (for longer time travel)
VACUUM orders RETAIN 720 HOURS;   -- 30 days

-- Keep last 14 days
VACUUM orders RETAIN 336 HOURS;
```

### DRY RUN — Preview What Would Be Deleted
```sql
-- See what files would be deleted WITHOUT actually deleting them
VACUUM orders DRY RUN;
-- Returns: list of files that would be deleted

-- Always do a DRY RUN first before vacuuming a production table
```

### VACUUM on Specific Path
```sql
-- By path (ADLS, S3, GCS)
VACUUM delta.`abfss://container@storageaccount.dfs.core.windows.net/path/orders`;

-- By table name
VACUUM catalog.schema.orders;
```

---

## 5. How VACUUM Works Internally

```
1. VACUUM reads the current Delta log
2. Identifies all files currently referenced (valid versions within retention)
3. Lists all physical files in the table directory
4. Files in directory BUT NOT in any valid version → candidate for deletion
5. Check: was the file's "remove" action older than retention period?
   YES → delete the physical file
   NO  → keep it (still needed for time travel)
6. Updates are NOT made to the Delta log (VACUUM is a physical cleanup only)
```

### VACUUM and the Delta Log
```
_delta_log/
  00000000000000000001.json  ← shows file_001 added
  00000000000000000002.json  ← shows file_001 removed, file_002 added
  00000000000000000003.json  ← shows file_002 removed, file_003 added

If retention = 7 days and version 1 is 10 days old:
  VACUUM deletes: file_001.parquet (referenced only by log version 1, now expired)
  VACUUM keeps:   file_002.parquet (removed only 3 days ago, within retention)
  VACUUM keeps:   file_003.parquet (active, always kept)
```

---

## 6. Overriding Minimum Retention (DANGER — for testing only)

Delta Lake has a **safety check** preventing retention below 7 days to avoid accidental deletion of files still in use by concurrent readers.

```sql
-- DANGER: Do NOT use in production
-- Only for development/testing when you need immediate cleanup

-- Step 1: Disable the safety check
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- Step 2: VACUUM with 0 retention
VACUUM orders RETAIN 0 HOURS;

-- Step 3: Re-enable the check
SET spark.databricks.delta.retentionDurationCheck.enabled = true;
```

**Why it's dangerous:** Concurrent readers (streaming jobs, BI tools) may still be reading files from recent snapshots. Deleting with 0 retention can cause those reads to fail mid-query.

---

## 7. VACUUM and Streaming

Active streaming queries maintain a **watermark** on the Delta table they read. VACUUM is safe as long as the retention period covers the streaming lag.

```sql
-- Streaming job is 3 days behind (reads data from 3 days ago)
-- VACUUM RETAIN 168 HOURS (7 days) → SAFE (7 > 3)
-- VACUUM RETAIN 24 HOURS (1 day)   → UNSAFE (1 < 3, may delete files streaming needs)

-- Best practice for tables with streaming readers:
VACUUM orders RETAIN 720 HOURS;  -- 30 days (generous buffer)
```

---

## 8. VACUUM and Shallow Clones

If you use Delta Shallow Clone (`CREATE TABLE clone SHALLOW CLONE source`), the clone references the **source table's physical files**. VACUUM on the source will delete those files even if the clone still references them.

```sql
-- DANGER: Do not VACUUM source table if shallow clones exist
CREATE TABLE orders_clone SHALLOW CLONE orders;  -- clone references orders' files
VACUUM orders;  -- may delete files that orders_clone still needs!

-- SAFE: Deep clone copies the files
CREATE TABLE orders_deep_clone DEEP CLONE orders;
VACUUM orders;  -- safe — orders_deep_clone has its own copies
```

---

## 9. Configure Automatic VACUUM (Databricks)

```sql
-- Enable automatic VACUUM via Predictive Optimization
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true'  -- reduces files that need cleanup
);

-- Or schedule via Databricks Job / Workflow
-- (no built-in auto-vacuum — must be scheduled externally)
```

### Recommended Scheduling Pattern
```python
# Nightly maintenance job
def run_vacuum(tables, retention_hours=168):
    for table in tables:
        # Dry run first
        print(f"DRY RUN for {table}:")
        spark.sql(f"VACUUM {table} DRY RUN").show()
        
        # Actual vacuum
        spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
        print(f"VACUUM complete: {table}")

run_vacuum([
    "catalog.schema.orders",
    "catalog.schema.events",
    "catalog.schema.users"
], retention_hours=168)
```

---

## 10. VACUUM vs Delta Log Retention

Two separate retention settings — do not confuse them:

| Setting | Controls | Default |
|---|---|---|
| `VACUUM RETAIN X HOURS` | How long **data files** are kept after removal | 168h (7 days) |
| `delta.logRetentionDuration` | How long **Delta log files** are kept | 30 days |

```sql
-- Set log retention separately
ALTER TABLE orders SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 60 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'  -- same as VACUUM retention
);
```

---

## 11. Performance Impact of Not Running VACUUM

```
Table with 3 months of OPTIMIZE + MERGE operations without VACUUM:
  Active files: 500 (referenced by current version)
  Dead files:   8,000 (removed from log but still on disk)

DESCRIBE DETAIL shows:
  numFiles: 500
  
But directory listing shows:
  8,500 total files on disk

Impact:
  - Storage costs 17x what it should be
  - Directory listing overhead during planning (Delta must scan log to exclude dead files)
  - Cloud storage metadata costs
  - VACUUM itself becomes slower (more files to check)
```

---

## 12. Pros and Cons

| Pros | Cons |
|---|---|
| Reclaims storage (critical at scale) | Deletes time travel history permanently |
| Reduces cloud storage costs | Must be scheduled manually |
| Faster directory listing (fewer files) | Can break shallow clone references |
| Required for Delta table health | Unsafe with low retention for streaming tables |
| DRY RUN preview before committing | Irreversible — deleted files cannot be recovered |

---

## 13. Best Practices

1. **Always do a DRY RUN first** before vacuuming production tables
2. **Set retention ≥ streaming lag + buffer** to protect active streaming readers
3. **Never set retention < 7 days** in production (Databricks blocks it by default)
4. **Schedule nightly** as part of table maintenance jobs
5. **Vacuum frequently** — small frequent vacuums are faster than rare large ones
6. **Avoid shallow clones** on frequently vacuumed tables — use deep clones
7. **Adjust `delta.logRetentionDuration`** to match your time travel SLA

---

## 14. Quick Reference

```sql
-- Preview (safe — no deletion)
VACUUM my_table DRY RUN;

-- Standard vacuum (7-day retention)
VACUUM my_table;

-- Extended retention
VACUUM my_table RETAIN 720 HOURS;  -- 30 days

-- By path
VACUUM delta.`/path/to/table`;

-- Check table properties
SHOW TBLPROPERTIES my_table;

-- Set retention via property
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 14 days'
);
```
