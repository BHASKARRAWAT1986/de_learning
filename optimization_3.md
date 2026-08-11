"""
================================================================================
COMPLETE OPTIMIZATION TECHNIQUES - POINTERS, PROS, LIMITATIONS & PREREQUISITES
================================================================================
Comprehensive Reference for all Databricks optimization techniques with:
- Key Pointers (critical points)
- Pros (advantages)
- Limitations (constraints)
- Prerequisites (requirements)
- Best Practices
- Anti-patterns (what NOT to do)

Date: 2024
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

# ================================================================================
# OPTIMIZATION TECHNIQUE 1: ANALYZE TABLE
# ================================================================================

class AnalyzeTable:
    """ANALYZE TABLE - Compute statistics for query optimization"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 1: ANALYZE TABLE - COMPUTE STATISTICS")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                        ANALYZE TABLE OVERVIEW                             ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  ANALYZE TABLE table_name COMPUTE STATISTICS;
  ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
  ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS col1, col2;

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. Purpose
   ├─ Computes table and column statistics
   ├─ Helps Catalyst optimizer make better decisions
   ├─ Enables cost-based optimization (CBO)
   └─ Foundation for all other optimizations

2. Statistics Computed
   ├─ Row count
   ├─ Column cardinality (distinct values)
   ├─ Null count
   ├─ Min/Max values
   ├─ Data distribution
   ├─ Histogram information
   └─ Byte size estimation

3. Execution
   ├─ Full table scan (processes all rows)
   ├─ Runs sequentially or parallel
   ├─ Time increases with table size
   ├─ Can be resource intensive
   └─ Results cached in metastore

4. Scope
   ├─ Table-level: Total row count
   ├─ Column-level: Per-column statistics
   ├─ Index-level: NOT computed (Delta Lake)
   └─ Partition-level: By partition if partitioned

5. Staleness
   ├─ Valid until table changes significantly
   ├─ INSERT/UPDATE/DELETE invalidate stats
   ├─ OPTIMIZE does not invalidate
   ├─ Need to re-run after bulk changes
   └─ Set expiration policy for large tables


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. Better Query Plans
   └─ Catalyst makes smarter decisions with statistics
   └─ Join order optimization
   └─ Predicate pushdown
   └─ Cost-based pruning

✅ 2. Improved Performance
   └─ 10-30% query speedup on average
   └─ Better for complex queries
   └─ Aggregation queries benefit most
   └─ Join queries significantly improved

✅ 3. No Rewrite Needed
   └─ Doesn't change table data
   └─ No downtime required
   └─ Can run in background
   └─ Backward compatible

✅ 4. Foundation for Other Optimizations
   └─ Required for Z-ORDER effectiveness
   └─ Improves OPTIMIZE recommendations
   └─ Helps partition pruning
   └─ Enables predictive optimization

✅ 5. Diagnostic Value
   └─ Discover data quality issues
   └─ Identify skew problems
   └─ Understand cardinality
   └─ Find anomalies

✅ 6. Low Risk
   └─ Read-only operation
   └─ Cannot corrupt data
   └─ Easy to rollback (just skip stats)
   └─ No side effects


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. Time Cost
   └─ Full table scan required
   └─ 5 minutes to 2 hours depending on size
   └─ Blocks queries during execution
   └─ High CPU/IO usage

❌ 2. One-time Benefit
   └─ Statistics become stale quickly
   └─ Need re-runs after bulk changes
   └─ No continuous optimization
   └─ Manual effort to maintain

❌ 3. Limited Effectiveness
   └─ Only helps if Catalyst can use stats
   └─ Some query patterns ignore stats
   └─ Doesn't optimize bad query structure
   └─ Can't fix fundamental design issues

❌ 4. Storage Overhead
   └─ Stats stored in metastore
   └─ Minimal but adds to metadata
   └─ Multiple stat versions accumulate
   └─ Cleanup needed for old stats

❌ 5. False Optimization
   └─ Outdated stats can hurt performance
   └─ Stale statistics = wrong decisions
   └─ Can make queries slower (rare but possible)
   └─ Need monitoring to detect

❌ 6. Limited to Column Level
   └─ No index statistics (Delta Lake)
   └─ No histogram for complex types
   └─ No correlation stats
   └─ No multi-column stats (partial)

❌ 7. Delta Lake Limitations
   └─ Full table scan always needed
   └─ Cannot use Delta statistics incrementally
   └─ No sampling available
   └─ Approximate statistics not available


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Delta Lake format table
     └─ Works with: CREATE TABLE, CTAS, Parquet converted
  ✅ Table must exist
     └─ Not for views or external tables
  ✅ Read access to table
     └─ Permissions required
  ✅ Sufficient disk space
     └─ For statistics storage
  ✅ Connected to Spark cluster
     └─ Can't run locally

Optional but Recommended:
  ⚠️  Column list (if targeting specific columns)
     └─ More efficient than ALL COLUMNS
  ⚠️  Off-peak scheduling
     └─ Minimize impact on other queries
  ⚠️  Cluster with sufficient resources
     └─ Larger clusters faster execution
  ⚠️  Latest Databricks version
     └─ Better stats, more accurate


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Run first, before other optimizations
   ANALYZE TABLE → OPTIMIZE → Z-ORDER → VACUUM
   
2. Target specific columns if possible
   ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, region
   
3. Schedule regularly (monthly/quarterly)
   schedule: ANALYZE TABLE large_table COMPUTE STATISTICS
   
4. Run during off-peak hours
   Time: 2 AM - 4 AM (low query volume)
   
5. Monitor statistics freshness
   Track: When was ANALYZE last run?
   
6. Include in pipeline after bulk loads
   Pipeline: LOAD DATA → ANALYZE → OPTIMIZE

7. Use with cost-based optimizer
   Config: spark.sql.cbo.enabled = true


❌ DON'T:

1. Run on every query
   ✗ Unnecessary overhead
   ✗ Stats become stale anyway
   
2. Ignore statistics staleness
   ✗ Old stats = bad decisions
   ✗ Monitor last_analyze_time
   
3. Analyze very small tables
   ✗ Overhead > benefit
   ✗ Skip for <1GB tables
   
4. Skip for immutable historical data
   ✗ One-time analysis sufficient
   ✓ But re-run after OPTIMIZE
   
5. Expect miracles
   ✗ Statistics help, not magic
   ✗ Won't fix bad SQL design
   
6. Ignore resource impact
   ✗ Monitor cluster during ANALYZE
   ✗ Can spike CPU/IO
   
7. Use without CBO enabled
   ✗ Check: spark.sql.cbo.enabled
   ✗ Must be enabled for full benefit


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ ALWAYS use when:
  ├─ Table size > 10GB
  ├─ Complex queries (joins, aggregations)
  ├─ Multiple team members querying
  ├─ Before OPTIMIZE or Z-ORDER
  ├─ Planning to use PREDICTIVE OPT

✅ CONSIDER using when:
  ├─ Table size 1-10GB
  ├─ Running analytical queries
  ├─ Performance is critical
  ├─ New table or after bulk changes

✅ SKIP if:
  ├─ Table < 1GB
  ├─ One-off queries
  ├─ Development/testing only
  ├─ Simple full table scans


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PERFORMANCE IMPACT MATRIX
═════════════════════════════════════════════════════════════════════════════

Table Size    Execution Time    Query Improvement    Benefit
────────────────────────────────────────────────────────────────────────────
1 GB          1 minute          5-10%               Low (skip it)
10 GB         5 minutes         10-15%              Medium
100 GB        30 minutes        15-25%              High
1 TB          2-3 hours         20-30%              Very High
10 TB         8+ hours          25-35%              Critical


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE IMPLEMENTATION
═════════════════════════════════════════════════════════════════════════════

# Full analysis
ANALYZE TABLE production.analytics.orders 
COMPUTE STATISTICS FOR ALL COLUMNS;

# Specific columns only (faster)
ANALYZE TABLE production.analytics.orders 
COMPUTE STATISTICS FOR COLUMNS customer_id, region, order_date;

# Just table stats
ANALYZE TABLE production.analytics.orders 
COMPUTE STATISTICS;

# Check statistics
DESCRIBE FORMATTED production.analytics.orders;

# Scheduled in pipeline
# Weekly Monday 2 AM
ANALYZE TABLE production.analytics.orders 
COMPUTE STATISTICS FOR ALL COLUMNS;

OPTIMIZE production.analytics.orders;

VACUUM production.analytics.orders RETAIN 7 DAYS;
"""
        print(guide)


# ================================================================================
# OPTIMIZATION TECHNIQUE 2: OPTIMIZE
# ================================================================================

class OptimizeOperation:
    """OPTIMIZE - Compact small files into larger files"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 2: OPTIMIZE - SMALL FILE COMPACTION")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                           OPTIMIZE OVERVIEW                               ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  OPTIMIZE table_name;
  OPTIMIZE table_name ZORDER BY col1, col2;

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. Core Function
   ├─ Rewrites table combining small files into larger ones
   ├─ Reduces file count (1000s → 10s/100s)
   ├─ Keeps all data (no loss)
   ├─ Creates new files, marks old as deleted
   └─ Updates table metadata

2. File Operations
   ├─ Reads all small files
   ├─ Combines into larger chunks
   ├─ Default max file size: 1GB
   ├─ Default min file size: 1MB
   ├─ Configurable via TBLPROPERTIES

3. Performance Impact
   ├─ Fewer files = faster queries
   ├─ Reduced metadata operations
   ├─ Faster file listing
   ├─ Better I/O patterns
   └─ 10-30% query speedup typical

4. Data Layout
   ├─ Without ZORDER: Random order
   ├─ With ZORDER: Clustered order
   ├─ Improves range queries
   ├─ Reduces bytes scanned
   └─ 30-60% additional speedup

5. Execution Context
   ├─ Full table rewrite
   ├─ Creates new Parquet files
   ├─ Keeps transaction log
   ├─ Can be time-consuming
   └─ CPU and I/O intensive


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. Dramatic Performance Improvement
   └─ 10-30% faster queries (from compaction)
   └─ 30-60% faster with ZORDER
   └─ Consistent performance

✅ 2. Reduced Metadata Overhead
   └─ Fewer files = faster listing
   └─ Smaller transaction log
   └─ Less memory for file tracking
   └─ Faster DESCRIBE/INFO operations

✅ 3. Better I/O Efficiency
   └─ Fewer file handles open
   └─ Fewer network calls
   └─ Better read coalescing
   └─ Improved cache utilization

✅ 4. Combined with ZORDER
   └─ Multi-dimensional clustering
   └─ Optimal data layout
   └─ Multi-column range query improvement
   └─ One operation = two benefits

✅ 5. Handles Insert Overhead
   └─ Solves small file problem
   └─ Each INSERT creates files
   └─ OPTIMIZE consolidates them
   └─ Regular inserts can scale

✅ 6. Improves Downstream Operations
   └─ Partition elimination faster
   └─ Z-ORDER scans more efficient
   └─ VACUUM operates faster
   └─ Cascading benefits

✅ 7. Visible Results
   └─ Clear file count reduction
   └─ Easy to measure success
   └─ Quick validation
   └─ Easy to justify effort


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. Time Cost
   └─ Full table rewrite required
   └─ 10 minutes to 12+ hours
   └─ Size dependent (100GB = 30min+)
   └─ Resources blocked during operation

❌ 2. Resource Intensive
   └─ High CPU usage
   └─ High I/O throughput
   └─ Memory spikes possible
   └─ Network bandwidth needed

❌ 3. Temporary Storage Bloat
   └─ Old files not deleted immediately
   └─ New files created first
   └─ Requires 2x storage temporarily
   └─ Only freed after VACUUM

❌ 4. One-time Benefit
   └─ Not permanent
   └─ New data creates new small files
   └─ Need regular re-runs (weekly/monthly)
   └─ Cumulative fragmentation over time

❌ 5. Cannot Optimize While Writing
   └─ Blocks concurrent writes
   └─ Must stop inserts during OPTIMIZE
   └─ Scheduled downtime required
   └─ Not suitable for real-time ingestion

❌ 6. ZORDER Limitations
   └─ Requires knowing which columns to order on
   └─ Full data rewrite (more expensive than OPTIMIZE alone)
   └─ 2-4 columns recommended max
   └─ Wrong column selection doesn't help

❌ 7. Version History Clutter
   └─ Old files kept in transaction log
   └─ VACUUM needed to clean up
   └─ Takes extra step
   └─ Requires retention management

❌ 8. Limited to Table Level
   └─ Cannot selectively optimize partitions
   └─ Cannot optimize specific file ranges
   └─ All-or-nothing operation
   └─ No granular control


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Delta Lake table
     └─ Supports: All Delta format tables
  ✅ Write access to table
     └─ Permissions required
  ✅ No concurrent writes
     └─ Must stop ingestion during OPTIMIZE
  ✅ Sufficient temporary storage
     └─ Need 2x table size briefly
  ✅ Connected Spark cluster
     └─ With sufficient resources

Optional:
  ⚠️  For ZORDER: Known clustering columns
     └─ Which columns appear in filters?
  ⚠️  Off-peak window
     └─ Avoid during peak hours
  ⚠️  Cluster with enough resources
     └─ Larger = faster execution
  ⚠️  Monitoring/alerting setup
     └─ Track duration and resource usage


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Schedule regularly (weekly/monthly)
   OPTIMIZE production.table
   
2. Use ZORDER for multi-column queries
   OPTIMIZE table_name ZORDER BY region, product
   
3. Run after ANALYZE
   ANALYZE → OPTIMIZE → VACUUM sequence
   
4. Schedule during off-peak (2-4 AM)
   Avoid: Peak business hours
   
5. Monitor execution duration
   Track: How long does OPTIMIZE take?
   
6. Follow with VACUUM
   OPTIMIZE → VACUUM RETAIN 7 DAYS
   
7. Set reasonable file size limits
   Config: spark.databricks.delta.optimize.maxFileSize
   
8. Check file count improvement
   Before: DESCRIBE DETAIL table
   After: DESCRIBE DETAIL table


❌ DON'T:

1. Run constantly
   ✗ Unnecessary if few inserts
   ✗ Overhead outweighs benefit
   
2. ZORDER on wrong columns
   ✗ No improvement if columns not filtered
   ✗ Wasted effort
   
3. Optimize during peak hours
   ✗ Blocks writes
   ✗ Resource contention
   
4. Leave old files behind
   ✗ Run VACUUM after OPTIMIZE
   ✗ Doubles storage briefly
   
5. Use ZORDER on all columns
   ✗ Max 2-4 columns recommended
   ✗ Diminishing returns
   
6. Ignore file size settings
   ✗ Review minFileSize/maxFileSize
   ✗ Adjust if needed
   
7. Optimize immutable historical data
   ✗ One-time optimization OK
   ✗ Don't need to re-run
   
8. Expect consistency with unoptimized
   ✗ File order changes
   ✗ Row order changes (if using ZORDER)
   ✗ Results still correct


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ ALWAYS use when:
  ├─ File count > 100
  ├─ Many INSERT operations
  ├─ Queries feel slow
  ├─ Query latency SLA at risk

✅ RECOMMEND using when:
  ├─ Weekly for tables > 10GB
  ├─ Monthly for smaller tables
  ├─ After bulk loads
  ├─ Before critical operations

✅ SKIP if:
  ├─ File count < 10
  ├─ One-time immutable table
  ├─ No room for 2x storage
  ├─ Can't schedule downtime


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CONFIGURATION OPTIONS
═════════════════════════════════════════════════════════════════════════════

spark.databricks.delta.optimize.minFileSize
  └─ Default: 1MB
  └─ Files smaller than this are combined
  
spark.databricks.delta.optimize.maxFileSize
  └─ Default: 1GB
  └─ Target file size after optimization
  
spark.databricks.delta.autoCompact.enabled
  └─ Default: false
  └─ Auto-compact small files
  
spark.databricks.delta.autoCompact.minNumFiles
  └─ Default: 50
  └─ Trigger auto-compact after N files


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE IMPLEMENTATION
═════════════════════════════════════════════════════════════════════════════

# Standard optimization
OPTIMIZE production.orders;

# With Z-ordering
OPTIMIZE production.orders ZORDER BY customer_id, region;

# Check before/after
DESC DETAIL production.orders;  -- Before
OPTIMIZE production.orders;
DESC DETAIL production.orders;  -- After

# Automated scheduling (weekly 2 AM)
# Monday - Wednesday - Friday 2 AM:
OPTIMIZE production.orders ZORDER BY customer_id, region;
VACUUM production.orders RETAIN 7 DAYS;
"""
        print(guide)


# ================================================================================
# OPTIMIZATION TECHNIQUE 3: Z-ORDER CLUSTERING
# ================================================================================

class ZOrderClustering:
    """Z-ORDER - Multi-dimensional data clustering"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 3: Z-ORDER CLUSTERING")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                        Z-ORDER OVERVIEW                                   ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  OPTIMIZE table_name ZORDER BY col1, col2, col3;
  
  OR (without OPTIMIZE):
  -- Cannot be used standalone; must use OPTIMIZE ZORDER

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. What is Z-ORDER?
   ├─ Space-filling curve algorithm
   ├─ Maps multi-dimensional points to 1D
   ├─ Preserves spatial locality
   ├─ Groups similar values together
   └─ Industry standard for clustering

2. Data Organization
   ├─ Reorders rows within files
   ├─ Colocates related data
   ├─ Improves cache locality
   ├─ Reduces bytes scanned
   └─ Works for range queries

3. Columns
   ├─ 2-4 columns recommended
   ├─ Order matters (primary to secondary)
   ├─ Can be numeric or string
   ├─ Cardinality not critical
   └─ Works with any data type

4. Performance Impact
   ├─ 30-60% query speedup
   ├─ For range queries: 40-70%
   ├─ For multi-column filters: 50-80%
   ├─ For full scans: 0% improvement
   └─ Depends on query patterns

5. Execution
   ├─ Full table rewrite (like OPTIMIZE)
   ├─ Sorts data by Z-order curve
   ├─ Time proportional to table size
   ├─ One-time cost
   └─ Subsequent queries faster

6. Persistence
   ├─ Sorted order maintained
   ├─ Survives inserts (if using clustering)
   ├─ Can degrade with many updates
   ├─ May need re-run after bulk changes
   └─ Immutable tables stay sorted


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. Excellent Query Performance
   └─ 30-60% faster queries
   └─ 50-80% faster for multi-column queries
   └─ Sustained performance
   └─ Predictable improvement

✅ 2. Multi-dimensional Optimization
   └─ Handles 2-4 columns effectively
   └─ Better than single-column clustering
   └─ Useful for complex queries
   └─ Natural data locality

✅ 3. Works with Range Queries
   └─ WHERE col > X AND col < Y
   └─ Excellent for analytical queries
   └─ Better than equality filters
   └─ Range query speedup: 40-70%

✅ 4. Simple Implementation
   └─ One command: OPTIMIZE ZORDER BY ...
   └─ No schema changes
   └─ No special setup
   └─ Works with existing tables

✅ 5. Zero Storage Overhead
   └─ No extra storage needed
   └─ Just reorders data
   └─ Same number of files
   └─ No metadata bloat

✅ 6. Works with Partitioning
   └─ Can combine PARTITION + ZORDER
   └─ PARTITION by year, ZORDER by region
   └─ Two-level optimization
   └─ Best of both worlds

✅ 7. Diagnostic Benefits
   └─ Shows data distribution
   └─ Reveals clustering effectiveness
   └─ Measurable improvement
   └─ Validates optimization choice


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. One-time Benefit Only
   └─ Needs periodic re-runs
   └─ Degrades with updates
   └─ Manual re-optimization required
   └─ Not automatic like liquid clustering

❌ 2. High Time Cost
   └─ Full table rewrite (more than OPTIMIZE)
   └─ Adds sorting overhead
   └─ 2-4x slower than basic OPTIMIZE
   └─ 100GB table: 1-2 hours

❌ 3. Requires Known Query Patterns
   └─ Must know which columns to ZORDER on
   └─ Wrong columns = wasted effort
   └─ Cannot adapt to new patterns
   └─ Requires analysis first

❌ 4. Limited Column Support
   ├─ 2-4 columns maximum
   └─ More columns = diminishing returns
   └─ Cannot handle all patterns
   └─ May miss some queries

❌ 5. Not Effective for All Queries
   ├─ No benefit for full table scans
   ├─ Limited help for aggregations
   ├─ ORDER BY not improved
   ├─ GROUP BY depends on grouping columns
   └─ Only helps filter/join queries

❌ 6. Cannot Handle High-Frequency Updates
   └─ Best for immutable data
   └─ Degrades with frequent changes
   └─ Need re-optimization after bulk updates
   └─ Not suitable for streaming

❌ 7. Temporary Storage Required
   └─ Need 2x storage temporarily
   └─ Old + new files during operation
   └─ Only freed after VACUUM
   └─ May not be available in constrained envs

❌ 8. No Automatic Maintenance
   └─ Requires manual re-runs
   └─ No monitoring of degradation
   └─ User must decide when to re-sort
   └─ Missing optimizations not detected

❌ 9. Column Order Matters
   └─ First column most important
   └─ Secondary columns have less impact
   └─ Choosing order requires understanding queries
   └─ Suboptimal order = poor results


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Delta Lake table (not external Parquet)
     └─ CREATE TABLE, CTAS, Delta format required
  ✅ Know which columns to ZORDER on
     └─ Analyze query patterns first
  ✅ No concurrent writes
     └─ Stop ingestion during OPTIMIZE ZORDER
  ✅ 2x temporary storage
     └─ Needs space for old + new files
  ✅ Write access to table
     └─ Permissions required
  ✅ Immutable or low-update frequency
     └─ Works best with stable data

Optional:
  ⚠️  Pre-run ANALYZE TABLE
     └─ Helps optimizer understand ZORDER effectiveness
  ⚠️  Off-peak scheduling
     └─ Minimize impact
  ⚠️  Larger cluster
     └─ Parallel sorting faster
  ⚠️  Monitoring setup
     └─ Track before/after metrics


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Analyze query patterns first
   ├─ What columns are most filtered?
   ├─ What combinations appear?
   ├─ What queries are slow?
   
2. Choose 2-4 columns maximum
   ├─ More columns = less benefit
   ├─ Diminishing returns
   
3. Put most frequent column first
   ├─ Primary column gets priority
   ├─ More impact on performance
   
4. Use for immutable data
   ├─ Data warehouse tables
   ├─ Historical/archive data
   ├─ Reference tables
   
5. Combine with partitioning
   ├─ PARTITION BY date
   ├─ ZORDER BY customer_id
   ├─ Two levels of optimization
   
6. Measure improvement
   ├─ Query time before
   ├─ Query time after
   ├─ Calculate improvement %
   
7. Re-run monthly for active tables
   ├─ After bulk loads
   ├─ After major changes
   └─ Maintenance cycle

8. Document the ZORDER strategy
   ├─ Why these columns?
   ├─ What patterns does it optimize?
   ├─ When should it be re-run?


❌ DON'T:

1. ZORDER without analysis
   ✗ Wrong columns = wasted time
   ✗ No benefit if not filtering on columns
   
2. Use ZORDER for streaming
   ✗ Doesn't work with frequent updates
   ✗ Use LIQUID CLUSTERING instead
   
3. ZORDER more than 4 columns
   ✗ Diminishing returns
   ✗ Sorting becomes very expensive
   
4. Ignore column ordering
   ✗ Order matters significantly
   ✗ Put frequent filters first
   
5. Run too frequently
   ✗ One-time/monthly for stable data
   ✗ Overhead not justified
   
6. Forget to vacuum after
   ✗ Old files keep accumulating
   ✗ Run VACUUM to clean up
   
7. Use for all query types
   ✗ Only helps filter/join queries
   ✗ No benefit for aggregations
   ✗ No benefit for ORDER BY
   
8. Expect perfect performance
   ✗ Z-ORDER helps, doesn't guarantee
   ✗ Query structure matters
   ✗ Data volume still important


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ IDEAL for:
  ├─ Data warehouse tables
  ├─ Immutable fact tables
  ├─ Historical analytics data
  ├─ Reference tables
  ├─ Backup/archive tables
  ├─ Tables with known query patterns

✅ GOOD for:
  ├─ Batch-loaded tables (low update frequency)
  ├─ Multi-column filter queries
  ├─ Range query analytics
  ├─ Production analytical queries

❌ NOT good for:
  ├─ Streaming tables (frequent updates)
  ├─ Tables with random query patterns
  ├─ High cardinality single-column tables
  ├─ Real-time transactional data
  ├─ Tables you can't take offline


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE IMPLEMENTATION
═════════════════════════════════════════════════════════════════════════════

# Analyze table first
ANALYZE TABLE production.orders COMPUTE STATISTICS FOR ALL COLUMNS;

# Apply Z-ORDER (2 columns)
OPTIMIZE production.orders ZORDER BY region, product;

# Apply Z-ORDER (combined with partitioning)
# -- Table must be created with: PARTITION BY year, month
OPTIMIZE production.orders ZORDER BY customer_id, region;

# Measure before/after
-- Before: 50 second queries
-- After: 15 second queries
-- Improvement: 70%

# Monthly re-optimization
-- Schedule: First Monday of each month, 2 AM
OPTIMIZE production.orders ZORDER BY region, product;
VACUUM production.orders RETAIN 7 DAYS;

# Check effectiveness
SELECT COUNT(*) FROM production.orders 
WHERE region = 'US' AND product = 'Laptop';
-- Should be fast!
"""
        print(guide)


# ================================================================================
# OPTIMIZATION TECHNIQUE 4: PARTITIONING
# ================================================================================

class PartitioningStrategy:
    """Partitioning - Directory-based data organization"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 4: PARTITIONING - DIRECTORY-BASED ORGANIZATION")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                      PARTITIONING OVERVIEW                                ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  -- During creation
  df.write.partitionBy("year", "month").saveAsTable("table_name")
  
  -- Using SQL
  CREATE TABLE table_name (...) PARTITIONED BY (year INT, month INT)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. Core Concept
   ├─ Organizes data into subdirectories
   ├─ Based on column values
   ├─ Creates directory structure
   ├─ Enables partition pruning
   └─ Fundamental technique

2. Directory Structure
   Example: /data/table/year=2023/month=1/
           /data/table/year=2023/month=2/
           /data/table/year=2024/month=1/
   ├─ Each partition = one directory
   ├─ Can nest levels (year/month/day)
   ├─ Metadata tracks partition values
   └─ Query optimizer understands structure

3. Partition Pruning
   ├─ WHERE year = 2023 AND month = 1
   ├─ Reads ONLY year=2023/month=1/ directory
   ├─ Skips ALL other directories
   ├─ Huge performance boost
   └─ Up to 99% data skipped

4. Cardinality Requirement
   ├─ Low cardinality columns: < 100-1000 values
   ├─ Year: 5 partitions
   ├─ Month: 12 partitions
   ├─ Customer ID: Millions (TOO HIGH)
   ├─ Region: 7-50 partitions
   └─ Product category: 100-1000 partitions

5. Performance Characteristics
   ├─ Partition filter: 90-99% data skipped
   ├─ Query speedup: 50-99%
   ├─ Non-partition filter: 0% skipped
   ├─ Overhead: 5-10% for small partitions
   └─ Sweet spot: 100-10000 partitions


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. Extreme Query Performance
   └─ 50-99% faster for partition filters
   └─ 90-99% of data skipped
   └─ Best optimization for date-based queries
   └─ Predictable, guaranteed benefit

✅ 2. Efficient Data Management
   └─ Drop entire partition: DROP PARTITION year=2022
   └─ Much faster than DELETE WHERE
   └─ No scanning needed
   └─ Instant for large partitions

✅ 3. Incremental Loading
   └─ Add one partition per day
   └─ Historical partitions untouched
   └─ Faster loads
   └─ Better for batch pipelines

✅ 4. Simple Implementation
   └─ Just partition by column at creation time
   └─ Automatic directory structure
   └─ No complex setup
   └─ Easy to understand

✅ 5. Works with All Query Types
   └─ Filters benefit most
   └─ JOINs on partition columns fast
   └─ Aggregations faster (less data)
   └─ ORDER BY faster (less data)

✅ 6. Multiple Partition Levels
   └─ PARTITION BY year, month, day
   └─ Three levels of granularity
   └─ Fine-grained pruning
   └─ Maximum flexibility

✅ 7. Low Overhead
   └─ No additional storage
   └─ Just directory structure
   └─ No data duplication
   └─ Minimal metadata

✅ 8. Proven, Stable Technology
   └─ Used for decades (Hive)
   └─ Well understood
   └─ Reliable
   └─ Industry standard


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. Schema Lock-In
   └─ Partition columns chosen at table creation
   ├─ Cannot easily add new partition columns
   ├─ Cannot remove partition columns
   ├─ Requires table recreation
   └─ Limited flexibility

❌ 2. Low Cardinality Requirement
   └─ Cannot partition by customer_id (millions)
   └─ Cannot partition by transaction_id
   └─ Cannot partition by IP address
   └─ Limited to low-cardinality columns
   └─ Many high-cardinality queries not optimized

❌ 3. Small File Problem
   └─ Many partitions = many small files
   ├─ 365 days = 365 partitions
   ├─ 365 * regions (7) = 2555 partitions
   ├─ Each partition has small files
   └─ Needs OPTIMIZE to solve

❌ 4. Query Overhead for Non-partition Filters
   └─ WHERE customer_id = 'X' doesn't prune
   └─ Reads ALL partitions
   └─ No benefit for non-partition queries
   └─ Mixed workload suffers

❌ 5. Partition Explosion Risk
   └─ Too many partitions = overhead
   └─ > 10000 partitions = metadata bloat
   └─ Partition listing becomes slow
   └─ Metadata operations degrade

❌ 6. Management Complexity
   └─ Must manage partition values
   ├─ Add new partitions manually
   ├─ Remove old partitions
   ├─ Update partition stats
   └─ Operational overhead

❌ 7. INSERT Performance
   └─ Each partition = separate files
   └─ Many inserts = many small files
   └─ Needs regular OPTIMIZE
   └─ Operational burden

❌ 8. Not Ideal for Real-time
   └─ Cannot easily modify partitions
   └─ Best for batch loads
   └─ Streaming requires new partition per micro-batch
   └─ Can create partition explosion

❌ 9. ZORDER Conflicts
   └─ Cannot use ZORDER with partitions easily
   └─ ZORDER within partitions is option
   ├─ Adds complexity
   └─ Coordination required


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Low-cardinality column
     └─ Partition columns must have < 100-1000 values
  ✅ Known partition scheme
     └─ Decide columns at table creation
  ✅ Batch load compatible
     └─ Columns values stable over time
  ✅ CREATE TABLE with PARTITIONED BY
     └─ Must be in table definition
  ✅ Data organized by partition values
     └─ Data written to correct partitions

Recommended:
  ⚠️  Time-based partitioning (year/month/day)
     └─ Most common and effective
  ⚠️  ANALYZE TABLE after changes
     └─ Update partition statistics
  ⚠️  Regular OPTIMIZE
     └─ Manage small files
  ⚠️  MSCK REPAIR TABLE
     └─ Sync metadata with actual partitions


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Partition by date (year/month/day)
   PARTITION BY year, month
   
2. Limit to 2-3 partition levels
   Good: PARTITION BY year, month
   Bad: PARTITION BY year, month, day, hour
   
3. Use low-cardinality columns
   Good: region (7 values), category (100 values)
   Bad: customer_id (millions), product_id (millions)
   
4. Align with query patterns
   Partition by: Most common WHERE clauses
   
5. Run OPTIMIZE monthly
   OPTIMIZE table_name ZORDER BY ...
   
6. Monitor partition count
   SHOW PARTITIONS table_name | wc -l
   
7. Document partitioning scheme
   Why: What queries optimized?
   
8. Combine with ZORDER
   PARTITION BY date, ZORDER BY customer_id


❌ DON'T:

1. Partition by high-cardinality columns
   ✗ Customer ID, User ID, Transaction ID
   ✗ Millions of partitions
   
2. Use > 3 partition levels
   ✗ Excessive partitions
   ✗ Metadata overhead
   ✗ Management complexity
   
3. Partition statically if data changes
   ✗ New partition values appear
   ✗ Old partitions become sparse
   ✗ Operational issues
   
4. Ignore small file problem
   ✗ Many partitions = many small files
   ✗ Must run OPTIMIZE regularly
   
5. Forget to update partitions
   ✗ New data = new partitions
   ✗ Add partitions explicitly
   ✗ Or use dynamic partition discovery
   
6. Partition by frequently changing columns
   ✗ Partitions become unbalanced
   ✗ Some partitions get huge
   ✗ Others become tiny
   
7. Use partitioning for everything
   ✗ Only use when really needed
   ✗ Simple tables don't need it
   ✗ Can add complexity
   
8. Combine with very fine-grained partitions
   ✗ Year is good, day okay, hour bad
   ✗ Too many partitions
   ✗ Overhead exceeds benefit


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ PERFECT for:
  ├─ Time-series data (transactions, logs, events)
  ├─ Batch-loaded data (daily, weekly loads)
  ├─ Historical archives (quarterly/yearly data)
  ├─ Data warehouses (fact tables)
  ├─ Low-cardinality dimensions

✅ GOOD for:
  ├─ Regional data (partition by geography)
  ├─ Customer segments (partition by tier)
  ├─ Product categories (fixed categories)
  ├─ Long-term analytics

❌ NOT GOOD for:
  ├─ Real-time streaming (too many partitions)
  ├─ High-cardinality columns
  ├─ Rapidly changing data structures
  ├─ Ad-hoc exploratory queries
  ├─ Mixed query patterns


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE IMPLEMENTATION
═════════════════════════════════════════════════════════════════════════════

# Create partitioned table
CREATE TABLE production.orders (
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(10,2),
    order_date DATE
)
PARTITIONED BY (year INT, month INT)
USING DELTA;

# Write partitioned data
df.write
  .partitionBy("year", "month")
  .mode("append")
  .format("delta")
  .saveAsTable("production.orders")

# Query with partition filter (fast!)
SELECT * FROM production.orders 
WHERE year = 2024 AND month = 8;
-- Only reads: year=2024/month=8/ partition

# Show partitions
SHOW PARTITIONS production.orders;

# Drop old partition
ALTER TABLE production.orders 
DROP PARTITION (year=2020, month=1);

# Add new partition
ALTER TABLE production.orders 
ADD PARTITION (year=2024, month=9);

# Repair table metadata
MSCK REPAIR TABLE production.orders;
"""
        print(guide)


# ================================================================================
# OPTIMIZATION TECHNIQUE 5: LIQUID CLUSTERING
# ================================================================================

class LiquidClusteringGuide:
    """LIQUID CLUSTERING - Adaptive intelligent clustering"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 5: LIQUID CLUSTERING")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                    LIQUID CLUSTERING OVERVIEW                             ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  CREATE TABLE table_name (...) 
  CLUSTER BY col1, col2, col3
  USING DELTA;

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. What is Liquid Clustering?
   ├─ Databricks proprietary feature
   ├─ Combines partitioning + Z-ORDER benefits
   ├─ Adaptive clustering that evolves
   ├─ Works well with high-cardinality columns
   ├─ Automatic re-clustering on updates
   └─ No fixed directory structure (like partitioning)

2. Core Mechanism
   ├─ Hash-based bucketing internally
   ├─ Columns specified at creation
   ├─ Data automatically distributed
   ├─ Incremental re-clustering on insert/update
   ├─ No external directories
   └─ Transparent to user

3. Supported Columns
   ├─ ANY cardinality (10 - 1B+ values)
   ├─ Works with IDs (customer, user, product)
   ├─ Works with numeric (values, amounts)
   ├─ Works with strings (names, categories)
   ├─ Multiple columns (2-4 recommended)
   └─ Order matters (primary to secondary)

4. Performance Characteristics
   ├─ 20-50% query improvement typical
   ├─ Better for multi-column patterns
   ├─ Works with INSERT/UPDATE/DELETE
   ├─ Automatic re-clustering
   └─ Persistent ordering

5. Execution
   ├─ Defined at table creation
   ├─ No full table rewrite needed
   ├─ Incremental clustering with data
   ├─ Low overhead
   └─ Automatic maintenance


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. Handles High Cardinality
   └─ Works with millions of unique values
   └─ Customer IDs, User IDs, Product IDs
   ├─ Where partitioning fails
   └─ Fills gap between partitioning and Z-ORDER

✅ 2. Works with Updates
   └─ Automatic re-clustering on INSERT/UPDATE
   └─ Maintains ordering over time
   └─ No manual re-clustering needed
   └─ Perfect for mutable tables

✅ 3. Multiple Columns
   └─ Handles 2-4 columns effectively
   └─ Better than single-column approaches
   ├─ Multi-dimensional optimization
   └─ Handles complex query patterns

✅ 4. No Directory Overhead
   └─ No partition explosion
   └─ No directory structure to manage
   ├─ Cleaner metadata
   └─ Simpler file listing

✅ 5. Transparent Operation
   └─ Works automatically
   ├─ No manual OPTIMIZE needed
   ├─ No complex scheduling
   └─ Hands-off optimization

✅ 6. Flexible Query Patterns
   └─ Works with various query patterns
   ├─ Filters, joins, aggregations
   ├─ Mixed workloads
   └─ Not tied to specific query type

✅ 7. Persistent Order
   └─ Data stays clustered
   ├─ Survives updates
   ├─ Automatic re-clustering
   └─ Continuous benefit

✅ 8. Low Overhead
   └─ Incremental clustering (not full rewrite)
   ├─ Minimal performance impact
   ├─ Works during normal operations
   └─ Scales well


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. Databricks-Specific Feature
   └─ Only available on Databricks
   ├─ Not standard Spark
   └─ Not portable to other systems

❌ 2. Requires Newer Runtime
   └─ Databricks Runtime 11.0+
   ├─ Not available on older versions
   ├─ Cannot downgrade runtime
   └─ Compatibility requirements

❌ 3. Limited Control
   └─ Cannot control exact bucket assignment
   ├─ No visibility into bucketing
   ├─ Must trust system decisions
   └─ Less transparent than manual clustering

❌ 4. Storage Overhead
   └─ 5-15% metadata overhead
   ├─ More than partitioning
   ├─ Less than some Z-ORDER scenarios
   └─ Trade-off for automation

❌ 5. Not Optimal for Known Patterns
   └─ If you know exact clustering pattern
   ├─ Manual LIQUID CLUSTERING may be better
   ├─ AUTO CLUSTERING better for learning
   └─ LIQUID CLUSTERING general purpose

❌ 6. Cannot Change Cluster Columns
   └─ Defined at creation
   ├─ Cannot modify easily
   ├─ Would require table recreation
   └─ Limited flexibility

❌ 7. No Guarantees on Order
   └─ System decides bucketing strategy
   ├─ May change between versions
   ├─ Not guaranteed stable
   └─ Reproducibility concerns

❌ 8. Complex Interaction with Partitioning
   └─ Partitioning + LC together is rare
   ├─ Can combine but complex
   ├─ Coordination overhead
   └─ Usually use one or other


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Databricks Premium
     └─ Community/Standard editions don't support
  ✅ Databricks Runtime 11.0+
     └─ Earlier versions not supported
  ✅ Delta Lake format
     └─ Required, not optional
  ✅ Cluster columns selected
     └─ Decide at table creation
  ✅ Table creation rights
     └─ Must be able to CREATE TABLE

Recommended:
  ⚠️  Analyze queries first
     └─ Understand access patterns
  ⚠️  Choose high-cardinality columns
     └─ Where partitioning would fail
  ⚠️  2-4 columns
     └─ More not recommended
  ⚠️  Columns with good selectivity
     └─ Frequent in WHERE clauses


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Use for high-cardinality data
   CLUSTER BY customer_id, product_id
   
2. Choose frequently filtered columns
   What columns appear most in WHERE clauses?
   
3. Put primary column first
   Most important filter should be first
   
4. Use 2-4 columns maximum
   More columns = diminishing returns
   
5. Combine with PREDICTIVE OPT (if available)
   Both together = better results
   
6. Monitor performance
   Compare before/after metrics
   
7. Use for mixed workloads
   Handles diverse query patterns
   
8. Leverage automatic re-clustering
   Don't need manual OPTIMIZE


❌ DON'T:

1. Use for low-cardinality data
   ✗ Use PARTITIONING instead
   
2. Over-specify clustering columns
   ✗ 2-4 columns, not more
   
3. Change cluster columns after creation
   ✗ Not easily done
   ✗ Would need recreate
   
4. Expect instant perfect performance
   ✗ Clustering improves over time
   ✗ Takes a few micro-batches
   
5. Use without understanding patterns
   ✗ Analyze queries first
   
6. Skip monitoring
   ✗ Track effectiveness
   
7. Combine with ZORDER
   ✗ Both do similar things
   ✗ Choose one or the other


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ PERFECT for:
  ├─ High-cardinality IDs (customer, user, product)
  ├─ Real-time + batch mixed workload
  ├─ Frequent updates/inserts
  ├─ Multiple query patterns
  ├─ SaaS applications (tenant data)
  ├─ E-commerce (customer transactions)

✅ GOOD for:
  ├─ Medium-to-large tables (>10GB)
  ├─ Streaming + analytics mix
  ├─ Production environments
  ├─ Complex query patterns

❌ NOT good for:
  ├─ Low-cardinality data (use partitioning)
  ├─ Immutable historical data (use Z-ORDER)
  ├─ Very small tables (<1GB)
  ├─ Non-Databricks systems


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE IMPLEMENTATION
═════════════════════════════════════════════════════════════════════════════

# Create liquid clustered table
CREATE TABLE production.orders (
    order_id STRING,
    customer_id STRING,
    region STRING,
    product STRING,
    amount DECIMAL(10,2)
)
CLUSTER BY customer_id, region
USING DELTA;

# Insert data (auto-clustering happens)
INSERT INTO production.orders
SELECT * FROM staging.orders;

# Insert more data (re-clustering continues)
INSERT INTO production.orders
SELECT * FROM staging.orders_daily;

# Queries automatically benefit
SELECT * FROM production.orders
WHERE customer_id = 'CUST_123' 
AND region = 'US';
-- Fast! (data is clustered by these columns)

# Update data (clustering persists)
UPDATE production.orders
SET amount = amount * 1.1
WHERE year = 2024;

# Check clustering stats
DESC FORMATTED production.orders;
-- Shows clustering information
"""
        print(guide)


# ================================================================================
# OPTIMIZATION TECHNIQUE 6: AUTO LIQUID CLUSTERING
# ================================================================================

class AutoLiquidClusteringGuide:
    """AUTO LIQUID CLUSTERING - ML-driven automatic clustering"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 6: AUTO LIQUID CLUSTERING")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                   AUTO LIQUID CLUSTERING OVERVIEW                         ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  ALTER TABLE table_name SET TBLPROPERTIES (
    'delta.clustering.enabled' = 'true'
  );

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. Core Mechanism
   ├─ ML model learns query patterns
   ├─ Automatically recommends clustering columns
   ├─ System applies clustering
   ├─ Continuous monitoring
   └─ Adapts to pattern changes

2. Timeline
   ├─ Week 1-2: Data collection (queries monitored)
   ├─ Week 2-3: ML analysis & training
   ├─ Week 3-4: Recommendations generated
   ├─ Week 4+: Auto-clustering applied
   ├─ Month 1+: Continuous improvement
   └─ Benefits visible after 2-4 weeks

3. ML Algorithm
   ├─ Analyzes WHERE clause patterns
   ├─ Measures column frequency
   ├─ Calculates cardinality
   ├─ Measures correlation between columns
   ├─ Predicts performance improvement
   ├─ Ranks recommendations by ROI
   └─ High confidence threshold

4. Automation Level
   ├─ Can auto-apply (if enabled)
   ├─ Or show recommendations (manual approval)
   ├─ Configurable via settings
   ├─ Governance options available
   └─ Audit trail included

5. Adaptation
   ├─ Weekly analysis re-runs
   ├─ Detects pattern changes
   ├─ Can recommend re-clustering
   ├─ Incremental updates
   └─ Continuous optimization


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. No Manual Analysis Needed
   └─ No need to know query patterns
   ├─ System learns automatically
   ├─ ML makes decisions
   └─ Hands-off optimization

✅ 2. Adapts to Changing Patterns
   └─ Query patterns change over time
   ├─ AUTO detects changes
   ├─ Re-analyzes weekly
   ├─ Can recommend new clustering
   └─ Continuous adaptation

✅ 3. Multiple Column Discovery
   └─ Finds optimal column combinations
   ├─ Not limited to single column
   ├─ Discovers multi-column patterns
   ├─ Score and rank options
   └─ Best option selected

✅ 4. Risk Mitigation
   └─ ML model validates recommendations
   ├─ High confidence threshold
   ├─ Lower risk than manual guessing
   ├─ Backed by data
   └─ Proven effective

✅ 5. Handles Unknown Patterns
   └─ Perfect for exploratory analytics
   ├─ Diverse query patterns
   ├─ Ad-hoc analytics
   └─ No need to predict

✅ 6. Continuous Learning
   └─ Model improves over time
   ├─ More data = better predictions
   ├─ Error rates decrease
   ├─ Better recommendations
   └─ ROI improves

✅ 7. Development-Friendly
   └─ Great for dev/test environments
   ├─ Patterns evolving
   ├─ Queries changing
   ├─ AUTO handles it
   └─ Automatic optimization

✅ 8. Visualization & Monitoring
   └─ Dashboard shows recommendations
   ├─ Confidence scores visible
   ├─ Expected benefits shown
   ├─ Easy to validate
   └─ Track effectiveness


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. Initial Delay
   └─ 2-4 weeks to see benefits
   ├─ Data collection time
   ├─ Analysis time
   ├─ Recommendation generation
   └─ Not suitable for urgent optimization

❌ 2. Databricks-Specific
   └─ Only on Databricks
   ├─ Premium tier required
   ├─ Not portable
   └─ Vendor lock-in

❌ 3. Less Transparent
   └─ ML model decisions may seem opaque
   ├─ Not immediately obvious why
   ├─ Harder to explain to stakeholders
   ├─ Trust required
   └─ Audit trail complex

❌ 4. Unpredictable Recommendations
   └─ Output depends on query mix
   ├─ Different workloads = different recommendations
   ├─ Cannot guarantee specific columns
   └─ Less control than manual

❌ 5. Not Optimal for Known Patterns
   └─ If you know exact optimal clustering
   ├─ Manual LIQUID CLUSTERING may be better
   ├─ Less overhead
   ├─ Immediate results
   └─ Wasted analysis effort

❌ 6. Requires Sufficient Queries
   └─ Needs many queries for good data
   ├─ Small workloads have less data
   ├─ Recommendations less confident
   ├─ New tables take longer
   └─ Quiet tables not optimized

❌ 7. ML Model Errors
   └─ Model can be wrong
   ├─ Rare but possible
   ├─ Recommendations not always perfect
   ├─ Needs human validation
   └─ Potential false negatives

❌ 8. Storage Overhead
   └─ Metadata for clustering
   ├─ Telemetry storage
   ├─ Model artifacts
   ├─ Adds cost
   └─ Non-trivial overhead

❌ 9. Integration Complexity
   └─ Works best with PREDICTIVE OPT
   ├─ Simpler alone
   ├─ More complex with multiple systems
   ├─ Orchestration needed
   └─ Monitoring overhead


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Databricks Premium or Enterprise
     └─ Not available on Community/Standard
  ✅ Unity Catalog (strongly recommended)
     └─ Works better with catalog
  ✅ Delta Lake table
     └─ Must be Delta format
  ✅ Query workload
     └─ Need many queries for good data
     └─ Quiet tables don't get optimized
  ✅ 2-4 weeks time
     └─ Wait for recommendations
     └─ Cannot rush process

Recommended:
  ⚠️  Sufficient query volume
     └─ 100+ queries/day for good patterns
  ⚠️  Stable/predictable workload
     └─ Not erratic patterns
  ⚠️  Large table (>10GB)
     └─ AUTO overhead justified
  ⚠️  Monitoring capability
     └─ Track results


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Use for unknown patterns
   Unknown query mix? Use AUTO
   
2. Enable early in lifecycle
   Let it learn from day 1
   
3. Monitor recommendations
   Check what it recommends weekly
   
4. Validate recommendations
   Do they make sense? Check metrics
   
5. Use in dev/test heavily
   Patterns changing? AUTO adapts
   
6. Review monthly
   Is clustering still optimal?
   
7. Combine with PREDICTIVE OPT
   Both together = better results
   
8. Track metrics over time
   Before/after improvement %


❌ DON'T:

1. Use if patterns are known
   ✗ Manual clustering faster
   
2. Expect immediate results
   ✗ Takes 2-4 weeks
   
3. Ignore recommendations
   ✗ Review and validate
   
4. Disable monitoring
   ✗ Need to track effectiveness
   
5. Use for small tables
   ✗ Overhead not justified
   
6. Expect perfect recommendations
   ✗ ML model can be wrong
   ✗ Validate before trusting
   
7. Set and forget
   ✗ Monitor continuously
   ✗ Patterns may change
   
8. Run in production without testing
   ✗ Test in staging first


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ PERFECT for:
  ├─ Development environments
  ├─ Exploratory analytics
  ├─ Unknown/evolving patterns
  ├─ Large diverse tables
  ├─ SaaS platforms (varied usage)
  ├─ Data lakes (many query types)

✅ GOOD for:
  ├─ Learning what queries look like
  ├─ Validating manual clustering choices
  ├─ Monitoring pattern changes
  ├─ Backup optimization system

❌ NOT good for:
  ├─ Urgent optimization needed
  ├─ Known, stable patterns
  ├─ Small tables
  ├─ Quiet tables (few queries)
  ├─ Non-Databricks systems


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE IMPLEMENTATION
═════════════════════════════════════════════════════════════════════════════

# Enable AUTO clustering
ALTER TABLE production.orders 
SET TBLPROPERTIES (
    'delta.clustering.enabled' = 'true'
);

# Wait 2-4 weeks for recommendations
# Week 1-2: Queries monitored
# Week 2-3: ML analysis
# Week 3-4: Recommendations ready

# Check recommendations
SELECT * FROM system.clustering_recommendations 
WHERE table_name = 'orders';

# Recommendation shows:
# CLUSTER BY customer_id, region
# Expected improvement: 45%
# Confidence: 94%

# System applies clustering
# Continue monitoring...

# Monthly review
SELECT * FROM system.clustering_metrics 
WHERE table_name = 'orders'
AND date >= current_date() - 30;

# Adjust if needed
"""
        print(guide)


# ================================================================================
# Continue with remaining techniques...
# ================================================================================

class PredictiveOptimizationGuide:
    """PREDICTIVE OPTIMIZATION - ML-driven system optimization"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 7: PREDICTIVE OPTIMIZATION")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                  PREDICTIVE OPTIMIZATION OVERVIEW                         ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  ALTER TABLE table_name SET TBLPROPERTIES (
    'delta.predictiveOptimization.enabled' = 'true'
  );

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. Scope of Optimization
   ├─ File compaction (OPTIMIZE)
   ├─ Multi-column clustering (Z-ORDER)
   ├─ Cleanup scheduling (VACUUM)
   ├─ Metadata optimization
   └─ Comprehensive system optimization

2. Automation Level
   ├─ Analyzes query patterns
   ├─ Plans optimization strategy
   ├─ Auto-schedules operations
   ├─ Executes in background
   ├─ Monitors effectiveness
   └─ Continuous adjustment

3. Time Investment
   ├─ Week 1-7: Data collection
   ├─ Week 2-3: Analysis
   ├─ Week 3-4: Recommendations
   ├─ Week 4+: Background operations
   ├─ Month 1: Visible benefits
   └─ Month 2+: Full optimization

4. Collaboration with AUTO CLUSTERING
   ├─ AUTO handles data layout
   ├─ PREDICTIVE handles compaction
   ├─ Together = synergistic effect
   ├─ Combined improvement > separate
   └─ 85%+ combined benefit possible

5. Intelligence Level
   ├─ ML learns query patterns
   ├─ Predicts optimal OPTIMIZE frequency
   ├─ Determines ZORDER strategy
   ├─ Schedules VACUUM intelligently
   ├─ Monitors cost-benefit
   └─ Adjusts strategy continuously


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. Comprehensive Optimization
   └─ Handles multiple aspects
   ├─ Compaction
   ├─ Clustering
   ├─ Cleanup
   └─ All coordinated

✅ 2. Hands-Off Operation
   └─ Set and forget
   ├─ No manual scheduling
   ├─ Automatic execution
   ├─ No tuning needed
   └─ Background operation

✅ 3. Cost Optimization
   └─ Minimizes storage costs
   ├─ Optimizes query costs
   ├─ Reduces wasted compute
   └─ Maximum ROI

✅ 4. Proven ROI
   └─ 40-70% overall improvement
   ├─ 30-50% query speedup
   ├─ 50-70% bytes scanned reduction
   └─ Clear cost savings

✅ 5. Adaptive Strategy
   └─ Learns and adjusts
   ├─ Detects pattern changes
   ├─ Updates recommendations
   ├─ Improves over time
   └─ Self-optimizing

✅ 6. Multi-Table Support
   └─ Can optimize multiple tables
   ├─ Prioritizes by ROI
   ├─ Manages resources
   └─ Cost-aware

✅ 7. Monitoring Dashboard
   └─ Visibility into optimization
   ├─ Before/after metrics
   ├─ Cost savings shown
   ├─ Recommendations tracked
   └─ Easy to validate

✅ 8. Production-Ready
   └─ Thoroughly tested
   ├─ Reliability proven
   ├─ Used at scale
   ├─ Battle-tested
   └─ Enterprise grade


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. Premium Only
   └─ Requires Databricks Premium+
   ├─ Significant cost
   ├─ Not for budget-conscious
   └─ ROI must justify cost

❌ 2. Long Time to Benefit
   └─ 2-4 weeks for recommendations
   ├─ Month+ for full optimization
   ├─ Not for urgent needs
   └─ Delayed gratification

❌ 3. Complex Interaction
   └─ Many subsystems involved
   ├─ OPTIMIZE, ZORDER, VACUUM
   ├─ Coordination complexity
   ├─ More things to monitor
   └─ Operational complexity

❌ 4. Less Control
   └─ ML makes decisions
   ├─ Less transparency
   ├─ Harder to override
   ├─ Governance challenges
   └─ Audit trails complex

❌ 5. Integration Overhead
   └─ Works best with AUTO CLUSTERING
   ├─ Separate = limited benefit
   ├─ Together more complex
   ├─ Coordination needed
   └─ Operational burden

❌ 6. Not Optimal for Stable Tables
   └─ If table never changes
   ├─ One-time optimization OK
   ├─ Continuous optimization wastes resources
   └─ Manual approach may be better

❌ 7. Metadata Storage
   └─ Collects lots of telemetry
   ├─ Storage costs
   ├─ Analytics overhead
   ├─ Privacy considerations
   └─ Data retention complexity

❌ 8. ML Model Errors
   └─ Model can make wrong decisions
   ├─ Rare but possible
   ├─ Can optimize for wrong metric
   ├─ Validation needed
   └─ Needs human oversight


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Databricks Premium or Enterprise
     └─ Mandatory
  ✅ Unity Catalog
     └─ Strongly recommended
  ✅ Delta Lake tables
     └─ All tables must be Delta format
  ✅ Sufficient storage for telemetry
     └─ Metadata collection needs space
  ✅ Query workload
     └─ Tables must have queries to optimize
  ✅ 2-4 weeks timeline
     └─ Cannot expedite learning

Recommended:
  ⚠️  Large tables (>100GB)
     └─ ROI better
  ⚠️  Complex workloads
     └─ More to optimize
  ⚠️  High query volume
     └─ Better learning data
  ⚠️  Monitoring/alerting setup
     └─ Track results


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Enable for large production tables
   Tables > 100GB
   
2. Combine with AUTO CLUSTERING
   Both together for maximum benefit
   
3. Monitor dashboard weekly
   Track optimization progress
   
4. Review cost-benefit monthly
   Is ROI positive?
   
5. Set expectations on timeline
   4 weeks for full benefit
   
6. Budget for Premium tier
   Cost justified by savings
   
7. Have governance process
   Review recommendations
   
8. Track metrics
   Before/after, month-over-month


❌ DON'T:

1. Use for small tables
   ✗ Overhead not justified
   
2. Expect immediate results
   ✗ Takes 4 weeks minimum
   
3. Ignore recommendations
   ✗ Review and validate
   
4. Combine with manual OPTIMIZE
   ✗ Conflicting strategies
   
5. Use for immutable tables
   ✗ Manual optimization sufficient
   
6. Expect perfect decisions
   ✗ Monitor for issues
   
7. Forget about VACUUM retention
   ✗ Still need to manage
   
8. Set and truly forget
   ✗ Monitor continuously


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ PERFECT for:
  ├─ Large tables (>100GB)
  ├─ Production environments
  ├─ Mixed real-time + batch workloads
  ├─ Complex query patterns
  ├─ Cost-sensitive (optimize spend)
  ├─ Hands-off optimization desired

✅ GOOD for:
  ├─ Enterprise deployments
  ├─ Mission-critical tables
  ├─ When budget allows

❌ NOT GOOD for:
  ├─ Small tables (<10GB)
  ├─ Community/Standard editions
  ├─ Immutable tables
  ├─ Urgent optimization
  ├─ Budget-constrained
"""
        print(guide)


# ================================================================================
# OPTIMIZATION TECHNIQUE 8: VACUUM
# ================================================================================

class VacuumGuide:
    """VACUUM - Cleanup and storage management"""
    
    @staticmethod
    def overview():
        print("\n" + "="*80)
        print("TECHNIQUE 8: VACUUM - CLEANUP & MAINTENANCE")
        print("="*80)
        
        guide = """
╔════════════════════════════════════════════════════════════════════════════╗
║                         VACUUM OVERVIEW                                   ║
╚════════════════════════════════════════════════════════════════════════════╝

SYNTAX:
  VACUUM table_name;                    -- 30 days retention
  VACUUM table_name RETAIN 7 DAYS;      -- 7 days retention
  VACUUM table_name RETAIN 0 DAYS;      -- Immediate (risky!)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KEY POINTERS
═════════════════════════════════════════════════════════════════════════════

1. Purpose
   ├─ Removes old file versions
   ├─ Cleans up after OPTIMIZE
   ├─ Cleans up after deletes
   ├─ Reclaims storage space
   └─ Maintenance operation

2. What Gets Deleted
   ├─ Files older than retention period
   ├─ Files not referenced by any version
   ├─ Temporary files
   ├─ Failed transaction artifacts
   └─ NOT current table data

3. Storage Impact
   ├─ Typical: 10-30% storage reduction
   ├─ After OPTIMIZE: Extra 15-30% freed
   ├─ After deletes: 20-50% freed
   ├─ Long-term: Continuous improvement
   └─ Monthly runs = steady cleanup

4. Data Retention
   ├─ Default: 30 days (production safe)
   ├─ Can be 0-30+ days
   ├─ 0 days = immediate deletion (risky)
   ├─ 30+ days = keep longer history
   └─ Affects time-travel capability

5. Time-Travel Impact
   ├─ Can query old versions
   ├─ RESTORE to previous version
   ├─ VACUUM removes old versions
   ├─ After VACUUM = can't restore far back
   └─ Trade-off between storage and history


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PROS (Advantages)
═════════════════════════════════════════════════════════════════════════════

✅ 1. Storage Cost Reduction
   └─ 15-30% typical savings
   ├─ 50%+ after bulk operations
   ├─ Significant cost impact
   └─ Quick ROI

✅ 2. Maintenance Necessity
   └─ Part of lifecycle management
   ├─ Keeps storage clean
   ├─ Removes orphaned files
   ├─ Prevents bloat
   └─ Required for health

✅ 3. Simple Operation
   └─ One command
   ├─ Easy to schedule
   ├─ Minimal complexity
   ├─ Quick to execute (usually)
   └─ Low operational overhead

✅ 4. Configurable Retention
   └─ Choose retention period
   ├─ 7 days for frequent tables
   ├─ 30 days for production
   ├─ 90+ days for compliance
   └─ Flexible policy

✅ 5. Metadata Cleanup
   └─ Reduces transaction log
   ├─ Smaller metastore
   ├─ Faster directory listing
   ├─ Better metadata performance
   └─ Less data to manage

✅ 6. Enables Other Optimizations
   └─ OPTIMIZE creates old files
   ├─ VACUUM cleans them up
   ├─ Together = complete process
   ├─ Synergistic benefit
   └─ Part of optimization pipeline

✅ 7. Low Risk
   └─ Only removes old versions
   ├─ Current data never touched
   ├─ Cannot corrupt current table
   ├─ Easy to schedule
   └─ Safe for automation

✅ 8. Auditable
   └─ Changes are logged
   ├─ Transaction history
   ├─ Can track cleanup
   ├─ Compliance friendly
   └─ Transparent operation


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LIMITATIONS (Constraints)
═════════════════════════════════════════════════════════════════════════════

❌ 1. Permanent Deletion
   └─ Once deleted, cannot recover
   ├─ No undo (beyond retention)
   ├─ Lost history
   ├─ Cannot time-travel far back
   └─ Requires careful retention

❌ 2. Time-Travel Impact
   └─ Removes ability to restore
   ├─ Cannot RESTORE to deleted versions
   ├─ Cannot query old snapshots
   ├─ Short history window
   └─ Compliance/audit considerations

❌ 3. Execution Time
   └─ Can take hours for large tables
   ├─ Full directory scan
   ├─ File deletion overhead
   ├─ Must complete uninterrupted
   └─ Schedule carefully

❌ 4. Storage Still Consumed
   └─ Deleted files not reclaimed immediately
   ├─ File system cleanup delayed
   ├─ Space shows as used temporarily
   ├─ May take hours to fully reclaim
   └─ Allocate extra space

❌ 5. Concurrent Operations
   └─ Cannot run during heavy writes
   ├─ Blocks with lock
   ├─ Slows concurrent inserts
   ├─ Schedule during off-peak
   └─ Operational constraint

❌ 6. Risky with 0 Days
   └─ RETAIN 0 DAYS is dangerous
   ├─ Immediate deletion
   ├─ No safety window
   ├─ Cannot recover errors
   ├─ Should only be done after backups
   └─ Requires overrides

❌ 7. Compliance Complications
   └─ May conflict with retention policy
   ├─ Legal holds
   ├─ Audit requirements
   ├─ Regulatory compliance
   ├─ May not be allowed
   └─ Check with compliance team

❌ 8. Metadata Lag
   └─ Deletion not instantaneous
   ├─ Metastore updates delayed
   ├─ Directory listing lag
   ├─ Space reclamation delayed
   └─ Takes time to fully clean


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PREREQUISITES
═════════════════════════════════════════════════════════════════════════════

Required:
  ✅ Delta Lake table
     └─ Non-Delta tables don't have versions
  ✅ Write access to table
     └─ Permissions required
  ✅ No open transactions
     └─ Cannot VACUUM while queries running
  ✅ Sufficient permissions
     └─ Cannot run as read-only user
  ✅ Storage space
     └─ Temporary files need space

Optional:
  ⚠️  Off-peak scheduling
     └─ Minimize impact
  ⚠️  Backups completed
     └─ Safety measure
  ⚠️  Monitoring/alerting
     └─ Track execution
  ⚠️  Compliance review
     └─ Check retention policy


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BEST PRACTICES
═════════════════════════════════════════════════════════════════════════════

✅ DO:

1. Run regularly (weekly/monthly)
   Prevents bloat
   
2. Use appropriate retention
   Dev: 1-7 days
   Prod: 30 days
   Compliance: 90-180 days
   
3. Schedule off-peak (2-4 AM)
   Minimize impact
   
4. Follow OPTIMIZE with VACUUM
   OPTIMIZE → VACUUM sequence
   
5. Monitor execution
   Track duration and space freed
   
6. Document retention policy
   Why chosen period?
   
7. Set retention in TBLPROPERTIES
   Automate retention logic
   
8. Have backup strategy
   Before running VACUUM


❌ DON'T:

1. Use RETAIN 0 without caution
   ✗ Immediate deletion is risky
   ✗ Only after validation
   
2. Run during active queries
   ✗ Blocks operations
   ✗ Schedule off-peak
   
3. Forget about backups
   ✗ Safety first
   
4. Ignore retention requirements
   ✗ Check compliance
   ✗ Check legal holds
   
5. Run on critical data without testing
   ✗ Test on non-critical first
   
6. Expect instant space reclaim
   ✗ Takes time
   ✗ Space freed gradually
   
7. VACUUM too frequently
   ✗ Unnecessary overhead
   ✗ Schedule appropriately
   
8. Forget documentation
   ✗ Document retention policy
   ✗ Why this choice?


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHEN TO USE
═════════════════════════════════════════════════════════════════════════════

✅ ALWAYS use when:
  ├─ After OPTIMIZE operations
  ├─ After bulk DELETE operations
  ├─ Storage costs high
  ├─ Compliance allows
  ├─ Production tables (monthly+)
  ├─ Development tables (weekly)

✅ CONSIDER using when:
  ├─ Storage growing unbounded
  ├─ Metadata operations slow
  ├─ Failed operations leave orphans
  ├─ Need to manage costs

❌ SKIP if:
  ├─ Compliance prevents deletion
  ├─ Legal holds in place
  ├─ Need full history
  ├─ Archive tables (preserve all)


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLE IMPLEMENTATION
═════════════════════════════════════════════════════════════════════════════

# Standard production cleanup
VACUUM production.orders RETAIN 30 DAYS;

# More aggressive cleanup
VACUUM production.orders RETAIN 7 DAYS;

# After bulk operations
OPTIMIZE production.orders;
VACUUM production.orders RETAIN 7 DAYS;

# Automated scheduling (weekly Monday 2 AM)
-- spark.sql("VACUUM production.orders RETAIN 7 DAYS")

# Check cleanup effect
-- Before: DESCRIBE DETAIL production.orders
VACUUM production.orders RETAIN 7 DAYS;
-- After: DESCRIBE DETAIL production.orders
-- Storage reduced by 15-30%

# Compliance-aware retention
ALTER TABLE production.orders 
SET TBLPROPERTIES (
  'delta.retentionDurationCheck.enabled' = 'false'
);
-- Then: VACUUM production.orders RETAIN 90 DAYS;
"""
        print(guide)


# ================================================================================
# MAIN EXECUTION
# ================================================================================

def main():
    """Execute complete guide"""
    
    print("\n" + "="*80)
    print("COMPLETE DATABRICKS OPTIMIZATION GUIDE")
    print("ALL TECHNIQUES - POINTERS, PROS, LIMITATIONS & PREREQUISITES")
    print("="*80)
    
    try:
        # Technique 1
        AnalyzeTable.overview()
        
        # Technique 2
        OptimizeOperation.overview()
        
        # Technique 3
        ZOrderClustering.overview()
        
        # Technique 4
        PartitioningStrategy.overview()
        
        # Technique 5
        LiquidClusteringGuide.overview()
        
        # Technique 6
        AutoLiquidClusteringGuide.overview()
        
        # Technique 7
        PredictiveOptimizationGuide.overview()
        
        # Technique 8
        VacuumGuide.overview()
        
        print("\n" + "="*80)
        print("✅ COMPLETE GUIDE FINISHED!")
        print("="*80)
        
        print("""
QUICK REFERENCE - WHEN TO USE WHAT
═════════════════════════════════════════════════════════════════════════════

1. ANALYZE TABLE
   ├─ When: Before any other optimization
   ├─ Time: 5 min - 2 hours
   ├─ Benefit: 10-20% (indirect)
   └─ Frequency: Before major changes

2. OPTIMIZE
   ├─ When: Weekly/monthly or after inserts
   ├─ Time: 10 min - 8 hours
   ├─ Benefit: 10-30%
   └─ Frequency: Weekly

3. Z-ORDER
   ├─ When: Immutable analytical data
   ├─ Time: 1-12 hours
   ├─ Benefit: 30-60%
   └─ Frequency: Monthly/after bulk loads

4. PARTITIONING
   ├─ When: Time-series, low cardinality
   ├─ Time: At table creation
   ├─ Benefit: 50-99%
   └─ Frequency: One-time (at creation)

5. LIQUID CLUSTERING
   ├─ When: High cardinality IDs, updates
   ├─ Time: At table creation
   ├─ Benefit: 20-50%
   └─ Frequency: Automatic (incremental)

6. AUTO LIQUID CLUSTERING
   ├─ When: Unknown patterns
   ├─ Time: 2-4 weeks for benefits
   ├─ Benefit: 30-50% (after learning)
   └─ Frequency: Continuous

7. PREDICTIVE OPTIMIZATION
   ├─ When: Large tables, mixed workloads
   ├─ Time: 4 weeks for full benefit
   ├─ Benefit: 40-70%
   └─ Frequency: Continuous

8. VACUUM
   ├─ When: After OPTIMIZE, monthly
   ├─ Time: Minutes to hours
   ├─ Benefit: 15-30% storage savings
   └─ Frequency: Weekly/monthly


OPTIMIZATION PIPELINE (RECOMMENDED)
═════════════════════════════════════════════════════════════════════════════

Week 1:
  Day 1: ANALYZE TABLE
  Day 2: OPTIMIZE
  Day 3: Apply clustering strategy (Z-ORDER or LIQUID)
  Day 4: Measure improvement

Week 2+:
  Monthly: ANALYZE TABLE + OPTIMIZE + VACUUM
  Weekly: Check performance
  Quarterly: Review strategy

With Predictive Optimization:
  Enable alongside AUTO CLUSTERING for 85%+ improvement


DECISION MATRIX BY SCENARIO
═════════════════════════════════════════════════════════════════════════════

Time-Series Data (Logs, Events):
  └─ PARTITION BY date + ZORDER BY key

E-Commerce Orders:
  └─ LIQUID CLUSTER BY customer_id + region

Analytics Warehouse:
  └─ PARTITION BY date + ZORDER BY columns

Real-Time Streaming:
  └─ LIQUID CLUSTER BY key columns

SaaS Multi-Tenant:
  └─ AUTO CLUSTERING + PREDICTIVE OPT

Development/Exploratory:
  └─ AUTO CLUSTERING

Small Tables:
  └─ Just ANALYZE + VACUUM

Large Tables:
  └─ Full stack: ANALYZE + OPTIMIZE + Clustering + PREDICTIVE OPT
""")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


Summary Table
Technique |	When	Time	Benefit	Frequency	Complexity
ANALYZE   |	First	5m-2h	10-20%	Before changes	Low
OPTIMIZE  |	Weekly	10m-8h	10-30%	Weekly	Low
Z-ORDER	Immutable	| 1-12h	30-60%	Monthly	Medium
PARTITION |	Creation	Once	50-99%	One-time	Medium
LIQUID CLUSTER |	Creation	Once	20-50%	Automatic	Low
AUTO CLUSTER |	Unknown patterns	2-4w	30-50%	Continuous	Medium
PREDICTIVE OPT |	Large tables	4w	40-70%	Continuous	High
VACUUM |	After OPTIMIZE	Min-Hours	15-30% storage	Weekly/Monthly	Low
