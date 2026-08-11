"""
================================================================================
COMPLETE DATABRICKS OPTIMIZATION GUIDE
================================================================================
Covers: OPTIMIZE, ANALYZE, FILE STATS, Z-ORDER, PARTITIONING, 
LIQUID CLUSTERING, PREDICTIVE OPTIMIZATION, VACUUM
================================================================================
Date: 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, max, min, count, 
    year, month, day, rand, date_add, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from datetime import datetime, timedelta
import time

# ================================================================================
# SECTION 1: INITIALIZATION & SETUP
# ================================================================================

def create_optimized_session():
    """Create Spark session with optimization settings"""
    spark = SparkSession.builder \
        .appName("OptimizationDemo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.databricks.io.cache.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.databricks.delta.optimize.minFileSize", "1048576") \
        .config("spark.databricks.delta.optimize.maxFileSize", "1073741824") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ================================================================================
# SECTION 2: CREATE SAMPLE DATASETS
# ================================================================================

def create_sales_dataset(spark, num_records=100000):
    """Create realistic sales dataset for optimization examples"""
    print("\n" + "="*80)
    print("CREATING SAMPLE SALES DATASET")
    print("="*80)
    
    # Generate data
    data = []
    start_date = datetime(2023, 1, 1)
    
    regions = ["US", "EU", "APAC", "LATAM", "MIDDLE_EAST"]
    products = ["Laptop", "Desktop", "Monitor", "Keyboard", "Mouse", "Headphones"]
    sales_persons = ["Person_" + str(i) for i in range(1, 50)]
    
    for i in range(num_records):
        record_date = start_date + timedelta(days=i % 730)  # 2 years of data
        data.append((
            f"ORD{i}",                              # order_id
            regions[i % len(regions)],              # region
            products[i % len(products)],            # product
            sales_persons[i % len(sales_persons)],  # sales_person
            record_date.strftime("%Y-%m-%d"),       # order_date
            1000 + (i % 50000),                     # sales_amount
            (i % 100)                               # quantity
        ))
    
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("region", StringType()),
        StructField("product", StringType()),
        StructField("sales_person", StringType()),
        StructField("order_date", StringType()),
        StructField("sales_amount", IntegerType()),
        StructField("quantity", IntegerType()),
    ])
    
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("order_date", col("order_date").cast(DateType()))
    df = df.withColumn("year", year(col("order_date")))
    df = df.withColumn("month", month(col("order_date")))
    
    # Write to Delta table
    df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("sales_raw")
    
    print(f"✅ Created table: sales_raw with {num_records:,} records")
    print(f"   Columns: order_id, region, product, sales_person, order_date, sales_amount, quantity, year, month")
    
    return df


# ================================================================================
# SECTION 3: UNDERSTANDING TABLE STATISTICS
# ================================================================================

def section_1_table_stats(spark):
    """Section 1: Table-Level Statistics and Analysis"""
    print("\n" + "="*80)
    print("SECTION 1: TABLE-LEVEL STATISTICS & FILE INFORMATION")
    print("="*80)
    
    # Create sample table
    create_sales_dataset(spark, 50000)
    
    print("\n1️⃣ DESCRIBE TABLE - Basic Information")
    print("-" * 80)
    spark.sql("DESCRIBE sales_raw").show(truncate=False)
    
    print("\n2️⃣ DESCRIBE EXTENDED - Detailed Information")
    print("-" * 80)
    result = spark.sql("DESCRIBE EXTENDED sales_raw").collect()
    for row in result[:15]:  # Show first 15 rows
        print(f"{row[0]:30} | {row[1]}")
    
    print("\n3️⃣ TABLE PROPERTIES - Metadata")
    print("-" * 80)
    spark.sql("SHOW TBLPROPERTIES sales_raw").show()
    
    print("\n4️⃣ FILE STATISTICS - Storage Information")
    print("-" * 80)
    
    # Get table path and file info
    table_path = spark.sql("DESC EXTENDED sales_raw").filter("col_name == 'Location'").collect()[0][1]
    print(f"Table location: {table_path}")
    
    # Count files
    files = spark.read.format("delta").load(table_path).inputFiles()
    print(f"Number of files: {len(files)}")
    print(f"Sample files: {files[:3] if files else 'N/A'}")
    
    # Get row count and data size
    df = spark.read.table("sales_raw")
    row_count = df.count()
    print(f"\nRow count: {row_count:,}")
    
    print("\n5️⃣ ANALYZE TABLE - Column Statistics")
    print("-" * 80)
    spark.sql("ANALYZE TABLE sales_raw COMPUTE STATISTICS")
    spark.sql("ANALYZE TABLE sales_raw COMPUTE STATISTICS FOR ALL COLUMNS")
    print("✅ Statistics computed for all columns")
    
    # Display statistics
    stats = spark.sql("DESC FORMATTED sales_raw").collect()
    print("\nTable statistics:")
    for row in stats[15:30]:
        if row[0].strip():
            print(f"{row[0]:30} | {row[1]}")


# ================================================================================
# SECTION 4: OPTIMIZE - Small Files Compaction
# ================================================================================

def section_2_optimize(spark):
    """Section 2: OPTIMIZE - Compacting small files"""
    print("\n" + "="*80)
    print("SECTION 2: OPTIMIZE - Small Files Compaction")
    print("="*80)
    
    # Create table with many small files
    print("\n1️⃣ CREATING TABLE WITH MANY SMALL FILES")
    print("-" * 80)
    
    # Write multiple times to create small files
    for i in range(5):
        df = spark.read.table("sales_raw")
        df.filter(col("region") == "US").write.mode("append").format("delta").saveAsTable("sales_unoptimized")
    
    print("✅ Table created with multiple small files (5 append operations)")
    
    # Check file count before optimization
    print("\n2️⃣ BEFORE OPTIMIZE")
    print("-" * 80)
    
    before_stats = spark.sql("DESCRIBE DETAIL sales_unoptimized").collect()[0]
    print(f"Number of files: {before_stats['numFiles']}")
    print(f"Size in bytes: {before_stats['sizeInBytes']:,}")
    print(f"Rows: {before_stats['numRows']:,}")
    
    # Execute OPTIMIZE
    print("\n3️⃣ EXECUTING OPTIMIZE")
    print("-" * 80)
    start_time = time.time()
    result = spark.sql("OPTIMIZE sales_unoptimized")
    optimize_time = time.time() - start_time
    
    # Show optimization results
    result.show()
    print(f"Optimization completed in {optimize_time:.2f} seconds")
    
    # Check file count after optimization
    print("\n4️⃣ AFTER OPTIMIZE")
    print("-" * 80)
    
    after_stats = spark.sql("DESCRIBE DETAIL sales_unoptimized").collect()[0]
    print(f"Number of files: {after_stats['numFiles']}")
    print(f"Size in bytes: {after_stats['sizeInBytes']:,}")
    print(f"Rows: {after_stats['numRows']:,}")
    
    # Calculate improvement
    file_reduction = ((before_stats['numFiles'] - after_stats['numFiles']) / before_stats['numFiles']) * 100
    print(f"\n✅ File reduction: {file_reduction:.1f}%")
    print(f"   Before: {before_stats['numFiles']} files → After: {after_stats['numFiles']} files")
    
    print("\n5️⃣ OPTIMIZE WITH Z-ORDER (Covered in Section 4)")
    print("-" * 80)
    print("OPTIMIZE ZORDER BY column1, column2, ...")
    print("Combines compaction with optimized data layout")


# ================================================================================
# SECTION 5: Z-ORDER CLUSTERING
# ================================================================================

def section_3_zorder(spark):
    """Section 3: Z-ORDER - Multi-dimensional clustering"""
    print("\n" + "="*80)
    print("SECTION 3: Z-ORDER CLUSTERING - Optimal Data Layout")
    print("="*80)
    
    # Create table with unordered data
    print("\n1️⃣ UNDERSTANDING Z-ORDER")
    print("-" * 80)
    print("""
Z-ORDER is a space-filling curve that organizes multi-dimensional data
into a single linear order while preserving locality.

Benefits:
- Groups similar values together
- Improves query performance for range filters
- Works best with 2-4 columns
- Combines compaction with data reordering

Syntax:
  OPTIMIZE table_name ZORDER BY col1, col2, col3
""")
    
    print("\n2️⃣ CREATE TEST TABLE")
    print("-" * 80)
    spark.sql("CREATE TABLE IF NOT EXISTS sales_zorder AS SELECT * FROM sales_raw")
    print("✅ Created sales_zorder table")
    
    print("\n3️⃣ BEFORE Z-ORDER - Random data layout")
    print("-" * 80)
    spark.sql("SELECT region, product, COUNT(*) as count FROM sales_zorder GROUP BY region, product ORDER BY region, product").show(10)
    
    print("\n4️⃣ EXECUTING Z-ORDER OPTIMIZATION")
    print("-" * 80)
    start_time = time.time()
    
    # Z-order by frequently queried columns
    result = spark.sql("OPTIMIZE sales_zorder ZORDER BY region, product")
    zorder_time = time.time() - start_time
    
    result.show()
    print(f"\n✅ Z-ORDER completed in {zorder_time:.2f} seconds")
    
    print("\n5️⃣ AFTER Z-ORDER - Data is clustered by region, product")
    print("-" * 80)
    spark.sql("SELECT region, product, COUNT(*) as count FROM sales_zorder GROUP BY region, product ORDER BY region, product").show(10)
    
    print("\n6️⃣ PERFORMANCE COMPARISON")
    print("-" * 80)
    
    # Query on Z-ordered table
    print("Query: SELECT * FROM sales_zorder WHERE region = 'US' AND product = 'Laptop'")
    
    start_time = time.time()
    result1 = spark.sql("SELECT * FROM sales_zorder WHERE region = 'US' AND product = 'Laptop'").count()
    zorder_query_time = time.time() - start_time
    
    print(f"✅ Z-ORDER table query time: {zorder_query_time:.3f} seconds ({result1} rows)")
    
    print("\n7️⃣ WHEN TO USE Z-ORDER")
    print("-" * 80)
    print("""
✅ Use Z-ORDER when:
   - Frequently filtering on 2-4 columns
   - Doing range queries (WHERE col > value AND col < value)
   - Columns have good cardinality
   - Query patterns are known and stable
   
❌ Don't use Z-ORDER when:
   - Table updates frequently
   - Many different query patterns
   - Very high cardinality columns (IDs, timestamps)
   - Real-time streaming data

Best for:
   - Historical data (fact tables)
   - Analytical queries
   - Dimensional tables
""")


# ================================================================================
# SECTION 6: PARTITIONING
# ================================================================================

def section_4_partitioning(spark):
    """Section 4: PARTITIONING - Directory-based optimization"""
    print("\n" + "="*80)
    print("SECTION 4: PARTITIONING - Directory-Based Organization")
    print("="*80)
    
    print("\n1️⃣ UNDERSTANDING PARTITIONING")
    print("-" * 80)
    print("""
Partitioning organizes data into subdirectories based on column values.
Spark can skip entire partitions (partition pruning).

Directory structure example:
/sales_partitioned/
  year=2023/
    month=1/
      data_files.parquet
    month=2/
      data_files.parquet
  year=2024/
    month=1/
      data_files.parquet

Benefits:
- Partition pruning skips irrelevant data
- Faster queries on date/region/category columns
- Easier data management (delete by partition)
- Better for incremental loads

Syntax:
  df.write.partitionBy("year", "month").mode("overwrite").saveAsTable("table_name")
""")
    
    print("\n2️⃣ CREATE PARTITIONED TABLE")
    print("-" * 80)
    
    # Create partitioned table
    df = spark.read.table("sales_raw")
    df.write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .saveAsTable("sales_partitioned")
    
    print("✅ Created sales_partitioned table partitioned by year, month")
    
    print("\n3️⃣ VERIFY PARTITION STRUCTURE")
    print("-" * 80)
    
    spark.sql("SHOW PARTITIONS sales_partitioned").show(10)
    
    print("\n4️⃣ PARTITION PRUNING EXAMPLE")
    print("-" * 80)
    
    print("Query 1: SELECT * FROM sales_partitioned WHERE year = 2023 AND month = 1")
    print("   → Only reads partition: year=2023/month=1/")
    
    result1 = spark.sql("""
        SELECT * FROM sales_partitioned 
        WHERE year = 2023 AND month = 1
    """)
    print(f"   ✅ Rows matched: {result1.count():,}")
    
    print("\nQuery 2: SELECT * FROM sales_partitioned WHERE year = 2024")
    print("   → Reads all month partitions in year=2024/")
    
    result2 = spark.sql("""
        SELECT * FROM sales_partitioned 
        WHERE year = 2024
    """)
    print(f"   ✅ Rows matched: {result2.count():,}")
    
    print("\nQuery 3: SELECT * FROM sales_partitioned WHERE region = 'US'")
    print("   → No partition pruning - scans all partitions ❌")
    
    print("\n5️⃣ PARTITION ELIMINATION")
    print("-" * 80)
    
    # Enable query plan explanation
    explained = spark.sql("""
        EXPLAIN SELECT * FROM sales_partitioned 
        WHERE year = 2023 AND month = 1
    """).collect()
    
    print("Query plan shows partition filters:")
    for row in explained[:10]:
        print(row[0])
    
    print("\n6️⃣ PARTITION STATISTICS")
    print("-" * 80)
    
    stats = spark.sql("""
        SELECT year, month, COUNT(*) as row_count, 
               MIN(sales_amount) as min_sales, 
               MAX(sales_amount) as max_sales
        FROM sales_partitioned
        GROUP BY year, month
        ORDER BY year, month
    """)
    stats.show()
    
    print("\n7️⃣ BENEFITS OF PARTITIONING")
    print("-" * 80)
    print("""
✅ Partition Pruning:
   - Skips entire directories
   - Massive speedup for large tables
   - Only works on partition columns

✅ Efficient Deletes:
   - DROP PARTITION year=2023, month=1
   - Deletes entire directory
   - Much faster than row-level deletes

✅ Incremental Loads:
   - Add new partitions daily
   - Only new data in new partitions
   - Historical partitions untouched

✅ Parallel Processing:
   - Each partition processed independently
   - Better distribution across cluster
   - Faster aggregations

⚠️  Downsides:
   - Many small files (mitigated by OPTIMIZE)
   - Limits filter options (only partition columns)
   - Schema changes complex
""")


# ================================================================================
# SECTION 7: LIQUID CLUSTERING
# ================================================================================

def section_5_liquid_clustering(spark):
    """Section 5: LIQUID CLUSTERING - Adaptive clustering"""
    print("\n" + "="*80)
    print("SECTION 5: LIQUID CLUSTERING - Adaptive Clustering")
    print("="*80)
    
    print("\n1️⃣ UNDERSTANDING LIQUID CLUSTERING")
    print("-" * 80)
    print("""
Liquid Clustering (Databricks-specific) combines benefits of:
- Partitioning (locality, pruning)
- Z-ORDER (multi-column optimization)
- Without fixed partition structure

How it works:
- Data clustered on specified columns
- Adaptive, grows with data
- Automatic re-clustering on updates
- No external directories

Benefits:
✅ Better than partitioning for high-cardinality columns
✅ Better than Z-ORDER for frequent updates
✅ Works with INSERT/UPDATE/DELETE
✅ Multi-column support
✅ Automatic optimization

Syntax:
  CREATE TABLE table_name (
      col1 STRING,
      col2 INT,
      ...
  )
  CLUSTER BY col1, col2;
""")
    
    print("\n2️⃣ CREATE LIQUID CLUSTERED TABLE")
    print("-" * 80)
    
    # Create liquid clustered table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS sales_liquid_cluster (
            order_id STRING,
            region STRING,
            product STRING,
            sales_person STRING,
            order_date DATE,
            sales_amount INT,
            quantity INT,
            year INT,
            month INT
        )
        CLUSTER BY region, product
        USING DELTA
    """)
    
    print("✅ Created sales_liquid_cluster with CLUSTER BY region, product")
    
    print("\n3️⃣ INSERT DATA INTO LIQUID CLUSTERED TABLE")
    print("-" * 80)
    
    # Insert data
    spark.sql("""
        INSERT INTO sales_liquid_cluster
        SELECT * FROM sales_raw
    """)
    
    print("✅ Inserted data into liquid clustered table")
    
    print("\n4️⃣ QUERY LIQUID CLUSTERED TABLE")
    print("-" * 80)
    
    result = spark.sql("""
        SELECT region, product, COUNT(*) as count
        FROM sales_liquid_cluster
        GROUP BY region, product
        ORDER BY count DESC
        LIMIT 10
    """)
    result.show()
    
    print("\n5️⃣ LIQUID CLUSTERING vs PARTITIONING vs Z-ORDER")
    print("-" * 80)
    
    comparison = """
┌────────────────────┬──────────────────┬──────────────────┬──────────────────┐
│ Feature            │ Partitioning     │ Z-ORDER          │ Liquid Cluster   │
├────────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Setup              │ Simple           │ Simple           │ Simple           │
│ Updates            │ Creates files    │ Rewrites all     │ Auto recluster   │
│ Cardinality        │ Low (10-100s)    │ Any              │ Any (10-1000s+)  │
│ Multiple columns   │ Limited (2-3)    │ 2-4              │ Multiple         │
│ Partition pruning  │ ✅               │ ❌               │ ✅               │
│ Query patterns     │ Fixed cols       │ Fixed cols       │ Flexible         │
│ Small files issue  │ ⚠️  Many files    │ ⚠️  Files        │ ✅ Handled auto  │
│ Storage overhead   │ Low              │ Low              │ Low              │
│ Best for           │ Date/category    │ Analytics        │ Real-time+       │
│                    │ columns          │ queries          │ analytics        │
└────────────────────┴──────────────────┴──────────────────┴──────────────────┘
"""
    print(comparison)
    
    print("\n6️⃣ WHEN TO USE LIQUID CLUSTERING")
    print("-" * 80)
    print("""
✅ Use Liquid Clustering when:
   - High cardinality columns (millions of values)
   - Frequent INSERT/UPDATE/DELETE operations
   - Multiple query patterns
   - Real-time + analytical workloads
   - Want automated data organization
   
Example use cases:
   - User IDs (high cardinality)
   - Product IDs in e-commerce
   - Transaction IDs in banking
   - Customer orders (frequent updates)

✅ Use Partitioning when:
   - Low cardinality (< 100 values)
   - Time-based data (year/month/day)
   - Batch loads (no frequent updates)
   - Need manual partition management
   
✅ Use Z-ORDER when:
   - 2-4 well-defined columns
   - Historical data (immutable)
   - Known query patterns
   - Aggregation-heavy queries
""")


# ================================================================================
# SECTION 8: AUTO LIQUID CLUSTERING
# ================================================================================

def section_6_auto_liquid_clustering(spark):
    """Section 6: AUTO LIQUID CLUSTERING - Automatic column selection"""
    print("\n" + "="*80)
    print("SECTION 6: AUTO LIQUID CLUSTERING - Automatic Optimization")
    print("="*80)
    
    print("\n1️⃣ UNDERSTANDING AUTO LIQUID CLUSTERING")
    print("-" * 80)
    print("""
AUTO LIQUID CLUSTERING automatically selects optimal clustering columns
based on:
- Query patterns
- Data skewness
- Cardinality
- Access frequency

Enabled with:
  ALTER TABLE table_name SET TBLPROPERTIES (
    'delta.clustering.enabled' = 'true'
  );

OR during table creation:
  CREATE TABLE table_name (...)
  CLUSTER BY col1, col2
  PROPERTIES (
    'delta.clustering.enabled' = 'true'
  );

Benefits:
✅ Automatic column selection
✅ No manual tuning needed
✅ Adapts to changing query patterns
✅ Optimizes continuously
✅ Best for unknown workloads
""")
    
    print("\n2️⃣ ENABLE AUTO CLUSTERING")
    print("-" * 80)
    
    # Create table with auto clustering
    spark.sql("""
        CREATE TABLE IF NOT EXISTS sales_auto_cluster (
            order_id STRING,
            region STRING,
            product STRING,
            sales_person STRING,
            order_date DATE,
            sales_amount INT,
            quantity INT,
            year INT,
            month INT
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.clustering.enabled' = 'true'
        )
    """)
    
    print("✅ Created table with AUTO CLUSTERING enabled")
    
    print("\n3️⃣ INSERT DATA AND LET AUTO CLUSTERING WORK")
    print("-" * 80)
    
    # Insert data multiple times to trigger clustering
    for i in range(3):
        spark.sql("""
            INSERT INTO sales_auto_cluster
            SELECT * FROM sales_raw WHERE order_id LIKE 'ORD%'
        """)
        print(f"   ✅ Inserted batch {i+1}")
    
    print("\n✅ Auto clustering monitoring data access patterns...")
    
    print("\n4️⃣ CHECK AUTO CLUSTERING STATISTICS")
    print("-" * 80)
    
    # Get clustering info
    properties = spark.sql("SHOW TBLPROPERTIES sales_auto_cluster").collect()
    print("Table properties:")
    for prop in properties:
        if 'cluster' in str(prop).lower():
            print(f"  {prop[0]}: {prop[1]}")
    
    print("\n5️⃣ AUTO CLUSTERING VS MANUAL LIQUID CLUSTERING")
    print("-" * 80)
    
    comparison = """
Manual Liquid Clustering:
  ✅ Full control over columns
  ✅ Predictable behavior
  ❌ Requires knowing query patterns
  ❌ Manual changes needed if patterns change

Auto Liquid Clustering:
  ✅ No manual configuration
  ✅ Adapts to query patterns
  ✅ Continuous optimization
  ✅ Best for exploratory queries
  ❌ Less predictable
  ❌ Overhead of monitoring

Recommendation:
  - Auto: Development, exploration, unknown patterns
  - Manual: Production, known query patterns, strict SLA
"""
    print(comparison)


# ================================================================================
# SECTION 9: PREDICTIVE OPTIMIZATION
# ================================================================================

def section_7_predictive_optimization(spark):
    """Section 7: PREDICTIVE OPTIMIZATION - ML-driven optimization"""
    print("\n" + "="*80)
    print("SECTION 7: PREDICTIVE OPTIMIZATION - ML-Driven Optimization")
    print("="*80)
    
    print("\n1️⃣ UNDERSTANDING PREDICTIVE OPTIMIZATION")
    print("-" * 80)
    print("""
Predictive Optimization uses machine learning to automatically:
- Predict table access patterns
- Recommend optimal clustering strategy
- Auto-run OPTIMIZE operations
- Suggest partitioning scheme
- Monitor and adjust continuously

Benefits:
✅ No manual tuning required
✅ ML learns from query patterns
✅ Automatic OPTIMIZE runs
✅ Optimizes for actual workload
✅ Reduces storage and query time
✅ Adapts over time

Requires:
- Databricks Premium/Enterprise
- Unity Catalog enabled
- Table in Unity Catalog

Syntax:
  ALTER TABLE catalog.schema.table SET TBLPROPERTIES (
    'delta.clustering.enabled' = 'true',
    'delta.autoClustering.enabled' = 'true'
  );
""")
    
    print("\n2️⃣ ENABLE PREDICTIVE OPTIMIZATION")
    print("-" * 80)
    print("(Requires Databricks Premium with Unity Catalog)")
    
    # Note: This requires premium features
    try:
        # Create table for predictive optimization
        spark.sql("""
            CREATE TABLE IF NOT EXISTS sales_predictive (
                order_id STRING,
                region STRING,
                product STRING,
                sales_person STRING,
                order_date DATE,
                sales_amount INT,
                quantity INT,
                year INT,
                month INT
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.predictiveOptimization.enabled' = 'true',
                'delta.autoClustering.enabled' = 'true'
            )
        """)
        
        print("✅ Predictive Optimization enabled on sales_predictive")
        
    except Exception as e:
        print(f"ℹ️  Predictive Optimization requires premium: {str(e)[:50]}")
    
    print("\n3️⃣ HOW PREDICTIVE OPTIMIZATION WORKS")
    print("-" * 80)
    print("""
Phase 1: Collection
  - Tracks all queries on the table
  - Monitors query patterns and performance
  - Collects metrics (5-7 days)

Phase 2: Analysis
  - ML model analyzes query patterns
  - Identifies frequently filtered columns
  - Detects access patterns
  - Calculates optimization recommendations

Phase 3: Optimization
  - Auto-applies clustering strategy
  - Runs OPTIMIZE periodically
  - Adjusts based on new patterns
  - Monitors improvement

Phase 4: Monitoring
  - Tracks optimization effectiveness
  - Measures query improvement
  - Storage savings
  - Re-adjusts if patterns change

Timeline:
  Day 1-7:    Data collection
  Day 7-14:   Analysis and recommendations
  Day 14+:    Automatic optimization
  Day 30+:    Continuous adjustment
""")
    
    print("\n4️⃣ PREDICTIVE OPTIMIZATION METRICS")
    print("-" * 80)
    print("""
Tracked metrics:
✅ Query latency improvement
✅ Bytes scanned reduction
✅ Files read reduction
✅ Storage efficiency
✅ Query frequency by column
✅ Access patterns

Sample results:
  Query time:      -45% (5s → 2.75s)
  Bytes scanned:   -65% (500MB → 175MB)
  Files read:      -80% (1000 → 200)
  Storage saved:   ~30%
""")
    
    print("\n5️⃣ PREDICTIVE VS MANUAL OPTIMIZATION")
    print("-" * 80)
    
    comparison = """
Manual Optimization (OPTIMIZE + Z-ORDER):
  ✅ Immediate results
  ✅ Full control
  ✅ Works with standard Spark
  ❌ Requires expertise
  ❌ Manual tuning needed
  ❌ One-time benefit
  
Predictive Optimization:
  ✅ Zero configuration
  ✅ Continuous improvement
  ✅ Adapts to changes
  ✅ ML-driven optimization
  ❌ Requires Databricks Premium
  ❌ 1-2 weeks to see benefits
  ❌ Less control
  
Best practice:
  - Use both!
  - Manual for known queries
  - Predictive for exploratory workloads
  - Predictive catches patterns you missed
"""
    print(comparison)


# ================================================================================
# SECTION 10: VACUUM - Cleanup & Maintenance
# ================================================================================

def section_8_vacuum(spark):
    """Section 8: VACUUM - Cleanup old files"""
    print("\n" + "="*80)
    print("SECTION 8: VACUUM - Cleanup & Data Retention")
    print("="*80)
    
    print("\n1️⃣ UNDERSTANDING VACUUM")
    print("-" * 80)
    print("""
VACUUM removes old files no longer needed by the table.

Delta Lake keeps old files for:
- Time-travel queries
- ROLLBACK operations
- Failure recovery

After a retention period, files can be safely deleted:

Syntax:
  VACUUM table_name [RETAIN duration];
  
Default retention: 30 days (7 days for auto-cleanup)

Example:
  VACUUM my_table RETAIN 7 DAYS;  # Keep 7 days
  VACUUM my_table;                 # Default 30 days (in production)
  VACUUM my_table RETAIN 0 DAYS;  # Immediate cleanup (risky!)
""")
    
    print("\n2️⃣ CREATE TABLE WITH HISTORY")
    print("-" * 80)
    
    # Create table with modifications to create history
    spark.sql("CREATE TABLE IF NOT EXISTS sales_with_history AS SELECT * FROM sales_raw")
    print("✅ Created sales_with_history")
    
    # Make several modifications
    for i in range(3):
        spark.sql(f"""
            INSERT INTO sales_with_history
            SELECT * FROM sales_raw WHERE order_id LIKE 'ORD%{i}'
        """)
        print(f"✅ Modification {i+1}")
    
    print("\n3️⃣ CHECK TABLE HISTORY")
    print("-" * 80)
    
    history = spark.sql("DESCRIBE HISTORY sales_with_history").collect()
    print(f"Total versions: {len(history)}")
    for version in history[:5]:
        print(f"Version: {version['version']}, Operation: {version['operation']}, Timestamp: {version['timestamp']}")
    
    print("\n4️⃣ GET TABLE FILE INFORMATION BEFORE VACUUM")
    print("-" * 80)
    
    before_detail = spark.sql("DESCRIBE DETAIL sales_with_history").collect()[0]
    print(f"Number of files: {before_detail['numFiles']}")
    print(f"Size in bytes: {before_detail['sizeInBytes']:,}")
    print(f"Rows: {before_detail['numRows']:,}")
    
    print("\n5️⃣ EXECUTE VACUUM")
    print("-" * 80)
    print("Running: VACUUM sales_with_history RETAIN 0 DAYS")
    print("(WARNING: This removes ALL historical versions!)")
    
    try:
        # Note: VACUUM with 0 DAYS is risky, normally use longer duration
        spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
        spark.sql("VACUUM sales_with_history RETAIN 0 DAYS")
        print("✅ Vacuum completed")
        
    except Exception as e:
        print(f"ℹ️  Note: {str(e)[:50]}")
        print("   Running VACUUM with safe retention...")
        spark.sql("VACUUM sales_with_history RETAIN 1 DAYS")
        print("✅ Vacuum completed with 1-day retention")
    
    print("\n6️⃣ GET FILE INFORMATION AFTER VACUUM")
    print("-" * 80)
    
    after_detail = spark.sql("DESCRIBE DETAIL sales_with_history").collect()[0]
    print(f"Number of files: {after_detail['numFiles']}")
    print(f"Size in bytes: {after_detail['sizeInBytes']:,}")
    print(f"Rows: {after_detail['numRows']:,}")
    
    # Calculate cleanup
    space_saved = before_detail['sizeInBytes'] - after_detail['sizeInBytes']
    print(f"\n✅ Space saved: {space_saved:,} bytes")
    
    print("\n7️⃣ VACUUM BEST PRACTICES")
    print("-" * 80)
    print("""
✅ DO:
   - Run VACUUM regularly (weekly/monthly)
   - Use appropriate retention (7-30 days)
   - Schedule during low-traffic periods
   - Monitor space savings
   - Keep backups of critical data
   - Run on production tables in batches

❌ DON'T:
   - Use RETAIN 0 DAYS in production
   - VACUUM without retention check
   - VACUUM during active queries
   - Vacuum without understanding implications
   - Vacuum tables you might need to restore

Recommended retention:
   Development:    1-7 days
   Staging:        3-14 days
   Production:     30-90 days
   Critical data:  180-365 days

Frequency:
   Lightweight:    Weekly (with 30-day retention)
   Aggressive:     Daily (with 7-day retention)
   Minimal:        Monthly (with 90-day retention)

Storage savings:
   Typical: 10-30% storage reduction
   Heavy updates: 30-50%
   After bulk deletes: 50-80%
""")


# ================================================================================
# SECTION 11: HOW THEY ALL CONNECT
# ================================================================================

def section_9_how_they_connect(spark):
    """Section 9: How all optimization techniques work together"""
    print("\n" + "="*80)
    print("SECTION 9: HOW ALL TECHNIQUES CONNECT & OPTIMIZATION WORKFLOW")
    print("="*80)
    
    print("\n1️⃣ THE OPTIMIZATION STACK")
    print("-" * 80)
    
    stack = """
┌─────────────────────────────────────────────────────────────────────────┐
│                     OPTIMIZATION DECISION TREE                          │
├─────────────────────────────────────────────────────────────────────────┤
│
│ START: Need to optimize table query performance
│   ↓
│   ├─→ Step 1: ANALYZE TABLE
│   │   Purpose: Gather statistics
│   │   Command: ANALYZE TABLE table_name COMPUTE STATISTICS
│   │   Result: Column min/max, count, nulls
│   │   Time: 5-60 min depending on size
│   │
│   ├─→ Step 2: OPTIMIZE (Compact Files)
│   │   Purpose: Remove small files
│   │   Command: OPTIMIZE table_name
│   │   Result: Fewer, larger files
│   │   Time: 10-300 min depending on size
│   │   Impact: 10-30% query speedup
│   │
│   ├─→ Step 3: Choose Clustering Strategy
│   │   ↓
│   │   ├─→ Option A: PARTITIONING
│   │   │   When: Low cardinality (< 100), time-based, batch loads
│   │   │   Impact: 50-90% speedup for partition-column filters
│   │   │   Overhead: Many small files
│   │   │   Example: year, month
│   │   │
│   │   ├─→ Option B: Z-ORDER
│   │   │   When: 2-4 columns, known query patterns, immutable data
│   │   │   Impact: 30-60% speedup for range queries
│   │   │   Overhead: One-time cost, rewrites all data
│   │   │   Example: OPTIMIZE ZORDER BY region, product
│   │   │
│   │   ├─→ Option C: LIQUID CLUSTERING
│   │   │   When: High cardinality, frequent updates, unknown patterns
│   │   │   Impact: 20-50% speedup
│   │   │   Overhead: Automatic re-clustering on updates
│   │   │   Example: user_id, product_id
│   │   │
│   │   └─→ Option D: AUTO LIQUID CLUSTERING
│   │       When: Development, exploratory, no manual tuning
│   │       Impact: Varies, learns over time
│   │       Overhead: Monitoring costs
│   │
│   ├─→ Step 4: PREDICTIVE OPTIMIZATION (Optional)
│   │   When: Premium Databricks, real-time + analytics mix
│   │   Duration: 1-4 weeks to see full benefit
│   │   Impact: 40-70% query improvement
│   │   Overhead: ML analysis and background runs
│   │
│   ├─→ Step 5: VACUUM (Regular Maintenance)
│   │   When: After OPTIMIZE, delete operations
│   │   Command: VACUUM table_name RETAIN 7 DAYS
│   │   Frequency: Weekly/Monthly
│   │   Result: 10-30% storage reduction
│   │
│   └─→ Step 6: MONITOR & ADJUST
│       Check: Query performance, storage, file counts
│       Frequency: Weekly
│       Action: Re-run OPTIMIZE if needed
│
└─────────────────────────────────────────────────────────────────────────┘
"""
    print(stack)
    
    print("\n2️⃣ COMPLETE OPTIMIZATION WORKFLOW")
    print("-" * 80)
    
    print("""
WEEK 1: Initial Optimization
═════════════════════════════════════════════════════════════════════════════
Day 1:  ANALYZE TABLE sales_raw
        ├─ Gathers statistics
        ├─ Min/max values
        └─ Identifies skewness
        
Day 2:  OPTIMIZE sales_raw
        ├─ Compacts 1000 files → 50 files
        ├─ Removes old versions
        └─ 20% faster queries

Day 3:  Choose clustering strategy
        ├─ Analyze query patterns
        ├─ Identify frequently filtered columns
        ├─ Select: PARTITIONING by date (low cardinality)
        │          Z-ORDER by region, product (analysis)
        │          OR LIQUID CLUSTER by user_id (high cardinality)
        └─ Apply strategy

Days 4-7: Monitor query performance
        ├─ Measure latency improvement
        ├─ Check storage efficiency
        └─ Adjust if needed

ONGOING: Regular Maintenance
═════════════════════════════════════════════════════════════════════════════
Weekly:   VACUUM sales_raw RETAIN 7 DAYS
          ├─ Remove old file versions
          ├─ Save 5-10% storage
          └─ Keep 7 days for time-travel

Monthly:  Re-run OPTIMIZE + ANALYZE
          ├─ New data accumulates
          ├─ File fragmentation increases
          └─ Maintain performance

Quarterly: Review optimization strategy
          ├─ Query patterns change
          ├─ Data volume increases
          ├─ May need new clustering
          └─ Adjust VACUUM retention

PREDICTIVE OPTIMIZATION (If Premium)
═════════════════════════════════════════════════════════════════════════════
Week 1-7:   ML monitors query patterns
Week 2-3:   Recommendations generated
Week 3+:    Auto-optimization runs
Month 1:    Significant improvements visible
Month 2+:   Continuous improvement & adaptation
""")
    
    print("\n3️⃣ REAL-WORLD OPTIMIZATION SCENARIOS")
    print("-" * 80)
    
    scenarios = """
SCENARIO 1: E-COMMERCE ORDERS TABLE
───────────────────────────────────────────────────────────────────────────
Data: 100M daily orders, 2 years history
Queries:
  - WHERE order_date > '2024-08-01'           (HIGH: partition prune)
  - WHERE customer_id = '12345'               (HIGH: cluster by)
  - WHERE region = 'US' AND product = 'Shoe'  (MEDIUM: filter)

Recommendation:
  ✅ PARTITION BY year, month (time-based, batch loads)
  ✅ LIQUID CLUSTER BY customer_id (high cardinality)
  ✅ Benefit: 80% partition pruning + 40% customer lookup speedup

Timeline:
  Week 1: ANALYZE + OPTIMIZE
  Week 2: Add LIQUID CLUSTERING by customer_id
  Week 3: Monitor & measure improvement
  Week 4: VACUUM cleanup


SCENARIO 2: ANALYTICS DATA WAREHOUSE
───────────────────────────────────────────────────────────────────────────
Data: 500M immutable fact table, historical analytics
Queries:
  - Aggregations by region, product, date
  - Range queries on metrics
  - Multi-column filters (2-4 columns)

Recommendation:
  ✅ Z-ORDER BY region, product (known query patterns)
  ✅ ANALYZE for Catalyst optimizer
  ✅ Monthly OPTIMIZE (historical data)
  ✅ Benefit: 60% faster aggregate queries

Timeline:
  Week 1: ANALYZE
  Week 1: OPTIMIZE ZORDER BY region, product (4-8 hours)
  Week 1: Measure 60% improvement
  Week 2: VACUUM old versions
  Monthly: Re-run OPTIMIZE


SCENARIO 3: REAL-TIME STREAMING + ANALYTICS
───────────────────────────────────────────────────────────────────────────
Data: 10M/hour streaming events + ad-hoc analytics
Queries:
  - Real-time: WHERE user_id = 123 AND timestamp > now()
  - Analytics: WHERE date >= '2024-08-01' GROUP BY category
  - Many different query patterns

Recommendation:
  ✅ LIQUID CLUSTER BY user_id, category (handles both)
  ✅ PREDICTIVE OPTIMIZATION (learns patterns)
  ✅ Weekly OPTIMIZE (frequent updates)
  ✅ Benefit: 50% real-time + 30% analytics improvement

Timeline:
  Week 1: Create LIQUID CLUSTERED table
  Week 2: Enable PREDICTIVE OPTIMIZATION
  Week 3: Auto-optimization kicks in
  Month 1: 40-50% improvement visible
  Ongoing: Auto-adjusts continuously


SCENARIO 4: LOW-CARDINALITY REFERENCE DATA
───────────────────────────────────────────────────────────────────────────
Data: 1M product master data (50 categories, 100 regions)
Queries:
  - WHERE category = 'Electronics' AND region = 'US'
  - JOIN in fact tables

Recommendation:
  ✅ PARTITION BY category, region (low cardinality)
  ✅ Z-ORDER BY product_id (within partitions)
  ✅ OPTIMIZE monthly
  ✅ Benefit: 95% partition pruning + fast JOINs

Timeline:
  One-time: CREATE with partitioning + Z-ORDER
  Monthly: OPTIMIZE
""")
    print(scenarios)


# ================================================================================
# SECTION 12: MONITORING & PERFORMANCE COMPARISON
# ================================================================================

def section_10_monitoring(spark):
    """Section 10: Monitoring and performance comparison"""
    print("\n" + "="*80)
    print("SECTION 10: MONITORING & PERFORMANCE COMPARISON")
    print("="*80)
    
    print("\n1️⃣ CREATE PERFORMANCE BASELINE")
    print("-" * 80)
    
    # Create unoptimized table
    spark.sql("CREATE TABLE IF NOT EXISTS sales_baseline AS SELECT * FROM sales_raw")
    
    print("✅ Created baseline table (no optimization)")
    
    print("\n2️⃣ MONITOR BEFORE OPTIMIZATION")
    print("-" * 80)
    
    # Get baseline statistics
    baseline = spark.sql("DESCRIBE DETAIL sales_baseline").collect()[0]
    
    metrics_before = {
        'files': baseline['numFiles'],
        'size': baseline['sizeInBytes'],
        'rows': baseline['numRows'],
    }
    
    print(f"Files: {metrics_before['files']}")
    print(f"Size: {metrics_before['size']:,} bytes")
    print(f"Rows: {metrics_before['rows']:,}")
    
    print("\n3️⃣ ANALYZE TABLE")
    print("-" * 80)
    start = time.time()
    spark.sql("ANALYZE TABLE sales_baseline COMPUTE STATISTICS FOR ALL COLUMNS")
    analyze_time = time.time() - start
    print(f"✅ ANALYZE completed in {analyze_time:.2f} seconds")
    
    print("\n4️⃣ OPTIMIZE TABLE")
    print("-" * 80)
    start = time.time()
    spark.sql("OPTIMIZE sales_baseline")
    optimize_time = time.time() - start
    print(f"✅ OPTIMIZE completed in {optimize_time:.2f} seconds")
    
    print("\n5️⃣ MONITOR AFTER OPTIMIZATION")
    print("-" * 80)
    
    optimized = spark.sql("DESCRIBE DETAIL sales_baseline").collect()[0]
    
    metrics_after = {
        'files': optimized['numFiles'],
        'size': optimized['sizeInBytes'],
        'rows': optimized['numRows'],
    }
    
    print(f"Files: {metrics_after['files']}")
    print(f"Size: {metrics_after['size']:,} bytes")
    print(f"Rows: {metrics_after['rows']:,}")
    
    print("\n6️⃣ IMPROVEMENTS")
    print("-" * 80)
    
    file_reduction = ((metrics_before['files'] - metrics_after['files']) / metrics_before['files']) * 100
    size_reduction = ((metrics_before['size'] - metrics_after['size']) / metrics_before['size']) * 100
    
    print(f"File reduction: {file_reduction:.1f}%")
    print(f"   Before: {metrics_before['files']} → After: {metrics_after['files']}")
    print(f"\nSize reduction: {size_reduction:.1f}%")
    print(f"   Before: {metrics_before['size']:,} → After: {metrics_after['size']:,}")
    
    print("\n7️⃣ QUERY PERFORMANCE COMPARISON")
    print("-" * 80)
    
    test_query = "SELECT region, COUNT(*) FROM sales_baseline WHERE year = 2023 GROUP BY region"
    
    start = time.time()
    result = spark.sql(test_query)
    result.collect()
    query_time = time.time() - start
    
    print(f"Query: {test_query[:50]}...")
    print(f"Time: {query_time:.3f} seconds")


# ================================================================================
# SECTION 13: COMPLETE OPTIMIZATION SCRIPT
# ================================================================================

def complete_optimization_pipeline(spark):
    """Complete end-to-end optimization pipeline"""
    print("\n" + "="*80)
    print("COMPLETE OPTIMIZATION PIPELINE - Production Ready")
    print("="*80)
    
    table_name = "sales_optimized_final"
    
    print(f"\n1️⃣ CREATE RAW TABLE")
    print("-" * 80)
    df = spark.read.table("sales_raw")
    df.write.mode("overwrite").format("delta").saveAsTable(table_name)
    print(f"✅ Created {table_name}")
    
    print(f"\n2️⃣ ANALYZE TABLE")
    print("-" * 80)
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
    print("✅ Statistics computed")
    
    print(f"\n3️⃣ OPTIMIZE TABLE")
    print("-" * 80)
    spark.sql(f"OPTIMIZE {table_name}")
    print("✅ Files compacted")
    
    print(f"\n4️⃣ APPLY Z-ORDER CLUSTERING")
    print("-" * 80)
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY region, product")
    print("✅ Data re-ordered by region, product")
    
    print(f"\n5️⃣ FINAL STATISTICS")
    print("-" * 80)
    
    final_stats = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    print(f"Files: {final_stats['numFiles']}")
    print(f"Size: {final_stats['sizeInBytes']:,} bytes")
    print(f"Rows: {final_stats['numRows']:,}")
    
    print(f"\n6️⃣ SCHEDULE REGULAR MAINTENANCE")
    print("-" * 80)
    print(f"""
Weekly:   spark.sql("VACUUM {table_name} RETAIN 7 DAYS")
Monthly:  spark.sql("ANALYZE TABLE {table_name} COMPUTE STATISTICS")
Monthly:  spark.sql("OPTIMIZE {table_name}")
""")
    
    print(f"\n✅ {table_name} is now fully optimized!")


# ================================================================================
# MAIN EXECUTION
# ================================================================================

def main():
    """Execute all demonstrations"""
    spark = create_optimized_session()
    
    try:
        print("\n" + "="*80)
        print("COMPLETE DATABRICKS OPTIMIZATION GUIDE")
        print("="*80)
        
        # Run all sections
        section_1_table_stats(spark)
        section_2_optimize(spark)
        section_3_zorder(spark)
        section_4_partitioning(spark)
        section_5_liquid_clustering(spark)
        section_6_auto_liquid_clustering(spark)
        section_7_predictive_optimization(spark)
        section_8_vacuum(spark)
        section_9_how_they_connect(spark)
        section_10_monitoring(spark)
        complete_optimization_pipeline(spark)
        
        print("\n" + "="*80)
        print("✅ ALL DEMONSTRATIONS COMPLETED!")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()