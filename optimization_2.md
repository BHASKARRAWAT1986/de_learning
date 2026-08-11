"""
================================================================================
COMPLETE DATABRICKS OPTIMIZATION GUIDE - ALL TECHNIQUES IN ONE FILE
================================================================================
Comprehensive Reference for:
- ANALYZE TABLE
- OPTIMIZE
- FILE STATS & TABLE STATS
- Z-ORDER CLUSTERING
- PARTITIONING
- LIQUID CLUSTERING
- AUTO LIQUID CLUSTERING
- PREDICTIVE OPTIMIZATION
- VACUUM

Date: 2024
Author: Databricks Optimization Team
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, max, min, count, countDistinct,
    year, month, day, rand, date_add, current_timestamp, lit, 
    concat, upper, lower
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    DateType, LongType, BooleanType
)
from datetime import datetime, timedelta
import time
import os

# ================================================================================
# PART 1: SESSION SETUP & CONFIGURATION
# ================================================================================

class OptimizationEnvironment:
    """Setup Spark session with optimization settings"""
    
    @staticmethod
    def create_session(app_name="DatabricksOptimization"):
        """Create and configure Spark session"""
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.databricks.io.cache.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.databricks.delta.optimize.minFileSize", "1048576") \
            .config("spark.databricks.delta.optimize.maxFileSize", "1073741824") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.minNumFiles", "50") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    
    @staticmethod
    def print_config(spark):
        """Print Spark configuration"""
        print("\n" + "="*80)
        print("SPARK OPTIMIZATION CONFIGURATION")
        print("="*80)
        
        config_items = [
            "spark.sql.adaptive.enabled",
            "spark.sql.adaptive.coalescePartitions.enabled",
            "spark.databricks.io.cache.enabled",
            "spark.sql.shuffle.partitions",
            "spark.databricks.delta.optimize.minFileSize",
            "spark.databricks.delta.optimize.maxFileSize",
        ]
        
        for config in config_items:
            value = spark.conf.get(config, "Not Set")
            print(f"{config:50} = {value}")


# ================================================================================
# PART 2: SAMPLE DATA GENERATION
# ================================================================================

class SampleDataGenerator:
    """Generate realistic sample datasets for optimization examples"""
    
    @staticmethod
    def create_sales_dataset(spark, num_records=100000, table_name="sales_raw"):
        """
        Create comprehensive sales dataset with realistic data
        
        Columns:
        - order_id: Unique order identifier
        - region: Geographic region (US, EU, APAC, LATAM, MIDDLE_EAST)
        - product: Product category
        - sales_person: Sales representative
        - order_date: Date of order
        - sales_amount: Amount of sale
        - quantity: Quantity ordered
        - year: Year extracted from date
        - month: Month extracted from date
        - customer_id: Customer identifier (high cardinality)
        - discount: Discount applied
        - category: Product category (high cardinality)
        """
        print("\n" + "="*80)
        print(f"GENERATING SAMPLE SALES DATASET: {table_name}")
        print("="*80)
        
        start_time = time.time()
        
        data = []
        start_date = datetime(2022, 1, 1)
        
        # Define dimensions
        regions = ["US", "EU", "APAC", "LATAM", "MIDDLE_EAST", "AFRICA", "OCEANIA"]
        products = ["Laptop", "Desktop", "Monitor", "Keyboard", "Mouse", "Headphones", 
                   "Webcam", "Microphone", "Speaker", "Router"]
        sales_persons = [f"SalesPerson_{i}" for i in range(1, 101)]
        categories = ["Electronics", "Accessories", "Peripherals", "Components", "Software"]
        
        for i in range(num_records):
            record_date = start_date + timedelta(days=i % 1095)  # 3 years of data
            customer_id = f"CUST_{str((i % 50000) + 1).zfill(6)}"
            
            data.append((
                f"ORD{str(i + 1).zfill(10)}",           # order_id
                regions[i % len(regions)],              # region
                products[i % len(products)],            # product
                sales_persons[i % len(sales_persons)],  # sales_person
                record_date.strftime("%Y-%m-%d"),       # order_date
                int(500 + (i * 7) % 50000),             # sales_amount
                int(1 + (i % 20)),                      # quantity
                customer_id,                            # customer_id
                int(i % 50),                            # discount
                categories[i % len(categories)],        # category
            ))
        
        # Define schema
        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("region", StringType(), True),
            StructField("product", StringType(), True),
            StructField("sales_person", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("sales_amount", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("customer_id", StringType(), True),
            StructField("discount", IntegerType(), True),
            StructField("category", StringType(), True),
        ])
        
        # Create DataFrame
        df = spark.createDataFrame(data, schema)
        
        # Add date columns
        df = df.withColumn("order_date", col("order_date").cast(DateType()))
        df = df.withColumn("year", year(col("order_date")))
        df = df.withColumn("month", month(col("order_date")))
        df = df.withColumn("day", day(col("order_date")))
        
        # Write to Delta table
        df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(table_name)
        
        elapsed = time.time() - start_time
        print(f"\n✅ Created table: {table_name}")
        print(f"   Records: {num_records:,}")
        print(f"   Time: {elapsed:.2f} seconds")
        print(f"   Columns: order_id, region, product, sales_person, order_date, sales_amount,")
        print(f"            quantity, customer_id, discount, category, year, month, day")
        
        return df


# ================================================================================
# PART 3: TABLE STATISTICS & ANALYSIS
# ================================================================================

class TableStatistics:
    """Comprehensive table statistics and analysis"""
    
    @staticmethod
    def analyze_table(spark, table_name):
        """
        Execute ANALYZE TABLE to compute statistics
        
        This helps Catalyst optimizer make better decisions
        """
        print("\n" + "="*80)
        print(f"ANALYZING TABLE: {table_name}")
        print("="*80)
        
        print("\n1️⃣ BASIC TABLE INFORMATION")
        print("-" * 80)
        
        # Describe table
        describe_df = spark.sql(f"DESCRIBE {table_name}")
        print("\nColumn Information:")
        describe_df.show(truncate=False)
        
        print("\n2️⃣ EXTENDED TABLE INFORMATION")
        print("-" * 80)
        
        extended = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
        print("\nExtended Properties:")
        for i, row in enumerate(extended[:20]):
            print(f"{row[0]:30} | {str(row[1])[:50]}")
        
        print("\n3️⃣ TABLE LOCATION & FORMAT")
        print("-" * 80)
        
        location = spark.sql(f"DESC EXTENDED {table_name}") \
            .filter("col_name == 'Location'").collect()[0][1]
        print(f"Location: {location}")
        
        print("\n4️⃣ COMPUTE TABLE STATISTICS")
        print("-" * 80)
        
        print("Running: ANALYZE TABLE ... COMPUTE STATISTICS")
        start_time = time.time()
        
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
        spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
        
        analyze_time = time.time() - start_time
        print(f"✅ Statistics computed in {analyze_time:.2f} seconds")
        
        print("\n5️⃣ STATISTICS RESULTS")
        print("-" * 80)
        
        stats_df = spark.sql(f"DESC FORMATTED {table_name}")
        print("Table Statistics:")
        stats_df.show(30, truncate=False)
        
        return True
    
    @staticmethod
    def get_table_details(spark, table_name):
        """Get detailed table information"""
        print("\n" + "="*80)
        print(f"TABLE DETAILS: {table_name}")
        print("="*80)
        
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        
        print(f"\nNumber of Files: {detail['numFiles']:,}")
        print(f"Size in Bytes: {detail['sizeInBytes']:,}")
        print(f"Size in MB: {detail['sizeInBytes'] / (1024*1024):.2f}")
        print(f"Number of Rows: {detail['numRows']:,}")
        print(f"Created At: {detail['createdAt']}")
        print(f"Last Modified: {detail['lastModified']}")
        
        # Calculate average file size
        avg_file_size = detail['sizeInBytes'] / detail['numFiles'] if detail['numFiles'] > 0 else 0
        print(f"Average File Size: {avg_file_size / (1024*1024):.2f} MB")
        
        return detail
    
    @staticmethod
    def get_column_statistics(spark, table_name):
        """Get column-level statistics"""
        print("\n" + "="*80)
        print(f"COLUMN STATISTICS: {table_name}")
        print("="*80)
        
        df = spark.read.table(table_name)
        
        print(f"\nDataFrame Info:")
        print(f"Rows: {df.count():,}")
        print(f"Columns: {len(df.columns)}")
        
        for col_name in df.columns:
            col_type = df.schema[col_name].dataType
            print(f"\n{col_name} ({col_type}):")
            
            # Get statistics
            stats = df.select(
                count(col(col_name)).alias("count"),
                countDistinct(col(col_name)).alias("distinct"),
            ).collect()[0]
            
            print(f"  Non-null count: {stats['count']:,}")
            print(f"  Distinct values: {stats['distinct']:,}")
            print(f"  Cardinality: {(stats['distinct'] / stats['count'] * 100):.2f}%")
    
    @staticmethod
    def show_table_properties(spark, table_name):
        """Show table properties"""
        print("\n" + "="*80)
        print(f"TABLE PROPERTIES: {table_name}")
        print("="*80)
        
        props = spark.sql(f"SHOW TBLPROPERTIES {table_name}")
        print("\nProperties:")
        props.show(100, truncate=False)


# ================================================================================
# PART 4: OPTIMIZE - FILE COMPACTION
# ================================================================================

class OptimizeOperation:
    """OPTIMIZE operations for file compaction"""
    
    @staticmethod
    def optimize_table(spark, table_name, use_zorder=False, zorder_cols=None):
        """
        Optimize table by compacting small files
        
        Benefits:
        - Reduces number of files (from 1000s to 10s)
        - Improves query performance (fewer I/O operations)
        - Reduces metadata overhead
        
        Overhead:
        - Rewrites all data (can take time for large tables)
        - Should run during off-peak hours
        """
        print("\n" + "="*80)
        print(f"OPTIMIZING TABLE: {table_name}")
        print("="*80)
        
        # Get before stats
        print("\n1️⃣ BEFORE OPTIMIZATION")
        print("-" * 80)
        
        before = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        print(f"Files: {before['numFiles']}")
        print(f"Size: {before['sizeInBytes'] / (1024*1024):.2f} MB")
        print(f"Rows: {before['numRows']:,}")
        
        # Execute optimization
        print("\n2️⃣ EXECUTING OPTIMIZATION")
        print("-" * 80)
        
        start_time = time.time()
        
        if use_zorder and zorder_cols:
            zorder_clause = ", ".join(zorder_cols)
            print(f"Command: OPTIMIZE {table_name} ZORDER BY {zorder_clause}")
            result = spark.sql(f"OPTIMIZE {table_name} ZORDER BY {zorder_clause}")
        else:
            print(f"Command: OPTIMIZE {table_name}")
            result = spark.sql(f"OPTIMIZE {table_name}")
        
        optimize_time = time.time() - start_time
        
        print("\nOptimization Results:")
        result.show()
        print(f"\n✅ Optimization completed in {optimize_time:.2f} seconds")
        
        # Get after stats
        print("\n3️⃣ AFTER OPTIMIZATION")
        print("-" * 80)
        
        after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        print(f"Files: {after['numFiles']}")
        print(f"Size: {after['sizeInBytes'] / (1024*1024):.2f} MB")
        print(f"Rows: {after['numRows']:,}")
        
        # Calculate improvements
        print("\n4️⃣ IMPROVEMENTS")
        print("-" * 80)
        
        if before['numFiles'] > 0:
            file_reduction = ((before['numFiles'] - after['numFiles']) / before['numFiles']) * 100
            print(f"File reduction: {file_reduction:.1f}%")
            print(f"  Before: {before['numFiles']} files → After: {after['numFiles']} files")
        
        if before['sizeInBytes'] > 0:
            size_reduction = ((before['sizeInBytes'] - after['sizeInBytes']) / before['sizeInBytes']) * 100
            print(f"Size reduction: {size_reduction:.1f}%")
            print(f"  Before: {before['sizeInBytes'] / (1024*1024):.2f} MB → After: {after['sizeInBytes'] / (1024*1024):.2f} MB")
        
        print("\n5️⃣ EXPECTED PERFORMANCE IMPROVEMENT")
        print("-" * 80)
        print("Typical improvements:")
        print("  - Query latency: 10-30% faster")
        print("  - Bytes scanned: 5-15% reduction")
        print("  - File operations: 50-80% reduction")
        print("  - Metadata operations: Much faster")


# ================================================================================
# PART 5: Z-ORDER CLUSTERING
# ================================================================================

class ZOrderClustering:
    """Z-ORDER multi-dimensional clustering"""
    
    @staticmethod
    def explain_zorder(spark):
        """Explain Z-ORDER concept"""
        print("\n" + "="*80)
        print("Z-ORDER CLUSTERING - CONCEPT & BENEFITS")
        print("="*80)
        
        explanation = """
WHAT IS Z-ORDER?
════════════════════════════════════════════════════════════════════════════════
Z-ORDER (Z-order curve) is a space-filling curve that organizes multi-dimensional
data into a single linear order while preserving locality.

Example visualization:
  ┌─────────────────────────────────┐
  │  Region/Product clustering      │
  │  (after Z-ORDER)                │
  │                                 │
  │  US/Laptop ──→ US/Mouse ──→ US/ │
  │      ↓            ↓             │
  │  EU/Laptop ──→ EU/Mouse ──→ EU/ │
  │      ↓            ↓             │
  │  APAC/Laptop ──→ APAC/Mouse     │
  │                                 │
  └─────────────────────────────────┘

Benefits of Z-ORDER:
✅ Groups similar values together
✅ Improves query performance for range filters
✅ Reduces bytes scanned for multi-column queries
✅ Works best with 2-4 columns
✅ Combines compaction with data reordering

When to use Z-ORDER:
✅ Immutable/historical data (data warehouses)
✅ Known query patterns (analysis queries)
✅ 2-4 frequently filtered columns
✅ Range queries (WHERE col1 > x AND col1 < y)
✅ Multi-column filters (WHERE col1 = 'A' AND col2 = 'B')

When NOT to use Z-ORDER:
❌ Frequently updated data
❌ High cardinality columns (IDs, timestamps)
❌ Real-time streaming data
❌ Many different query patterns
❌ Very large tables (expensive to re-order)

Performance expectations:
  Query improvement: 30-60%
  For partition-column filters: 50-80%
  For range queries: 40-70%
  Storage overhead: None (data is reordered, not copied)
  One-time cost: High (rewrites entire table)
  Ongoing cost: Queries only (immutable data)
"""
        print(explanation)
    
    @staticmethod
    def create_zorder_table(spark, source_table, target_table, zorder_cols):
        """Create Z-ordered version of table"""
        print("\n" + "="*80)
        print(f"CREATING Z-ORDER CLUSTERED TABLE: {target_table}")
        print("="*80)
        
        print(f"\n1️⃣ ZORDER COLUMNS: {', '.join(zorder_cols)}")
        print("-" * 80)
        
        # Copy table
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM {source_table}")
        print(f"✅ Created {target_table} from {source_table}")
        
        # Apply Z-order
        print(f"\n2️⃣ APPLYING Z-ORDER")
        print("-" * 80)
        
        zorder_clause = ", ".join(zorder_cols)
        print(f"Command: OPTIMIZE {target_table} ZORDER BY {zorder_clause}")
        
        start_time = time.time()
        result = spark.sql(f"OPTIMIZE {target_table} ZORDER BY {zorder_clause}")
        zorder_time = time.time() - start_time
        
        result.show()
        print(f"\n✅ Z-ORDER applied in {zorder_time:.2f} seconds")
        
        print(f"\n3️⃣ DATA LAYOUT AFTER Z-ORDER")
        print("-" * 80)
        
        # Show data organization
        sample_query = f"""
        SELECT {', '.join(zorder_cols)}, COUNT(*) as record_count
        FROM {target_table}
        GROUP BY {', '.join(zorder_cols)}
        ORDER BY {', '.join(zorder_cols)}
        LIMIT 10
        """
        
        spark.sql(sample_query).show(truncate=False)
        
        print(f"\n✅ {target_table} is now Z-ordered by {', '.join(zorder_cols)}")
    
    @staticmethod
    def demonstrate_zorder_benefit(spark, table_name, filter_cols):
        """Demonstrate Z-ORDER performance benefits"""
        print("\n" + "="*80)
        print(f"Z-ORDER BENEFIT DEMONSTRATION: {table_name}")
        print("="*80)
        
        print(f"\n1️⃣ TEST QUERY WITH Z-ORDERED DATA")
        print("-" * 80)
        
        # Build WHERE clause
        where_conditions = []
        where_conditions.append(f"{filter_cols[0]} = '{filter_cols[0]}'")
        if len(filter_cols) > 1:
            where_conditions.append(f"{filter_cols[1]} = '{filter_cols[1]}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT COUNT(*) as row_count
        FROM {table_name}
        WHERE {where_clause}
        """
        
        print(f"Query: {query.strip()}")
        
        start_time = time.time()
        result = spark.sql(query).collect()
        query_time = time.time() - start_time
        
        print(f"\n✅ Query completed in {query_time:.3f} seconds")
        print(f"   Rows returned: {result[0][0]:,}")
        
        print(f"\n2️⃣ EXPECTED BENEFITS")
        print("-" * 80)
        print(f"With Z-ORDER on {', '.join(filter_cols)}:")
        print(f"  - Bytes scanned: 30-60% reduction")
        print(f"  - Query time: 30-60% faster")
        print(f"  - Files read: Fewer (data is grouped)")


# ================================================================================
# PART 6: PARTITIONING
# ================================================================================

class PartitioningStrategy:
    """Table partitioning strategies"""
    
    @staticmethod
    def explain_partitioning(spark):
        """Explain partitioning concept"""
        print("\n" + "="*80)
        print("PARTITIONING - CONCEPT & BENEFITS")
        print("="*80)
        
        explanation = """
WHAT IS PARTITIONING?
════════════════════════════════════════════════════════════════════════════════
Partitioning organizes data into subdirectories based on column values.
Spark can skip entire partitions without reading them (partition pruning).

Directory structure example:
/sales_partitioned/
  year=2022/
    month=1/
      part-00001.parquet
      part-00002.parquet
    month=2/
      part-00001.parquet
  year=2023/
    month=1/
      part-00001.parquet

Benefits:
✅ Partition pruning: Skips entire directories
✅ Faster queries: Only reads relevant partitions
✅ Efficient deletes: DROP PARTITION deletes entire directory
✅ Incremental loads: Add new partitions daily
✅ Better scalability: Partition-level parallelism
✅ Easy management: Organize by date/region/category

When to use PARTITIONING:
✅ Low cardinality columns (< 100-1000 unique values)
✅ Date/time columns (year, month, day)
✅ Geographic regions (country, state, city)
✅ Categories (product type, industry)
✅ Batch loads (append-only data)
✅ Table size > 100GB

When NOT to use PARTITIONING:
❌ High cardinality columns (millions of unique values)
❌ User IDs, customer IDs, transaction IDs
❌ Continuous values
❌ Frequently updated data
❌ Many small partitions

Performance expectations:
  Query improvement: 50-90% (for partition filters)
  Partition pruning: 90%+ of data skipped
  Storage overhead: Minimal (just directory structure)
  Scaling: Slower with > 10,000 partitions

Common partitioning schemes:
  - By date: year/month/day
  - By region: country/state/city
  - By category: product_type/subcategory
  - By tenant: customer_id/org_id
  - Hybrid: year/month/region
"""
        print(explanation)
    
    @staticmethod
    def create_partitioned_table(spark, source_table, target_table, partition_cols):
        """Create partitioned version of table"""
        print("\n" + "="*80)
        print(f"CREATING PARTITIONED TABLE: {target_table}")
        print("="*80)
        
        print(f"\n1️⃣ PARTITION COLUMNS: {', '.join(partition_cols)}")
        print("-" * 80)
        
        # Read source
        df = spark.read.table(source_table)
        
        # Create partitioned table
        print(f"Command: df.write.partitionBy({', '.join(partition_cols)})")
        
        df.write \
            .partitionBy(*partition_cols) \
            .mode("overwrite") \
            .format("delta") \
            .option("mergeSchema", "true") \
            .saveAsTable(target_table)
        
        print(f"\n✅ Created partitioned table: {target_table}")
        
        print(f"\n2️⃣ VERIFY PARTITION STRUCTURE")
        print("-" * 80)
        
        partitions = spark.sql(f"SHOW PARTITIONS {target_table}").collect()
        print(f"Total partitions: {len(partitions)}")
        print(f"\nSample partitions:")
        for partition in partitions[:10]:
            print(f"  {partition[0]}")
        if len(partitions) > 10:
            print(f"  ... and {len(partitions) - 10} more")
        
        print(f"\n3️⃣ PARTITION PRUNING EXAMPLES")
        print("-" * 80)
        
        # Example 1: Single partition filter
        if "year" in partition_cols:
            print(f"\nExample 1: Single partition filter")
            print(f"Query: SELECT * FROM {target_table} WHERE year = 2023")
            print(f"Result: Reads only year=2023/ partition (90%+ data skipped)")
            
            result1 = spark.sql(f"SELECT COUNT(*) FROM {target_table} WHERE year = 2023").collect()
            print(f"  Rows matched: {result1[0][0]:,}")
        
        # Example 2: Multiple partition filters
        if "year" in partition_cols and "month" in partition_cols:
            print(f"\nExample 2: Multiple partition filters")
            print(f"Query: SELECT * FROM {target_table} WHERE year = 2023 AND month = 1")
            print(f"Result: Reads only year=2023/month=1/ partition (99%+ data skipped)")
            
            result2 = spark.sql(f"SELECT COUNT(*) FROM {target_table} WHERE year = 2023 AND month = 1").collect()
            print(f"  Rows matched: {result2[0][0]:,}")
        
        # Example 3: Non-partition filter (no pruning)
        print(f"\nExample 3: Non-partition filter")
        print(f"Query: SELECT * FROM {target_table} WHERE region = 'US'")
        print(f"Result: Reads ALL partitions (no partition pruning) ❌")
        print(f"  Solution: Also partition by region, or use LIQUID CLUSTERING")
    
    @staticmethod
    def show_partition_statistics(spark, table_name):
        """Show partition-level statistics"""
        print("\n" + "="*80)
        print(f"PARTITION STATISTICS: {table_name}")
        print("="*80)
        
        # Get partition columns
        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        
        if not partitions:
            print("❌ Table is not partitioned")
            return
        
        print(f"\n1️⃣ PARTITION INFORMATION")
        print("-" * 80)
        print(f"Total partitions: {len(partitions)}")
        
        # Sample partitions
        print(f"\nSample partitions (first 10):")
        for partition in partitions[:10]:
            print(f"  {partition[0]}")


# ================================================================================
# PART 7: LIQUID CLUSTERING
# ================================================================================

class LiquidClustering:
    """Liquid clustering (Databricks proprietary)"""
    
    @staticmethod
    def explain_liquid_clustering():
        """Explain liquid clustering concept"""
        print("\n" + "="*80)
        print("LIQUID CLUSTERING - CONCEPT & BENEFITS")
        print("="*80)
        
        explanation = """
WHAT IS LIQUID CLUSTERING?
════════════════════════════════════════════════════════════════════════════════
Liquid Clustering combines benefits of:
- Partitioning (locality, pruning)
- Z-ORDER (multi-column optimization)
- WITHOUT fixed partition structure

How it works:
- Data automatically clustered on specified columns
- Adaptive clustering that grows with data
- Automatic re-clustering on INSERT/UPDATE/DELETE
- No external directories (cleaner metadata)
- Uses hash-based bucketing internally

Syntax:
  CREATE TABLE table_name (...)
  CLUSTER BY col1, col2, col3;

Benefits over Partitioning:
✅ High cardinality columns (millions of values)
✅ Automatic re-clustering on updates
✅ Works with INSERT/UPDATE/DELETE
✅ Multiple column support
✅ No subdirectory explosion (high cardinality problem)
✅ Adaptive to data changes
✅ Cleaner metadata

Benefits over Z-ORDER:
✅ Works well with frequent updates
✅ Incremental re-clustering (not full rewrite)
✅ Better for streaming data
✅ Automatic optimization
✅ No one-time expensive operation

When to use LIQUID CLUSTERING:
✅ High cardinality columns (100K-millions of values)
✅ Frequent INSERT/UPDATE/DELETE operations
✅ Real-time + analytical mixed workloads
✅ User IDs, customer IDs, product IDs
✅ When multiple query patterns
✅ When you want automated optimization

Example use cases:
  - E-commerce: CLUSTER BY customer_id, order_id
  - SaaS: CLUSTER BY user_id, tenant_id
  - Analytics: CLUSTER BY user_id, event_type
  - Finance: CLUSTER BY account_id, transaction_id

Performance expectations:
  Query improvement: 20-50%
  Storage overhead: 5-15% (metadata overhead)
  One-time cost: Low (incremental)
  Ongoing cost: Low (incremental re-clustering)
  Adaptation speed: Automatic

Comparison:
  Partitioning:       Best for low cardinality, simple patterns
  Z-ORDER:            Best for immutable, 2-4 columns
  LIQUID CLUSTER:     Best for high cardinality, mixed workloads
"""
        print(explanation)
    
    @staticmethod
    def create_liquid_clustered_table(spark, table_name, cluster_cols):
        """Create liquid clustered table"""
        print("\n" + "="*80)
        print(f"CREATING LIQUID CLUSTERED TABLE: {table_name}")
        print("="*80)
        
        print(f"\n1️⃣ CLUSTER COLUMNS: {', '.join(cluster_cols)}")
        print("-" * 80)
        
        # Define schema for sales table
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("region", StringType()),
            StructField("product", StringType()),
            StructField("sales_person", StringType()),
            StructField("order_date", DateType()),
            StructField("sales_amount", IntegerType()),
            StructField("quantity", IntegerType()),
            StructField("customer_id", StringType()),
            StructField("discount", IntegerType()),
            StructField("category", StringType()),
        ])
        
        # Create liquid clustered table
        cluster_clause = ", ".join(cluster_cols)
        
        print(f"Command: CREATE TABLE {table_name} (...)")
        print(f"         CLUSTER BY {cluster_clause}")
        
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    order_id STRING,
                    region STRING,
                    product STRING,
                    sales_person STRING,
                    order_date DATE,
                    sales_amount INT,
                    quantity INT,
                    customer_id STRING,
                    discount INT,
                    category STRING
                )
                CLUSTER BY {cluster_clause}
                USING DELTA
            """)
            
            print(f"\n✅ Created liquid clustered table: {table_name}")
            
            print(f"\n2️⃣ TABLE PROPERTIES")
            print("-" * 80)
            
            props = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            for prop in props:
                if 'cluster' in str(prop).lower():
                    print(f"  {prop[0]}: {prop[1]}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:100]}")
            print(f"   Note: Liquid Clustering requires Databricks Runtime 11.0+")
            return False
    
    @staticmethod
    def insert_into_liquid_clustered(spark, target_table, source_table):
        """Insert data into liquid clustered table"""
        print(f"\n✅ Inserting data into {target_table}...")
        
        try:
            spark.sql(f"INSERT INTO {target_table} SELECT * FROM {source_table}")
            print(f"✅ Data inserted successfully")
            
            # Show stats
            count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
            print(f"   Total rows: {count:,}")
            
        except Exception as e:
            print(f"❌ Error inserting data: {str(e)[:50]}")


# ================================================================================
# PART 8: AUTO LIQUID CLUSTERING
# ================================================================================

class AutoLiquidClustering:
    """Automatic liquid clustering"""
    
    @staticmethod
    def explain_auto_clustering():
        """Explain auto liquid clustering"""
        print("\n" + "="*80)
        print("AUTO LIQUID CLUSTERING - AUTOMATIC OPTIMIZATION")
        print("="*80)
        
        explanation = """
WHAT IS AUTO LIQUID CLUSTERING?
════════════════════════════════════════════════════════════════════════════════
AUTO LIQUID CLUSTERING automatically selects optimal clustering columns based on:
- Query patterns (which columns are frequently filtered)
- Data skewness (uneven distribution)
- Cardinality (number of unique values)
- Access frequency (how often each column is used)

How it works:
1. Monitor: Track all queries executed on table
2. Analyze: ML model analyzes query patterns
3. Recommend: Suggest optimal clustering columns
4. Apply: Auto-enable liquid clustering
5. Improve: Continuously adjust as patterns change

Enable with:
  ALTER TABLE table_name SET TBLPROPERTIES (
    'delta.clustering.enabled' = 'true'
  );

Benefits:
✅ No manual configuration
✅ Adapts to query patterns
✅ Learns from real workload
✅ Continuous optimization
✅ Best for exploratory queries
✅ Works with changing patterns

When to use AUTO CLUSTERING:
✅ Development environments
✅ Exploratory analytics
✅ Unknown query patterns
✅ Tables with changing access patterns
✅ Rapid prototyping

When NOT to use AUTO CLUSTERING:
❌ Production with strict SLA
❌ Known fixed query patterns
❌ Need predictable performance
❌ Low overhead requirement

Timeline:
  Day 1-7:    Data collection (monitor queries)
  Day 7-14:   Analysis and pattern detection
  Day 14-21:  Recommendations generation
  Day 21+:    Auto-optimization runs
  Day 30+:    Continuous improvement

Expected benefits:
  After 1 month: 20-40% query improvement
  After 2 months: 30-50% query improvement
  Long term: 40-60% improvement
"""
        print(explanation)
    
    @staticmethod
    def enable_auto_clustering(spark, table_name):
        """Enable auto clustering on table"""
        print("\n" + "="*80)
        print(f"ENABLING AUTO LIQUID CLUSTERING: {table_name}")
        print("="*80)
        
        print(f"\n1️⃣ CREATE TABLE WITH AUTO CLUSTERING")
        print("-" * 80)
        
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_auto (
                    order_id STRING,
                    region STRING,
                    product STRING,
                    sales_person STRING,
                    order_date DATE,
                    sales_amount INT,
                    quantity INT,
                    customer_id STRING
                )
                USING DELTA
                TBLPROPERTIES (
                    'delta.clustering.enabled' = 'true'
                )
            """)
            
            print(f"✅ Created table with AUTO CLUSTERING enabled")
            
            print(f"\n2️⃣ TABLE PROPERTIES")
            print("-" * 80)
            
            props = spark.sql(f"SHOW TBLPROPERTIES {table_name}_auto").collect()
            print("Properties:")
            for prop in props:
                print(f"  {prop[0]}: {prop[1]}")
            
            print(f"\n3️⃣ MONITORING SCHEDULE")
            print("-" * 80)
            print(f"""
Query Pattern Detection: Continuous
Pattern Analysis: Every 24 hours
Recommendations: Generated after 7-14 days
Auto-optimization: Applied after recommendations
Performance Impact: Visible after 2-4 weeks

Tracked metrics:
  ✅ Query frequency per column
  ✅ Filter combinations
  ✅ Join patterns
  ✅ Aggregation columns
  ✅ Access patterns over time
""")
            
            return True
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:100]}")
            return False


# ================================================================================
# PART 9: PREDICTIVE OPTIMIZATION
# ================================================================================

class PredictiveOptimization:
    """Predictive optimization using machine learning"""
    
    @staticmethod
    def explain_predictive_optimization():
        """Explain predictive optimization"""
        print("\n" + "="*80)
        print("PREDICTIVE OPTIMIZATION - ML-DRIVEN OPTIMIZATION")
        print("="*80)
        
        explanation = """
WHAT IS PREDICTIVE OPTIMIZATION?
════════════════════════════════════════════════════════════════════════════════
Predictive Optimization uses machine learning to automatically:
- Analyze query patterns
- Recommend optimal clustering strategy
- Auto-run OPTIMIZE operations
- Suggest partitioning scheme
- Monitor effectiveness continuously

Benefits:
✅ Zero manual tuning required
✅ ML learns from actual query patterns
✅ Automatic OPTIMIZE runs
✅ Optimizes for real workload (not assumptions)
✅ Adapts to changing access patterns
✅ Continuous improvement

Requirements:
✅ Databricks Premium or Enterprise
✅ Unity Catalog enabled
✅ Table in Unity Catalog
✅ Delta Lake format

How it works:
Phase 1: Collection (Week 1-2)
  - Tracks all queries
  - Monitors query patterns
  - Collects performance metrics
  - Measures bytes scanned
  - Records execution times

Phase 2: Analysis (Week 2-3)
  - ML model analyzes patterns
  - Identifies frequently filtered columns
  - Detects range queries
  - Analyzes multi-column patterns
  - Calculates optimization value

Phase 3: Recommendation (Week 3-4)
  - Recommends clustering columns
  - Suggests OPTIMIZE frequency
  - Predicts performance improvements
  - Estimates storage impact
  - Proposes optimization strategy

Phase 4: Optimization (Week 4+)
  - Auto-applies liquid clustering
  - Auto-runs OPTIMIZE periodically
  - Monitors improvement
  - Adjusts strategy if patterns change
  - Re-optimizes continuously

Expected improvements:
  Query time:        30-50% faster
  Bytes scanned:     40-60% reduction
  Files read:        60-80% reduction
  Storage efficiency:  10-30% improvement
  Overall throughput: 50-70% increase

Example: E-commerce analytics table
  Before Predictive Optimization:
    - Query latency: 30 seconds
    - Bytes scanned: 500 MB
    - Files read: 1000
    - Query cost: $0.50 per query

  After Predictive Optimization (Week 4):
    - Query latency: 12 seconds (-60%)
    - Bytes scanned: 150 MB (-70%)
    - Files read: 200 (-80%)
    - Query cost: $0.10 per query (-80%)
    - Monthly savings: $300 (1000 queries/day)

When to use:
✅ Mixed real-time + analytical workloads
✅ Frequently changing query patterns
✅ Unknown or complex workloads
✅ Production tables needing optimization
✅ When you want hands-off optimization
✅ Tables with diverse query patterns

When NOT to use:
❌ Small tables (< 10GB)
❌ Simple, known query patterns
❌ Batch jobs with fixed schedules
❌ Development/testing environments
❌ Cost-sensitive (small potential gain)
❌ Standard Databricks (not Premium)

Monitoring dashboard:
  - Optimization status
  - Query improvement metrics
  - Bytes scanned reduction
  - File count reduction
  - Storage savings
  - Recommended actions
  - Performance timeline
"""
        print(explanation)
    
    @staticmethod
    def enable_predictive_optimization(spark, table_name):
        """Enable predictive optimization (requires Premium)"""
        print("\n" + "="*80)
        print(f"ENABLING PREDICTIVE OPTIMIZATION: {table_name}")
        print("="*80)
        
        print(f"\n1️⃣ ENABLE PREDICTIVE OPTIMIZATION")
        print("-" * 80)
        
        print(f"Command: ALTER TABLE {table_name} SET TBLPROPERTIES(...)")
        
        try:
            spark.sql(f"""
                ALTER TABLE {table_name} SET TBLPROPERTIES (
                    'delta.clustering.enabled' = 'true',
                    'delta.autoClustering.enabled' = 'true'
                )
            """)
            
            print(f"✅ Predictive Optimization enabled on {table_name}")
            
        except Exception as e:
            print(f"ℹ️  Note: Predictive Optimization requires Databricks Premium/Enterprise")
            print(f"   Error: {str(e)[:80]}")
        
        print(f"\n2️⃣ EXPECTED TIMELINE")
        print("-" * 80)
        print(f"""
Week 1:     Data collection begins
Week 2:     Pattern analysis starts
Week 3:     Recommendations generated
Week 4+:    Automatic optimizations run
Month 1:    30-40% improvement visible
Month 2:    50-60% improvement
Month 3+:   Continuous optimization
""")
        
        print(f"\n3️⃣ MONITORED METRICS")
        print("-" * 80)
        print(f"""
✅ Query latency distribution
✅ Bytes scanned per query
✅ Files read per query
✅ Query frequency per column
✅ Filter combinations
✅ Join patterns
✅ Aggregation patterns
✅ Optimization effectiveness
✅ Storage usage
✅ Cost per query
""")


# ================================================================================
# PART 10: VACUUM OPERATION
# ================================================================================

class VacuumOperation:
    """VACUUM cleanup and maintenance"""
    
    @staticmethod
    def explain_vacuum():
        """Explain VACUUM operation"""
        print("\n" + "="*80)
        print("VACUUM - CLEANUP & DATA RETENTION")
        print("="*80)
        
        explanation = """
WHAT IS VACUUM?
════════════════════════════════════════════════════════════════════════════════
VACUUM removes old files no longer needed by the table.

Delta Lake keeps old files for:
- Time-travel queries (RESTORE to previous versions)
- ROLLBACK operations (undo writes)
- Failure recovery (replay transaction log)
- Concurrent read consistency

After a retention period, files can be safely deleted:

Syntax:
  VACUUM table_name;                      -- 30 days (default, production)
  VACUUM table_name RETAIN 7 DAYS;        -- 7 days
  VACUUM table_name RETAIN 0 DAYS;        -- Immediate (risky!)

Behavior:
  - Finds all files not referenced by current table version
  - Deletes files older than retention period
  - Cannot undo (permanent deletion)
  - Does NOT delete data in current table
  - Only removes version history

Benefits:
✅ Reduces storage costs (10-50% typical)
✅ Improves list operations (fewer files to scan)
✅ Cleans up failed transactions
✅ Reduces metadata overhead
✅ Improves table discovery performance

Overhead:
⚠️  Permanent deletion (cannot restore old versions)
⚠️  Disrupts time-travel queries (old versions removed)
⚠️  Should run during low-activity periods
⚠️  Can take time for large tables

Storage savings typical:
  After OPTIMIZE:           10-15% reduction
  After bulk deletes:       20-50% reduction
  After failed attempts:    5-10% reduction
  Long-term regular VACUUM: 15-30% reduction

Timeline:
  Day 1:       DELETE/UPDATE statement runs
  Day 1-7:     Old file versions kept (for rollback)
  Day 7+:      VACUUM RETAIN 7 DAYS deletes old files
  Week 2:      Storage reduced by 15-30%

When to run VACUUM:
✅ After OPTIMIZE (clean up old files)
✅ After bulk DELETE operations
✅ After failed transactions
✅ Weekly/monthly maintenance
✅ Before major reorganization
✅ When storage costs high

Safe retention periods:
  Development:    1-7 days (fast iteration)
  Staging:        3-14 days (pre-production testing)
  Production:     30-90 days (safety margin)
  Critical data:  180-365 days (regulatory/compliance)
  Archive:        Don't VACUUM (keep history)

Risk management:
  ✅ Always: Schedule during off-peak hours
  ✅ Always: Monitor VACUUM execution
  ✅ Always: Verify backups exist
  ✅ Always: Use appropriate retention
  ✅ Never:  Use RETAIN 0 DAYS in production
  ✅ Never:  VACUUM during active queries
  ✅ Never:  VACUUM critical data without backup
"""
        print(explanation)
    
    @staticmethod
    def perform_vacuum(spark, table_name, retention_days=7):
        """Execute VACUUM operation"""
        print("\n" + "="*80)
        print(f"EXECUTING VACUUM: {table_name}")
        print("="*80)
        
        print(f"\n1️⃣ PRE-VACUUM STATISTICS")
        print("-" * 80)
        
        before = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        print(f"Files: {before['numFiles']}")
        print(f"Size: {before['sizeInBytes'] / (1024*1024):.2f} MB")
        print(f"Rows: {before['numRows']:,}")
        
        # Get history
        print(f"\n2️⃣ TABLE VERSION HISTORY")
        print("-" * 80)
        
        history = spark.sql(f"DESCRIBE HISTORY {table_name}").collect()
        print(f"Total versions: {len(history)}")
        if history:
            print(f"Oldest version: {history[-1]['timestamp']}")
            print(f"Latest version: {history[0]['timestamp']}")
        
        print(f"\n3️⃣ EXECUTING VACUUM WITH {retention_days} DAY RETENTION")
        print("-" * 80)
        
        print(f"Command: VACUUM {table_name} RETAIN {retention_days} DAYS")
        
        try:
            start_time = time.time()
            
            # Run VACUUM
            if retention_days == 0:
                print(f"\n⚠️  WARNING: Using 0-day retention - this is risky!")
                print(f"   Enabling retention check override...")
                spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
            
            spark.sql(f"VACUUM {table_name} RETAIN {retention_days} DAYS")
            
            vacuum_time = time.time() - start_time
            print(f"\n✅ VACUUM completed in {vacuum_time:.2f} seconds")
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:100]}")
            print(f"   Tip: Set retention duration check to false")
            return False
        
        print(f"\n4️⃣ POST-VACUUM STATISTICS")
        print("-" * 80)
        
        after = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        print(f"Files: {after['numFiles']}")
        print(f"Size: {after['sizeInBytes'] / (1024*1024):.2f} MB")
        print(f"Rows: {after['numRows']:,}")
        
        print(f"\n5️⃣ STORAGE SAVINGS")
        print("-" * 80)
        
        space_saved = before['sizeInBytes'] - after['sizeInBytes']
        if before['sizeInBytes'] > 0:
            percent_saved = (space_saved / before['sizeInBytes']) * 100
            print(f"Space saved: {space_saved / (1024*1024):.2f} MB ({percent_saved:.1f}%)")
        
        print(f"\n6️⃣ NEXT STEPS")
        print("-" * 80)
        print(f"""
✅ Schedule regular VACUUM:
   - Weekly: For frequently updated tables
   - Monthly: For batch-loaded tables
   - Quarterly: For stable reference data

✅ Monitor storage costs:
   - After VACUUM, storage should decrease 10-30%
   - Track month-over-month cost reduction
   - Calculate ROI

✅ Document retention policy:
   - Document why you chose {retention_days} days
   - Document when to adjust retention
   - Document backup strategy
""")
        
        return True
    
    @staticmethod
    def show_vacuum_recommendations(spark, table_name):
        """Show VACUUM recommendations"""
        print("\n" + "="*80)
        print(f"VACUUM RECOMMENDATIONS: {table_name}")
        print("="*80)
        
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        history = spark.sql(f"DESCRIBE HISTORY {table_name}").collect()
        
        print(f"\nTable characteristics:")
        print(f"  Size: {detail['sizeInBytes'] / (1024*1024):.2f} MB")
        print(f"  Files: {detail['numFiles']}")
        print(f"  Versions: {len(history)}")
        print(f"  Row count: {detail['numRows']:,}")
        
        print(f"\nRecommendation:")
        
        # Analyze and recommend
        if detail['numFiles'] < 100:
            retention = 30
            frequency = "Monthly"
        elif detail['numFiles'] < 1000:
            retention = 14
            frequency = "Bi-weekly"
        else:
            retention = 7
            frequency = "Weekly"
        
        print(f"  Retention: {retention} days")
        print(f"  Frequency: {frequency}")
        print(f"  Expected savings: 15-30% of storage")
        
        print(f"\nCommand:")
        print(f"  VACUUM {table_name} RETAIN {retention} DAYS")


# ================================================================================
# PART 11: HOW OPTIMIZATIONS CONNECT
# ================================================================================

class OptimizationFramework:
    """How all optimization techniques connect"""
    
    @staticmethod
    def show_optimization_flow():
        """Show optimization decision flow"""
        print("\n" + "="*80)
        print("OPTIMIZATION DECISION FLOW & WORKFLOW")
        print("="*80)
        
        flow = """
START: Table performance needs optimization
  ↓
  ├─→ Step 1: ANALYZE TABLE
  │   ├─ Purpose: Gather statistics for Catalyst optimizer
  │   ├─ Command: ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS
  │   ├─ Duration: 5 min - 2 hours (depends on table size)
  │   ├─ Impact: Improves query planning
  │   └─ Benefit: 10-20% query optimization (through better plans)
  │
  ├─→ Step 2: OPTIMIZE (File Compaction)
  │   ├─ Purpose: Compact small files into larger ones
  │   ├─ Command: OPTIMIZE table_name
  │   ├─ Duration: 10 min - 8 hours (depends on files)
  │   ├─ Impact: Reduces file count, improves I/O
  │   └─ Benefit: 10-30% query speedup (fewer file operations)
  │
  ├─→ Step 3: Choose Clustering Strategy
  │   ├─ Analyze data characteristics
  │   ├─ Analyze query patterns
  │   │
  │   ├─→ Option A: PARTITIONING
  │   │   ├─ When: Low cardinality (< 100), time-based
  │   │   ├─ Impact: 50-90% for partition filters
  │   │   ├─ Cost: One-time (write time)
  │   │   ├─ Overhead: Many small files (mitigated by OPTIMIZE)
  │   │   └─ Example: PARTITION BY year, month
  │   │
  │   ├─→ Option B: Z-ORDER
  │   │   ├─ When: 2-4 columns, immutable data, known patterns
  │   │   ├─ Command: OPTIMIZE table_name ZORDER BY col1, col2
  │   │   ├─ Impact: 30-60% for multi-column queries
  │   │   ├─ Cost: One-time full rewrite
  │   │   └─ Duration: 1-12 hours (depends on size)
  │   │
  │   ├─→ Option C: LIQUID CLUSTERING
  │   │   ├─ When: High cardinality, frequent updates
  │   │   ├─ Impact: 20-50% query improvement
  │   │   ├─ Cost: Incremental (with each update)
  │   │   └─ Automatic: Re-clusters with updates
  │   │
  │   └─→ Option D: AUTO CLUSTERING
  │       ├─ When: Unknown patterns, exploratory
  │       ├─ Impact: Varies, learns over time
  │       └─ Duration: Recommendations after 2 weeks
  │
  ├─→ Step 4: PREDICTIVE OPTIMIZATION (Optional)
  │   ├─ When: Premium Databricks, mixed workloads
  │   ├─ Duration: 2-4 weeks to see benefits
  │   ├─ Impact: 40-70% overall improvement
  │   ├─ Overhead: ML analysis + background runs
  │   └─ Benefit: Learns and adapts continuously
  │
  ├─→ Step 5: Schedule Regular VACUUM
  │   ├─ When: After OPTIMIZE, after bulk deletes
  │   ├─ Command: VACUUM table_name RETAIN 7 DAYS
  │   ├─ Frequency: Weekly to Monthly
  │   ├─ Duration: Minutes to Hours
  │   ├─ Benefit: 15-30% storage reduction
  │   └─ Next: Repeat OPTIMIZE+VACUUM monthly
  │
  └─→ Step 6: MONITOR & ADJUST
      ├─ Check: Query performance, storage, file counts
      ├─ Frequency: Weekly reviews
      ├─ Measure: Compare before/after metrics
      ├─ Action: Adjust retention, clustering if needed
      └─ Repeat: Continuous improvement cycle
"""
        print(flow)
    
    @staticmethod
    def show_real_world_example():
        """Show real-world optimization scenario"""
        print("\n" + "="*80)
        print("REAL-WORLD OPTIMIZATION EXAMPLE")
        print("="*80)
        
        example = """
SCENARIO: E-commerce Orders Table
════════════════════════════════════════════════════════════════════════════════

DATA CHARACTERISTICS:
  - 100M orders per day
  - 2 years of history (730B rows)
  - 500 GB total size
  - Grows 100 GB per month

QUERY PATTERNS:
  1. Recent orders (80% of queries):
     WHERE order_date >= today - 7 days         [HIGH FREQUENCY]
  
  2. Customer orders (15% of queries):
     WHERE customer_id = 'X'                    [HIGH FREQUENCY]
  
  3. Regional sales (4% of queries):
     WHERE region = 'US' AND product = 'Shoes'  [MEDIUM FREQUENCY]
  
  4. Historical analysis (1% of queries):
     Various aggregations on old data            [LOW FREQUENCY]

CURRENT PERFORMANCE:
  - Query latency: 45 seconds (average)
  - Bytes scanned: 400 MB (per query)
  - Files read: 2000+ (per query)
  - Query cost: $0.40 per query
  - Monthly cost: $480K (1.2M queries/day)

OPTIMIZATION STRATEGY:
════════════════════════════════════════════════════════════════════════════════

Week 1: Foundation
  ├─ Monday: ANALYZE TABLE
  │          Command: ANALYZE TABLE orders COMPUTE STATISTICS FOR ALL COLUMNS
  │          Time: 4 hours
  │          Result: Improved query planning
  │
  ├─ Tuesday: OPTIMIZE
  │          Command: OPTIMIZE orders
  │          Time: 6 hours
  │          Result: 2000 files → 200 files
  │          Benefit: 20% faster queries (fewer file ops)
  │
  └─ Wednesday: Apply Clustering Strategy
             Strategy: PARTITION BY year, month (date-based)
                       + LIQUID CLUSTER BY customer_id
                       (handles both query patterns)
             Reason: Low cardinality on dates (24 partitions)
                    High cardinality on customer_id (millions)

Week 1-2: Initial Results
  ├─ Performance:
  │  - Query latency: 45s → 18s (-60%)
  │  - Bytes scanned: 400MB → 60MB (-85%)
  │  - Files read: 2000 → 50 (-97%)
  │
  ├─ Cost:
  │  - Query cost: $0.40 → $0.06 (-85%)
  │  - Monthly: $480K → $72K (SAVINGS: $408K/month!)
  │
  └─ Storage:
     - Original: 500 GB
     - After: 510 GB (slight overhead from partitioning)

Week 2+: Ongoing Optimization
  ├─ Weekly:
  │  └─ OPTIMIZE orders (compact new files)
  │     Command: OPTIMIZE orders ZORDER BY region, product
  │     Time: 2 hours
  │     Benefit: Keeps region/product queries fast
  │
  ├─ Monthly:
  │  ├─ VACUUM orders RETAIN 7 DAYS
  │  │  Benefit: 20 GB cleanup (-4%)
  │  │
  │  └─ ANALYZE TABLE
  │     Benefit: Updated statistics for planning
  │
  └─ Quarterly:
     └─ Review & Adjust
        - Check query patterns (changed?)
        - Check cardinality (outgrown partitions?)
        - Adjust clustering if needed
        - Consider predictive optimization

PREDICTIVE OPTIMIZATION (Optional, Week 4+):
  ├─ Enable: ALTER TABLE orders SET TBLPROPERTIES (delta.clustering.enabled=true)
  ├─ Wait: 2-4 weeks for ML to learn patterns
  ├─ Benefit: Additional 30-40% improvement
  └─ Result: Query time 18s → 8s, Cost: $0.06 → $0.02

FINAL RESULTS (After 1 month):
════════════════════════════════════════════════════════════════════════════════

PERFORMANCE:
  Query latency:   45s → 8s (-82%)
  Bytes scanned:   400MB → 50MB (-87.5%)
  Files read:      2000 → 30 (-98.5%)
  P95 latency:     120s → 20s (-83%)

COST:
  Per query:       $0.40 → $0.02 (-95%)
  Daily queries:   1.2M
  Daily cost:      $480K → $24K
  Monthly savings: $480K - $24K = $456K
  Annual savings:  $5.5M

STORAGE:
  Original:        500 GB
  After vacuum:    480 GB (-4%)
  Monthly growth:  100 GB → 100 GB (no change)
  Annual cleanup:  VACUUM saves 600 GB/year

OPERATIONAL IMPROVEMENTS:
  ✅ Faster dashboards (45s → 8s)
  ✅ Better user experience
  ✅ Lower infrastructure costs
  ✅ More queries possible (capacity)
  ✅ Reduced data transfer costs
  ✅ Better resource utilization
"""
        print(example)


# ================================================================================
# PART 12: COMPLETE OPTIMIZATION PIPELINE
# ================================================================================

class CompletePipeline:
    """End-to-end optimization pipeline"""
    
    @staticmethod
    def run_complete_optimization(spark, table_name, optimize_strategy="full"):
        """
        Run complete optimization pipeline
        
        Strategies:
        - "quick": OPTIMIZE only
        - "full": OPTIMIZE + Z-ORDER
        - "clustered": LIQUID CLUSTERING
        - "predictive": All + Predictive (Premium)
        """
        print("\n" + "="*80)
        print(f"COMPLETE OPTIMIZATION PIPELINE: {table_name}")
        print(f"Strategy: {optimize_strategy.upper()}")
        print("="*80)
        
        pipeline_start = time.time()
        
        # Step 1: Analyze
        print(f"\n✅ STEP 1: ANALYZE TABLE")
        print("-" * 80)
        
        analyze_start = time.time()
        try:
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
            spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
            analyze_time = time.time() - analyze_start
            print(f"   Completed in {analyze_time:.2f} seconds")
        except Exception as e:
            print(f"   Warning: {str(e)[:50]}")
        
        # Step 2: Optimize
        print(f"\n✅ STEP 2: OPTIMIZE")
        print("-" * 80)
        
        optimize_start = time.time()
        try:
            if optimize_strategy == "full":
                spark.sql(f"OPTIMIZE {table_name} ZORDER BY region, product")
                print(f"   Applied Z-ORDER BY region, product")
            else:
                spark.sql(f"OPTIMIZE {table_name}")
                print(f"   Applied standard optimization")
            
            optimize_time = time.time() - optimize_start
            print(f"   Completed in {optimize_time:.2f} seconds")
        except Exception as e:
            print(f"   Error: {str(e)[:50]}")
        
        # Step 3: Get statistics
        print(f"\n✅ STEP 3: FINAL STATISTICS")
        print("-" * 80)
        
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
            print(f"   Files: {detail['numFiles']}")
            print(f"   Size: {detail['sizeInBytes'] / (1024*1024):.2f} MB")
            print(f"   Rows: {detail['numRows']:,}")
        except:
            pass
        
        # Step 4: Vacuum
        print(f"\n✅ STEP 4: VACUUM")
        print("-" * 80)
        
        try:
            vacuum_start = time.time()
            spark.sql(f"VACUUM {table_name} RETAIN 7 DAYS")
            vacuum_time = time.time() - vacuum_start
            print(f"   Completed in {vacuum_time:.2f} seconds")
            print(f"   Retention: 7 days")
        except Exception as e:
            print(f"   Warning: {str(e)[:50]}")
        
        pipeline_time = time.time() - pipeline_start
        
        print(f"\n" + "="*80)
        print(f"✅ OPTIMIZATION PIPELINE COMPLETED")
        print(f"   Total time: {pipeline_time:.2f} seconds")
        print(f"   Table: {table_name}")
        print(f"   Strategy: {optimize_strategy}")
        print("="*80)
        
        print(f"\n📋 NEXT STEPS:")
        print("-" * 80)
        print(f"""
✅ Schedule maintenance:
   - Weekly: Run optimization
   - Monthly: Run VACUUM
   - Quarterly: Review strategy

✅ Monitor improvements:
   - Query latency
   - Storage usage
   - Query costs
   - File counts

✅ Adjust if needed:
   - Change clustering columns
   - Adjust VACUUM retention
   - Switch strategies
   - Consider Predictive Optimization
""")


# ================================================================================
# PART 13: MAIN DEMONSTRATION
# ================================================================================

def main():
    """Execute complete demonstration"""
    
    # Setup
    spark = OptimizationEnvironment.create_session()
    OptimizationEnvironment.print_config(spark)
    
    # Create sample data
    generator = SampleDataGenerator()
    generator.create_sales_dataset(spark, num_records=50000, table_name="sales_raw")
    
    try:
        print("\n" + "="*80)
        print("COMPLETE DATABRICKS OPTIMIZATION GUIDE - EXECUTION")
        print("="*80)
        
        # Part 1: Table Statistics
        stats = TableStatistics()
        stats.analyze_table(spark, "sales_raw")
        stats.get_table_details(spark, "sales_raw")
        stats.show_table_properties(spark, "sales_raw")
        
        # Part 2: Optimize
        optimize = OptimizeOperation()
        optimize.optimize_table(spark, "sales_raw")
        
        # Part 3: Z-ORDER
        zorder = ZOrderClustering()
        zorder.explain_zorder(spark)
        zorder.create_zorder_table(spark, "sales_raw", "sales_zorder", ["region", "product"])
        zorder.demonstrate_zorder_benefit(spark, "sales_zorder", ["region", "product"])
        
        # Part 4: Partitioning
        partition = PartitioningStrategy()
        partition.explain_partitioning(spark)
        partition.create_partitioned_table(spark, "sales_raw", "sales_partitioned", ["year", "month"])
        partition.show_partition_statistics(spark, "sales_partitioned")
        
        # Part 5: Liquid Clustering
        liquid = LiquidClustering()
        liquid.explain_liquid_clustering()
        if liquid.create_liquid_clustered_table(spark, "sales_liquid_cluster", ["customer_id", "region"]):
            liquid.insert_into_liquid_clustered(spark, "sales_liquid_cluster", "sales_raw")
        
        # Part 6: Auto Clustering
        auto = AutoLiquidClustering()
        auto.explain_auto_clustering()
        auto.enable_auto_clustering(spark, "sales_auto_cluster")
        
        # Part 7: Predictive Optimization
        pred = PredictiveOptimization()
        pred.explain_predictive_optimization()
        pred.enable_predictive_optimization(spark, "sales_raw")
        
        # Part 8: VACUUM
        vacuum = VacuumOperation()
        vacuum.explain_vacuum()
        vacuum.perform_vacuum(spark, "sales_raw", retention_days=7)
        vacuum.show_vacuum_recommendations(spark, "sales_raw")
        
        # Part 9: How they connect
        framework = OptimizationFramework()
        framework.show_optimization_flow()
        framework.show_real_world_example()
        
        # Part 10: Complete Pipeline
        pipeline = CompletePipeline()
        pipeline.run_complete_optimization(spark, "sales_raw", optimize_strategy="full")
        
        print("\n" + "="*80)
        print("✅ COMPLETE DEMONSTRATION FINISHED!")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error during execution: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()