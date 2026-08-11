"""
================================================================================
COMPLETE SPARK BUILT-IN FUNCTIONS REFERENCE FOR REAL-TIME PROJECTS
================================================================================
Author: Data Engineering Team
Date: 2024
Description: All essential Spark functions organized by category for easy reference
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    # Core transformation functions
    col, lit, struct, named_struct, when, otherwise, coalesce, case,
    
    # Selection & Column operations
    select, withColumn, drop, alias,
    
    # Filtering & Conditional
    filter, where, between, isin, like, rlike, isnan, isnull, isnotnull,
    
    # Aggregation
    sum, avg, max, min, count, countDistinct, stddev, variance,
    collect_list, first, last,
    
    # String functions
    upper, lower, length, substring, trim, ltrim, rtrim, 
    replace, split, concat, concat_ws, instr, 
    regexp_replace, regexp_extract,
    
    # Date & Time
    current_date, current_timestamp, date_format, to_date, to_timestamp,
    datediff, date_add, date_sub, year, month, day, hour, minute, second,
    unix_timestamp, from_unixtime,
    
    # Array functions
    array, array_contains, array_length, array_union, array_intersect,
    array_except, explode, explode_outer, collect_list, split,
    flatten, reverse, sort_array, element_at, slice, concat,
    
    # Map functions
    map_from_arrays, map_keys, map_values, map_concat, size,
    
    # JSON functions
    get_json_object, json_tuple, from_json, to_json, schema_of_json,
    
    # Math functions
    abs, sqrt, round, ceil, floor, pow, log, exp, greatest, least,
    
    # Type casting
    cast,
    
    # Window functions
    row_number, rank, dense_rank, lag, lead,
    
    # Deduplication
    dropDuplicates, distinct,
    
    # Other
    input_file_name
)

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType, ArrayType, MapType, BooleanType, LongType,
    FloatType, DecimalType
)

from pyspark.sql.window import Window
from functools import reduce

# ================================================================================
# SECTION 1: INITIALIZATION & SESSION SETUP
# ================================================================================

def create_spark_session(app_name="SparkApp"):
    """Create Spark session with optimizations for real-time processing"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.databricks.io.cache.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    return spark


# ================================================================================
# SECTION 2: CORE TRANSFORMATION FUNCTIONS
# ================================================================================

def demo_select_operations(spark):
    """Demonstrate select, withColumn, and column operations"""
    print("\n" + "="*80)
    print("SECTION 2: SELECT & COLUMN OPERATIONS")
    print("="*80)
    
    # Sample data
    data = [
        ("C001", "john.doe@gmail.com", 25000.50, "2024-01-15"),
        ("C002", "jane.smith@yahoo.com", 35000.75, "2024-02-20"),
        ("C003", "bob.jones@gmail.com", 45000.00, "2024-03-10"),
    ]
    df = spark.createDataFrame(data, ["customer_id", "email", "salary", "join_date"])
    
    print("\n1. BASIC SELECT - Select specific columns")
    df_selected = df.select("customer_id", "email", "salary")
    df_selected.show()
    
    print("\n2. SELECT WITH ALIAS - Rename columns")
    df_aliased = df.select(
        col("customer_id").alias("id"),
        col("email").alias("contact_email"),
        col("salary").alias("annual_salary")
    )
    df_aliased.show()
    
    print("\n3. SELECT WITH CALCULATED COLUMN")
    df_with_calc = df.select(
        "*",
        (col("salary") * 0.1).alias("bonus")
    )
    df_with_calc.show()
    
    print("\n4. WITHCOLUMN - Add single column")
    df_upper = df.withColumn("email_upper", upper(col("email")))
    df_upper.show()
    
    print("\n5. MULTIPLE WITHCOLUMN - Chain operations")
    df_enhanced = df \
        .withColumn("bonus", col("salary") * 0.1) \
        .withColumn("total_comp", col("salary") + col("bonus")) \
        .withColumn("salary_bracket", col("salary").cast("double"))
    df_enhanced.show()
    
    print("\n6. DROP COLUMN - Remove columns")
    df_dropped = df.drop("join_date")
    df_dropped.show()
    
    return df


# ================================================================================
# SECTION 3: FILTERING & CONDITIONAL FUNCTIONS
# ================================================================================

def demo_filter_operations(spark, df):
    """Demonstrate filter, where, and conditional operations"""
    print("\n" + "="*80)
    print("SECTION 3: FILTERING & CONDITIONAL FUNCTIONS")
    print("="*80)
    
    print("\n1. BASIC FILTER - Single condition")
    high_earners = df.filter(col("salary") > 30000)
    high_earners.show()
    
    print("\n2. FILTER WITH AND - Multiple conditions")
    df_filtered = df.filter(
        (col("salary") > 25000) & 
        (col("join_date") >= "2024-01-01")
    )
    df_filtered.show()
    
    print("\n3. FILTER WITH OR - Multiple OR conditions")
    df_or = df.filter(
        (col("email").like("%gmail%")) | 
        (col("email").like("%yahoo%"))
    )
    df_or.show()
    
    print("\n4. FILTER WITH NOT - Negation")
    df_not = df.filter(~col("email").like("%gmail%"))
    df_not.show()
    
    print("\n5. BETWEEN - Range filtering")
    df_range = df.filter(between(col("salary"), 25000, 40000))
    df_range.show()
    
    print("\n6. ISIN - Multiple values")
    df_in = df.filter(col("customer_id").isin("C001", "C003"))
    df_in.show()
    
    print("\n7. RLIKE - Regex pattern matching")
    df_regex = df.filter(col("email").rlike("^[a-z]+\\."))
    df_regex.show()
    
    print("\n8. WHEN-OTHERWISE - Conditional assignment")
    df_when = df.withColumn(
        "tier_assigned",
        when(col("salary") > 40000, "VIP")
        .when(col("salary") > 30000, "Premium")
        .when(col("salary") > 0, "Standard")
        .otherwise("Inactive")
    )
    df_when.show()
    
    print("\n9. CASE STATEMENT - Alternative conditional")
    df_case = df.select(
        col("customer_id"),
        case()
        .when(col("salary") > 40000, "VIP")
        .when(col("salary") > 30000, "Premium")
        .when(col("salary") > 0, "Standard")
        .otherwise("Inactive")
        .alias("tier_assigned")
    )
    df_case.show()
    
    print("\n10. COALESCE - First non-null value")
    data_multi = [
        ("C001", "john@gmail.com", "john@yahoo.com"),
        ("C002", None, "jane@yahoo.com"),
    ]
    df_multi = spark.createDataFrame(data_multi, ["customer_id", "primary_email", "backup_email"])
    df_coalesce = df_multi.withColumn(
        "contact_email",
        coalesce(col("primary_email"), col("backup_email"))
    )
    df_coalesce.show()
    
    print("\n11. ISNULL / ISNOTNULL - NULL checking")
    df_null_check = df.withColumn(
        "has_join_date",
        isnotnull(col("join_date"))
    )
    df_null_check.show()


# ================================================================================
# SECTION 4: AGGREGATION FUNCTIONS
# ================================================================================

def demo_aggregation_operations(spark):
    """Demonstrate groupBy and aggregation functions"""
    print("\n" + "="*80)
    print("SECTION 4: AGGREGATION FUNCTIONS")
    print("="*80)
    
    # Sales data
    data = [
        ("C001", "2024-01", 1000),
        ("C001", "2024-02", 2000),
        ("C002", "2024-01", 3000),
        ("C002", "2024-02", 4000),
        ("C003", "2024-01", 2000),
    ]
    df = spark.createDataFrame(data, ["customer_id", "month", "amount"])
    
    print("\n1. SIMPLE GROUPBY WITH SUM")
    df_sum = df.groupBy("customer_id").agg(
        sum("amount").alias("total_amount")
    )
    df_sum.show()
    
    print("\n2. MULTIPLE AGGREGATIONS")
    df_multi_agg = df.groupBy("customer_id").agg(
        sum("amount").alias("total"),
        avg("amount").alias("average"),
        max("amount").alias("max"),
        min("amount").alias("min"),
        count("amount").alias("count")
    )
    df_multi_agg.show()
    
    print("\n3. GROUPBY MULTIPLE COLUMNS")
    df_multi_group = df.groupBy("customer_id", "month").agg(
        sum("amount").alias("monthly_total")
    )
    df_multi_group.show()
    
    print("\n4. COUNTDISTINCT")
    df_distinct = df.groupBy().agg(
        countDistinct("customer_id").alias("unique_customers")
    )
    df_distinct.show()
    
    print("\n5. GROUPBY WITH FILTER")
    df_filtered_agg = df.groupBy("customer_id") \
        .agg(sum("amount").alias("total")) \
        .filter(col("total") > 3000)
    df_filtered_agg.show()
    
    print("\n6. STDDEV & VARIANCE - Statistical functions")
    df_stats = df.groupBy("customer_id").agg(
        sum("amount").alias("total"),
        stddev("amount").alias("std_dev"),
        variance("amount").alias("variance")
    )
    df_stats.show()
    
    print("\n7. COLLECT_LIST - Aggregate to list")
    df_collect = df.groupBy("customer_id").agg(
        collect_list("amount").alias("all_amounts")
    )
    df_collect.show()


# ================================================================================
# SECTION 5: JOIN OPERATIONS
# ================================================================================

def demo_join_operations(spark):
    """Demonstrate different types of joins"""
    print("\n" + "="*80)
    print("SECTION 5: JOIN OPERATIONS")
    print("="*80)
    
    # Create datasets
    data1 = [("C001", 1000), ("C002", 2000), ("C003", 1500)]
    df1 = spark.createDataFrame(data1, ["customer_id", "amount"])
    
    data2 = [
        ("C001", "John Doe", "New York"),
        ("C002", "Jane Smith", "Los Angeles"),
        ("C004", "Bob Jones", "Chicago"),
    ]
    df2 = spark.createDataFrame(data2, ["customer_id", "name", "city"])
    
    print("\n1. INNER JOIN (default) - Only matching records")
    df_inner = df1.join(df2, "customer_id", "inner")
    df_inner.show()
    
    print("\n2. LEFT JOIN - Keep all from left table")
    df_left = df1.join(df2, "customer_id", "left")
    df_left.show()
    
    print("\n3. RIGHT JOIN - Keep all from right table")
    df_right = df1.join(df2, "customer_id", "right")
    df_right.show()
    
    print("\n4. OUTER JOIN - Keep all from both tables")
    df_outer = df1.join(df2, "customer_id", "outer")
    df_outer.show()
    
    print("\n5. ANTI JOIN - Records NOT in second table")
    df_anti = df1.join(df2, "customer_id", "anti")
    df_anti.show()
    
    print("\n6. CROSS JOIN - Cartesian product")
    df_cross = df1.crossJoin(df2)
    df_cross.show()
    
    print("\n7. JOIN ON MULTIPLE CONDITIONS")
    data3 = [("C001", "2024-01", 100)]
    df3 = spark.createDataFrame(data3, ["customer_id", "month", "value"])
    
    data4 = [("C001", "2024-01", 200)]
    df4 = spark.createDataFrame(data4, ["customer_id", "month", "count"])
    
    df_multi_join = df3.join(
        df4,
        (df3["customer_id"] == df4["customer_id"]) &
        (df3["month"] == df4["month"]),
        "inner"
    )
    df_multi_join.show()


# ================================================================================
# SECTION 6: WINDOW FUNCTIONS
# ================================================================================

def demo_window_operations(spark):
    """Demonstrate window functions for real-time analytics"""
    print("\n" + "="*80)
    print("SECTION 6: WINDOW FUNCTIONS")
    print("="*80)
    
    # Time series data
    data = [
        ("C001", "2024-01-01", 1000),
        ("C001", "2024-01-02", 2000),
        ("C001", "2024-01-03", 1500),
        ("C002", "2024-01-01", 3000),
        ("C002", "2024-01-02", 4000),
        ("C002", "2024-01-03", 3500),
    ]
    df = spark.createDataFrame(data, ["customer_id", "date", "amount"])
    
    print("\n1. ROW_NUMBER - Unique ranking")
    window_rn = Window.partitionBy("customer_id").orderBy("date")
    df_rn = df.withColumn("row_num", row_number().over(window_rn))
    df_rn.show()
    
    print("\n2. RANK - Ranking with gaps on ties")
    window_rank = Window.partitionBy("customer_id").orderBy(col("amount").desc())
    df_rank = df.withColumn("rank", rank().over(window_rank))
    df_rank.show()
    
    print("\n3. DENSE_RANK - Ranking without gaps")
    df_dense = df.withColumn("dense_rank", dense_rank().over(window_rank))
    df_dense.show()
    
    print("\n4. LAG - Previous row value")
    window_lag = Window.partitionBy("customer_id").orderBy("date")
    df_lag = df.withColumn("prev_amount", lag("amount", 1).over(window_lag))
    df_lag.show()
    
    print("\n5. LEAD - Next row value")
    df_lead = df.withColumn("next_amount", lead("amount", 1).over(window_lag))
    df_lead.show()
    
    print("\n6. RUNNING SUM (Cumulative)")
    window_sum = Window.partitionBy("customer_id").orderBy("date") \
        .rangeBetween(Window.unboundedPreceding, 0)
    df_running = df.withColumn(
        "cumulative_amount",
        sum("amount").over(window_sum)
    )
    df_running.show()
    
    print("\n7. MOVING AVERAGE (2-day)")
    window_avg = Window.partitionBy("customer_id").orderBy("date") \
        .rangeBetween(-1, 0)
    df_moving_avg = df.withColumn(
        "moving_avg_2day",
        sum("amount").over(window_avg) / 2
    )
    df_moving_avg.show()


# ================================================================================
# SECTION 7: STRING FUNCTIONS
# ================================================================================

def demo_string_operations(spark):
    """Demonstrate string manipulation functions"""
    print("\n" + "="*80)
    print("SECTION 7: STRING FUNCTIONS")
    print("="*80)
    
    data = [
        ("  john.doe@gmail.com  ", "John Doe"),
        ("jane-smith@yahoo.com", "JANE SMITH"),
    ]
    df = spark.createDataFrame(data, ["email", "name"])
    
    print("\n1. UPPER - Convert to uppercase")
    df_upper = df.withColumn("email_upper", upper(col("email")))
    df_upper.show()
    
    print("\n2. LOWER - Convert to lowercase")
    df_lower = df.withColumn("name_lower", lower(col("name")))
    df_lower.show()
    
    print("\n3. LENGTH - String length")
    df_len = df.withColumn("email_length", length(col("email")))
    df_len.show()
    
    print("\n4. SUBSTRING - Extract substring")
    df_substr = df.withColumn(
        "first_char",
        substring(col("email"), 1, 1)
    )
    df_substr.show()
    
    print("\n5. TRIM - Remove leading/trailing spaces")
    df_trim = df.withColumn("email_trimmed", trim(col("email")))
    df_trim.show()
    
    print("\n6. LTRIM / RTRIM - Remove left or right spaces")
    df_ltrim = df.withColumn("email_ltrimmed", ltrim(col("email")))
    df_ltrim.show()
    
    print("\n7. REPLACE - Replace substring")
    df_replace = df.withColumn(
        "email_replaced",
        replace(col("email"), "gmail", "outlook")
    )
    df_replace.show()
    
    print("\n8. SPLIT - Split string into array")
    df_split = df.withColumn(
        "email_parts",
        split(col("email"), "@")
    )
    df_split.show()
    
    print("\n9. CONCAT - Combine strings")
    df_concat = df.withColumn(
        "full_info",
        concat(col("name"), lit(" - "), col("email"))
    )
    df_concat.show()
    
    print("\n10. CONCAT_WS - Combine with separator")
    df_concat_ws = df.withColumn(
        "formatted",
        concat_ws(" | ", col("name"), col("email"))
    )
    df_concat_ws.show()
    
    print("\n11. INSTR - Find position of substring")
    df_instr = df.withColumn(
        "at_position",
        instr(col("email"), "@")
    )
    df_instr.show()
    
    print("\n12. REGEXP_REPLACE - Replace using regex")
    df_regexp_replace = df.withColumn(
        "email_clean",
        regexp_replace(col("email"), " ", "")
    )
    df_regexp_replace.show()
    
    print("\n13. REGEXP_EXTRACT - Extract using regex")
    df_regexp_extract = df.withColumn(
        "domain",
        regexp_extract(col("email"), r"(@\w+)", 1)
    )
    df_regexp_extract.show()


# ================================================================================
# SECTION 8: DATE & TIME FUNCTIONS
# ================================================================================

def demo_date_time_operations(spark):
    """Demonstrate date and time functions"""
    print("\n" + "="*80)
    print("SECTION 8: DATE & TIME FUNCTIONS")
    print("="*80)
    
    data = [
        ("C001", "2024-01-15 10:30:45", 500),
        ("C002", "2024-02-20 14:20:30", 1000),
    ]
    df = spark.createDataFrame(data, ["customer_id", "transaction_time", "amount"])
    
    print("\n1. CURRENT_DATE - Today's date")
    df_current = df.withColumn("today", current_date())
    df_current.show()
    
    print("\n2. CURRENT_TIMESTAMP - Current timestamp")
    df_timestamp = df.withColumn("now", current_timestamp())
    df_timestamp.show()
    
    print("\n3. DATE_FORMAT - Format date/timestamp")
    df_formatted = df.withColumn(
        "formatted_date",
        date_format(col("transaction_time"), "yyyy-MM-dd HH:mm:ss")
    )
    df_formatted.show()
    
    print("\n4. TO_DATE - Convert string to date")
    df_to_date = df.withColumn(
        "date_only",
        to_date(col("transaction_time"), "yyyy-MM-dd HH:mm:ss")
    )
    df_to_date.show()
    
    print("\n5. TO_TIMESTAMP - Convert string to timestamp")
    df_to_ts = df.withColumn(
        "ts",
        to_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss")
    )
    df_to_ts.show()
    
    print("\n6. DATEDIFF - Days between dates")
    df_datediff = df.withColumn(
        "days_since",
        datediff(current_date(), to_date(col("transaction_time")))
    )
    df_datediff.show()
    
    print("\n7. DATE_ADD - Add days")
    df_add = df.withColumn(
        "future_date",
        date_add(to_date(col("transaction_time")), 30)
    )
    df_add.show()
    
    print("\n8. DATE_SUB - Subtract days")
    df_sub = df.withColumn(
        "past_date",
        date_sub(to_date(col("transaction_time")), 30)
    )
    df_sub.show()
    
    print("\n9. YEAR, MONTH, DAY extraction")
    df_extract = df.withColumn("year", year(col("transaction_time"))) \
        .withColumn("month", month(col("transaction_time"))) \
        .withColumn("day", day(col("transaction_time")))
    df_extract.show()
    
    print("\n10. HOUR, MINUTE, SECOND extraction")
    df_time = df.withColumn("hour", hour(col("transaction_time"))) \
        .withColumn("minute", minute(col("transaction_time"))) \
        .withColumn("second", second(col("transaction_time")))
    df_time.show()
    
    print("\n11. UNIX_TIMESTAMP - Convert to Unix epoch")
    df_unix = df.withColumn(
        "unix_ts",
        unix_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss")
    )
    df_unix.show()
    
    print("\n12. FROM_UNIXTIME - Convert from Unix epoch")
    df_from_unix = df_unix.withColumn(
        "readable_date",
        from_unixtime(col("unix_ts"), "yyyy-MM-dd HH:mm:ss")
    )
    df_from_unix.show()


# ================================================================================
# SECTION 9: ARRAY FUNCTIONS
# ================================================================================

def demo_array_operations(spark):
    """Demonstrate array manipulation functions"""
    print("\n" + "="*80)
    print("SECTION 9: ARRAY FUNCTIONS")
    print("="*80)
    
    data = [
        ("C001", ["product1", "product2", "product3"], [100, 200, 300]),
        ("C002", ["product1", "product4"], [150, 250]),
    ]
    df = spark.createDataFrame(data, ["customer_id", "products", "prices"])
    
    print("\n1. ARRAY_LENGTH - Get array size")
    df_len = df.withColumn(
        "num_products",
        array_length(col("products"))
    )
    df_len.show()
    
    print("\n2. ARRAY_CONTAINS - Check if array contains element")
    df_contains = df.withColumn(
        "has_product1",
        array_contains(col("products"), "product1")
    )
    df_contains.show()
    
    print("\n3. EXPLODE - Convert array to rows")
    df_explode = df.select(
        col("customer_id"),
        explode(col("products")).alias("product")
    )
    df_explode.show()
    
    print("\n4. EXPLODE_OUTER - Keep rows with NULL arrays")
    df_explode_outer = df.select(
        col("customer_id"),
        explode_outer(col("products")).alias("product")
    )
    df_explode_outer.show()
    
    print("\n5. COLLECT_LIST - Aggregate rows into array")
    df_collect = df.groupBy("customer_id").agg(
        collect_list("products").alias("all_products")
    )
    df_collect.show()
    
    print("\n6. SPLIT - Convert string to array")
    data_string = [("C001", "a,b,c")]
    df_string = spark.createDataFrame(data_string, ["id", "values"])
    df_split = df_string.withColumn(
        "values_array",
        split(col("values"), ",")
    )
    df_split.show()
    
    print("\n7. FLATTEN - Flatten nested arrays")
    nested_data = [("C001", [["a", "b"], ["c", "d"]])]
    df_nested = spark.createDataFrame(nested_data, ["id", "nested"])
    df_flat = df_nested.withColumn(
        "flattened",
        flatten(col("nested"))
    )
    df_flat.show()
    
    print("\n8. REVERSE - Reverse array order")
    df_reverse = df.withColumn(
        "reversed_products",
        reverse(col("products"))
    )
    df_reverse.show()
    
    print("\n9. SORT_ARRAY - Sort array")
    df_sort = df.withColumn(
        "sorted_products",
        sort_array(col("products"))
    )
    df_sort.show()
    
    print("\n10. ELEMENT_AT - Get element by index")
    df_element = df.withColumn(
        "first_product",
        element_at(col("products"), 1)
    )
    df_element.show()
    
    print("\n11. SLICE - Get subset of array")
    df_slice = df.withColumn(
        "first_two",
        slice(col("products"), 1, 2)
    )
    df_slice.show()
    
    print("\n12. ARRAY_UNION - Union of arrays")
    data_union = [
        ("C001", ["a", "b"], ["b", "c"]),
        ("C002", ["x"], ["y", "z"]),
    ]
    df_union_data = spark.createDataFrame(data_union, ["id", "array1", "array2"])
    df_array_union = df_union_data.withColumn(
        "union_result",
        array_union(col("array1"), col("array2"))
    )
    df_array_union.show()
    
    print("\n13. ARRAY_INTERSECT - Common elements")
    df_intersect = df_union_data.withColumn(
        "common",
        array_intersect(col("array1"), col("array2"))
    )
    df_intersect.show()
    
    print("\n14. ARRAY_EXCEPT - Elements in first but not second")
    df_except = df_union_data.withColumn(
        "only_in_first",
        array_except(col("array1"), col("array2"))
    )
    df_except.show()


# ================================================================================
# SECTION 10: MAP FUNCTIONS
# ================================================================================

def demo_map_operations(spark):
    """Demonstrate map (key-value) functions"""
    print("\n" + "="*80)
    print("SECTION 10: MAP FUNCTIONS")
    print("="*80)
    
    data = [
        ("C001", {"email": "john@gmail.com", "phone": "555-1234"}),
        ("C002", {"email": "jane@yahoo.com", "phone": "555-5678"}),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "contact"]
    )
    
    print("\n1. MAP_KEYS - Get all keys")
    df_keys = df.withColumn(
        "contact_keys",
        map_keys(col("contact"))
    )
    df_keys.show(truncate=False)
    
    print("\n2. MAP_VALUES - Get all values")
    df_values = df.withColumn(
        "contact_values",
        map_values(col("contact"))
    )
    df_values.show(truncate=False)
    
    print("\n3. ELEMENT_AT - Get value by key")
    df_element = df.withColumn(
        "email",
        element_at(col("contact"), "email")
    )
    df_element.show()
    
    print("\n4. SIZE - Get map size")
    df_size = df.withColumn(
        "num_contacts",
        size(col("contact"))
    )
    df_size.show()
    
    print("\n5. MAP_CONCAT - Merge maps")
    data_merge = [
        ("C001", {"type": "personal"}, {"email": "john@gmail.com"}),
        ("C002", {"type": "business"}, {"email": "jane@company.com"}),
    ]
    df_merge = spark.createDataFrame(
        data_merge,
        ["id", "metadata", "contact"]
    )
    df_map_concat = df_merge.withColumn(
        "merged",
        map_concat(col("metadata"), col("contact"))
    )
    df_map_concat.show(truncate=False)
    
    print("\n6. EXPLODE - Convert map to rows")
    df_explode_map = df.select(
        col("customer_id"),
        explode(col("contact")).alias("key", "value")
    )
    df_explode_map.show()
    
    print("\n7. CREATE MAP FROM ARRAYS")
    data_arrays = [
        ("C001", ["email", "phone"], ["john@gmail.com", "555-1234"]),
        ("C002", ["email", "phone"], ["jane@yahoo.com", "555-5678"]),
    ]
    df_from_arrays = spark.createDataFrame(
        data_arrays,
        ["id", "keys", "values"]
    )
    df_map_from = df_from_arrays.withColumn(
        "contact_map",
        map_from_arrays(col("keys"), col("values"))
    )
    df_map_from.show(truncate=False)


# ================================================================================
# SECTION 11: STRUCT FUNCTIONS
# ================================================================================

def demo_struct_operations(spark):
    """Demonstrate struct (nested) functions"""
    print("\n" + "="*80)
    print("SECTION 11: STRUCT FUNCTIONS")
    print("="*80)
    
    data = [
        ("C001", "2024-01-15", 500, "shipped"),
        ("C002", "2024-02-20", 1500, "delivered"),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "order_date", "amount", "status"]
    )
    
    print("\n1. CREATE STRUCT - Combine columns into struct")
    df_struct = df.withColumn(
        "order_info",
        struct(
            col("order_date").alias("date"),
            col("amount").alias("total"),
            col("status").alias("state")
        )
    )
    df_struct.show(truncate=False)
    
    print("\n2. ACCESS STRUCT FIELDS")
    df_access = df_struct.withColumn(
        "order_amount",
        col("order_info.total")
    )
    df_access.show()
    
    print("\n3. NAMED_STRUCT - Create named struct")
    df_named = df.withColumn(
        "order_details",
        named_struct(
            "date", col("order_date"),
            "amount", col("amount"),
            "status", col("status")
        )
    )
    df_named.show(truncate=False)
    
    print("\n4. NESTED STRUCT - Multiple levels")
    df_nested_struct = df.withColumn(
        "order",
        struct(
            col("customer_id").alias("cust_id"),
            struct(
                col("order_date").alias("date"),
                col("amount").alias("total")
            ).alias("details"),
            col("status").alias("state")
        )
    )
    df_nested_struct.show(truncate=False)
    
    print("\n5. ACCESS NESTED FIELDS")
    df_access_nested = df_nested_struct.withColumn(
        "nested_amount",
        col("order.details.total")
    )
    df_access_nested.show()


# ================================================================================
# SECTION 12: JSON FUNCTIONS
# ================================================================================

def demo_json_operations(spark):
    """Demonstrate JSON parsing and serialization"""
    print("\n" + "="*80)
    print("SECTION 12: JSON FUNCTIONS")
    print("="*80)
    
    json_data = [
        ('{"name":"John","age":30,"email":"john@gmail.com"}',),
        ('{"name":"Jane","age":25,"email":"jane@yahoo.com"}',),
    ]
    df_raw_json = spark.createDataFrame(json_data, ["json_string"])
    
    print("\n1. GET_JSON_OBJECT - Extract specific field")
    df_extract = df_raw_json.withColumn(
        "name",
        get_json_object(col("json_string"), "$.name")
    )
    df_extract.show()
    
    print("\n2. JSON_TUPLE - Extract multiple fields")
    df_tuple = df_raw_json.select(
        json_tuple(col("json_string"), "$.name", "$.email")
        .alias("name", "email")
    )
    df_tuple.show()
    
    print("\n3. FROM_JSON - Parse JSON to struct")
    schema = StructType([
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("email", StringType()),
    ])
    
    df_parsed = df_raw_json.withColumn(
        "parsed",
        from_json(col("json_string"), schema)
    )
    df_parsed.show(truncate=False)
    
    print("\n4. ACCESS PARSED JSON FIELDS")
    df_access = df_parsed.withColumn(
        "person_name",
        col("parsed.name")
    )
    df_access.show()
    
    print("\n5. TO_JSON - Convert struct to JSON")
    data_struct = [
        ("John", 30, "john@gmail.com"),
        ("Jane", 25, "jane@yahoo.com"),
    ]
    df_struct_data = spark.createDataFrame(
        data_struct,
        ["name", "age", "email"]
    )
    
    df_to_json_col = df_struct_data.select(
        to_json(struct("*")).alias("json_output")
    )
    df_to_json_col.show(truncate=False)
    
    print("\n6. NESTED JSON PARSING")
    nested_json = [
        ('{"user":{"name":"John","contact":{"email":"john@gmail.com","phone":"555-1234"}}}',),
    ]
    df_nested = spark.createDataFrame(nested_json, ["nested_json"])
    
    df_nested_extract = df_nested.withColumn(
        "user_name",
        get_json_object(col("nested_json"), "$.user.name")
    ).withColumn(
        "email",
        get_json_object(col("nested_json"), "$.user.contact.email")
    )
    df_nested_extract.show()


# ================================================================================
# SECTION 13: MATHEMATICAL FUNCTIONS
# ================================================================================

def demo_math_operations(spark):
    """Demonstrate mathematical functions"""
    print("\n" + "="*80)
    print("SECTION 13: MATHEMATICAL FUNCTIONS")
    print("="*80)
    
    data = [
        ("C001", 100.456),
        ("C002", 250.789),
        ("C003", -50.123),
        ("C004", 200.500),
    ]
    df = spark.createDataFrame(data, ["customer_id", "amount"])
    
    print("\n1. ABS - Absolute value")
    df_abs = df.withColumn("abs_amount", abs(col("amount")))
    df_abs.show()
    
    print("\n2. SQRT - Square root")
    df_sqrt = df.filter(col("amount") > 0).withColumn("sqrt_amount", sqrt(col("amount")))
    df_sqrt.show()
    
    print("\n3. ROUND - Round to decimal places")
    df_round = df.withColumn("rounded", round(col("amount"), 2))
    df_round.show()
    
    print("\n4. CEIL - Round up")
    df_ceil = df.withColumn("ceiled", ceil(col("amount")))
    df_ceil.show()
    
    print("\n5. FLOOR - Round down")
    df_floor = df.withColumn("floored", floor(col("amount")))
    df_floor.show()
    
    print("\n6. POW - Power function")
    df_pow = df.withColumn("squared", pow(col("amount"), 2))
    df_pow.show()
    
    print("\n7. LOG - Logarithm")
    df_log = df.filter(col("amount") > 0).withColumn("log_amount", log(col("amount")))
    df_log.show()
    
    print("\n8. GREATEST - Max of multiple columns")
    data_multi = [
        ("C001", 100, 200, 150),
        ("C002", 250, 100, 200),
    ]
    df_multi = spark.createDataFrame(data_multi, ["id", "val1", "val2", "val3"])
    df_greatest = df_multi.withColumn(
        "max_val",
        greatest(col("val1"), col("val2"), col("val3"))
    )
    df_greatest.show()
    
    print("\n9. LEAST - Min of multiple columns")
    df_least = df_multi.withColumn(
        "min_val",
        least(col("val1"), col("val2"), col("val3"))
    )
    df_least.show()


# ================================================================================
# SECTION 14: TYPE CASTING FUNCTIONS
# ================================================================================

def demo_type_casting(spark):
    """Demonstrate type casting functions"""
    print("\n" + "="*80)
    print("SECTION 14: TYPE CASTING FUNCTIONS")
    print("="*80)
    
    data = [
        ("123", "456.78", "2024-01-15", "2024-01-15 10:30:45"),
    ]
    df = spark.createDataFrame(
        data,
        ["str_int", "str_double", "str_date", "str_timestamp"]
    )
    
    print("\n1. CAST TO INTEGER")
    df_int = df.withColumn("int_val", col("str_int").cast(IntegerType()))
    df_int.show()
    
    print("\n2. CAST TO DOUBLE")
    df_double = df.withColumn("double_val", col("str_double").cast(DoubleType()))
    df_double.show()
    
    print("\n3. CAST TO STRING")
    df_string = df.withColumn("str_val", col("str_int").cast(StringType()))
    df_string.show()
    
    print("\n4. CAST TO DATE")
    df_date = df.withColumn("date_val", col("str_date").cast(DateType()))
    df_date.show()
    
    print("\n5. CAST TO TIMESTAMP")
    df_timestamp = df.withColumn("ts_val", col("str_timestamp").cast(TimestampType()))
    df_timestamp.show()


# ================================================================================
# SECTION 15: DEDUPLICATION FUNCTIONS
# ================================================================================

def demo_deduplication(spark):
    """Demonstrate deduplication functions"""
    print("\n" + "="*80)
    print("SECTION 15: DEDUPLICATION FUNCTIONS")
    print("="*80)
    
    data = [
        ("C001", "john@gmail.com", 1000),
        ("C001", "john@gmail.com", 1000),  # Duplicate
        ("C002", "jane@yahoo.com", 2000),
        ("C002", "jane@yahoo.com", 2000),  # Duplicate
        ("C003", "bob@gmail.com", 1500),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "email", "amount"]
    )
    
    print("\n1. DROP_DUPLICATES - Remove all duplicates")
    df_distinct = df.dropDuplicates()
    df_distinct.show()
    
    print("\n2. DROP_DUPLICATES ON SPECIFIC COLUMNS")
    df_distinct_cols = df.dropDuplicates(["customer_id"])
    df_distinct_cols.show()
    
    print("\n3. DISTINCT - Same as dropDuplicates")
    df_dist = df.distinct()
    df_dist.show()
    
    print("\n4. REMOVE DUPLICATES KEEPING FIRST OCCURRENCE")
    window_dedup = Window.partitionBy("customer_id").orderBy("amount")
    df_dedup_first = df.withColumn(
        "rn",
        row_number().over(window_dedup)
    ).filter(col("rn") == 1).drop("rn")
    df_dedup_first.show()


# ================================================================================
# SECTION 16: NULL & MISSING DATA HANDLING
# ================================================================================

def demo_null_handling(spark):
    """Demonstrate NULL and missing data handling"""
    print("\n" + "="*80)
    print("SECTION 16: NULL & MISSING DATA HANDLING")
    print("="*80)
    
    data = [
        ("C001", "john@gmail.com", None),
        ("C002", None, "555-1234"),
        ("C003", "bob@yahoo.com", "555-5678"),
        (None, "jane@gmail.com", None),
    ]
    df = spark.createDataFrame(
        data,
        ["customer_id", "email", "phone"]
    )
    
    print("\n1. DROP NULL ROWS")
    df_dropnull = df.dropna()
    df_dropnull.show()
    
    print("\n2. DROP NULL IN SPECIFIC COLUMN")
    df_drop_specific = df.dropna(subset=["customer_id"])
    df_drop_specific.show()
    
    print("\n3. FILL NULL VALUES")
    df_fill = df.fillna({
        "customer_id": "UNKNOWN",
        "email": "no-email@unknown.com",
        "phone": "0000000000"
    })
    df_fill.show()
    
    print("\n4. COALESCE - USE FIRST NON-NULL VALUE")
    data_multi = [
        ("C001", "john@gmail.com", "john@yahoo.com"),
        ("C002", None, "jane@yahoo.com"),
    ]
    df_multi = spark.createDataFrame(
        data_multi,
        ["customer_id", "primary_email", "backup_email"]
    )
    df_coal = df_multi.withColumn(
        "contact_email",
        coalesce(col("primary_email"), col("backup_email"))
    )
    df_coal.show()


# ================================================================================
# SECTION 17: UNION & COMBINE OPERATIONS
# ================================================================================

def demo_union_operations(spark):
    """Demonstrate union and combine operations"""
    print("\n" + "="*80)
    print("SECTION 17: UNION & COMBINE OPERATIONS")
    print("="*80)
    
    data1 = [("C001", 1000)]
    df1 = spark.createDataFrame(data1, ["customer_id", "amount"])
    
    data2 = [("C002", 2000)]
    df2 = spark.createDataFrame(data2, ["customer_id", "amount"])
    
    print("\n1. UNION - Combine rows")
    df_union = df1.union(df2)
    df_union.show()
    
    print("\n2. UNION ALL - Include duplicates")
    df_union_all = df1.unionByName(df2)
    df_union_all.show()


# ================================================================================
# COMPLETE REAL-TIME PRODUCTION EXAMPLE
# ================================================================================

def complete_realtime_example(spark):
    """Complete production-ready real-time example"""
    print("\n" + "="*80)
    print("SECTION 18: COMPLETE REAL-TIME PRODUCTION EXAMPLE")
    print("="*80)
    
    # Simulate Kafka JSON events
    kafka_events = [
        ('{"transaction_id":"T1","customer_id":"C001","amount":500.0,"timestamp":"2024-01-15 10:30:45","merchant_category":"Electronics","items":[{"product_id":"P1","quantity":2,"price":250.0}]}',),
        ('{"transaction_id":"T2","customer_id":"C002","amount":1500.0,"timestamp":"2024-01-15 11:20:30","merchant_category":"Grocery","items":[{"product_id":"P2","quantity":3,"price":500.0}]}',),
    ]
    df_kafka = spark.createDataFrame(kafka_events, ["value"])
    
    # Define schema
    event_schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("timestamp", StringType()),
        StructField("merchant_category", StringType()),
        StructField("items", ArrayType(
            StructType([
                StructField("product_id", StringType()),
                StructField("quantity", IntegerType()),
                StructField("price", DoubleType()),
            ])
        )),
    ])
    
    # Parse JSON
    print("\n1. PARSE JSON EVENTS")
    df_parsed = df_kafka.select(
        from_json(col("value").cast(StringType()), event_schema).alias("event")
    )
    df_parsed.show(truncate=False)
    
    # Flatten structure
    print("\n2. FLATTEN STRUCTURE")
    df_flat = df_parsed.select(
        col("event.transaction_id"),
        col("event.customer_id"),
        col("event.amount"),
        col("event.timestamp"),
        col("event.merchant_category"),
        col("event.items")
    )
    df_flat.show(truncate=False)
    
    # Explode items
    print("\n3. EXPLODE ITEMS ARRAY")
    df_exploded = df_flat.select(
        col("transaction_id"),
        col("customer_id"),
        col("amount"),
        col("timestamp"),
        col("merchant_category"),
        explode(col("items")).alias("item")
    )
    df_exploded.show(truncate=False)
    
    # Flatten items
    print("\n4. FLATTEN ITEMS")
    df_items = df_exploded.select(
        col("transaction_id"),
        col("customer_id"),
        col("amount"),
        col("timestamp"),
        col("merchant_category"),
        col("item.product_id"),
        col("item.quantity"),
        col("item.price")
    )
    df_items.show()
    
    # Add business logic
    print("\n5. ADD BUSINESS LOGIC")
    df_enriched = df_items.withColumn(
        "transaction_type",
        when(col("amount") > 1000, "HIGH_VALUE")
        .when(col("amount") > 500, "MEDIUM_VALUE")
        .otherwise("LOW_VALUE")
    ).withColumn(
        "processing_time",
        current_timestamp()
    )
    df_enriched.show()
    
    # Aggregations
    print("\n6. AGGREGATIONS BY CUSTOMER")
    df_agg = df_enriched.groupBy("customer_id").agg(
        count("transaction_id").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount")
    )
    df_agg.show()
    
    # Window function
    print("\n7. WINDOW RANKING")
    window_rank = Window.partitionBy("customer_id").orderBy(col("amount").desc())
    df_ranked = df_enriched.withColumn(
        "rank",
        rank().over(window_rank)
    )
    df_ranked.show()
    
    # Deduplication
    print("\n8. REMOVE DUPLICATES")
    df_dedup = df_enriched.dropDuplicates(["transaction_id"])
    df_dedup.show()


# ================================================================================
# QUICK REFERENCE CHEAT SHEET
# ================================================================================

def print_cheat_sheet():
    """Print quick reference guide"""
    print("\n" + "="*80)
    print("QUICK REFERENCE CHEAT SHEET - TOP 30 FUNCTIONS")
    print("="*80)
    
    cheat_sheet = """
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MOST CRITICAL FUNCTIONS FOR REAL-TIME                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ 1. from_json()          - Parse JSON events from streams                    │
│ 2. explode()            - Convert arrays to rows                            │
│ 3. window()             - Time-based aggregations                           │
│ 4. filter()             - Row filtering                                     │
│ 5. groupBy() + agg()    - Real-time metrics                                 │
│ 6. col()                - Column reference                                  │
│ 7. when() + otherwise() - Conditional logic                                 │
│ 8. withColumn()         - Add/transform columns                             │
│ 9. join()               - Enrich data                                       │
│ 10. select()            - Column selection                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                        STRING MANIPULATION (Top 5)                          │
├─────────────────────────────────────────────────────────────────────────────┤
│ - upper(), lower()      - Case conversion                                   │
│ - split()               - Split strings                                     │
│ - concat()              - Combine strings                                   │
│ - trim()                - Remove whitespace                                 │
│ - regexp_extract()      - Extract with regex                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                        DATE & TIME (Top 5)                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ - to_timestamp()        - Convert to timestamp                              │
│ - date_format()         - Format dates                                      │
│ - datediff()            - Calculate date differences                        │
│ - current_timestamp()   - Current time                                      │
│ - year(), month(), day()- Extract date parts                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                        ARRAY OPERATIONS (Top 5)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ - explode()             - Array to rows                                     │
│ - array_contains()      - Check array membership                            │
│ - collect_list()        - Aggregate to array                                │
│ - array_length()        - Get array size                                    │
│ - flatten()             - Flatten nested arrays                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                        NULL HANDLING (Top 5)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ - coalesce()            - Return first non-null                             │
│ - fillna()              - Fill null values                                  │
│ - dropna()              - Drop null rows                                    │
│ - isnotnull()           - Check for non-null                                │
│ - isnull()              - Check for null                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                    AGGREGATION FUNCTIONS (Top 7)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│ - sum()                 - Sum values                                        │
│ - avg()                 - Calculate average                                 │
│ - max() / min()         - Maximum/minimum                                   │
│ - count()               - Count rows                                        │
│ - countDistinct()       - Count unique                                      │
│ - stddev()              - Standard deviation                                │
│ - collect_list()        - Aggregate to list                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                    WINDOW FUNCTIONS (Top 5)                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ - row_number()          - Unique ranking                                    │
│ - rank()                - Ranking with gaps                                 │
│ - dense_rank()          - Ranking without gaps                              │
│ - lag() / lead()        - Previous/next row                                 │
│ - sum().over()          - Running aggregate                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                        JSON FUNCTIONS (Top 3)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│ - from_json()           - Parse JSON strings                                │
│ - to_json()             - Convert to JSON                                   │
│ - get_json_object()     - Extract JSON field                                │
└─────────────────────────────────────────────────────────────────────────────┘

QUICK SYNTAX REFERENCE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Basic DataFrame Operations:
   df.select("col1", "col2")                    # Select columns
   df.filter(col("col") > 100)                  # Filter rows
   df.withColumn("new_col", col("old_col") * 2) # Add column
   df.dropDuplicates()                          # Remove duplicates

2. Aggregations:
   df.groupBy("key").agg(sum("value"))          # Group and sum
   df.groupBy().agg(count("*"))                 # Count total rows

3. Joins:
   df1.join(df2, "key", "inner")               # Inner join
   df1.join(df2, "key", "left")                # Left join

4. Window Functions:
   window_spec = Window.partitionBy("key").orderBy("date")
   df.withColumn("rank", rank().over(window_spec))

5. Array Operations:
   df.withColumn("exploded", explode(col("array_col")))
   df.groupBy("key").agg(collect_list("value").alias("values"))

6. String Operations:
   col("text").substr(0, 5)                    # Substring
   upper(col("text"))                          # Uppercase
   split(col("text"), ",")                     # Split

7. JSON Operations:
   from_json(col("json_col"), schema)          # Parse JSON
   to_json(struct("*"))                        # To JSON

8. Conditional Logic:
   when(col("x") > 10, "high").otherwise("low") # If-then-else

9. Type Casting:
   col("text").cast(IntegerType())              # Cast type

10. Null Handling:
    coalesce(col("a"), col("b"))               # First non-null
    fillna({"col": "default_value"})           # Fill nulls
"""
    
    print(cheat_sheet)


# ================================================================================
# MAIN EXECUTION
# ================================================================================

def main():
    """Execute all demonstrations"""
    # Create Spark session
    spark = create_spark_session("SparkFunctionsDemo")
    
    try:
        # Print cheat sheet
        print_cheat_sheet()
        
        # Run demonstrations
        df = demo_select_operations(spark)
        demo_filter_operations(spark, df)
        demo_aggregation_operations(spark)
        demo_join_operations(spark)
        demo_window_operations(spark)
        demo_string_operations(spark)
        demo_date_time_operations(spark)
        demo_array_operations(spark)
        demo_map_operations(spark)
        demo_struct_operations(spark)
        demo_json_operations(spark)
        demo_math_operations(spark)
        demo_type_casting(spark)
        demo_deduplication(spark)
        demo_null_handling(spark)
        demo_union_operations(spark)
        complete_realtime_example(spark)
        
        print("\n" + "="*80)
        print("ALL DEMONSTRATIONS COMPLETED SUCCESSFULLY!")
        print("="*80)
        
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()