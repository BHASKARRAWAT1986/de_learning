# Column Pruning — Complete Guide

---

## 1. What Is Column Pruning?

**Column pruning** (also called **projection pushdown**) is an optimization where Spark reads **only the columns referenced in the query** from disk, instead of reading all columns.

Since Parquet is a **columnar storage format**, each column is stored independently. Reading only needed columns avoids loading unnecessary column data entirely.

```
Table schema: id, name, email, age, address, salary, department, created_at (8 columns)
Query: SELECT name, salary FROM employees WHERE department = 'Engineering'

Without column pruning: Read all 8 columns from disk
With column pruning:    Read only 3 columns: name, salary, department
                        → 62.5% less I/O for this query
```

---

## 2. How Parquet Enables Column Pruning

Parquet stores data in **column stripes** — all values of column A together, all values of column B together, etc.

```
Parquet file layout:
  [Row Group 1]
    Column A: [val1, val2, val3 ...]   ← can read independently
    Column B: [val1, val2, val3 ...]   ← can skip entirely if not needed
    Column C: [val1, val2, val3 ...]   ← can skip entirely if not needed
  [Row Group 2]
    Column A: [...]
    Column B: [...]
    ...
```

Spark's Parquet reader accepts a list of required columns and physically skips the bytes for unreferenced columns.

---

## 3. Automatic Column Pruning in Spark

Spark's Catalyst optimizer automatically applies column pruning — you do not need to do anything special. The key is to **write queries that only select needed columns**.

### Spark Does It Automatically
```python
# Spark automatically prunes: only reads 'name', 'salary', 'department'
df = spark.table("employees") \
    .filter("department = 'Engineering'") \
    .select("name", "salary")

df.explain()
# Physical plan shows: ReadSchema: struct<department:string,name:string,salary:double>
# department (filter col) and name, salary (select cols) — only 3 of 8 columns read
```

### When Column Pruning Is NOT Applied
```python
# BAD: select(*) defeats column pruning
df = spark.table("employees").select("*").filter("department = 'Engineering'")
# Reads ALL columns even though we may only need a few

# BAD: Using a wide CTE then selecting subset — Catalyst usually still prunes,
# but explicit column selection is safer and clearer

# GOOD: Be explicit
df = spark.table("employees") \
    .select("name", "salary", "department") \
    .filter("department = 'Engineering'") \
    .select("name", "salary")
```

---

## 4. Column Pruning in SQL

```sql
-- GOOD: Only referenced columns are read
SELECT name, salary 
FROM employees 
WHERE department = 'Engineering';
-- Parquet reads: name, salary, department (3 cols)

-- BAD: SELECT * reads everything
SELECT * 
FROM employees 
WHERE department = 'Engineering';
-- Parquet reads: all 8 columns

-- GOOD: Even in JOINs, Catalyst prunes unused columns
SELECT e.name, e.salary, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
WHERE d.location = 'NYC';
-- Only reads: e.name, e.salary, e.dept_id + d.dept_name, d.dept_id, d.location
```

---

## 5. Column Pruning vs Row Filtering — They Work Together

```
Query: SELECT name, salary FROM employees WHERE dept = 'Eng' AND salary > 100000

Column pruning:  Read only 3 columns (name, salary, dept) from Parquet — reduces I/O width
Predicate pushdown: Skip files/row-groups where dept ≠ 'Eng' — reduces I/O depth
Together: Minimal data loaded into Spark memory
```

---

## 6. Nested Column Pruning (Struct / Map / Array)

For complex types (structs, maps), Spark can prune at the **nested field level**.

```python
# Table with nested struct: profile.address.city, profile.address.zip, profile.phone
df = spark.table("users").select("user_id", "profile.address.city")
# Only reads: user_id column + profile.address.city sub-field
# Skips: profile.address.zip, profile.phone entirely
```

```sql
-- SQL with nested field access
SELECT user_id, profile.address.city 
FROM users;
-- Spark reads only the city sub-field from the profile struct column
```

### Enable Nested Schema Pruning (default ON in DBR)
```sql
SET spark.sql.optimizer.nestedSchemaPruning.enabled = true;
-- Default: true in Databricks Runtime
```

---

## 7. Column Pruning in Delta Lake

Delta adds an extra layer: **schema evolution** tracking means the physical files may have different schemas over time. Spark handles this gracefully:

```python
# Delta tracks schema per file in the transaction log
# Even if you added new columns later, old files are read with projection pushdown
# New columns in old files = NULL (no extra I/O — column doesn't exist in those files)

spark.sql("""
  SELECT original_col, new_col  -- new_col added after some files were written
  FROM my_delta_table
""")
# Old files: read original_col, new_col returns NULL (no bytes read for missing col)
# New files: read both columns normally
```

---

## 8. How to Verify Column Pruning

```python
df = spark.table("orders").select("order_id", "amount").filter("region = 'US'")

# Check physical plan
df.explain(True)

# Look for in Physical Plan:
# FileScan parquet [...] ReadSchema: struct<order_id:bigint,amount:double,region:string>
# Only 3 columns in ReadSchema — pruning is working!

# If ReadSchema shows all columns → pruning is NOT working
# (usually because of select(*) or a function that forces full schema read)
```

---

## 9. Column Pruning with Wide Tables (100+ columns)

Wide tables are where column pruning has the biggest impact.

```python
# 500-column fact table — typical in data warehouse scenarios
wide_table = spark.table("fact_sales_500cols")

# BAD: Full scan — reads 500 columns
report = wide_table.groupBy("region", "year").agg(F.sum("revenue"))
# If there are intermediate select(*) anywhere in the pipeline → reads all 500 cols

# GOOD: Explicit column selection first
report = wide_table.select("region", "year", "revenue") \
    .groupBy("region", "year") \
    .agg(F.sum("revenue"))
# Parquet reads: ONLY 3 columns out of 500 → 99.4% column I/O reduction
```

---

## 10. Pros and Cons

| Pros | Cons |
|---|---|
| Automatic in Spark — no code change needed | Only works with columnar formats (Parquet, ORC, Delta) |
| Dramatic I/O reduction for wide tables | SELECT * defeats it |
| Reduces memory pressure in Spark | Functions over struct columns may limit nested pruning |
| Works with nested types | Requires explicit column references in queries |
| Complements row-level predicate pushdown | |

---

## 11. Best Practices

1. **Always use explicit column names** — avoid `SELECT *` in production code
2. **Push select early** in DataFrames — add `.select(needed_cols)` right after `.table()`
3. **For nested types** — access only the sub-fields you need (e.g., `profile.city` not `profile`)
4. **Wide tables** — pre-materialize a narrow view if the same subset is always queried
5. **Check physical plans** during development — verify `ReadSchema` shows only needed columns
6. **Views as guardrails** — expose only needed columns in a view to prevent accidental `SELECT *`

```sql
-- Good practice: Create a narrow view over a wide table
CREATE VIEW orders_reporting AS
SELECT order_id, customer_id, order_date, total_amount, region
FROM orders_wide_500cols;
-- Downstream queries on this view can only over-select 5 columns, not 500
```
