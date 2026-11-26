# Pandas vs PySpark — Why Same Result, Different Engines

## Why Both Give the Same Result

Your dataset contains only 5 rows (EmployeeID, Name, Department, Salary, etc.).

Whether you use **Pandas** or **PySpark**, both will calculate:
- Highest salary
- Average salary
- Employee count

...exactly the same. **The difference is how they process the data internally, not the final answer.**

---

## Pandas ETL (`etl_pipeline.py`)

```
CSV
 ↓
Read into RAM
 ↓
Transform
 ↓
Save CSV
```

Uses:
```python
import pandas as pd
```

Everything is loaded into **one machine's memory**.

```
Laptop
-----------------
RAM
-----------------
Entire CSV
```

If the CSV becomes 5 GB, 10 GB, or 50 GB, your laptop may crash because Pandas wants the whole dataset in memory.

---

## PySpark ETL (`pyspark_etl_pipeline.py`)

```
CSV
 ↓
Spark DataFrame
 ↓
Distributed Processing
 ↓
Parquet + CSV
```

Uses:
```python
from pyspark.sql import SparkSession
```

Spark divides the data into **partitions**.

Imagine a 100 GB CSV — Spark splits it into:

| Machine | Data |
|---|---|
| Machine 1 | 25 GB |
| Machine 2 | 25 GB |
| Machine 3 | 25 GB |
| Machine 4 | 25 GB |

All machines work together. **That is why companies use Spark.**

---

## Difference in Execution

**Pandas**
```
CSV
 ↓
One CPU
 ↓
One RAM
 ↓
Output
```
Everything happens sequentially.

**Spark**
```
CSV
 ├── Partition 1 → Executor 1
 ├── Partition 2 → Executor 2
 ├── Partition 3 → Executor 3
 └── Partition 4 → Executor 4
        ↓
   Combine Results
```
Multiple CPUs process the data simultaneously.

---

## Pipeline Comparison

**Your Pandas Pipeline**
```
Read CSV
 ↓
Clean Missing Values
 ↓
Create ExperienceLevel
 ↓
Department Average
 ↓
City Count
 ↓
Save CSV
```

**Your Spark Pipeline**
```
Read CSV
 ↓
Spark DataFrame
 ↓
Cleaning
 ↓
withColumn()
 ↓
Window Function
 ↓
Spark SQL
 ↓
Write Parquet
 ↓
Write CSV
```

Your Spark project includes additional features such as **Window Functions**, **Spark SQL**, and **Parquet output**.

---

## Biggest Difference

**Pandas**
```python
df.groupby("Department")["Salary"].mean()
```
Works on a single machine.

**Spark**
```python
df.groupBy("Department").avg("Salary")
```
Spark performs a **distributed aggregation**. Even with 500 million rows, the syntax stays almost the same.

---

## Spark SQL

```python
df.createOrReplaceTempView("employees")

spark.sql("""
    SELECT Department, AVG(Salary)
    FROM employees
    GROUP BY Department
""")
```

This is one of Spark's strengths. Many data engineers prefer SQL because it's expressive and works well with large datasets.

**Pandas doesn't have this built-in SQL interface.**

---

## Window Functions

```python
from pyspark.sql import Window
from pyspark.sql.functions import col

Window.partitionBy("Department") \
      .orderBy(col("Salary").desc())
```

This lets you answer questions like:
- Highest-paid employee in each department
- Top 5 salaries
- Ranking employees
- Running totals

Window functions are a major feature in data engineering pipelines.

---

## Output Formats

**Pandas** produces:
```
employee_report.csv
```

**Spark** produces:
```
employee_report_spark/
    ├── parquet/
    └── csv/
```

Spark writes **Parquet**, the industry-standard analytics format — compressed, columnar, and much faster for large-scale queries than CSV.

---

## Performance Comparison

| Dataset Size | Pandas | PySpark |
|---|---|---|
| 5 rows | ✅ Very Fast | ✅ Fast |
| 50,000 rows | ✅ Very Fast | ✅ Fast |
| 5 million rows | ⚠️ Slower | ✅ Fast |
| 100 million rows | ❌ May run out of memory | ✅ Designed for this |
| 1 billion rows | ❌ Not practical | ✅ Typical Spark workload |

---

## Why Learn PySpark if Pandas Works?

As a data engineer, you'll often work with datasets that are far too large for Pandas.

**Typical pipeline:**
```
500 GB CSV
    ↓
Amazon S3
    ↓
Spark Cluster
    ↓
Cleaning
    ↓
Transformations
    ↓
Parquet
    ↓
Snowflake / Redshift / BigQuery
    ↓
Power BI / Tableau
```

Pandas cannot realistically process a 500 GB file on a typical laptop.

---

## For Your Current Project

Your PySpark project already demonstrates several important industry concepts:

- ✅ Reading CSV with Spark
- ✅ Data cleaning
- ✅ DataFrame transformations (`withColumn`)
- ✅ Window functions
- ✅ Spark SQL
- ✅ Aggregations (`groupBy`)
- ✅ Writing Parquet and CSV
- ✅ ETL pipeline structure
